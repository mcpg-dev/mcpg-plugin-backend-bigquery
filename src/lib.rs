//! Google BigQuery cloud-warehouse backend binding plugin for mcpg.
//!
//! Implements [`BigQueryBackendPlugin`] — `BackendPlugin` for
//! `kind: "bigquery"`. Runs one operator-fixed Standard-SQL statement against
//! BigQuery over the REST jobs API (`jobs.query`) and returns the rows as JSON,
//! typed by the result schema. Auth is a GCP service-account key (JSON),
//! resolved through the gateway secret-resolver. BigQuery-specific machinery
//! lives in [`bigquery`] + [`envelope`]. The driver is async (reqwest-based)
//! and authenticates on the first request, so the client is built lazily on
//! first `execute` (and cached) — `register_profile` never touches the network
//! or parses the key, and the unit tests run with a dummy key and no
//! credentials.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mcpg_plugin_protocol::audit::{AuditEvent, AuditOutcome};
use mcpg_plugin_protocol::types::PluginIdentity;
use mcpg_plugin_protocol::{
    BackendError, BackendHost, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
    firstparty_manifest,
};
use mcpg_plugin_sdk::{HostHandle, SpanGuard};
use serde_json::{Value, json};
use tokio::sync::{OnceCell, RwLock};
use tracing::debug;

/// cdylib sync bridge.
pub mod cdylib;
// The driver-facing module reaches the `gcloud_bigquery` crate by its full
// path; this module shadows the name only locally.
mod bigquery;
mod envelope;
mod params;
mod surface;
mod types;
/// Polling `watch_strategy` entity (kind `bigquery_poll`). PUB so the default
/// (non-cdylib-export) lane does not flag it as dead code.
pub mod watch;

use bigquery::{
    EstimateOutcome, QueryOutcome, build_client, enforce_read_only, run_dry_run, run_query,
};
use envelope::{build_estimate_envelope, build_result_envelope, classify_error, estimate_cost_usd};
use mcpg_plugin_protocol::ResourcePage;
use params::{CompiledParam, compile_params, evaluate_params, json_to_bq_param};
pub use types::{
    BigQueryAuth, BigQueryAuthMode, BigQueryBackendSpec, BigQueryQueryConfig,
    CompletionConfig as BigQueryCompletionConfig, ListQueryConfig as BigQueryListQueryConfig,
    validate_completion, validate_list_query,
};

/// Embedded plugin descriptor.
pub const BINDING_DESCRIPTOR_YAML: &str = include_str!("../plugin.yaml");

// --------------------------------------------------------------------- obs

fn audit_action_for_outcome(label: &str) -> Option<&'static str> {
    match label {
        "timeout" => Some("dev.mcpg.backend.bigquery.request_timeout"),
        "transport_error" => Some("dev.mcpg.backend.bigquery.request_failed"),
        "bigquery_error" => Some("dev.mcpg.backend.bigquery.query_rejected"),
        "invalid_spec" => Some("dev.mcpg.backend.bigquery.request_failed"),
        _ => None,
    }
}

fn rfc3339_now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn synthetic_system_identity() -> PluginIdentity {
    PluginIdentity {
        kind: "system".into(),
        trust_level: "verified".into(),
        subject_id: Some("dev.mcpg.backend.bigquery".into()),
        auth_provider: None,
        issuer: None,
        roles: vec![],
        groups: vec![],
        scopes: vec![],
        attributes: Default::default(),
    }
}

fn finalize_payload(envelope: Value) -> Result<BackendResponse, BackendError> {
    let payload = serde_json::to_vec(&envelope).map_err(|e| BackendError::Transport {
        message: format!("BigQuery plugin envelope serialization failed: {e}"),
    })?;
    Ok(BackendResponse {
        payload,
        truncated: false,
    })
}

// ------------------------------------------------------------------ plugin

/// Per-binding BigQuery runtime — connection parameters + the secret + the
/// statement, plus a lazily-built, cached REST client. The client is built on
/// first `execute` (parsing the key only then), so `register_profile` stays
/// offline. Cheap to clone (the `OnceCell` is shared behind `Arc`).
#[derive(Clone)]
struct BigQueryProfile {
    project_id: String,
    dataset: Option<String>,
    location: Option<String>,
    /// The resolved service-account key JSON.
    credentials_json: String,
    /// Emulator endpoint override (integration only); `None` in production.
    endpoint_override: Option<String>,
    statement: String,
    /// Compiled CEL `params`; `params[i]` binds the i-th `?` placeholder.
    compiled_params: Arc<[CompiledParam]>,
    use_legacy_sql: bool,
    read_only: bool,
    maximum_bytes_billed: Option<u64>,
    max_rows: usize,
    /// Dry-run cost-estimate mode: plan the query without executing it and
    /// return a bytes/cost estimate envelope instead of rows.
    dry_run: bool,
    /// On-demand price per TiB (USD) used to derive an estimated cost in dry-run
    /// mode; `None` → the estimate carries bytes only.
    price_per_tib_usd: Option<f64>,
    timeout: Duration,
    timeout_ms: u64,
    surface: surface::Surface,
    surface_uri: Option<String>,
    list_query: Option<BigQueryListQueryConfig>,
    /// Per-`{id}` single-row read statement for a `resource_templates[]`
    /// binding. Bound from the same `compiled_params` as `statement`; when None
    /// the resource-read branch falls back to `statement`.
    read_query: Option<String>,
    variable_completions: Arc<BTreeMap<String, BigQueryCompletionConfig>>,
    /// Built on first use; shared across calls.
    client: Arc<OnceCell<Arc<gcloud_bigquery::client::Client>>>,
}

impl BigQueryProfile {
    /// Return the cached client, building (and caching) it on first use. The
    /// build parses the service-account key and sets up the auth token
    /// providers, so it errors on a malformed key / unreachable token source.
    async fn client(&self) -> Result<Arc<gcloud_bigquery::client::Client>, String> {
        self.client
            .get_or_try_init(|| async {
                let client =
                    build_client(&self.credentials_json, self.endpoint_override.as_deref()).await?;
                Ok::<_, String>(Arc::new(client))
            })
            .await
            .cloned()
    }
}

/// `BackendPlugin` implementation for `kind: "bigquery"`.
pub struct BigQueryBackendPlugin {
    manifest: PluginManifest,
    profiles: RwLock<BTreeMap<String, BigQueryProfile>>,
    host_handle: OnceLock<HostHandle>,
}

impl Default for BigQueryBackendPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl BigQueryBackendPlugin {
    #[must_use]
    pub fn new() -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.bigquery",
                name: "BigQuery Binding",
                class: Backend,
            },
            profiles: RwLock::new(BTreeMap::new()),
            host_handle: OnceLock::new(),
        }
    }

    pub fn set_host_handle(&self, host: HostHandle) -> bool {
        self.host_handle.set(host).is_ok()
    }

    fn host_handle(&self) -> Option<&HostHandle> {
        self.host_handle.get()
    }

    /// Per-call observability triad (latency + counter + optional audit).
    async fn emit_host_observability(
        &self,
        backend_name: &str,
        outcome_label: &'static str,
        reason: Option<&str>,
        identity: Option<&PluginIdentity>,
        request_id: &str,
        duration: Duration,
    ) {
        let Some(host) = self.host_handle() else {
            return;
        };
        host.histogram(
            "mcpg_bigquery_backend_latency_seconds",
            duration.as_secs_f64(),
            &[("outcome", outcome_label)],
        );
        host.counter(
            "mcpg_bigquery_backend_calls_total",
            1,
            &[("outcome", outcome_label)],
        );
        if let Some(action) = audit_action_for_outcome(outcome_label) {
            let actor = identity.cloned().unwrap_or_else(synthetic_system_identity);
            let mut details = json!({
                "backend": backend_name,
                "duration_ms": duration.as_millis() as u64,
                "outcome": outcome_label,
                "alias": host.alias(),
            });
            if let Some(reason) = reason {
                details
                    .as_object_mut()
                    .expect("json object")
                    .insert("reason".into(), Value::String(reason.to_owned()));
            }
            let event = AuditEvent {
                event_id: format!("bigquery-{}-{}", request_id, duration.as_nanos()),
                occurred_at: rfc3339_now(),
                actor,
                action: action.to_owned(),
                resource: Some(format!("bigquery-binding://{backend_name}")),
                outcome: AuditOutcome::Failure,
                request_id: Some(request_id.to_owned()),
                upstream_request_id: None,
                node_id: None,
                details,
                prev_event_hash: None,
            };
            let host_for_audit = host.clone();
            if let Err(join_err) = tokio::task::spawn_blocking(move || {
                let _ = host_for_audit.audit_event(event);
            })
            .await
            {
                debug!(target: "mcpg::bigquery::host_handle", error = %join_err, "audit spawn_blocking failed");
            }
        }
    }

    /// Build an error envelope, emit the triad, and return it as a normal
    /// payload — matching the snowflake/oracle/http backends.
    #[allow(clippy::too_many_arguments)]
    async fn finish_error(
        &self,
        profile: &BigQueryProfile,
        backend_name: &str,
        tool_name: &str,
        message: &str,
        label: &'static str,
        identity: Option<&PluginIdentity>,
        request_id: &str,
        started: Instant,
        host_span: Option<SpanGuard>,
    ) -> Result<BackendResponse, BackendError> {
        let downstream = classify_error(message);
        let envelope = build_result_envelope(
            tool_name,
            backend_name,
            &profile.project_id,
            profile.dataset.as_deref(),
            None,
            None,
            false,
            started.elapsed().as_millis(),
            Some(&downstream),
            Some(message),
        );
        self.emit_host_observability(
            backend_name,
            label,
            Some(message),
            identity,
            request_id,
            started.elapsed(),
        )
        .await;
        drop(host_span);
        finalize_payload(envelope)
    }

    /// Run the statement as a dry run, build the cost-estimate envelope, emit the
    /// triad, and return it. A failed dry run (invalid query / auth) degrades to
    /// the error envelope, matching the row path.
    #[allow(clippy::too_many_arguments)]
    async fn finish_estimate(
        &self,
        profile: &BigQueryProfile,
        backend_name: &str,
        tool_name: &str,
        client: &gcloud_bigquery::client::Client,
        query_parameters: Vec<gcloud_bigquery::http::types::QueryParameter>,
        identity: Option<&PluginIdentity>,
        request_id: &str,
        started: Instant,
        host_span: Option<SpanGuard>,
    ) -> Result<BackendResponse, BackendError> {
        let result: Result<EstimateOutcome, String> = match tokio::time::timeout(
            profile.timeout,
            run_dry_run(
                client,
                &profile.project_id,
                &profile.statement,
                profile.dataset.as_deref(),
                profile.location.as_deref(),
                profile.use_legacy_sql,
                profile.maximum_bytes_billed,
                profile.timeout_ms,
                query_parameters,
            ),
        )
        .await
        {
            Ok(inner) => inner,
            Err(_) => Err("BigQuery dry run timed out".to_owned()),
        };

        let (envelope, outcome_label, audit_reason): (Value, &'static str, Option<String>) =
            match result {
                Ok(estimate) => {
                    let cost = estimate_cost_usd(
                        estimate.total_bytes_processed,
                        profile.price_per_tib_usd,
                    );
                    (
                        build_estimate_envelope(
                            tool_name,
                            backend_name,
                            &profile.project_id,
                            profile.dataset.as_deref(),
                            estimate.total_bytes_processed,
                            cost,
                            estimate.cache_hit,
                            &estimate.schema,
                            started.elapsed().as_millis(),
                        ),
                        "ok",
                        None,
                    )
                }
                Err(message) => {
                    let downstream = classify_error(&message);
                    let lower = message.to_ascii_lowercase();
                    let label = if lower.contains("timed out") || lower.contains("timeout") {
                        "timeout"
                    } else if downstream["kind"] == json!("transport_error") {
                        "transport_error"
                    } else {
                        "bigquery_error"
                    };
                    let env = build_result_envelope(
                        tool_name,
                        backend_name,
                        &profile.project_id,
                        profile.dataset.as_deref(),
                        None,
                        None,
                        false,
                        started.elapsed().as_millis(),
                        Some(&downstream),
                        Some(&message),
                    );
                    (env, label, Some(message))
                }
            };

        self.emit_host_observability(
            backend_name,
            outcome_label,
            audit_reason.as_deref(),
            identity,
            request_id,
            started.elapsed(),
        )
        .await;
        drop(host_span);
        finalize_payload(envelope)
    }
}

impl std::fmt::Debug for BigQueryBackendPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryBackendPlugin")
            .field("id", &self.manifest.id)
            .finish()
    }
}

#[async_trait]
impl BackendPlugin for BigQueryBackendPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        "bigquery"
    }

    async fn register_profile(
        &self,
        backend_name: &str,
        spec: &Value,
        _host: Arc<dyn BackendHost>,
    ) -> Result<(), BackendError> {
        let parsed: BigQueryBackendSpec =
            serde_json::from_value(spec.clone()).map_err(|e| BackendError::InvalidSpec {
                message: format!("BigQuery binding spec: {e}"),
            })?;

        let invalid = |m: String| BackendError::InvalidSpec { message: m };
        if parsed.project_id.trim().is_empty() {
            return Err(invalid("project_id must not be empty".into()));
        }
        // A resource_template binding may supply only `read_query` (the per-`{id}`
        // single-row read) and omit `statement`; otherwise the operator-fixed
        // `statement` is required.
        if parsed.statement.trim().is_empty()
            && parsed
                .read_query
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(invalid(
                "statement must not be empty (or set `read_query` for a resource_template read binding)".into(),
            ));
        }
        if parsed.query.timeout_ms == 0 {
            return Err(invalid("query.timeout_ms must be greater than 0".into()));
        }
        if parsed.query.max_rows == 0 {
            return Err(invalid("query.max_rows must be greater than 0".into()));
        }

        let credentials_json = parsed.auth.credentials_json.clone();
        // Per-caller `cred://` is unsupported (the connection is one service
        // identity). Point operators at the config secret-resolver.
        if credentials_json.starts_with("cred://") {
            return Err(invalid(
                "auth.credentials_json must not be a cred:// URI — per-caller \
                 credentials are unsupported (the connection is one service identity); \
                 use ${env.X} / vault:// (resolved at config load) instead"
                    .into(),
            ));
        }
        if credentials_json.trim().is_empty() {
            return Err(invalid(
                "auth.credentials_json must not be empty for mode service_account".into(),
            ));
        }

        // Fail-closed read-only guard, validated at registration (no network).
        // The guard runs on a present `statement`; a resource_template read
        // binding may omit it (the per-`{id}` read lives in `read_query`, guarded
        // below).
        if parsed.query.read_only && !parsed.statement.trim().is_empty() {
            enforce_read_only(&parsed.statement).map_err(invalid)?;
        }

        // Surface coherence: `uri` is only meaningful on the resource surface;
        // a static `uri` on a tool/prompt binding is a config mistake worth a
        // fail-closed rejection at register rather than a silent no-op.
        if parsed.uri.is_some() && parsed.surface != surface::Surface::Resource {
            return Err(invalid(format!(
                "`uri` is only valid with `surface: resource` (this binding is `surface: {}`)",
                parsed.surface.as_str()
            )));
        }
        if let Some(u) = &parsed.uri
            && u.trim().is_empty()
        {
            return Err(invalid("`uri` must not be empty".into()));
        }

        // `read_query` is the per-`{id}` single-row read for a resource_template
        // binding; like `statement` it is operator-fixed, must be read-only under
        // the guard, and must not carry a bare cred://. It only makes sense on the
        // resource surface — fail-closed elsewhere so a misplaced field is never a
        // silent no-op.
        if let Some(rq) = &parsed.read_query {
            if rq.trim().is_empty() {
                return Err(invalid("`read_query` must not be empty".into()));
            }
            if parsed.surface != surface::Surface::Resource {
                return Err(invalid(format!(
                    "`read_query` is only valid with `surface: resource` (this binding is `surface: {}`)",
                    parsed.surface.as_str()
                )));
            }
            // Secrets reach BigQuery through the config-resolved service-account
            // key; a bare `cred://` left in the read statement is always a mistake.
            if rq.contains("cred://") {
                return Err(invalid(
                    "`read_query` must not contain a bare cred:// URI — use ${cred://…} / ${env.X} (resolved at config load)".into(),
                ));
            }
            if parsed.query.read_only {
                enforce_read_only(rq).map_err(invalid)?;
            }
        }

        // Listing is an operator-fixed read surface; fail-closed at register so
        // misconfig never reaches a `resources/list` call. The list statement is
        // also subject to the read-only guard when enabled.
        if let Some(lq) = &parsed.list_query {
            validate_list_query(lq).map_err(invalid)?;
            if parsed.query.read_only {
                enforce_read_only(&lq.sql).map_err(invalid)?;
            }
        }

        // Completion queries are operator-fixed read surfaces; fail-closed at
        // register so misconfig never reaches a completion call. The query is
        // also subject to the read-only guard when enabled.
        for (name, cc) in &parsed.variable_completions {
            validate_completion(name, cc).map_err(invalid)?;
            if parsed.query.read_only {
                enforce_read_only(&cc.sql).map_err(invalid)?;
            }
        }

        let compiled_params: Arc<[CompiledParam]> =
            compile_params(&parsed.params).map_err(invalid)?.into();

        debug!(
            backend = %backend_name,
            project = %parsed.project_id,
            mode = parsed.auth.mode.as_str(),
            read_only = parsed.query.read_only,
            params = compiled_params.len(),
            "registered BigQuery binding profile"
        );

        self.profiles.write().await.insert(
            backend_name.to_owned(),
            BigQueryProfile {
                project_id: parsed.project_id,
                dataset: parsed.dataset,
                location: parsed.location,
                credentials_json,
                endpoint_override: None,
                statement: parsed.statement,
                compiled_params,
                use_legacy_sql: parsed.query.use_legacy_sql,
                read_only: parsed.query.read_only,
                maximum_bytes_billed: parsed.query.maximum_bytes_billed,
                max_rows: parsed.query.max_rows,
                dry_run: parsed.query.dry_run,
                price_per_tib_usd: parsed.query.price_per_tib_usd,
                timeout: Duration::from_millis(parsed.query.timeout_ms),
                timeout_ms: parsed.query.timeout_ms,
                surface: parsed.surface,
                surface_uri: parsed.uri,
                list_query: parsed.list_query,
                read_query: parsed.read_query,
                variable_completions: Arc::new(parsed.variable_completions),
                client: Arc::new(OnceCell::new()),
            },
        );
        Ok(())
    }

    async fn execute(
        &self,
        backend_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        let started = Instant::now();
        let request_id = request.request_id.clone();
        let identity = request.identity.clone();
        let host_span = self.host_handle().map(|h| {
            h.span(
                "bigquery_backend.execute",
                json!({ "backend": backend_name, "request_id": request_id }),
            )
        });

        let profile = {
            let guard = self.profiles.read().await;
            match guard.get(backend_name).cloned() {
                Some(p) => p,
                None => {
                    let err = BackendError::ProfileNotFound {
                        backend_name: backend_name.to_owned(),
                    };
                    self.emit_host_observability(
                        backend_name,
                        "profile_not_found",
                        Some(&err.to_string()),
                        identity.as_ref(),
                        &request_id,
                        started.elapsed(),
                    )
                    .await;
                    drop(host_span);
                    return Err(err);
                }
            }
        };

        let tool_name = request
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("mcpg-tool-name"))
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| backend_name.to_owned());

        // The statement is operator-fixed and caller args are not interpolated.
        // The args feed two things: the resource surface needs the requested
        // `uri`, and the CEL `params` are evaluated against them and bound as
        // query parameters (never reaching the SQL text).
        let request_args: Value = if request.payload.is_empty() {
            json!({})
        } else {
            serde_json::from_slice(&request.payload).unwrap_or_else(|_| json!({}))
        };

        // Evaluate the CEL parameter expressions, then lower each to a scalar
        // BigQuery query parameter (rejecting arrays/objects) — all offline.
        let query_parameters = match evaluate_params(&profile.compiled_params, &request_args) {
            Ok(values) => {
                let mut params = Vec::with_capacity(values.len());
                let mut err: Option<String> = None;
                for v in values {
                    match json_to_bq_param(v) {
                        Ok(p) => params.push(p),
                        Err(e) => {
                            err = Some(format!("binding params: {e}"));
                            break;
                        }
                    }
                }
                if let Some(message) = err {
                    return self
                        .finish_error(
                            &profile,
                            backend_name,
                            &tool_name,
                            &message,
                            "invalid_spec",
                            identity.as_ref(),
                            &request_id,
                            started,
                            host_span,
                        )
                        .await;
                }
                params
            }
            Err(e) => {
                return self
                    .finish_error(
                        &profile,
                        backend_name,
                        &tool_name,
                        &format!("evaluating params: {e}"),
                        "invalid_spec",
                        identity.as_ref(),
                        &request_id,
                        started,
                        host_span,
                    )
                    .await;
            }
        };

        // On the resource surface a per-`{id}` `read_query` (when configured) is
        // the single-row read for a `resource_templates[]` binding; it binds the
        // same `params` (the gateway-extracted template vars reach it as
        // `arguments.<var>`). Every other surface — and a resource binding without
        // `read_query` — runs the operator-fixed `statement`. The dry-run estimate
        // path is unaffected (it always plans `statement`).
        let effective_statement: &str = match (profile.surface, profile.read_query.as_deref()) {
            (surface::Surface::Resource, Some(rq)) => rq,
            _ => &profile.statement,
        };

        // The statement is operator-fixed; caller args are not interpolated in
        // v1. Re-assert the read-only guard at call time as defense in depth.
        if profile.read_only
            && let Err(message) = enforce_read_only(effective_statement)
        {
            return self
                .finish_error(
                    &profile,
                    backend_name,
                    &tool_name,
                    &message,
                    "bigquery_error",
                    identity.as_ref(),
                    &request_id,
                    started,
                    host_span,
                )
                .await;
        }

        // Build (or reuse) the cached client, then run the query under an
        // overall timeout covering auth + statement + read.
        let client = match profile.client().await {
            Ok(c) => c,
            Err(message) => {
                return self
                    .finish_error(
                        &profile,
                        backend_name,
                        &tool_name,
                        &message,
                        "bigquery_error",
                        identity.as_ref(),
                        &request_id,
                        started,
                        host_span,
                    )
                    .await;
            }
        };

        // Dry-run cost-estimate mode: plan the query without executing it and
        // return a bytes/cost estimate envelope (no rows). The same CEL-bound
        // parameters cross the wire so the estimate reflects the real query.
        if profile.dry_run {
            return self
                .finish_estimate(
                    &profile,
                    backend_name,
                    &tool_name,
                    &client,
                    query_parameters,
                    identity.as_ref(),
                    &request_id,
                    started,
                    host_span,
                )
                .await;
        }

        let result: Result<QueryOutcome, String> = match tokio::time::timeout(
            profile.timeout,
            run_query(
                &client,
                &profile.project_id,
                effective_statement,
                profile.dataset.as_deref(),
                profile.location.as_deref(),
                profile.use_legacy_sql,
                profile.maximum_bytes_billed,
                profile.timeout_ms,
                profile.max_rows,
                query_parameters,
            ),
        )
        .await
        {
            Ok(inner) => inner,
            Err(_) => Err("BigQuery call timed out".to_owned()),
        };

        let (envelope, outcome_label, audit_reason): (Value, &'static str, Option<String>) =
            match result {
                Ok(outcome) => {
                    // On the resource/prompt surfaces the gateway decoder
                    // requires a surface-shaped body; the tool surface keeps the
                    // historical envelope. A resource read with no resolvable URI
                    // falls back to the tool error envelope (carries
                    // `downstreamError` → gateway `is_error`) so the decoder sees
                    // a clean error rather than an invalid `{contents}`.
                    match profile.surface {
                        surface::Surface::Tool => (
                            build_result_envelope(
                                &tool_name,
                                backend_name,
                                &profile.project_id,
                                profile.dataset.as_deref(),
                                Some(&outcome.rows),
                                Some(outcome.row_count),
                                outcome.truncated,
                                started.elapsed().as_millis(),
                                None,
                                None,
                            ),
                            "ok",
                            None,
                        ),
                        surface::Surface::Resource => {
                            match surface::resolve_resource_uri(
                                profile.surface_uri.as_deref(),
                                &request_args,
                            ) {
                                Some(uri) => (
                                    surface::resource_contents_body(uri, &outcome.rows),
                                    "ok",
                                    None,
                                ),
                                None => {
                                    let message = "resource surface requires a `uri` (set a static `uri` on the binding or invoke via a resources/read request)".to_owned();
                                    let downstream = classify_error(&message);
                                    let env = build_result_envelope(
                                        &tool_name,
                                        backend_name,
                                        &profile.project_id,
                                        profile.dataset.as_deref(),
                                        None,
                                        None,
                                        false,
                                        started.elapsed().as_millis(),
                                        Some(&downstream),
                                        Some(&message),
                                    );
                                    (env, "bigquery_error", Some(message))
                                }
                            }
                        }
                        surface::Surface::Prompt => {
                            (surface::prompt_messages_body(&outcome.rows), "ok", None)
                        }
                    }
                }
                Err(message) => {
                    let downstream = classify_error(&message);
                    let lower = message.to_ascii_lowercase();
                    let label = if lower.contains("timed out") || lower.contains("timeout") {
                        "timeout"
                    } else if downstream["kind"] == json!("transport_error") {
                        "transport_error"
                    } else {
                        "bigquery_error"
                    };
                    let env = build_result_envelope(
                        &tool_name,
                        backend_name,
                        &profile.project_id,
                        profile.dataset.as_deref(),
                        None,
                        None,
                        false,
                        started.elapsed().as_millis(),
                        Some(&downstream),
                        Some(&message),
                    );
                    (env, label, Some(message))
                }
            };

        self.emit_host_observability(
            backend_name,
            outcome_label,
            audit_reason.as_deref(),
            identity.as_ref(),
            &request_id,
            started.elapsed(),
        )
        .await;
        drop(host_span);
        finalize_payload(envelope)
    }

    fn audit_metadata(&self, _backend_name: &str) -> serde_json::Map<String, Value> {
        let mut map = serde_json::Map::new();
        map.insert("bigquery.transport".to_owned(), json!("plugin"));
        map
    }

    /// JSON Schema for the result envelope this binding emits. A dry-run binding
    /// emits the estimate envelope (bytes / cacheHit / schema, no rows), so its
    /// schema differs from the row-returning query envelope.
    fn output_schema(&self, backend_name: &str) -> Option<Value> {
        let dry_run = self
            .profiles
            .try_read()
            .ok()
            .and_then(|g| g.get(backend_name).map(|p| p.dry_run))
            .unwrap_or(false);
        Some(if dry_run {
            envelope::estimate_envelope_schema()
        } else {
            envelope::result_envelope_schema()
        })
    }

    /// JSON Schema for the tool arguments. The binding's positional `params`
    /// are CEL expressions over `arguments.*`; the referenced argument names
    /// are surfaced as untyped, optional properties. The object stays open
    /// (`additionalProperties: true`) so the schema never rejects valid args.
    fn input_schema(&self, backend_name: &str) -> Option<Value> {
        // `try_read` (sync, non-blocking): `input_schema` is called from the
        // gateway's registration path with no concurrent writer.
        let names: Vec<String> = self
            .profiles
            .try_read()
            .ok()
            .and_then(|g| {
                g.get(backend_name)
                    .map(|p| arguments_referenced_by_params(&p.compiled_params))
            })
            .unwrap_or_default();
        Some(params_input_schema(&names))
    }

    /// Enumerate resources for `resources/list` via the operator-fixed
    /// `list_query`. The statement runs verbatim (no positional bind protocol)
    /// and the page is taken client-side by `page_size` — the opaque cursor is
    /// the integer offset into the full result. Bindings without a `list_query`
    /// inherit the empty page.
    async fn list_resources(
        &self,
        backend_name: &str,
        cursor: Option<&str>,
    ) -> Result<ResourcePage, BackendError> {
        let profile = {
            let guard = self.profiles.read().await;
            guard
                .get(backend_name)
                .cloned()
                .ok_or_else(|| BackendError::ProfileNotFound {
                    backend_name: backend_name.to_owned(),
                })?
        };
        let Some(list_cfg) = profile.list_query.clone() else {
            return Ok(ResourcePage::empty());
        };
        let offset = match cursor {
            Some(c) => c.parse::<u64>().map_err(|_| BackendError::InvalidSpec {
                message: format!("list cursor '{c}' is not a non-negative integer"),
            })?,
            None => 0,
        };

        let client = profile
            .client()
            .await
            .map_err(|message| BackendError::Transport { message })?;
        let outcome = tokio::time::timeout(
            profile.timeout,
            run_query(
                &client,
                &profile.project_id,
                &list_cfg.sql,
                profile.dataset.as_deref(),
                profile.location.as_deref(),
                profile.use_legacy_sql,
                profile.maximum_bytes_billed,
                profile.timeout_ms,
                profile.max_rows,
                // The list statement is operator-fixed and runs verbatim — no
                // caller-derived value reaches it, so no bound parameters.
                vec![],
            ),
        )
        .await
        .map_err(|_| BackendError::Timeout {
            timeout_ms: profile.timeout.as_millis() as u64,
        })?
        .map_err(|message| BackendError::Transport { message })?;

        Ok(surface::page_from_full_result(
            &outcome.rows,
            offset,
            list_cfg.page_size,
        ))
    }

    /// Return completion candidates for a resource-template variable via the
    /// operator-fixed `variable_completions[<variable_name>]` query. The single
    /// `?` placeholder is bound to the caller's typed `prefix` as a BigQuery
    /// STRING query parameter (never interpolated — injection-safe). Unconfigured
    /// variables inherit the empty list.
    async fn complete_template_variable(
        &self,
        backend_name: &str,
        variable_name: &str,
        prefix: &str,
        _config: &Value,
        _context: &BTreeMap<String, String>,
    ) -> Result<Vec<String>, BackendError> {
        let profile = {
            let guard = self.profiles.read().await;
            guard
                .get(backend_name)
                .cloned()
                .ok_or_else(|| BackendError::ProfileNotFound {
                    backend_name: backend_name.to_owned(),
                })?
        };
        let Some(cc) = profile.variable_completions.get(variable_name).cloned() else {
            return Ok(vec![]);
        };
        let max = cc.max_results.unwrap_or(100) as usize;

        let client = profile
            .client()
            .await
            .map_err(|message| BackendError::Transport { message })?;
        // Bind the caller-typed prefix as a single positional STRING parameter.
        let prefix_param = json_to_bq_param(Value::String(prefix.to_owned()))
            .map_err(|message| BackendError::InvalidSpec { message })?;
        let outcome = tokio::time::timeout(
            profile.timeout,
            run_query(
                &client,
                &profile.project_id,
                &cc.sql,
                profile.dataset.as_deref(),
                profile.location.as_deref(),
                profile.use_legacy_sql,
                profile.maximum_bytes_billed,
                profile.timeout_ms,
                max,
                vec![prefix_param],
            ),
        )
        .await
        .map_err(|_| BackendError::Timeout {
            timeout_ms: profile.timeout.as_millis() as u64,
        })?
        .map_err(|message| BackendError::Transport { message })?;

        let first_col = outcome
            .rows
            .first()
            .and_then(Value::as_object)
            .and_then(|m| m.keys().next().cloned());
        Ok(surface::rows_to_completion_values(
            &outcome.rows,
            first_col.as_deref(),
            max,
        ))
    }
}

/// Collect the distinct `arguments.<ident>` names referenced across a binding's
/// compiled CEL params, preserving first-seen order.
fn arguments_referenced_by_params(params: &[CompiledParam]) -> Vec<String> {
    let mut names = Vec::new();
    for p in params {
        for name in extract_argument_idents(&p.source) {
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }
    names
}

/// Build an open object schema from the referenced argument names. With no
/// known names this is the permissive `{type:object, additionalProperties:true}`.
fn params_input_schema(names: &[String]) -> Value {
    let mut properties = serde_json::Map::new();
    for name in names {
        properties.insert(name.clone(), json!({}));
    }
    json!({
        "type": "object",
        "properties": Value::Object(properties),
        "additionalProperties": true,
    })
}

/// Extract identifiers appearing as `arguments.<ident>` in a CEL source string.
/// Pure string scan (no CEL deps) — a best-effort hint, never a rejection
/// surface.
fn extract_argument_idents(source: &str) -> Vec<String> {
    const MARKER: &str = "arguments.";
    let mut out = Vec::new();
    let bytes = source.as_bytes();
    let mut search_from = 0;
    while let Some(rel) = source[search_from..].find(MARKER) {
        let start = search_from + rel + MARKER.len();
        let mut end = start;
        while end < bytes.len() {
            let c = bytes[end];
            if c.is_ascii_alphanumeric() || c == b'_' {
                end += 1;
            } else {
                break;
            }
        }
        if end > start {
            out.push(source[start..end].to_owned());
        }
        search_from = end.max(search_from + rel + MARKER.len());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_op_host() -> Arc<dyn BackendHost> {
        Arc::new(NoOpHost)
    }

    fn minimal_spec() -> Value {
        json!({
            "project_id": "my-proj",
            "dataset": "analytics",
            "location": "EU",
            "auth": {
                "mode": "service_account",
                "credentials_json": "{}",
            },
            "statement": "SELECT 1 AS one",
        })
    }

    #[test]
    fn kind_is_bigquery() {
        assert_eq!(BigQueryBackendPlugin::new().kind(), "bigquery");
    }

    #[test]
    fn manifest_id() {
        assert_eq!(
            BigQueryBackendPlugin::new().manifest().id,
            "dev.mcpg.backend.bigquery"
        );
    }

    #[test]
    fn output_schema_is_object() {
        let schema = BackendPlugin::output_schema(&BigQueryBackendPlugin::new(), "rpt").unwrap();
        assert_eq!(schema["type"], json!("object"));
    }

    #[test]
    fn input_schema_is_permissive_object_when_unregistered() {
        let schema = BackendPlugin::input_schema(&BigQueryBackendPlugin::new(), "rpt").unwrap();
        assert_eq!(schema["type"], json!("object"));
        assert_eq!(schema["additionalProperties"], json!(true));
    }

    #[test]
    fn extract_argument_idents_finds_names() {
        let got = extract_argument_idents("arguments.user_id + size(arguments.tags)");
        assert_eq!(got, vec!["user_id".to_owned(), "tags".to_owned()]);
        assert!(extract_argument_idents("1 + 2").is_empty());
    }

    #[tokio::test]
    async fn input_schema_lists_referenced_params() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["statement"] = json!("SELECT id FROM t WHERE id = ?");
        spec["params"] = json!(["arguments.id"]);
        plugin
            .register_profile("rpt", &spec, no_op_host())
            .await
            .expect("register");
        let schema = BackendPlugin::input_schema(&plugin, "rpt").unwrap();
        assert_eq!(schema["type"], json!("object"));
        assert_eq!(schema["additionalProperties"], json!(true));
        assert!(schema["properties"]["id"].is_object());
    }

    #[tokio::test]
    async fn register_accepts_params() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["statement"] = json!("SELECT id FROM t WHERE id = ? AND p = ?");
        spec["params"] = json!(["arguments.id", "arguments.p"]);
        plugin
            .register_profile("rpt", &spec, no_op_host())
            .await
            .expect("register");
        let profiles = plugin.profiles.read().await;
        assert_eq!(profiles.get("rpt").unwrap().compiled_params.len(), 2);
    }

    #[tokio::test]
    async fn register_rejects_bad_cel_param() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["params"] = json!(["this is not cel ((("]);
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("bad cel");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_stores_variable_completions() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["surface"] = json!("resource");
        spec["variable_completions"] = json!({
            "name": { "sql": "SELECT name FROM docs WHERE STARTS_WITH(name, ?)" }
        });
        plugin
            .register_profile("r", &spec, no_op_host())
            .await
            .expect("register");
        let profiles = plugin.profiles.read().await;
        assert!(
            profiles
                .get("r")
                .unwrap()
                .variable_completions
                .contains_key("name")
        );
    }

    #[tokio::test]
    async fn register_rejects_completion_without_placeholder() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["variable_completions"] = json!({
            "name": { "sql": "SELECT name FROM docs" }
        });
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("missing placeholder");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_accepts_minimal_spec() {
        let plugin = BigQueryBackendPlugin::new();
        plugin
            .register_profile("rpt", &minimal_spec(), no_op_host())
            .await
            .expect("register");
        let profiles = plugin.profiles.read().await;
        let p = profiles.get("rpt").unwrap();
        assert_eq!(p.project_id, "my-proj");
        assert!(p.read_only);
        assert_eq!(p.statement, "SELECT 1 AS one");
        // No client built at registration (offline).
        assert!(p.client.get().is_none());
        // Default surface is the unchanged tool envelope.
        assert_eq!(p.surface, surface::Surface::Tool);
        assert!(p.surface_uri.is_none());
    }

    #[tokio::test]
    async fn register_stores_resource_surface_and_uri() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["surface"] = json!("resource");
        spec["uri"] = json!("bigquery://docs/all");
        plugin
            .register_profile("r", &spec, no_op_host())
            .await
            .expect("register");
        let profiles = plugin.profiles.read().await;
        let p = profiles.get("r").unwrap();
        assert_eq!(p.surface, surface::Surface::Resource);
        assert_eq!(p.surface_uri.as_deref(), Some("bigquery://docs/all"));
    }

    #[tokio::test]
    async fn register_rejects_uri_on_tool_surface() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["uri"] = json!("bigquery://x");
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("uri on tool surface");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_rejects_cred_secret() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["auth"]["credentials_json"] = json!("cred://x");
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("cred secret");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_rejects_non_select_when_read_only() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["statement"] = json!("DROP TABLE t");
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("non-select under read_only");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_allows_non_select_when_not_read_only() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["statement"] = json!("CREATE TABLE t (a INT64)");
        spec["query"] = json!({ "read_only": false });
        plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect("register write under read_only=false");
    }

    #[tokio::test]
    async fn register_rejects_empty_statement() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["statement"] = json!("   ");
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("empty statement");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn execute_unknown_profile_is_profile_not_found() {
        let plugin = BigQueryBackendPlugin::new();
        let req = BackendRequest {
            payload: vec![],
            headers: vec![],
            request_id: "rq-1".into(),
            session_id: None,
            identity: None,
            idempotency: None,
        };
        let err = plugin.execute("missing", req).await.expect_err("missing");
        assert!(matches!(err, BackendError::ProfileNotFound { .. }));
    }

    #[tokio::test]
    async fn list_resources_empty_when_unconfigured() {
        let plugin = BigQueryBackendPlugin::new();
        plugin
            .register_profile("q", &minimal_spec(), no_op_host())
            .await
            .expect("register");
        let page = BackendPlugin::list_resources(&plugin, "q", None)
            .await
            .expect("list");
        assert!(page.resources.is_empty());
        assert!(page.next_cursor.is_none());
    }

    #[tokio::test]
    async fn complete_template_variable_empty_when_unconfigured() {
        let plugin = BigQueryBackendPlugin::new();
        plugin
            .register_profile("q", &minimal_spec(), no_op_host())
            .await
            .expect("register");
        let got = BackendPlugin::complete_template_variable(
            &plugin,
            "q",
            "v",
            "x",
            &json!({}),
            &BTreeMap::new(),
        )
        .await
        .expect("complete");
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn register_stores_dry_run_and_price() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["query"] = json!({ "dry_run": true, "price_per_tib_usd": 6.25 });
        plugin
            .register_profile("est", &spec, no_op_host())
            .await
            .expect("register");
        let profiles = plugin.profiles.read().await;
        let p = profiles.get("est").unwrap();
        assert!(p.dry_run);
        assert_eq!(p.price_per_tib_usd, Some(6.25));
    }

    #[tokio::test]
    async fn output_schema_is_estimate_shape_for_dry_run_binding() {
        let plugin = BigQueryBackendPlugin::new();
        // Default (non-dry-run) binding advertises the row envelope schema.
        plugin
            .register_profile("rows", &minimal_spec(), no_op_host())
            .await
            .expect("register");
        let row_schema = BackendPlugin::output_schema(&plugin, "rows").unwrap();
        assert!(row_schema["properties"].get("response").is_some());
        assert!(row_schema["properties"].get("estimate").is_none());

        // A dry-run binding advertises the estimate envelope schema instead.
        let mut spec = minimal_spec();
        spec["query"] = json!({ "dry_run": true });
        plugin
            .register_profile("est", &spec, no_op_host())
            .await
            .expect("register");
        let est_schema = BackendPlugin::output_schema(&plugin, "est").unwrap();
        assert!(est_schema["properties"].get("estimate").is_some());
        assert!(
            est_schema["properties"]["estimate"]["properties"]
                .get("totalBytesProcessed")
                .is_some()
        );
    }

    #[tokio::test]
    async fn register_stores_list_query() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["surface"] = json!("resource");
        spec["list_query"] = json!({ "sql": "SELECT uri FROM docs", "page_size": 10 });
        plugin
            .register_profile("r", &spec, no_op_host())
            .await
            .expect("register");
        let profiles = plugin.profiles.read().await;
        assert!(profiles.get("r").unwrap().list_query.is_some());
    }

    #[tokio::test]
    async fn register_rejects_empty_list_query_sql() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["list_query"] = json!({ "sql": "  " });
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("empty list sql");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    /// A resource_template binding may declare a per-`{id}` `read_query` and omit
    /// `statement`; the profile stores it and stays read-only-guarded.
    #[tokio::test]
    async fn register_resource_template_read_query() {
        let plugin = BigQueryBackendPlugin::new();
        let spec = json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "surface": "resource",
            "read_query": "SELECT * FROM dataset.orders WHERE id = ?",
            "params": ["arguments.id"],
        });
        plugin
            .register_profile("rt", &spec, no_op_host())
            .await
            .expect("read_query registers without a statement");
        let profiles = plugin.profiles.read().await;
        let p = profiles.get("rt").unwrap();
        assert_eq!(
            p.read_query.as_deref(),
            Some("SELECT * FROM dataset.orders WHERE id = ?")
        );
        assert!(p.statement.is_empty());
        assert_eq!(p.surface, surface::Surface::Resource);
        assert_eq!(p.compiled_params.len(), 1);
    }

    #[tokio::test]
    async fn register_rejects_read_query_on_tool_surface() {
        let plugin = BigQueryBackendPlugin::new();
        let mut spec = minimal_spec();
        spec["read_query"] = json!("SELECT * FROM t WHERE id = ?");
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("read_query on tool surface");
        match err {
            BackendError::InvalidSpec { message } => {
                assert!(message.contains("read_query"), "{message}");
                assert!(message.contains("surface: resource"), "{message}");
            }
            other => panic!("expected InvalidSpec, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn register_rejects_non_read_only_read_query() {
        let plugin = BigQueryBackendPlugin::new();
        let spec = json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "surface": "resource",
            "read_query": "DELETE FROM dataset.orders WHERE id = ?",
            "params": ["arguments.id"],
        });
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("non-read-only read_query");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    #[tokio::test]
    async fn register_rejects_bare_cred_read_query() {
        let plugin = BigQueryBackendPlugin::new();
        let spec = json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "surface": "resource",
            "read_query": "SELECT * FROM t WHERE k = 'cred://aws/x#id'",
            "params": [],
        });
        let err = plugin
            .register_profile("x", &spec, no_op_host())
            .await
            .expect_err("bare cred in read_query");
        assert!(matches!(err, BackendError::InvalidSpec { .. }));
    }

    /// The gateway delivers the extracted template variable as `arguments.<var>`;
    /// the binding's `params` CEL bind it to the `read_query`'s `?` placeholder. A
    /// value crafted to look like SQL is carried verbatim as a single positional
    /// BigQuery query parameter — it is data the engine treats as an opaque value,
    /// never spliced into the statement text.
    #[test]
    fn template_var_binds_as_param_not_interpolated() {
        let compiled = params::compile_params(&["arguments.id".to_owned()]).unwrap();
        // What the gateway hands the backend for `bigquery://orders/{id}` on a read
        // of `bigquery://orders/1 OR 1=1; DROP TABLE orders`.
        let injection = "1 OR 1=1; DROP TABLE orders";
        let args = json!({
            "uri": format!("bigquery://orders/{injection}"),
            "id": injection,
        });
        let values = params::evaluate_params(&compiled, &args).unwrap();
        assert_eq!(values, vec![json!(injection)]);
        // The whole injection string lowers to one positional STRING parameter —
        // the engine binds it as a value, it never reaches SQL as text.
        let param = params::json_to_bq_param(values.into_iter().next().unwrap()).unwrap();
        assert!(param.name.is_none());
        assert_eq!(param.parameter_type.parameter_type, "STRING");
        assert_eq!(param.parameter_value.value.as_deref(), Some(injection));
    }

    /// The resource-read branch shapes a single fabricated row into the
    /// `resources/read` contract body keyed on the concrete (gateway-supplied) URI.
    #[test]
    fn resource_template_read_shapes_single_row_contents() {
        let uri = "bigquery://orders/42";
        let row = json!({ "id": 42, "total": 19.99 });
        let body = surface::resource_contents_body(uri, std::slice::from_ref(&row));
        let contents = body["contents"].as_array().expect("contents");
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0]["uri"], json!(uri));
        assert_eq!(contents[0]["mimeType"], json!("application/json"));
        let decoded: Vec<Value> =
            serde_json::from_str(contents[0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(decoded, vec![row]);
    }

    struct NoOpHost;

    #[async_trait]
    impl BackendHost for NoOpHost {
        async fn invoke_tool(
            &self,
            _ctx: &mcpg_plugin_protocol::BackendInvocationContext,
            _tool_name: &str,
            _args: &serde_json::Value,
        ) -> Result<serde_json::Value, mcpg_plugin_protocol::BackendHostError> {
            Err(mcpg_plugin_protocol::BackendHostError::NotImplemented)
        }
    }
}
