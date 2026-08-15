//! `watch_strategy` entity (`bigquery_poll`) — the POLLING change-watch path.
//!
//! BigQuery has no native change-push channel, so this strategy polls a cheap
//! read-only scalar "high-water" query (`SELECT MAX(updated_at) FROM
//! dataset.table`, a row count, a monotonic sequence, …) on a cadence and
//! signals a change whenever that scalar advances. The poll thread, the cursor
//! diff, the stop signal and the opaque handle round-trip all live in the shared
//! [`mcpg_plugin_sdk::watch`] helper — this entity only supplies the per-tick
//! `poll` closure over its own engine.
//!
//! The helper's loop is synchronous and [`engine::run_query`] is async, so a
//! single current-thread tokio runtime is built once in [`watch`] and moved into
//! the closure; each tick `block_on`s one query (sequential ticks, so a
//! single-thread runtime is enough). The cached client is also built on that
//! runtime at watch start (auth + token providers), so a connect / auth failure
//! maps to [`WatchError::Subscribe`]; per-tick query failures map to the
//! closure's `Err(String)` — the helper logs and retries on the next tick.

use std::sync::Arc;
use std::time::Duration;

use mcpg_plugin_protocol::backend::WatchError;
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::watch::{cancel_polling_watch, spawn_polling_watch};
use serde::Deserialize;
use serde_json::Value;

use crate::bigquery::{self, QueryOutcome};
use crate::types::BigQueryAuth;

pub const PLUGIN_ID: &str = "dev.mcpg.backend.bigquery";

/// The strategy discriminator this entity handles.
pub const WATCH_KIND: &str = "bigquery_poll";

/// Default poll cadence when `interval_ms` is omitted (1 minute).
fn default_interval_ms() -> u64 {
    60_000
}

/// Default per-tick query budget when `timeout_ms` is omitted (10 seconds).
fn default_timeout_ms() -> u64 {
    10_000
}

/// Per-watch spec: the connection fields needed to build a client (reusing the
/// backend's connection shape) plus the read-only scalar high-water
/// `tracking_query` and the poll cadence. The connection is carried per-watch
/// (not at plugin level), so a watcher is self-contained.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    /// GCP project the tracking query bills to and runs in. Operator-fixed.
    project_id: String,
    /// Default dataset for unqualified table names in the tracking query.
    #[serde(default)]
    dataset: Option<String>,
    /// Geographic location the job runs in (e.g. `EU`, `US`, `europe-west1`).
    #[serde(default)]
    location: Option<String>,
    /// Auth block (service-account key JSON), same shape as the binding.
    auth: BigQueryAuth,
    /// The read-only scalar high-water query whose first-row first-column value
    /// is the cursor (e.g. `SELECT MAX(updated_at) FROM dataset.table`).
    /// REQUIRED.
    tracking_query: String,
    /// Poll cadence in milliseconds (default 60000; floored by the SDK helper).
    #[serde(default = "default_interval_ms")]
    interval_ms: u64,
    /// Per-tick server-side + wall-clock query budget in milliseconds
    /// (default 10000).
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

/// `watch_strategy` entity. Stateless beyond its manifest — every watcher's
/// connection + tracking query arrive on the per-watch spec.
pub struct BigQueryWatchCdylib {
    manifest: PluginManifest,
}

impl BigQueryWatchCdylib {
    /// Infallible cdylib factory. `config_json` + host are ignored — the watch
    /// carries no plugin-level config (the connection + `tracking_query` arrive
    /// via the per-watch spec).
    pub fn from_host_config(_config_json: &str, _host: HostHandle) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.bigquery",
                name: "BigQuery Poll Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

/// Extract the cursor scalar from a high-water query outcome: the first column
/// of the first row, stringified (numbers / bools / strings alike). `None` when
/// the query returned zero rows (no signal this tick) or the first row has no
/// columns. JSON-string values yield the bare string; everything else its JSON
/// rendering, so the cursor comparison is stable across ticks.
fn cursor_from_outcome(outcome: &QueryOutcome) -> Option<String> {
    let first = outcome.rows.first()?;
    let scalar = first.as_object()?.values().next()?;
    Some(match scalar {
        Value::String(s) => s.clone(),
        Value::Null => return None,
        other => other.to_string(),
    })
}

impl SyncWatchStrategyPlugin for BigQueryWatchCdylib {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let parsed: WatchSpec =
            serde_json::from_value(spec.clone()).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid bigquery_poll watch spec: {e}"),
            })?;

        let invalid = |m: String| WatchError::InvalidSpec { message: m };
        if parsed.project_id.trim().is_empty() {
            return Err(invalid("project_id must not be empty".into()));
        }
        if parsed.tracking_query.trim().is_empty() {
            return Err(invalid("tracking_query must not be empty".into()));
        }
        // The tracking query is read-only by contract — reuse the engine guard so
        // a polling watcher can never mutate the warehouse.
        bigquery::enforce_read_only(&parsed.tracking_query).map_err(invalid)?;
        // Per-caller `cred://` is unsupported (the connection is one service
        // identity); reject it as the backend register guard does.
        if parsed.auth.credentials_json.starts_with("cred://") {
            return Err(invalid(
                "auth.credentials_json must not be a cred:// URI — per-caller \
                 credentials are unsupported (the connection is one service identity); \
                 use ${env.X} / vault:// (resolved at config load) instead"
                    .into(),
            ));
        }
        if parsed.auth.credentials_json.trim().is_empty() {
            return Err(invalid(
                "auth.credentials_json must not be empty for mode service_account".into(),
            ));
        }

        // One current-thread runtime, moved into the closure: ticks are
        // sequential, so a single-thread runtime is enough to `block_on` each
        // async query.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| WatchError::Subscribe {
                message: format!("bigquery_poll: tokio runtime init failed: {e}"),
            })?;

        // Build (and authenticate) the client once at watch start; a connect /
        // auth failure surfaces as a subscribe failure rather than retrying
        // forever on a bad key.
        let client = rt
            .block_on(bigquery::build_client(&parsed.auth.credentials_json, None))
            .map_err(|message| WatchError::Subscribe {
                message: format!("bigquery_poll: client init failed: {message}"),
            })?;

        let project_id = parsed.project_id;
        let dataset = parsed.dataset;
        let location = parsed.location;
        let tracking_query = parsed.tracking_query;
        let timeout_ms = parsed.timeout_ms;
        let client = Arc::new(client);

        let poll = move || -> Result<Option<String>, String> {
            let outcome = rt.block_on(bigquery::run_query(
                &client,
                &project_id,
                &tracking_query,
                dataset.as_deref(),
                location.as_deref(),
                false,
                None,
                timeout_ms,
                1,
                Vec::new(),
            ))?;
            Ok(cursor_from_outcome(&outcome))
        };

        Ok(spawn_polling_watch(
            resource_uri,
            Duration::from_millis(parsed.interval_ms),
            emit_event,
            poll,
        ))
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        cancel_polling_watch(watch_handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn stub_host() -> HostHandle {
        // SAFETY: `stub_host_ref` returns a process-static no-op host ref; the
        // factory ignores the host entirely.
        #[allow(unsafe_code)]
        unsafe {
            HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref())
        }
    }

    fn plugin() -> BigQueryWatchCdylib {
        BigQueryWatchCdylib::from_host_config("", stub_host())
    }

    fn emit_noop() -> Box<dyn Fn(&str) + Send + Sync + 'static> {
        Box::new(|_| {})
    }

    #[test]
    fn manifest_and_kind_are_correct() {
        use mcpg_plugin_protocol::PluginClass;
        let p = plugin();
        let m = SyncWatchStrategyPlugin::manifest(&p);
        assert_eq!(m.id, PLUGIN_ID);
        assert_eq!(m.plugin_class, PluginClass::WatchStrategy);
        assert_eq!(p.kind(), WATCH_KIND);
    }

    #[test]
    fn spec_parses_with_defaults() {
        let parsed: WatchSpec = serde_json::from_value(json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "tracking_query": "SELECT MAX(updated_at) FROM analytics.events",
        }))
        .unwrap();
        assert_eq!(parsed.interval_ms, 60_000);
        assert_eq!(parsed.timeout_ms, 10_000);
        assert!(parsed.dataset.is_none());
        assert!(parsed.location.is_none());
    }

    #[test]
    fn spec_parses_overrides() {
        let parsed: WatchSpec = serde_json::from_value(json!({
            "project_id": "my-proj",
            "dataset": "analytics",
            "location": "EU",
            "auth": { "mode": "service_account", "credentials_json": "${env.BQ_KEY}" },
            "tracking_query": "SELECT COUNT(*) FROM analytics.events",
            "interval_ms": 30_000,
            "timeout_ms": 5_000,
        }))
        .unwrap();
        assert_eq!(parsed.dataset.as_deref(), Some("analytics"));
        assert_eq!(parsed.location.as_deref(), Some("EU"));
        assert_eq!(parsed.interval_ms, 30_000);
        assert_eq!(parsed.timeout_ms, 5_000);
    }

    #[test]
    fn unknown_field_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "bigquery://events",
                &json!({
                    "project_id": "my-proj",
                    "auth": { "credentials_json": "{}" },
                    "tracking_query": "SELECT 1",
                    "bogus": true,
                }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn empty_tracking_query_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "bigquery://events",
                &json!({
                    "project_id": "my-proj",
                    "auth": { "credentials_json": "{}" },
                    "tracking_query": "   ",
                }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn non_read_only_tracking_query_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "bigquery://events",
                &json!({
                    "project_id": "my-proj",
                    "auth": { "credentials_json": "{}" },
                    "tracking_query": "INSERT INTO events VALUES (CURRENT_TIMESTAMP())",
                }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn empty_project_id_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "bigquery://events",
                &json!({
                    "project_id": "  ",
                    "auth": { "credentials_json": "{}" },
                    "tracking_query": "SELECT MAX(t) FROM e",
                }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn cred_credentials_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "bigquery://events",
                &json!({
                    "project_id": "my-proj",
                    "auth": { "credentials_json": "cred://x/y" },
                    "tracking_query": "SELECT MAX(t) FROM e",
                }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn empty_credentials_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "bigquery://events",
                &json!({
                    "project_id": "my-proj",
                    "auth": { "credentials_json": "   " },
                    "tracking_query": "SELECT MAX(t) FROM e",
                }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn cursor_from_outcome_extracts_first_scalar() {
        // A monotonic timestamp string.
        let outcome = QueryOutcome {
            rows: vec![json!({ "f0_": "2026-06-23 10:00:00" })],
            truncated: false,
            row_count: 1,
        };
        assert_eq!(
            cursor_from_outcome(&outcome).as_deref(),
            Some("2026-06-23 10:00:00")
        );

        // A numeric high-water value stringifies to its JSON rendering.
        let outcome = QueryOutcome {
            rows: vec![json!({ "f0_": 42 })],
            truncated: false,
            row_count: 1,
        };
        assert_eq!(cursor_from_outcome(&outcome).as_deref(), Some("42"));
    }

    #[test]
    fn cursor_from_outcome_none_on_zero_rows_or_null() {
        let empty = QueryOutcome {
            rows: vec![],
            truncated: false,
            row_count: 0,
        };
        assert_eq!(cursor_from_outcome(&empty), None);

        let null = QueryOutcome {
            rows: vec![json!({ "f0_": Value::Null })],
            truncated: false,
            row_count: 1,
        };
        assert_eq!(cursor_from_outcome(&null), None);
    }
}
