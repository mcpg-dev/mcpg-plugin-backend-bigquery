//! Operator-facing spec for the BigQuery backend plugin.
//!
//! One binding = one operator-fixed Standard-SQL statement = one MCP tool (or
//! resource). The project / dataset / location, the auth (service-account key
//! JSON), the statement and the query bounds all live on the per-binding spec,
//! mirroring the snowflake / oracle / mssql one-profile-per-binding shape.

use serde::Deserialize;

/// How the binding authenticates to BigQuery.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BigQueryAuthMode {
    /// Service-account key JSON (`credentials_json`).
    #[default]
    ServiceAccount,
}

impl BigQueryAuthMode {
    pub fn as_str(self) -> &'static str {
        match self {
            BigQueryAuthMode::ServiceAccount => "service_account",
        }
    }
}

/// Auth block. `service_account` mode reads the GCP service-account key from
/// `credentials_json` (the full key JSON, secret-resolved at config load).
#[derive(Debug, Clone, Deserialize)]
pub struct BigQueryAuth {
    /// Auth mechanism (default `service_account`).
    #[serde(default)]
    pub mode: BigQueryAuthMode,

    /// The GCP service-account key, as the literal JSON document. A `${env.X}`
    /// / `vault://...` reference the gateway secret-resolver expands at config
    /// load — never plaintext in committed config. A bare per-caller `cred://`
    /// is rejected (the connection is one service identity).
    #[serde(default)]
    pub credentials_json: String,
}

/// Query-execution bounds.
#[derive(Debug, Clone, Deserialize)]
pub struct BigQueryQueryConfig {
    /// Use BigQuery legacy SQL when true (default false → Standard SQL).
    #[serde(default = "default_use_legacy_sql")]
    pub use_legacy_sql: bool,

    /// When true (default), the statement is rejected unless its first SQL
    /// keyword is SELECT / WITH — fail-closed before sending anything to
    /// BigQuery.
    #[serde(default = "default_read_only")]
    pub read_only: bool,

    /// Cost cap (bytes). Queries scanning more than this fail server-side
    /// without incurring a charge. Unset → the project default applies.
    #[serde(default)]
    pub maximum_bytes_billed: Option<u64>,

    /// Per-call ceiling (ms) on the whole REST round-trip (default 60 s). Also
    /// passed to BigQuery as the `jobs.query` timeout so the server waits for
    /// the result rather than returning an incomplete job.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    /// Client-side cap on returned rows (default 10000). Extra rows set the
    /// envelope `truncated` flag.
    #[serde(default = "default_max_rows")]
    pub max_rows: usize,

    /// When true, the statement is sent to BigQuery as a dry run (`dryRun=true`):
    /// the query is validated and planned but NOT executed, so it scans no data
    /// and incurs no charge. The binding returns a cost-estimate envelope
    /// (`totalBytesProcessed`, `cacheHit`, the result schema) instead of rows —
    /// letting an agent size a query before paying to run it. Default false.
    #[serde(default)]
    pub dry_run: bool,

    /// On-demand price per TiB scanned (USD), used to derive an estimated cost
    /// from the dry-run `totalBytesProcessed`. Only consulted when
    /// `dry_run = true`; unset → the estimate carries bytes only (no cost). The
    /// figure is operator-supplied because BigQuery on-demand pricing varies by
    /// region and edition and is not returned by the API.
    #[serde(default)]
    pub price_per_tib_usd: Option<f64>,
}

impl Default for BigQueryQueryConfig {
    fn default() -> Self {
        Self {
            use_legacy_sql: default_use_legacy_sql(),
            read_only: default_read_only(),
            maximum_bytes_billed: None,
            timeout_ms: default_timeout_ms(),
            max_rows: default_max_rows(),
            dry_run: false,
            price_per_tib_usd: None,
        }
    }
}

fn default_use_legacy_sql() -> bool {
    false
}
fn default_read_only() -> bool {
    true
}
fn default_timeout_ms() -> u64 {
    60_000
}
fn default_max_rows() -> usize {
    10_000
}

/// Operator-facing spec the gateway serializes when calling `register_profile`.
/// Mirrors `BigQueryBackendConfig` in the gateway crate.
// NOTE: intentionally NOT #[serde(deny_unknown_fields)] — the gateway injects
// the reserved `__mcpg_secret_refs` hint key into this spec at register_profile
// (secret-rotation scoping); denying unknown fields would reject it. The
// operator-facing schema is closed on the gateway-side *BackendConfig instead.
#[derive(Debug, Clone, Deserialize)]
pub struct BigQueryBackendSpec {
    /// GCP project the queries bill to and run in. Operator-configured (never
    /// caller-templated), so there is no SSRF / arg-injection vector.
    pub project_id: String,

    /// Default dataset for unqualified table names in the statement.
    #[serde(default)]
    pub dataset: Option<String>,

    /// Geographic location the job runs in (e.g. `EU`, `US`, `europe-west1`).
    #[serde(default)]
    pub location: Option<String>,

    /// Auth block (service-account key JSON).
    pub auth: BigQueryAuth,

    /// Query-execution bounds (read-only guard, cost cap, timeout, max rows). A
    /// bare `query:` or an omitted block applies all defaults.
    #[serde(default)]
    pub query: BigQueryQueryConfig,

    /// The operator-fixed Standard-SQL statement to run. Caller arguments are
    /// NOT templated into it; instead, `params[i]` binds the i-th `?` positional
    /// placeholder as a BigQuery query parameter (injection-safe — the statement
    /// text stays operator-fixed). Required for a tool/prompt binding; a
    /// `resource_templates[]` binding may instead supply only `read_query` (the
    /// per-`{id}` single-row read) and omit `statement`.
    #[serde(default)]
    pub statement: String,

    /// Ordered CEL expressions; `params[i]` → the i-th `?` placeholder in the
    /// statement. Each is evaluated against the call arguments (`arguments.*`)
    /// and bound as a positional BigQuery query parameter (`parameter_mode:
    /// POSITIONAL`) — never interpolated, so caller input cannot alter the
    /// query. Scalars only (BOOL / INT64 / FLOAT64 / STRING / typed NULL);
    /// arrays/objects are rejected.
    #[serde(default)]
    pub params: Vec<String>,

    /// MCP surface this binding serves. `tool` (default) emits the unchanged
    /// tool envelope; `resource` reshapes successful rows into the
    /// `resources/read` `{contents:[…]}` body; `prompt` reshapes them into the
    /// `prompts/get` `{messages:[…]}` body. Set to match the capability list the
    /// binding is placed under (`resources[]` / `prompts[]`).
    #[serde(default)]
    pub surface: crate::surface::Surface,

    /// Optional static resource URI for `surface: resource`. When set it is used
    /// verbatim as the emitted content `uri`; when omitted the binding uses the
    /// requested URI the gateway passes in the call arguments (`uri`). Ignored
    /// for `tool` / `prompt` surfaces.
    #[serde(default)]
    pub uri: Option<String>,

    /// Optional listing statement for `resources/list`. On a
    /// `surface: resource` binding this operator-fixed Standard-SQL SELECT runs
    /// at list time to enumerate concrete resource URIs (one `uri` column,
    /// optional `name` / `description` / `mime_type`). The binding has no
    /// positional bind protocol for caller input, so the statement runs verbatim
    /// — pagination is applied client-side over the full result by `page_size`.
    /// Empty → the binding returns no dynamic listing (the trait default).
    #[serde(default)]
    pub list_query: Option<ListQueryConfig>,

    /// Optional per-template-variable completion config for
    /// `completion/complete`. Keyed by the URI template variable name; each
    /// entry is an operator-fixed SELECT whose single `?` placeholder is bound
    /// to the caller-typed prefix as a BigQuery STRING query parameter (never
    /// interpolated — injection-safe). Empty → no completion candidates (the
    /// trait default).
    #[serde(default)]
    pub variable_completions: std::collections::BTreeMap<String, CompletionConfig>,

    /// Optional per-`{id}` single-row read statement for a `resource_templates[]`
    /// binding (`surface: resource` with a `uri_template` like
    /// `bigquery://orders/{id}`). On a `resources/read` of a concrete URI the
    /// gateway extracts the template variables and supplies them in the call
    /// arguments (each `{var}` as `arguments.<var>`); this statement's `?`
    /// placeholders are bound from the binding's `params` CEL expressions
    /// (`arguments.<var>`), so the extracted value binds SERVER-SIDE as a
    /// positional BigQuery query parameter — never interpolated into SQL
    /// (injection-safe). When omitted the resource-read branch falls back to
    /// `statement`. Operator-fixed; required to be read-only under the read-only
    /// guard.
    #[serde(default)]
    pub read_query: Option<String>,
}

/// Operator-fixed completion query for one template variable.
///
/// The statement is operator-fixed; the only caller-derived input is the typed
/// `prefix`, bound as a positional BigQuery STRING query parameter at the single
/// `?` placeholder — never interpolated.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct CompletionConfig {
    /// SELECT returning candidate values in its first column. MUST reference a
    /// single `?` placeholder — bound to the caller-typed prefix at call time
    /// (e.g. `SELECT name FROM repos WHERE STARTS_WITH(name, ?)`).
    pub sql: String,
    /// Optional cap on returned candidates; defaults to 100.
    #[serde(default)]
    pub max_results: Option<u32>,
}

/// Operator-fixed listing statement + client-side page bound for
/// `resources/list`.
///
/// BigQuery query parameters are a deferred follow-on, so the statement is run
/// verbatim (no caller-derived value reaches the SQL) and the page is taken
/// client-side: the opaque cursor is the integer offset into the full result.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ListQueryConfig {
    /// SELECT returning one row per resource. Required column: `uri`. Optional:
    /// `name`, `description`, `mime_type`. Operator-fixed — NOT templated from
    /// caller input.
    pub sql: String,
    /// Rows per page (1..=1000), applied client-side. Defaults to 100.
    #[serde(default = "default_list_page_size")]
    pub page_size: u64,
}

fn default_list_page_size() -> u64 {
    100
}

/// Fail-closed validation for an operator-fixed [`ListQueryConfig`].
pub fn validate_list_query(cfg: &ListQueryConfig) -> Result<(), String> {
    if cfg.sql.trim().is_empty() {
        return Err("list_query.sql must not be empty".into());
    }
    if cfg.page_size == 0 || cfg.page_size > 1_000 {
        return Err(format!(
            "list_query.page_size ({}) must be in 1..=1000",
            cfg.page_size
        ));
    }
    Ok(())
}

/// Validate an operator-fixed [`CompletionConfig`]: non-empty SQL referencing
/// exactly one `?` placeholder (the bound prefix).
pub fn validate_completion(name: &str, cfg: &CompletionConfig) -> Result<(), String> {
    if cfg.sql.trim().is_empty() {
        return Err(format!("variable_completions.{name}.sql must not be empty"));
    }
    if !cfg.sql.contains('?') {
        return Err(format!(
            "variable_completions.{name}.sql must reference the `?` placeholder (bound to the typed prefix)"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_mode_defaults_to_service_account() {
        assert_eq!(
            BigQueryAuthMode::default(),
            BigQueryAuthMode::ServiceAccount
        );
    }

    #[test]
    fn spec_applies_query_defaults_when_omitted() {
        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "statement": "SELECT 1 AS one",
        }))
        .unwrap();
        assert_eq!(spec.auth.mode, BigQueryAuthMode::ServiceAccount);
        assert!(!spec.query.use_legacy_sql);
        assert!(spec.query.read_only);
        assert_eq!(spec.query.timeout_ms, 60_000);
        assert_eq!(spec.query.max_rows, 10_000);
        assert!(spec.query.maximum_bytes_billed.is_none());
    }

    #[test]
    fn dry_run_defaults_off_and_parses_on() {
        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "statement": "SELECT 1 AS one",
        }))
        .unwrap();
        assert!(!spec.query.dry_run);
        assert!(spec.query.price_per_tib_usd.is_none());

        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "statement": "SELECT 1 AS one",
            "query": { "dry_run": true, "price_per_tib_usd": 6.25 },
        }))
        .unwrap();
        assert!(spec.query.dry_run);
        assert_eq!(spec.query.price_per_tib_usd, Some(6.25));
    }

    #[test]
    fn parses_query_overrides() {
        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "dataset": "analytics",
            "location": "EU",
            "auth": { "mode": "service_account", "credentials_json": "${env.BQ_KEY}" },
            "query": {
                "read_only": false,
                "timeout_ms": 5000,
                "max_rows": 50,
                "maximum_bytes_billed": 1048576,
            },
            "statement": "CREATE TABLE t (a INT64)",
        }))
        .unwrap();
        assert_eq!(spec.dataset.as_deref(), Some("analytics"));
        assert_eq!(spec.location.as_deref(), Some("EU"));
        assert!(!spec.query.read_only);
        assert_eq!(spec.query.timeout_ms, 5000);
        assert_eq!(spec.query.max_rows, 50);
        assert_eq!(spec.query.maximum_bytes_billed, Some(1_048_576));
    }

    #[test]
    fn parses_list_query() {
        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "statement": "SELECT 1 AS one",
            "surface": "resource",
            "list_query": { "sql": "SELECT uri FROM docs", "page_size": 25 },
        }))
        .unwrap();
        let lq = spec.list_query.expect("list_query");
        assert_eq!(lq.page_size, 25);
    }

    #[test]
    fn spec_parses_params() {
        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "statement": "SELECT id FROM t WHERE id = ?",
            "params": ["arguments.id"],
        }))
        .unwrap();
        assert_eq!(spec.params, vec!["arguments.id".to_owned()]);
    }

    #[test]
    fn spec_params_default_empty() {
        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "statement": "SELECT 1 AS one",
        }))
        .unwrap();
        assert!(spec.params.is_empty());
    }

    #[test]
    fn parses_variable_completions() {
        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "statement": "SELECT 1 AS one",
            "surface": "resource",
            "variable_completions": {
                "name": { "sql": "SELECT name FROM docs WHERE STARTS_WITH(name, ?)" },
            },
        }))
        .unwrap();
        assert!(spec.variable_completions.contains_key("name"));
    }

    #[test]
    fn validate_completion_requires_placeholder() {
        let mut cc = CompletionConfig {
            sql: "SELECT name FROM t WHERE STARTS_WITH(name, ?)".into(),
            max_results: None,
        };
        assert!(validate_completion("name", &cc).is_ok());
        cc.sql = "SELECT name FROM t".into();
        assert!(validate_completion("name", &cc).is_err());
        cc.sql = "   ".into();
        assert!(validate_completion("name", &cc).is_err());
    }

    #[test]
    fn parses_resource_template_read_query() {
        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "surface": "resource",
            "read_query": "SELECT * FROM dataset.orders WHERE id = ?",
            "params": ["arguments.id"],
        }))
        .unwrap();
        assert_eq!(
            spec.read_query.as_deref(),
            Some("SELECT * FROM dataset.orders WHERE id = ?")
        );
        // `statement` may be omitted when `read_query` carries the read.
        assert!(spec.statement.is_empty());
        assert_eq!(spec.params, vec!["arguments.id".to_owned()]);
    }

    #[test]
    fn read_query_defaults_to_none() {
        let spec: BigQueryBackendSpec = serde_json::from_value(serde_json::json!({
            "project_id": "my-proj",
            "auth": { "credentials_json": "{}" },
            "statement": "SELECT 1 AS one",
        }))
        .unwrap();
        assert!(spec.read_query.is_none());
    }

    #[test]
    fn validate_list_query_enforces_bounds() {
        let mut cfg = ListQueryConfig {
            sql: "SELECT uri FROM docs".into(),
            page_size: 100,
        };
        assert!(validate_list_query(&cfg).is_ok());
        cfg.page_size = 2000;
        assert!(validate_list_query(&cfg).is_err());
        cfg.page_size = 100;
        cfg.sql = "".into();
        assert!(validate_list_query(&cfg).is_err());
    }
}
