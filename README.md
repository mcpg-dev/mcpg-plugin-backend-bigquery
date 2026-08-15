# `mcpg-plugin-backend-bigquery`

Google BigQuery cloud-warehouse backend binding plugin for mcpg
(binding `kind: bigquery`). Runs one
**operator-fixed** Standard-SQL statement against
BigQuery over the REST jobs API (`jobs.query`) and returns the rows as JSON,
typed by the result schema.

The BigQuery complement to the `snowflake`, `sql` (Postgres / MySQL / SQLite),
`mssql` (SQL Server) and `oracle` backends — none of those drivers speak the
BigQuery REST protocol.

## How it works

One binding = one statement = one MCP tool (or resource). Per call:

1. The cached REST client is built on first use (parsing the service-account
   key only then; auth happens on the first request), then reused.
2. The `statement` runs over the BigQuery REST `jobs.query` endpoint. The
   response carries a result schema and the row tuples; each row is marshalled
   to a JSON object keyed by column name, typed by the schema. Rows are capped
   at `query.max_rows` (extra rows, or a larger server-reported total, set the
   `truncated` flag).
3. SQL / auth / permission / `maximum_bytes_billed`-exceeded failures become a
   non-retryable `downstreamError` (the gateway's `isError` signal);
   connection / timeout / rate-limit (429) / 5xx failures are marked retryable.

## Driver / runtime

The driver is [`gcloud-bigquery`](https://crates.io/crates/gcloud-bigquery)
(yoshidan/google-cloud-rust) — pure-Rust REST. Its default features
(`rustls-tls + auth + jwt-aws-lc-rs`) give **rustls** TLS over reqwest with
**aws-lc-rs**; there is **no openssl / native-tls / system library** (only
`openssl-probe` / `aws-lc-*`, which are allowed).

It is **async** (reqwest-based), so — like the snowflake / elasticsearch / LLM
backends — the cdylib bridge `block_on`s the async methods in a small 2-worker
tokio runtime (no `spawn_blocking`).

> The crate pulls a transitive reqwest/tonic/Arrow graph, so the first build is
> slow; that is expected.

## Auth

One `auth.mode` (default `service_account`):

| Mode | Secret field | Notes |
|---|---|---|
| `service_account` | `auth.credentials_json` | The GCP service-account key, as the full JSON document. |

The key resolves through the gateway secret-resolver (`${env.X}` / `vault://…`)
at config load — never plaintext in committed config. A **bare** per-caller
`cred://` is **rejected**: the connection is one service identity (per-caller
credentials are a deferred follow-on). Application-default-credentials (ADC) are
not supported in v1.

## Configuration

| Field | Type | Default | Notes |
|---|---|---|---|
| `project_id` | string (required) | — | GCP project the queries bill to and run in. Operator-configured (not caller-templated) → no SSRF vector. |
| `dataset` | string | — | Default dataset for unqualified table names. |
| `location` | string | — | Job location (e.g. `EU`, `US`, `europe-west1`). |
| `auth.mode` | `service_account` | `service_account` | Auth mechanism. |
| `auth.credentials_json` | string (required) | — | Service-account key JSON, secret-resolved. |
| `query.use_legacy_sql` | bool | `false` | Use BigQuery legacy SQL (default Standard SQL). |
| `query.read_only` | bool | `true` | When true, rejects a statement that doesn't begin with SELECT / WITH. |
| `query.maximum_bytes_billed` | int | — | Cost cap (bytes). Over-budget queries fail server-side **without incurring a charge**. Unset → project default. |
| `query.timeout_ms` | int | `60000` | Per-call ceiling on the REST round-trip; also passed to BigQuery so `jobs.query` waits for the result. |
| `query.max_rows` | int | `10000` | Client-side cap on returned rows; extra rows set `truncated`. |
| `query.dry_run` | bool | `false` | When true, the statement is sent as a dry run (`dryRun=true`): BigQuery validates + plans it but does **not** execute it (no scan, **no charge**). The binding returns a cost-**estimate** envelope (`estimate.totalBytesProcessed` / `cacheHit` / schema) instead of rows. |
| `query.price_per_tib_usd` | number | — | On-demand price per TiB scanned (USD). Only used when `dry_run = true`; with it set, the estimate envelope adds a derived `estimate.estimatedCostUsd`. Unset → bytes only. Operator-supplied (BigQuery pricing varies by region/edition and is not returned by the API). |
| `statement` | string (required) | — | The operator-fixed Standard-SQL. **Caller arguments are NOT templated into it**; bind caller values via `params` + `?` placeholders. |
| `params` | string[] | `[]` | Ordered CEL expressions over `arguments.*`; `params[i]` binds the i-th `?` placeholder as a positional BigQuery query parameter (`parameter_mode: POSITIONAL`). Scalars only (BOOL / INT64 / FLOAT64 / STRING / typed NULL); arrays/objects rejected. Injection-safe. |
| `variable_completions` | map | `{}` | Per-template-variable completion. Keyed by URI-template variable; each entry is `{ sql, max_results? }` — an operator-fixed SELECT whose single `?` is bound to the caller-typed prefix as a STRING parameter; the first column's values are returned (capped at `max_results`, default 100). |
| `read_query` | string | — | Per-`{id}` single-row read for a `resource_templates[]` binding (`surface: resource` only). On a `resources/read` of a concrete URI the gateway extracts the template variables into `arguments.<var>`; this statement's `?` placeholders are bound from `params` so the extracted value binds as a positional BigQuery query parameter (**never interpolated** — injection-safe). When set, `statement` may be omitted. Operator-fixed and read-only-guarded; a bare `cred://` is rejected. |

### As a tool

```yaml
# Bind a caller argument as a query parameter (injection-safe):
backend:
  kind: bigquery
  project_id: "my-gcp-project"
  auth: { mode: service_account, credentials_json: "${env.BIGQUERY_SA_KEY}" }
  statement: "SELECT name FROM `my-gcp-project.analytics.users` WHERE id = ?"
  params: ["arguments.id"]
```

```yaml
mcp:
  capabilities:
    tools:
      - name: analytics.daily_signups
        description: Daily signup counts for the last 30 days.
        input_schema: { type: object, properties: {} }
        backend:
          kind: bigquery
          project_id: "my-gcp-project"
          dataset: "analytics"
          location: "EU"
          auth:
            mode: service_account
            credentials_json: "${env.BIGQUERY_SA_KEY}"
          query:
            read_only: true
            max_rows: 1000
            maximum_bytes_billed: 1073741824   # 1 GiB cost cap
          statement: >
            SELECT day, count(*) AS signups
            FROM `my-gcp-project.analytics.events`
            WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY day ORDER BY day
```

### Dry-run cost estimation

Set `query.dry_run: true` to turn a binding into a **cost estimator**. BigQuery
runs the statement as a dry run (`dryRun=true`): it validates and plans the
query but does **not** execute it — nothing is scanned and **nothing is
billed**. Instead of rows, the binding returns an `estimate` envelope with the
bytes the query *would* process, whether it would hit the query cache, and the
result schema. The CEL `params` still bind into the dry run, so the estimate
reflects the exact parameterized query a caller would run.

Pair it with a sibling executing binding (one tool to *estimate*, one to *run*)
so an agent can size a query before paying to run it — a **cost guard** against
an unbounded agent query:

```yaml
mcp:
  capabilities:
    tools:
      # Estimate first — returns bytes + derived USD cost, runs nothing.
      - name: analytics.events_scan.estimate
        description: Estimate the bytes/cost of the events scan WITHOUT running it.
        input_schema: { type: object, properties: { since: { type: string } } }
        annotations: { read_only: true, open_world: false }
        backend:
          kind: bigquery
          project_id: "my-gcp-project"
          dataset: "analytics"
          auth: { mode: service_account, credentials_json: "${env.BIGQUERY_SA_KEY}" }
          query:
            read_only: true
            dry_run: true
            price_per_tib_usd: 6.25     # on-demand price (operator-supplied)
          statement: "SELECT * FROM `my-gcp-project.analytics.events` WHERE day >= ?"
          params: ["arguments.since"]
      # …and a matching executing binding (dry_run omitted) to actually run it.
```

**Estimate envelope**

```jsonc
{
  "toolName": "analytics.events_scan.estimate",
  "profile":  "analytics.events_scan.estimate",
  "request":  { "project": "my-gcp-project", "dataset": "analytics", "dryRun": true },
  "estimate": {
    "totalBytesProcessed": 1572864,         // bytes the query WOULD scan
    "estimatedCostUsd": 0.0000089,          // present only when price_per_tib_usd is set
    "cacheHit": false,
    "schema": [ { "name": "day", "type": "DATE", "mode": "NULLABLE" } ],
    "durationMs": 84
  },
  "truncated": false,
  "downstreamError": null,                  // an invalid query still surfaces here
  "downstreamErrors": [],
  "error": null
}
```

A dry-run binding advertises this estimate shape (not the row envelope) as its
`output_schema` in `tools/list`. An invalid query / auth failure degrades to the
usual `downstreamError` envelope, exactly as the executing path does.

## MCP surfaces & composition

The same binding works on every MCP surface. The surface is selected by the
capability list the binding sits under plus a `surface:` knob; composition is via
`pipeline` steps and child tools.

### As a pipeline step

Inside a `kind: pipeline` binding, a BigQuery step uses the `bigquery` step
discriminator (the same tag as the top-level binding and the registry/dispatch
kind). The backend config fields are flattened next to
`id` / `kind`; `input_transform` shapes the step's arguments from prior steps.

```yaml
      backend:
        kind: pipeline
        pipeline_timeout_ms: 60000
        steps:
          - id: report
            kind: bigquery
            project_id: "my-gcp-project"
            dataset: "analytics"
            location: "EU"
            auth: { mode: service_account, credentials_json: "${env.BIGQUERY_SA_KEY}" }
            query: { read_only: true, max_rows: 1000 }
            statement: "SELECT day, count(*) AS signups FROM `my-gcp-project.analytics.events` GROUP BY day"
            input_transform: "${arguments}"
          - id: summarize
            kind: transform
            expression: "{ 'first_day': steps.report.response.rows[0] }"
```

### As a resource

Place the binding under `mcp.capabilities.resources[]` with `surface: resource`.
Successful rows are reshaped into the `resources/read` `{contents:[…]}` body. Set
a static `uri:` or let the binding use the requested URI from the read call.

```yaml
  capabilities:
    resources:
      - name: analytics.signups
        uri: "bigquery://analytics/daily_signups"
        backend:
          kind: bigquery
          project_id: "my-gcp-project"
          dataset: "analytics"
          auth: { mode: service_account, credentials_json: "${env.BIGQUERY_SA_KEY}" }
          query: { read_only: true }
          surface: resource
          uri: "bigquery://analytics/daily_signups"
          statement: "SELECT day, count(*) AS signups FROM `my-gcp-project.analytics.events` GROUP BY day"
```

### As a resource template (per-`{id}` read)

Place the binding under `mcp.capabilities.resource_templates[]` with a
`uri_template` carrying one or more `{var}` placeholders and `surface: resource`.
On a `resources/read` of a concrete URI the gateway extracts each `{var}` into
`arguments.<var>`; the binding's `params` bind those into `read_query`'s `?`
placeholders as positional BigQuery query parameters — the extracted value binds
**server-side** and is never interpolated into the SQL (injection-safe). The
single row is returned as the `resources/read` `{contents:[{uri,text,mimeType}]}`
body keyed on the requested URI.

```yaml
  capabilities:
    resource_templates:
      - name: analytics.order
        uri_template: "bigquery://orders/{id}"
        backend:
          kind: bigquery
          project_id: "my-gcp-project"
          dataset: "analytics"
          auth: { mode: service_account, credentials_json: "${env.BIGQUERY_SA_KEY}" }
          query: { read_only: true }
          surface: resource
          read_query: "SELECT * FROM `my-gcp-project.analytics.orders` WHERE id = ?"
          params: ["arguments.id"]
```

### As a prompt

Under `mcp.capabilities.prompts[]` with `surface: prompt`, rows are reshaped into
the `prompts/get` `{messages:[…]}` body.

```yaml
  capabilities:
    prompts:
      - name: analytics.context
        backend:
          kind: bigquery
          project_id: "my-gcp-project"
          auth: { mode: service_account, credentials_json: "${env.BIGQUERY_SA_KEY}" }
          surface: prompt
          statement: "SELECT day, count(*) AS signups FROM `my-gcp-project.analytics.events` GROUP BY day"
```

### As a child tool

An LLM / generator binding can list this binding in its child-tool set, letting
the model call it during a turn. Child dispatch is governed by
`governance.child_invoke.enforce_gates` (depth cap + self-call cycle refusal
apply), so a read-only warehouse query is a safe child.

### Schemas & annotations

`output_schema` for the envelope wrapper is advertised in `tools/list`, and
`input_schema` is advertised too. Operators should mark read-only warehouse
bindings explicitly so clients treat them as side-effect-free:

```yaml
        annotations: { read_only: true, open_world: false }
```

## Response envelope

```jsonc
{
  "toolName": "analytics.daily_signups",
  "profile":  "analytics.daily_signups",
  "request":  { "project": "my-gcp-project", "dataset": "analytics" },
  "response": {
    "rows": [ { "day": "2026-06-01", "signups": 42 } ],
    "count": 1,
    "truncated": false,
    "durationMs": 312
  },
  "truncated": false,
  "downstreamError": null,    // non-null ⇒ isError:true (bigquery_error / transport_error)
  "downstreamErrors": [],
  "error": null
}
```

### Type mapping

Rows are marshalled by the result schema:

| BigQuery type | JSON |
|---|---|
| `INT64` / `INTEGER` | number (string if it overflows `i64`) |
| `FLOAT64` / `FLOAT` | number (string for NaN / Inf) |
| `BOOL` / `BOOLEAN` | bool |
| `NUMERIC` / `BIGNUMERIC` / `DECIMAL` | string (precision preserved) |
| `BYTES` | base64 string |
| `TIMESTAMP` / `DATE` / `TIME` / `DATETIME` / `INTERVAL` / `GEOGRAPHY` | string |
| `JSON` | embedded JSON value |
| `RECORD` / `STRUCT` | nested object |
| `REPEATED` (any) | array |
| `NULL` | null |

## Change-watching

A resource can subscribe to BigQuery changes through the plugin's second
entity — a **polling `watch_strategy`** (kind `bigquery_poll`). BigQuery has no
native change-push channel, so the strategy runs a cheap read-only scalar
**high-water query** (`tracking_query`) on a cadence and emits
`notifications/resources/updated` whenever that scalar advances. The first tick
only records a baseline, so a watcher never fires spuriously at startup.

Attach it under a resource's `watch:` block. The watch carries its own
connection (it is not tied to the binding's profile) plus the tracking query:

```yaml
mcp.configurations[].resources[].watch:
  type: plugin
  kind: bigquery_poll
  project_id: "my-gcp-project"
  dataset: "analytics"
  location: "EU"
  auth: { mode: service_account, credentials_json: "${env.BIGQUERY_SA_KEY}" }
  tracking_query: "SELECT MAX(updated_at) FROM `my-gcp-project.analytics.events`"
  interval_ms: 30000
```

**Watch spec fields**

| Field | Type | Default | Description |
|---|---|---|---|
| `project_id` | string | *(required)* | GCP project the tracking query bills to and runs in. Operator-fixed. |
| `dataset` | string | *(none)* | Default dataset for unqualified table names. |
| `location` | string | *(none)* | Job location (e.g. `EU`, `US`, `europe-west1`). |
| `auth` | object | *(required)* | Service-account auth block (`mode` + `credentials_json`), same shape as the binding. |
| `tracking_query` | string | *(required)* | Read-only scalar high-water query; its first-row first-column value is the cursor. |
| `interval_ms` | int | `60000` | Poll cadence (floored at 250 ms). |
| `timeout_ms` | int | `10000` | Per-tick query budget (server-side + wall-clock). |

The `tracking_query` is held to the same read-only keyword guard as the backend
`statement`; an empty or non-read-only query, a bare `cred://` credential, or an
empty credential is rejected at watch start. The client is built (and
authenticated) once at watch start, so a bad key / unreachable token source
fails the subscribe rather than retrying forever. A tick returning zero rows (or
a NULL scalar) is treated as "no change"; transient query failures are logged
and retried on the next tick.

## Security

- **Operator-fixed statement.** The SQL is fixed in config; caller arguments
  are never interpolated into it. The only caller-derived values are the CEL
  `params`, bound as positional BigQuery query parameters (`parameter_mode:
  POSITIONAL`) — there is no caller-driven SQL surface.
- **No SSRF.** `project_id` is operator-configured, never caller-templated.
- **No plaintext secrets.** The key resolves through the gateway
  secret-resolver; it is never committed.
- **Bare `cred://` not supported.** The connection is one service identity, so
  a bare per-caller `cred://` secret is rejected at config validation.
- **Read-only guard.** With `query.read_only` (default on), a statement that
  doesn't begin with SELECT / WITH is rejected fail-closed before anything is
  sent to BigQuery.
- **Cost cap.** `query.maximum_bytes_billed` is threaded into the BigQuery job
  so an over-budget query fails server-side without incurring a charge — the
  must-ship guard against an unbounded agent query.
- **Dry-run cost guard.** `query.dry_run` plans a query without executing it
  (no scan, no charge) and returns a bytes/cost estimate, so an agent can size a
  query before committing to run it.

## Build / test

```bash
nx build mcpg-plugin-backend-bigquery
nx test  mcpg-plugin-backend-bigquery                                       # unit tests (no network / credentials)
cargo test -p mcpg-plugin-backend-bigquery --features integration-tests     # live BigQuery (env-driven; skips when unset)
nx lint  mcpg-plugin-backend-bigquery
```

The integration test reads `BIGQUERY_TEST_PROJECT` /
`BIGQUERY_TEST_CREDENTIALS_JSON` / `BIGQUERY_TEST_DATASET` /
`BIGQUERY_TEST_LOCATION`; with the required ones unset it prints a skip notice
and passes as a no-op.

## Scope / deferred

- **Per-caller credentials** (per-cred connections) — v1 is one service
  identity per binding.
- **Application-default credentials (ADC).** v1 ships service-account JSON only.
- **Storage Read API.** v1 uses the REST `jobs.query` path; the high-throughput
  BigQuery Storage Read API is out of scope.
```
