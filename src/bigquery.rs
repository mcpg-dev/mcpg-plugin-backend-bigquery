//! BigQuery REST driver glue: the lazily-built client, the async query runner,
//! and the schema-driven row→JSON marshaller.
//!
//! `gcloud-bigquery` is async (reqwest/tonic-based) and authenticates on first
//! use, so the client is built lazily on first `execute` (parsing the
//! service-account key only then) and cached per profile — `register_profile`
//! stays offline and the unit tests need no real key. The marshaller walks the
//! result `TableSchema` against the raw `jobs.query` row tuples, so it is pure
//! and is exercised by synthetic-schema unit tests (no network).

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use gcloud_bigquery::client::{Client, ClientConfig};
use gcloud_bigquery::http::job::query::QueryRequest;
use gcloud_bigquery::http::table::{TableFieldMode, TableFieldSchema, TableFieldType, TableSchema};
use gcloud_bigquery::http::tabledata::list::{Tuple, Value as BqValue};
use gcloud_bigquery::http::types::QueryParameter;
use serde_json::{Map, Value, json};

/// Outcome of a completed query: the JSON rows (capped at `max_rows`) plus
/// whether more rows existed beyond the cap.
pub struct QueryOutcome {
    pub rows: Vec<Value>,
    pub truncated: bool,
    pub row_count: usize,
}

/// Outcome of a dry-run cost estimate: the bytes BigQuery would process if the
/// query were run, whether the result would have been served from cache, and
/// the column schema BigQuery returns at plan time. A dry run executes nothing
/// and returns no rows.
pub struct EstimateOutcome {
    /// Bytes that would be processed (the on-demand billing basis). `None` when
    /// BigQuery omits the statistic (e.g. a cached/zero-scan plan).
    pub total_bytes_processed: Option<u64>,
    /// Whether the query result would have come from the query cache.
    pub cache_hit: bool,
    /// The result column schema as `[{name, type, mode}, …]`, planned but not run.
    pub schema: Vec<Value>,
}

/// Reject a statement that is not read-only, delegating to the shared hardened
/// guard. Beyond the leading-keyword allowlist it also rejects write/DDL
/// keywords anywhere (write-CTEs), `EXPLAIN ANALYZE`, and stacked statements.
/// Fail-closed: an empty statement is rejected.
pub fn enforce_read_only(statement: &str) -> Result<(), String> {
    mcpg_plugin_sdk::sql_guard::enforce_read_only(statement)
}

/// Build a BigQuery `Client` for a profile from the service-account key JSON.
/// Async because the auth token providers (HTTP + gRPC) are built here; this is
/// the lazy step run on first `execute`, never at registration. When
/// `endpoint_override` is set (emulator), the HTTP endpoint is rewritten.
pub async fn build_client(
    credentials_json: &str,
    endpoint_override: Option<&str>,
) -> Result<Client, String> {
    let redact = |e: String| mcpg_plugin_protocol::redact::redact_in_text(&e);

    let config = if let Some(endpoint) = endpoint_override {
        // Emulator: no real credentials, talk to the local HTTP endpoint. The
        // config wants `'static` strings, so hand it owned copies.
        ClientConfig::new_with_emulator(endpoint, endpoint.to_owned())
    } else {
        let creds =
            gcloud_bigquery::client::google_cloud_auth::credentials::CredentialsFile::new_from_str(
                credentials_json,
            )
            .await
            .map_err(|e| redact(format!("BigQuery credentials parse failed: {e}")))?;
        let (config, _project) = ClientConfig::new_with_credentials(creds)
            .await
            .map_err(|e| redact(format!("BigQuery client auth failed: {e}")))?;
        config
    };

    Client::new(config)
        .await
        .map_err(|e| redact(format!("BigQuery client init failed: {e}")))
}

/// Run the statement against an already-built client and marshal the result to
/// capped JSON rows. `timeout_ms` is also passed to BigQuery so `jobs.query`
/// waits for the result server-side rather than returning an incomplete job.
#[allow(clippy::too_many_arguments)]
pub async fn run_query(
    client: &Client,
    project_id: &str,
    statement: &str,
    dataset: Option<&str>,
    location: Option<&str>,
    use_legacy_sql: bool,
    maximum_bytes_billed: Option<u64>,
    timeout_ms: u64,
    max_rows: usize,
    query_parameters: Vec<QueryParameter>,
) -> Result<QueryOutcome, String> {
    // Positional (`?`) parameters require GoogleSQL with parameter_mode set;
    // omit the mode entirely when there are no binds (legacy-SQL statements
    // carry no parameters).
    let parameter_mode = if query_parameters.is_empty() {
        None
    } else {
        Some("POSITIONAL".to_owned())
    };
    let request = QueryRequest {
        query: statement.to_owned(),
        use_legacy_sql,
        // i64 cap on the wire; clamp to keep a sane request even on absurd config.
        max_results: Some(max_rows.min(i64::MAX as usize) as i64),
        timeout_ms: Some(timeout_ms.min(i64::MAX as u64) as i64),
        maximum_bytes_billed: maximum_bytes_billed.map(|b| b.min(i64::MAX as u64) as i64),
        location: location.unwrap_or_default().to_owned(),
        default_dataset: dataset.map(|d| gcloud_bigquery::http::dataset::DatasetReference {
            project_id: project_id.to_owned(),
            dataset_id: d.to_owned(),
        }),
        parameter_mode,
        query_parameters,
        ..Default::default()
    };

    let response = client
        .job()
        .query(project_id, &request)
        .await
        .map_err(|e| format!("BigQuery query failed: {e}"))?;

    // A job that did not complete within the server timeout has no rows yet; for
    // the operator-fixed analytical statements this binding runs, surface it as
    // a timeout rather than silently returning zero rows.
    if !response.job_complete {
        return Err("BigQuery query timed out: job did not complete".to_owned());
    }

    let schema = response.schema.unwrap_or(TableSchema { fields: vec![] });
    let rows = response.rows.unwrap_or_default();
    let total = response.total_rows.unwrap_or(rows.len() as i64).max(0) as usize;

    marshal_rows(&schema, &rows, total, max_rows)
}

/// Run the statement as a BigQuery dry run (`dryRun=true`): BigQuery validates
/// and plans the query but does NOT execute it, so it scans no data and incurs
/// no charge. The response carries `statistics.totalBytesProcessed` (the bytes
/// that *would* be processed), `cacheHit`, and the result schema — but no rows.
/// The CEL-bound `query_parameters` still cross the wire so the estimate
/// reflects the parameterized query the caller would actually run.
#[allow(clippy::too_many_arguments)]
pub async fn run_dry_run(
    client: &Client,
    project_id: &str,
    statement: &str,
    dataset: Option<&str>,
    location: Option<&str>,
    use_legacy_sql: bool,
    maximum_bytes_billed: Option<u64>,
    timeout_ms: u64,
    query_parameters: Vec<QueryParameter>,
) -> Result<EstimateOutcome, String> {
    let parameter_mode = if query_parameters.is_empty() {
        None
    } else {
        Some("POSITIONAL".to_owned())
    };
    let request = QueryRequest {
        query: statement.to_owned(),
        use_legacy_sql,
        // A dry run returns no rows; do not request a page of them.
        dry_run: Some(true),
        timeout_ms: Some(timeout_ms.min(i64::MAX as u64) as i64),
        maximum_bytes_billed: maximum_bytes_billed.map(|b| b.min(i64::MAX as u64) as i64),
        location: location.unwrap_or_default().to_owned(),
        default_dataset: dataset.map(|d| gcloud_bigquery::http::dataset::DatasetReference {
            project_id: project_id.to_owned(),
            dataset_id: d.to_owned(),
        }),
        parameter_mode,
        query_parameters,
        ..Default::default()
    };

    let response = client
        .job()
        .query(project_id, &request)
        .await
        .map_err(|e| format!("BigQuery dry run failed: {e}"))?;

    Ok(estimate_from_response(&response))
}

/// Project a `jobs.query` response into an [`EstimateOutcome`]. Pure (no
/// network), so the dry-run envelope shaping is unit-testable against a
/// fabricated response.
pub fn estimate_from_response(
    response: &gcloud_bigquery::http::job::query::QueryResponse,
) -> EstimateOutcome {
    let total_bytes_processed = response
        .total_bytes_processed
        .filter(|b| *b >= 0)
        .map(|b| b as u64);
    let schema = response
        .schema
        .as_ref()
        .map(|s| s.fields.iter().map(schema_field_json).collect())
        .unwrap_or_default();
    EstimateOutcome {
        total_bytes_processed,
        cache_hit: response.cache_hit.unwrap_or(false),
        schema,
    }
}

/// Render one result-schema field as `{name, type, mode}` for the estimate
/// envelope (the dry run returns a schema but no rows).
fn schema_field_json(field: &TableFieldSchema) -> Value {
    let mode = match field.mode {
        Some(TableFieldMode::Required) => "REQUIRED",
        Some(TableFieldMode::Repeated) => "REPEATED",
        _ => "NULLABLE",
    };
    json!({
        "name": field.name,
        "type": format!("{:?}", field.data_type).to_uppercase(),
        "mode": mode,
    })
}

/// Marshal raw `jobs.query` row tuples against the result schema into JSON row
/// objects keyed by column name, capped at `max_rows`.
pub fn marshal_rows(
    schema: &TableSchema,
    rows: &[Tuple],
    total: usize,
    max_rows: usize,
) -> Result<QueryOutcome, String> {
    let kept: Vec<Value> = rows
        .iter()
        .take(max_rows)
        .map(|tuple| marshal_record(&schema.fields, tuple))
        .collect();

    // More rows exist than we kept when the server reported a larger total, or
    // when the page itself held more than the cap.
    let truncated = total > kept.len() || rows.len() > kept.len();

    Ok(QueryOutcome {
        row_count: kept.len(),
        truncated,
        rows: kept,
    })
}

/// Marshal one record (`Tuple` of cells) against its field schema into a JSON
/// object keyed by field name.
fn marshal_record(fields: &[TableFieldSchema], tuple: &Tuple) -> Value {
    let mut obj = Map::with_capacity(fields.len());
    for (field, cell) in fields.iter().zip(tuple.f.iter()) {
        obj.insert(field.name.clone(), marshal_value(field, &cell.v));
    }
    Value::Object(obj)
}

/// Marshal a single cell value, typed by its field schema. REPEATED fields
/// arrive as a `Value::Array` of cells; RECORD fields as a nested `Tuple`;
/// scalars as `Value::String` (the REST wire form) decoded by the field type.
fn marshal_value(field: &TableFieldSchema, value: &BqValue) -> Value {
    // REPEATED mode: an array of element cells, each marshalled as the scalar
    // (or record) element type of this same field.
    if matches!(field.mode, Some(TableFieldMode::Repeated)) {
        if let BqValue::Array(cells) = value {
            return Value::Array(cells.iter().map(|c| marshal_element(field, &c.v)).collect());
        }
        // A repeated field with a null / non-array payload → empty array.
        if matches!(value, BqValue::Null) {
            return Value::Array(vec![]);
        }
    }
    marshal_element(field, value)
}

/// Marshal a single (non-repeated) element value by its field type.
fn marshal_element(field: &TableFieldSchema, value: &BqValue) -> Value {
    match value {
        BqValue::Null => Value::Null,
        BqValue::Struct(tuple) => {
            let nested = field.fields.as_deref().unwrap_or(&[]);
            marshal_record(nested, tuple)
        }
        // A bare array on a non-repeated field (unusual) — pass through as an
        // array of elements typed by this field.
        BqValue::Array(cells) => {
            Value::Array(cells.iter().map(|c| marshal_element(field, &c.v)).collect())
        }
        BqValue::String(s) => marshal_scalar(field.data_type.clone(), s),
    }
}

/// Decode a scalar cell string into a typed JSON value per the BigQuery field
/// type. Integers that overflow `i64` and high-precision decimals stay strings
/// to preserve fidelity; temporal / geography / json / bytes follow the
/// documented rendering.
fn marshal_scalar(data_type: TableFieldType, raw: &str) -> Value {
    use TableFieldType as T;
    match data_type {
        T::Integer | T::Int64 => raw
            .parse::<i64>()
            .map(|n| Value::Number(n.into()))
            // > i64 (or malformed) → keep the exact string.
            .unwrap_or_else(|_| Value::String(raw.to_owned())),
        T::Float | T::Float64 => match raw.parse::<f64>() {
            Ok(f) => serde_json::Number::from_f64(f)
                .map(Value::Number)
                // NaN / Inf are not valid JSON numbers → string form.
                .unwrap_or_else(|| Value::String(raw.to_owned())),
            Err(_) => Value::String(raw.to_owned()),
        },
        T::Boolean | T::Bool => match raw {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            other => Value::String(other.to_owned()),
        },
        T::Bytes => {
            // The REST wire form is already base64; re-encode the decoded bytes
            // to normalize (and to fall back gracefully on odd input).
            match BASE64.decode(raw.as_bytes()) {
                Ok(bytes) => Value::String(BASE64.encode(bytes)),
                Err(_) => Value::String(raw.to_owned()),
            }
        }
        // NUMERIC / BIGNUMERIC / DECIMAL: keep precision as a string.
        // TIMESTAMP / DATE / TIME / DATETIME / GEOGRAPHY / INTERVAL: string.
        // JSON: the cell is a JSON document string; parse it through so the
        // structured value is embedded rather than double-encoded.
        T::Json => serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_owned())),
        T::String
        | T::Numeric
        | T::Bignumeric
        | T::Decimal
        | T::Bigdecimal
        | T::Timestamp
        | T::Date
        | T::Time
        | T::Datetime
        | T::Interval
        // RECORD/STRUCT never reach here (handled as Struct above); a stray
        // scalar on a record field degrades to its string form.
        | T::Record
        | T::Struct => Value::String(raw.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcloud_bigquery::http::tabledata::list::Cell;
    use serde_json::json;

    fn field(name: &str, ty: TableFieldType) -> TableFieldSchema {
        let mut f = TableFieldSchema {
            name: name.to_owned(),
            data_type: ty,
            ..Default::default()
        };
        f.mode = Some(TableFieldMode::Nullable);
        f
    }

    fn cell_str(s: &str) -> Cell {
        Cell {
            v: BqValue::String(s.to_owned()),
        }
    }
    fn cell_null() -> Cell {
        Cell { v: BqValue::Null }
    }

    #[test]
    fn read_only_allows_select_and_with() {
        for s in [
            "SELECT 1",
            "  select * from t",
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "-- a comment\nSELECT 1",
            "/* block */ SELECT 1",
            "(SELECT 1)",
        ] {
            assert!(enforce_read_only(s).is_ok(), "should allow: {s}");
        }
    }

    #[test]
    fn read_only_rejects_writes_and_ddl() {
        for s in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "CREATE TABLE t (a INT64)",
            "DROP TABLE t",
            "MERGE INTO t USING s ON t.id = s.id",
            "   ",
            "",
            "-- only a comment",
        ] {
            assert!(enforce_read_only(s).is_err(), "should reject: {s}");
        }
    }

    #[test]
    fn read_only_delegates_to_hardened_shared_guard() {
        // The shared guard catches what the old leading-keyword-only check
        // missed: write-CTEs, EXPLAIN ANALYZE, and stacked statements.
        for s in [
            "WITH x AS (INSERT INTO t SELECT 1) SELECT * FROM x",
            "EXPLAIN ANALYZE SELECT 1",
            "SELECT 1; DROP TABLE t",
        ] {
            assert!(enforce_read_only(s).is_err(), "should reject: {s}");
        }
        assert!(enforce_read_only("SELECT 1").is_ok());
    }

    #[test]
    fn marshal_typed_row_with_null() {
        let schema = TableSchema {
            fields: vec![
                field("id", TableFieldType::Integer),
                field("name", TableFieldType::String),
                field("active", TableFieldType::Boolean),
                field("ratio", TableFieldType::Float),
            ],
        };
        let rows = vec![
            Tuple {
                f: vec![
                    cell_str("1"),
                    cell_str("alice"),
                    cell_str("true"),
                    cell_str("0.5"),
                ],
            },
            Tuple {
                f: vec![
                    cell_str("2"),
                    cell_null(),
                    cell_str("false"),
                    cell_str("1.25"),
                ],
            },
        ];

        let out = marshal_rows(&schema, &rows, 2, 10_000).unwrap();
        assert_eq!(out.row_count, 2);
        assert!(!out.truncated);
        assert_eq!(
            out.rows[0],
            json!({ "id": 1, "name": "alice", "active": true, "ratio": 0.5 })
        );
        // The null name must appear as an explicit JSON null.
        assert_eq!(out.rows[1]["id"], json!(2));
        assert!(out.rows[1].get("name").is_some());
        assert_eq!(out.rows[1]["name"], Value::Null);
        assert_eq!(out.rows[1]["active"], json!(false));
        assert_eq!(out.rows[1]["ratio"], json!(1.25));
    }

    #[test]
    fn marshal_big_int_and_numeric_stay_strings() {
        let schema = TableSchema {
            fields: vec![
                field("huge", TableFieldType::Int64),
                field("amount", TableFieldType::Numeric),
            ],
        };
        let rows = vec![Tuple {
            f: vec![
                // > i64::MAX → kept as string to preserve the exact value.
                cell_str("99999999999999999999999999"),
                cell_str("12345.6789"),
            ],
        }];
        let out = marshal_rows(&schema, &rows, 1, 10).unwrap();
        assert_eq!(out.rows[0]["huge"], json!("99999999999999999999999999"));
        assert_eq!(out.rows[0]["amount"], json!("12345.6789"));
    }

    #[test]
    fn marshal_bytes_are_base64() {
        let schema = TableSchema {
            fields: vec![field("blob", TableFieldType::Bytes)],
        };
        let encoded = BASE64.encode(b"hello");
        let rows = vec![Tuple {
            f: vec![cell_str(&encoded)],
        }];
        let out = marshal_rows(&schema, &rows, 1, 10).unwrap();
        assert_eq!(out.rows[0]["blob"], json!(encoded));
    }

    #[test]
    fn marshal_repeated_field_is_array() {
        let mut tags = field("tags", TableFieldType::String);
        tags.mode = Some(TableFieldMode::Repeated);
        let schema = TableSchema { fields: vec![tags] };
        let rows = vec![Tuple {
            f: vec![Cell {
                v: BqValue::Array(vec![cell_str("a"), cell_str("b")]),
            }],
        }];
        let out = marshal_rows(&schema, &rows, 1, 10).unwrap();
        assert_eq!(out.rows[0]["tags"], json!(["a", "b"]));
    }

    #[test]
    fn marshal_record_field_is_nested_object() {
        let mut addr = field("addr", TableFieldType::Record);
        addr.fields = Some(vec![
            field("city", TableFieldType::String),
            field("zip", TableFieldType::Integer),
        ]);
        let schema = TableSchema { fields: vec![addr] };
        let rows = vec![Tuple {
            f: vec![Cell {
                v: BqValue::Struct(Tuple {
                    f: vec![cell_str("Berlin"), cell_str("10115")],
                }),
            }],
        }];
        let out = marshal_rows(&schema, &rows, 1, 10).unwrap();
        assert_eq!(
            out.rows[0]["addr"],
            json!({ "city": "Berlin", "zip": 10115 })
        );
    }

    #[test]
    fn marshal_json_field_is_parsed() {
        let schema = TableSchema {
            fields: vec![field("doc", TableFieldType::Json)],
        };
        let rows = vec![Tuple {
            f: vec![cell_str("{\"a\":1}")],
        }];
        let out = marshal_rows(&schema, &rows, 1, 10).unwrap();
        assert_eq!(out.rows[0]["doc"], json!({ "a": 1 }));
    }

    #[test]
    fn marshal_caps_and_flags_truncated() {
        let schema = TableSchema {
            fields: vec![field("id", TableFieldType::Integer)],
        };
        let rows: Vec<Tuple> = (1..=5)
            .map(|n| Tuple {
                f: vec![cell_str(&n.to_string())],
            })
            .collect();
        let out = marshal_rows(&schema, &rows, 5, 2).unwrap();
        assert_eq!(out.row_count, 2);
        assert!(out.truncated);
        assert_eq!(out.rows[0]["id"], json!(1));
        assert_eq!(out.rows[1]["id"], json!(2));
    }

    #[test]
    fn estimate_from_dry_run_response_carries_bytes_cache_and_schema() {
        use gcloud_bigquery::http::job::query::QueryResponse;
        // Fabricate the shape BigQuery returns for `dryRun=true`: a schema +
        // statistics.totalBytesProcessed + cacheHit, and NO rows.
        let response = QueryResponse {
            schema: Some(TableSchema {
                fields: vec![
                    field("day", TableFieldType::Date),
                    field("signups", TableFieldType::Integer),
                ],
            }),
            total_bytes_processed: Some(1_572_864),
            cache_hit: Some(true),
            rows: None,
            job_complete: true,
            ..Default::default()
        };
        let est = estimate_from_response(&response);
        assert_eq!(est.total_bytes_processed, Some(1_572_864));
        assert!(est.cache_hit);
        assert_eq!(est.schema.len(), 2);
        assert_eq!(est.schema[0]["name"], json!("day"));
        assert_eq!(est.schema[0]["type"], json!("DATE"));
        assert_eq!(est.schema[1]["name"], json!("signups"));
    }

    #[test]
    fn estimate_tolerates_missing_bytes_and_cache() {
        use gcloud_bigquery::http::job::query::QueryResponse;
        let est = estimate_from_response(&QueryResponse {
            job_complete: true,
            ..Default::default()
        });
        assert_eq!(est.total_bytes_processed, None);
        assert!(!est.cache_hit);
        assert!(est.schema.is_empty());
    }

    #[test]
    fn marshal_truncated_when_server_total_exceeds_page() {
        let schema = TableSchema {
            fields: vec![field("id", TableFieldType::Integer)],
        };
        let rows = vec![Tuple {
            f: vec![cell_str("1")],
        }];
        // Page held 1 row but the server reported 100 total.
        let out = marshal_rows(&schema, &rows, 100, 10).unwrap();
        assert_eq!(out.row_count, 1);
        assert!(out.truncated);
    }
}
