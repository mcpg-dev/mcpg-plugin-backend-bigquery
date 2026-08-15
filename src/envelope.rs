//! BigQuery structured response envelope — the `BackendResponse.payload` the
//! gateway projects onto `tools/call`. A non-null `downstreamError` slot is the
//! gateway's `isError` signal (same contract as the snowflake/oracle/http
//! backends).

use serde_json::{Value, json};

/// Build a downstream-error object for the envelope's `downstreamError` slot.
pub fn bigquery_downstream_error(kind: &str, message: &str, retryable: bool) -> Value {
    json!({
        "kind": kind,
        "code": format!("mcpg.downstream_bigquery.{kind}"),
        "message": message,
        "retryable": retryable,
        "retryClass": if retryable { "with_backoff" } else { "do_not_retry" },
        "suggestedAction": if retryable { "check_bigquery_connectivity_and_retry" } else { "inspect_sql_error" },
    })
}

/// Classify a `run_query` error string. Transport-level failures (connection /
/// timeout / rate-limit / 5xx) are retryable; SQL compilation, auth, permission
/// and `maximum_bytes_billed`-exceeded failures are caller/config problems and
/// are not (an over-budget query is a cost decision, not a transient fault).
pub fn classify_error(message: &str) -> Value {
    let lower = message.to_ascii_lowercase();
    // Non-retryable markers win: a "syntax error" that happens to contain the
    // word "timeout" must still classify as a query rejection, not transport.
    let non_retryable = lower.contains("syntax")
        || lower.contains("invalid query")
        || lower.contains("not found")
        || lower.contains("does not exist")
        || lower.contains("unrecognized name")
        || lower.contains("authentication")
        || lower.contains("auth ")
        || lower.contains("invalid_grant")
        || lower.contains("credential")
        || lower.contains("unauthorized")
        || lower.contains("permission")
        || lower.contains("access denied")
        || lower.contains("forbidden")
        || lower.contains("maximum_bytes_billed")
        || lower.contains("bytesbilledlimitexceeded")
        || lower.contains("bytes billed")
        || lower.contains("read-only guard");
    let retryable = !non_retryable
        && (lower.contains("connect")
            || lower.contains("timed out")
            || lower.contains("timeout")
            || lower.contains("broken pipe")
            || lower.contains("connection reset")
            || lower.contains("eof")
            || lower.contains("dns")
            || lower.contains("429")
            || lower.contains("too many requests")
            || lower.contains("rate limit")
            || lower.contains("ratelimitexceeded")
            || lower.contains("backenderror")
            || lower.contains("500")
            || lower.contains("502")
            || lower.contains("503")
            || lower.contains("504")
            || lower.contains("service unavailable"));
    let kind = if retryable {
        "transport_error"
    } else {
        "bigquery_error"
    };
    bigquery_downstream_error(kind, message, retryable)
}

/// JSON Schema (draft 2020-12) for the fixed envelope wrapper
/// [`build_result_envelope`] produces. Describes the stable top-level
/// shape; per-query `response.rows` items are intentionally left untyped
/// (`{}`) so any row shape validates.
pub fn result_envelope_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "toolName": { "type": "string" },
            "profile": { "type": "string" },
            "request": {
                "type": "object",
                "properties": {
                    "project": { "type": "string" },
                    "dataset": { "type": ["string", "null"] }
                },
                "additionalProperties": true
            },
            "response": {
                "type": ["object", "null"],
                "properties": {
                    "rows": { "type": ["array", "null"], "items": {} },
                    "count": { "type": ["integer", "null"] },
                    "truncated": { "type": "boolean" },
                    "durationMs": { "type": "integer" }
                },
                "additionalProperties": true
            },
            "truncated": { "type": "boolean" },
            "downstreamError": { "type": ["object", "null"] },
            "downstreamErrors": { "type": "array", "items": {} },
            "error": { "type": ["string", "null"] }
        },
        "additionalProperties": true
    })
}

/// Build the BigQuery structured-content envelope returned as the
/// `BackendResponse.payload`.
#[allow(clippy::too_many_arguments)]
pub fn build_result_envelope(
    tool_name: &str,
    profile_name: &str,
    project_id: &str,
    dataset: Option<&str>,
    rows: Option<&[Value]>,
    row_count: Option<usize>,
    truncated: bool,
    duration_ms: u128,
    downstream_error: Option<&Value>,
    error: Option<&str>,
) -> Value {
    let response = if downstream_error.is_some() {
        Value::Null
    } else {
        json!({
            "rows": rows,
            "count": row_count,
            "truncated": truncated,
            "durationMs": duration_ms,
        })
    };
    json!({
        "toolName": tool_name,
        "profile": profile_name,
        "request": {
            "project": project_id,
            "dataset": dataset,
        },
        "response": response,
        "truncated": truncated,
        "downstreamError": downstream_error,
        "downstreamErrors": downstream_error
            .map(|d| vec![d.clone()])
            .unwrap_or_default(),
        "error": error,
    })
}

/// One TiB in bytes (2^40), the unit BigQuery on-demand pricing is quoted in.
const BYTES_PER_TIB: f64 = 1_099_511_627_776.0;

/// Derive the estimated on-demand cost (USD) from the dry-run byte count and an
/// operator-supplied price per TiB. `None` when either input is absent so the
/// envelope carries bytes without a fabricated cost.
pub fn estimate_cost_usd(
    total_bytes_processed: Option<u64>,
    price_per_tib_usd: Option<f64>,
) -> Option<f64> {
    match (total_bytes_processed, price_per_tib_usd) {
        (Some(bytes), Some(price)) => Some(bytes as f64 / BYTES_PER_TIB * price),
        _ => None,
    }
}

/// JSON Schema (draft 2020-12) for the dry-run estimate envelope
/// [`build_estimate_envelope`] produces — distinct from the row-returning query
/// envelope: it carries `estimate` (bytes / cacheHit / schema / optional cost)
/// and no `rows`.
pub fn estimate_envelope_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "toolName": { "type": "string" },
            "profile": { "type": "string" },
            "request": {
                "type": "object",
                "properties": {
                    "project": { "type": "string" },
                    "dataset": { "type": ["string", "null"] },
                    "dryRun": { "type": "boolean" }
                },
                "additionalProperties": true
            },
            "estimate": {
                "type": ["object", "null"],
                "properties": {
                    "totalBytesProcessed": { "type": ["integer", "null"] },
                    "estimatedCostUsd": { "type": ["number", "null"] },
                    "cacheHit": { "type": "boolean" },
                    "schema": { "type": "array", "items": {} },
                    "durationMs": { "type": "integer" }
                },
                "additionalProperties": true
            },
            "truncated": { "type": "boolean" },
            "downstreamError": { "type": ["object", "null"] },
            "downstreamErrors": { "type": "array", "items": {} },
            "error": { "type": ["string", "null"] }
        },
        "additionalProperties": true
    })
}

/// Build the dry-run cost-estimate envelope returned as the
/// `BackendResponse.payload`. Distinct from the query envelope: it carries an
/// `estimate` object (bytes / optional cost / cacheHit / schema) and no rows,
/// since a dry run executes nothing. `request.dryRun` is set so a caller can
/// tell an estimate apart from a row result.
#[allow(clippy::too_many_arguments)]
pub fn build_estimate_envelope(
    tool_name: &str,
    profile_name: &str,
    project_id: &str,
    dataset: Option<&str>,
    total_bytes_processed: Option<u64>,
    estimated_cost_usd: Option<f64>,
    cache_hit: bool,
    schema: &[Value],
    duration_ms: u128,
) -> Value {
    json!({
        "toolName": tool_name,
        "profile": profile_name,
        "request": {
            "project": project_id,
            "dataset": dataset,
            "dryRun": true,
        },
        "estimate": {
            "totalBytesProcessed": total_bytes_processed,
            "estimatedCostUsd": estimated_cost_usd,
            "cacheHit": cache_hit,
            "schema": schema,
            "durationMs": duration_ms,
        },
        "truncated": false,
        "downstreamError": Value::Null,
        "downstreamErrors": Value::Array(vec![]),
        "error": Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_failure_is_retryable_transport_error() {
        let e = classify_error("BigQuery request failed: connection refused");
        assert_eq!(e["kind"], json!("transport_error"));
        assert_eq!(e["retryable"], json!(true));
    }

    #[test]
    fn rate_limit_is_retryable() {
        let e = classify_error("BigQuery API error. Code: 429. Message: too many requests");
        assert_eq!(e["kind"], json!("transport_error"));
        assert_eq!(e["retryable"], json!(true));
    }

    #[test]
    fn sql_syntax_is_not_retryable() {
        let e =
            classify_error("BigQuery error: Syntax error: Unexpected identifier 'BOGUS' at [1:8]");
        assert_eq!(e["kind"], json!("bigquery_error"));
        assert_eq!(e["retryable"], json!(false));
    }

    #[test]
    fn auth_failure_is_not_retryable() {
        let e = classify_error("token source failed: invalid_grant: authentication failed");
        assert_eq!(e["kind"], json!("bigquery_error"));
        assert_eq!(e["retryable"], json!(false));
    }

    #[test]
    fn bytes_billed_exceeded_is_not_retryable() {
        let e = classify_error(
            "BigQuery error: Query exceeded limit for bytes billed: 1048576. (bytesBilledLimitExceeded)",
        );
        assert_eq!(e["kind"], json!("bigquery_error"));
        assert_eq!(e["retryable"], json!(false));
    }

    #[test]
    fn query_envelope_has_rows_count_and_truncated() {
        let rows = vec![json!({ "id": 1 })];
        let env = build_result_envelope(
            "u.get",
            "u.get",
            "my-proj",
            Some("analytics"),
            Some(&rows),
            Some(1),
            false,
            7,
            None,
            None,
        );
        assert_eq!(env["response"]["count"], json!(1));
        assert_eq!(env["response"]["rows"][0]["id"], json!(1));
        assert_eq!(env["response"]["truncated"], json!(false));
        assert_eq!(env["request"]["project"], json!("my-proj"));
        assert_eq!(env["request"]["dataset"], json!("analytics"));
        assert!(env["downstreamError"].is_null());
    }

    #[test]
    fn truncated_flag_propagates() {
        let rows = vec![json!({ "id": 1 })];
        let env = build_result_envelope(
            "u.get",
            "u.get",
            "proj",
            None,
            Some(&rows),
            Some(1),
            true,
            7,
            None,
            None,
        );
        assert_eq!(env["truncated"], json!(true));
        assert_eq!(env["response"]["truncated"], json!(true));
    }

    #[test]
    fn error_envelope_nulls_response() {
        let d = classify_error("BigQuery error: Syntax error");
        let env = build_result_envelope(
            "u.q",
            "u.q",
            "proj",
            None,
            None,
            None,
            false,
            2,
            Some(&d),
            Some("Syntax error"),
        );
        assert!(env["response"].is_null());
        assert_eq!(env["downstreamError"]["kind"], json!("bigquery_error"));
    }

    #[test]
    fn estimate_cost_is_bytes_over_tib_times_price() {
        // 1 TiB at $5/TiB → $5; half a TiB → $2.5.
        let one_tib = 1_099_511_627_776u64;
        assert_eq!(estimate_cost_usd(Some(one_tib), Some(5.0)), Some(5.0));
        assert_eq!(estimate_cost_usd(Some(one_tib / 2), Some(5.0)), Some(2.5));
        // No price or no bytes → no fabricated cost.
        assert_eq!(estimate_cost_usd(Some(one_tib), None), None);
        assert_eq!(estimate_cost_usd(None, Some(5.0)), None);
    }

    #[test]
    fn estimate_envelope_carries_bytes_cache_and_cost() {
        let schema = vec![json!({ "name": "day", "type": "DATE", "mode": "NULLABLE" })];
        let env = build_estimate_envelope(
            "rpt",
            "rpt",
            "my-proj",
            Some("analytics"),
            Some(1_572_864),
            Some(0.0089),
            true,
            &schema,
            42,
        );
        assert_eq!(env["request"]["dryRun"], json!(true));
        assert_eq!(env["estimate"]["totalBytesProcessed"], json!(1_572_864));
        assert_eq!(env["estimate"]["cacheHit"], json!(true));
        assert_eq!(env["estimate"]["estimatedCostUsd"], json!(0.0089));
        assert_eq!(env["estimate"]["schema"][0]["name"], json!("day"));
        // A dry run never returns rows.
        assert!(env["estimate"].get("rows").is_none());
        assert!(env["downstreamError"].is_null());
    }

    #[test]
    fn estimate_envelope_omits_cost_without_price() {
        let env =
            build_estimate_envelope("rpt", "rpt", "proj", None, Some(1024), None, false, &[], 3);
        assert!(env["estimate"]["estimatedCostUsd"].is_null());
        assert_eq!(env["estimate"]["totalBytesProcessed"], json!(1024));
    }

    #[test]
    fn estimate_schema_matches_envelope_shape() {
        let schema = estimate_envelope_schema();
        let env = build_estimate_envelope(
            "rpt",
            "rpt",
            "proj",
            Some("d"),
            Some(10),
            Some(1.0),
            false,
            &[],
            1,
        );
        let props = schema["properties"].as_object().expect("properties object");
        for key in env.as_object().expect("envelope object").keys() {
            assert!(
                props.contains_key(key),
                "estimate schema missing key `{key}`"
            );
        }
    }

    #[test]
    fn output_schema_matches_envelope_shape() {
        let schema = result_envelope_schema();
        assert_eq!(schema["type"], json!("object"));
        let rows = vec![json!({ "id": 1 })];
        let env = build_result_envelope(
            "u.get",
            "u.get",
            "my-proj",
            Some("analytics"),
            Some(&rows),
            Some(1),
            false,
            7,
            None,
            None,
        );
        let props = schema["properties"].as_object().expect("properties object");
        for key in env.as_object().expect("envelope object").keys() {
            assert!(props.contains_key(key), "schema missing key `{key}`");
        }
        assert_eq!(
            schema["properties"]["response"]["properties"]["rows"]["items"],
            json!({})
        );
    }
}
