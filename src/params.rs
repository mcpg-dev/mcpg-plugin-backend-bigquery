//! CEL-computed query parameters, bound as BigQuery query parameters.
//!
//! Operators declare an ordered `params: ["<CEL>", …]` list on a binding.
//! At `register_profile` each expression is compiled once; per call each is
//! evaluated against the call's `arguments` object and the resulting value is
//! bound positionally — `params[i]` → the i-th `?` placeholder. Values cross
//! the wire as BigQuery query parameters (`parameter_mode: POSITIONAL`), never
//! interpolated into the statement text, so caller input cannot alter the
//! query (injection-safe).

use cel::{Context as CelContext, Program, Value as CelValue};
use gcloud_bigquery::http::types::{QueryParameter, QueryParameterType, QueryParameterValue};
use serde_json::Value;

/// A compiled `params` entry — the ordinal position and its CEL program.
/// `cel::Program` isn't `Clone`; the profile shares these via `Arc<[_]>`.
#[derive(Debug)]
pub struct CompiledParam {
    /// Zero-based position (bound to the i-th `?`).
    pub index: usize,
    /// Compiled CEL program — evaluated per call.
    pub program: Program,
    /// Original source, retained for diagnostics.
    pub source: String,
}

/// Compile every parameter expression. Errors name the offending position.
pub fn compile_params(exprs: &[String]) -> Result<Vec<CompiledParam>, String> {
    exprs
        .iter()
        .enumerate()
        .map(|(index, source)| {
            Program::compile(source)
                .map(|program| CompiledParam {
                    index,
                    program,
                    source: source.clone(),
                })
                .map_err(|e| format!("params[{index}] does not compile as CEL: {e}"))
        })
        .collect()
}

/// Evaluate every compiled expression against `arguments`, returning the
/// values in ordinal order.
pub fn evaluate_params(params: &[CompiledParam], arguments: &Value) -> Result<Vec<Value>, String> {
    let args_cel = json_to_cel(arguments);
    params
        .iter()
        .map(|p| {
            let mut ctx = CelContext::default();
            ctx.add_variable("arguments", args_cel.clone())
                .map_err(|e| format!("params[{}]: bind arguments: {e}", p.index))?;
            let out = p
                .program
                .execute(&ctx)
                .map_err(|e| format!("params[{}] failed: {e} (source: {})", p.index, p.source))?;
            Ok(cel_to_json(out))
        })
        .collect()
}

/// Lower one scalar JSON value into a positional BigQuery [`QueryParameter`].
///
/// BigQuery's REST `jobs.query` carries every scalar value as a STRING in
/// `parameter_value.value`; the declared `parameter_type.parameter_type`
/// (`BOOL` / `INT64` / `FLOAT64` / `STRING`) is what tells the engine how to
/// interpret it. A positional parameter has no `name` (`None`). Scalars only —
/// arrays/objects cannot bind as a scalar parameter (injection-safe); a typed
/// NULL is a `STRING`-typed parameter with `value: None`.
pub fn json_to_bq_param(value: Value) -> Result<QueryParameter, String> {
    let (type_name, value_str): (&str, Option<String>) = match value {
        Value::Null => ("STRING", None),
        Value::Bool(b) => ("BOOL", Some(if b { "true" } else { "false" }.to_owned())),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ("INT64", Some(i.to_string()))
            } else if let Some(f) = n.as_f64() {
                ("FLOAT64", Some(f.to_string()))
            } else {
                return Err("unsupported numeric parameter (out of i64/f64 range)".into());
            }
        }
        Value::String(s) => ("STRING", Some(s)),
        Value::Array(_) | Value::Object(_) => {
            return Err("array/object values cannot be bound as query parameters".into());
        }
    };
    Ok(QueryParameter {
        name: None,
        parameter_type: QueryParameterType {
            parameter_type: type_name.to_owned(),
            array_type: None,
            struct_types: None,
        },
        parameter_value: QueryParameterValue {
            value: value_str,
            array_values: None,
            struct_values: None,
        },
    })
}

/// serde_json → CEL value (recursive).
fn json_to_cel(v: &Value) -> CelValue {
    use cel::objects::{Key as CelKey, Map as CelMap};
    use std::sync::Arc;
    match v {
        Value::Null => CelValue::Null,
        Value::Bool(b) => CelValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                CelValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                CelValue::UInt(u)
            } else if let Some(f) = n.as_f64() {
                CelValue::Float(f)
            } else {
                CelValue::String(Arc::new(n.to_string()))
            }
        }
        Value::String(s) => CelValue::String(Arc::new(s.clone())),
        Value::Array(arr) => CelValue::List(Arc::new(arr.iter().map(json_to_cel).collect())),
        Value::Object(map) => {
            let mut out = std::collections::HashMap::new();
            for (k, v) in map {
                out.insert(CelKey::String(Arc::new(k.clone())), json_to_cel(v));
            }
            CelValue::Map(CelMap { map: Arc::new(out) })
        }
    }
}

/// CEL → serde_json value. Lossy for types without JSON equivalents (bytes →
/// base64; duration/timestamp → string).
fn cel_to_json(v: CelValue) -> Value {
    match v {
        CelValue::Null => Value::Null,
        CelValue::Bool(b) => Value::Bool(b),
        CelValue::Int(i) => Value::Number(i.into()),
        CelValue::UInt(u) => Value::Number(u.into()),
        CelValue::Float(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        CelValue::String(s) => Value::String(s.as_ref().clone()),
        CelValue::Bytes(b) => {
            use base64::Engine as _;
            Value::String(base64::engine::general_purpose::STANDARD.encode(b.as_ref()))
        }
        CelValue::List(items) => {
            Value::Array(items.iter().map(|v| cel_to_json(v.clone())).collect())
        }
        CelValue::Map(m) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in m.map.iter() {
                let key = match k {
                    cel::objects::Key::String(s) => s.as_ref().clone(),
                    cel::objects::Key::Int(i) => i.to_string(),
                    cel::objects::Key::Uint(u) => u.to_string(),
                    cel::objects::Key::Bool(b) => b.to_string(),
                };
                obj.insert(key, cel_to_json(v.clone()));
            }
            Value::Object(obj)
        }
        CelValue::Duration(d) => Value::String(d.to_string()),
        CelValue::Timestamp(t) => Value::String(t.to_rfc3339()),
        other => Value::String(format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compile_rejects_invalid_cel() {
        let err = compile_params(&["this is not cel (((".to_owned()]).unwrap_err();
        assert!(err.contains("params[0]"));
    }

    #[test]
    fn evaluates_positional_params_in_order() {
        let compiled =
            compile_params(&["arguments.id".to_owned(), "arguments.page * 10".to_owned()]).unwrap();
        let out = evaluate_params(&compiled, &json!({ "id": 7, "page": 3 })).unwrap();
        assert_eq!(out, vec![json!(7), json!(30)]);
    }

    #[test]
    fn evaluates_string_and_bool() {
        let compiled =
            compile_params(&["arguments.name".to_owned(), "arguments.active".to_owned()]).unwrap();
        let out = evaluate_params(&compiled, &json!({ "name": "alice", "active": true })).unwrap();
        assert_eq!(out, vec![json!("alice"), json!(true)]);
    }

    #[test]
    fn runtime_failure_reports_source() {
        let compiled = compile_params(&["arguments.missing.deeply".to_owned()]).unwrap();
        let err = evaluate_params(&compiled, &json!({})).unwrap_err();
        assert!(err.contains("params[0]") && err.contains("arguments.missing"));
    }

    #[test]
    fn bind_rejects_non_scalar() {
        let err = json_to_bq_param(json!([1, 2, 3])).unwrap_err();
        assert!(err.contains("cannot be bound"));
        let err = json_to_bq_param(json!({ "k": "v" })).unwrap_err();
        assert!(err.contains("cannot be bound"));
    }

    #[test]
    fn bind_maps_int() {
        let p = json_to_bq_param(json!(42)).unwrap();
        assert!(p.name.is_none());
        assert_eq!(p.parameter_type.parameter_type, "INT64");
        assert_eq!(p.parameter_value.value.as_deref(), Some("42"));
    }

    #[test]
    fn bind_maps_float() {
        let p = json_to_bq_param(json!(1.5)).unwrap();
        assert_eq!(p.parameter_type.parameter_type, "FLOAT64");
        assert_eq!(p.parameter_value.value.as_deref(), Some("1.5"));
    }

    #[test]
    fn bind_maps_bool() {
        let p = json_to_bq_param(json!(true)).unwrap();
        assert_eq!(p.parameter_type.parameter_type, "BOOL");
        assert_eq!(p.parameter_value.value.as_deref(), Some("true"));
        let p = json_to_bq_param(json!(false)).unwrap();
        assert_eq!(p.parameter_value.value.as_deref(), Some("false"));
    }

    #[test]
    fn bind_maps_string() {
        let p = json_to_bq_param(json!("alice")).unwrap();
        assert_eq!(p.parameter_type.parameter_type, "STRING");
        assert_eq!(p.parameter_value.value.as_deref(), Some("alice"));
    }

    #[test]
    fn bind_maps_typed_null() {
        let p = json_to_bq_param(Value::Null).unwrap();
        // A typed NULL: STRING type with no value.
        assert_eq!(p.parameter_type.parameter_type, "STRING");
        assert!(p.parameter_value.value.is_none());
    }
}
