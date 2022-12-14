use ops::{Op, OperatorFn};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use thiserror::Error;

mod ops;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Unsupported operator: {operator}")]
    UnsupportedOperator { operator: String },
    #[error("Operator error: {reason} (from {operator}")]
    OperatorError { operator: String, reason: String },
}

pub trait OperatorProvider: Debug {
    fn get_operators() -> HashMap<String, &'static OperatorFn>;
}

pub trait Querier {
    type Provider: OperatorProvider;

    fn new(query: &Value) -> Op<Self::Provider> {
        Op::from_value(query)
    }
}

pub fn value_partial_cmp(lhs: &Value, rhs: &Value) -> Option<Ordering> {
    if let (Value::Null, Value::Null) = (lhs, rhs) {
        Some(Ordering::Equal)
    } else if let (Value::Bool(lhs), Value::Bool(rhs)) = (lhs, rhs) {
        lhs.partial_cmp(rhs)
    } else if let (Value::Number(lhs), Value::Number(rhs)) = (lhs, rhs) {
        lhs.as_f64()?.partial_cmp(&rhs.as_f64()?)
    } else if let (Value::String(lhs), Value::String(rhs)) = (lhs, rhs) {
        lhs.partial_cmp(rhs)
    } else if let (Value::Array(lhs), Value::Array(rhs)) = (lhs, rhs) {
        lhs.len().partial_cmp(&rhs.len())
    } else if let (Value::Bool(_), Value::Number(rhs)) = (lhs, rhs) {
        (1f64).partial_cmp(&rhs.as_f64()?)
    } else if let (Value::Number(lhs), Value::Bool(_)) = (lhs, rhs) {
        lhs.as_f64()?.partial_cmp(&1f64)
    } else {
        None
    }
}

#[derive(Debug)]
pub struct BaseOperators {}
impl BaseOperators {
    fn exists(evaluatee: Option<&Value>, should_exist: &Value) -> Result<bool, QueryError> {
        if let Value::Bool(should_exist) = should_exist {
            if *should_exist {
                Ok(evaluatee.is_some())
            } else {
                Ok(evaluatee.is_none())
            }
        } else {
            Err(QueryError::OperatorError {
                operator: "exists".to_string(),
                reason: "non-boolean condition".to_string(),
            })
        }
    }
    fn eq(evaluatee: Option<&Value>, condition: &Value) -> Result<bool, QueryError> {
        Ok(evaluatee.map(|e| e == condition).unwrap_or(false))
    }
    fn ne(evaluatee: Option<&Value>, condition: &Value) -> Result<bool, QueryError> {
        Ok(!BaseOperators::eq(evaluatee, condition)?)
    }
    fn gt(evaluatee: Option<&Value>, condition: &Value) -> Result<bool, QueryError> {
        Ok(if let Some(evaluatee) = evaluatee {
            matches!(
                value_partial_cmp(evaluatee, condition),
                Some(Ordering::Greater)
            )
        } else {
            false
        })
    }
    fn gte(evaluatee: Option<&Value>, condition: &Value) -> Result<bool, QueryError> {
        Ok(if let Some(evaluatee) = evaluatee {
            matches!(
                value_partial_cmp(evaluatee, condition),
                Some(Ordering::Greater | Ordering::Equal)
            )
        } else {
            false
        })
    }
    fn lt(evaluatee: Option<&Value>, condition: &Value) -> Result<bool, QueryError> {
        Ok(if let Some(evaluatee) = evaluatee {
            matches!(
                value_partial_cmp(evaluatee, condition),
                Some(Ordering::Less)
            )
        } else {
            false
        })
    }
    fn lte(evaluatee: Option<&Value>, condition: &Value) -> Result<bool, QueryError> {
        Ok(if let Some(evaluatee) = evaluatee {
            matches!(
                value_partial_cmp(evaluatee, condition),
                Some(Ordering::Less | Ordering::Equal)
            )
        } else {
            false
        })
    }
    fn r#in(evaluatee: Option<&Value>, condition: &Value) -> Result<bool, QueryError> {
        if let Value::Array(cond) = condition {
            match evaluatee {
                Some(Value::Array(evaluatee)) => {
                    for i in cond {
                        for j in evaluatee {
                            if i == j {
                                return Ok(true);
                            }
                        }
                    }
                    return Ok(false);
                }
                Some(v) => Ok(cond.contains(v)),
                None => Ok(false),
            }
        } else {
            Err(QueryError::OperatorError {
                operator: "in".to_string(),
                reason: "condition must be a list".to_string(),
            })
        }
    }

    fn nin(evaluatee: Option<&Value>, condition: &Value) -> Result<bool, QueryError> {
        Ok(!BaseOperators::r#in(evaluatee, condition)?)
    }
}

impl OperatorProvider for BaseOperators {
    fn get_operators() -> HashMap<String, &'static OperatorFn> {
        let mut map: HashMap<String, &'static OperatorFn> = HashMap::new();
        map.insert("exists".into(), &BaseOperators::exists);
        map.insert("eq".into(), &BaseOperators::eq);
        map.insert("ne".into(), &BaseOperators::ne);
        map.insert("gt".into(), &BaseOperators::gt);
        map.insert("gte".into(), &BaseOperators::gte);
        map.insert("lt".into(), &BaseOperators::lt);
        map.insert("lte".into(), &BaseOperators::lte);
        map.insert("in".into(), &BaseOperators::r#in);
        map.insert("nin".into(), &BaseOperators::nin);
        map
    }
}

pub struct BaseQuerier {}
impl Querier for BaseQuerier {
    type Provider = BaseOperators;
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_query_1() {
        let v: Value = json!({
            "status": {
                "$in": ["A", "D"]
            },
            "qty": {
                "$lt": 30
            },
            "size": {
                "h": 14,
                "w": 21,
                "uom": "cm"
            },
            "size.uom": "in"
        });
        let op = BaseQuerier::new(&v);
        println!("{:#?}", op);
    }

    #[test]
    fn test_parse_query_2() {
        let v = json!({ "status": "D"});
        let op = BaseQuerier::new(&v);
        println!("{:#?}", op);
    }
    #[test]
    fn test_parse_query_3() {
        let v = json!({ "$or": [ { "status": "A" }, { "qty": { "$lt": 30 } } ] });
        let op = BaseQuerier::new(&v);
        println!("{:#?}", op);
    }

    #[test]
    fn test_query_match_1() {
        let doc = json!({ "item": "journal", "qty": 25, "size": { "h": 14, "w": 21, "uom": "cm" }, "status": "A" });
        let query = BaseQuerier::new(&json!(
            {"item": "journal"}
        ));
        assert!(query.evaluate(Some(&doc)).unwrap());

        let query = BaseQuerier::new(&json!(
            {"size": {"h": 14}}
        ));
        assert!(query.evaluate(Some(&doc)).unwrap());
    }

    #[test]
    fn test_query_match_empty_values() {
        let doc = json!({ "item": "journal", "qty": 25, "size": { "h": 14, "w": 21, "uom": "cm" }, "status": "A" });
        let query = BaseQuerier::new(&json!({}));
        assert!(query.evaluate(Some(&doc)).unwrap());

        let doc = Value::Null;
        let query = BaseQuerier::new(&Value::Null);
        assert!(query.evaluate(Some(&doc)).unwrap());
    }
}
