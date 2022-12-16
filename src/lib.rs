//! An incomplete Rust port of Python's [mongoquery] library.
//!
//! # Example
//! ```
//! use mongoquery::{BaseQuerier, Querier};
//! use serde_json::{Value, json};
//!
//! let object = json!({
//!     "item": "journal",
//!     "qty": 25,
//!     "size": { "h": 14, "w": 21, "uom": "cm" },
//!     "status": "A"
//! });
//! let querier = BaseQuerier::new(&json!({ "item": "journal"}));
//!
//! assert!(querier.evaluate(Some(&object)).unwrap());
//! ```
//! [mongoquery]: https://github.com/kapouille/mongoquery
pub use operator::{CustomOperator, StandardOperator};
pub use query::Query;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use thiserror::Error;

mod operator;
mod query;

/// An enum that denotes possible query failure conditions.
#[derive(Error, Debug)]
pub enum QueryError {
    /// An unsupported operator was encountered during query execution.
    #[error("Unsupported operator: {operator}")]
    UnsupportedOperator { operator: String },
    /// Operator raised an error.
    #[error("Operator error: {reason} (from {operator}")]
    OperatorError { operator: String, reason: String },
}

/// A trait that provides static operators to [Querier].
pub trait OperatorProvider: Debug {
    /// A function that provides [StandardOperator]s to [Querier].  
    ///
    /// [Querier] calls this function at the start of the query execution to retrieve
    /// all the available standard operators.
    fn get_operators() -> HashMap<String, StandardOperator>;
}

/// A main interface to [mongoquery](crate).
///
///
/// Start by constructing new querier:
/// ```
/// use mongoquery::{BaseQuerier, Querier, Query};
/// use serde_json::json;
/// // BaseQuerier implements Querier
/// let querier: Query<_> = BaseQuerier::new(&json!({"a": 1}));
/// ```
/// The `Query` object returned via [Querier::new] can now be used to query against a JSON value:
/// ```
/// # use mongoquery::{BaseQuerier, Querier, Query};
/// # use serde_json::json;
/// # let querier: Query<_> = BaseQuerier::new(&json!({"a": 1}));
/// let entry = json!({"a": 1, "b": 2});
/// assert!(querier.evaluate(Some(&entry)).expect("trivial query should succeed"));
/// ```
pub trait Querier {
    /// An associated OperatorProvider that provides operators to this Querier.
    type Provider: OperatorProvider;

    /// Constructs new Query object.
    fn new(query: &Value) -> Query<Self::Provider> {
        Query::from_value(query)
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

/// Basic [OperatorProvider] that implements some common MongoDB Query Operators.
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
    fn get_operators() -> HashMap<String, StandardOperator> {
        let mut map: HashMap<String, StandardOperator> = HashMap::new();
        map.insert("exists".into(), BaseOperators::exists);
        map.insert("eq".into(), BaseOperators::eq);
        map.insert("ne".into(), BaseOperators::ne);
        map.insert("gt".into(), BaseOperators::gt);
        map.insert("gte".into(), BaseOperators::gte);
        map.insert("lt".into(), BaseOperators::lt);
        map.insert("lte".into(), BaseOperators::lte);
        map.insert("in".into(), BaseOperators::r#in);
        map.insert("nin".into(), BaseOperators::nin);
        map
    }
}

/// An Querier that uses [BaseOperators] as its operator provider.
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
