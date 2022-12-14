use serde_json::{Map, Number, Value};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use thiserror::Error;

// evaluatee, condition -> bool
type OperatorFn = dyn Fn(Option<&Value>, &Value) -> bool;

#[derive(Debug)]
pub enum Op<T>
where
    T: OperatorProvider,
{
    NullScalar,
    NumericScalar(Number),
    BooleanScalar(bool),
    StringScalar(String),
    Sequence(Vec<Value>),
    Compound(Vec<Condition<T>>),
    _Marker(Infallible, PhantomData<T>),
}

#[derive(Debug)]
pub enum Condition<T>
where
    T: OperatorProvider,
{
    And(Vec<Op<T>>),
    Or(Vec<Op<T>>),
    Nor(Vec<Op<T>>),
    Not {
        op: Op<T>,
    },
    /// Condition evaluation on Field
    Field {
        field_name: String,
        op: Op<T>,
    },
    /// Non-compound operators that start with $
    Operator {
        operator: String,
        condition: Value,
    },
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Unsupported operator: {operator}")]
    UnsupportedOperator { operator: String },
}

impl<T> Op<T>
where
    T: OperatorProvider,
{
    fn from_value(v: &Value) -> Op<T> {
        match v {
            Value::Null => Op::NullScalar,
            Value::Bool(b) => Op::BooleanScalar(*b),
            Value::Number(n) => Op::NumericScalar(n.clone()),
            Value::String(s) => Op::StringScalar(s.clone()),
            Value::Array(a) => Op::Sequence(a.clone()),
            Value::Object(obj) => Op::Compound(Condition::from_map(obj)),
        }
    }

    pub fn evaluate(&self, value: Option<&Value>) -> Result<bool, QueryError> {
        self.evaluate_with_custom_ops(value, &HashMap::new())
    }
    pub fn evaluate_with_custom_ops(
        &self,
        value: Option<&Value>,
        custom_ops: &HashMap<String, &OperatorFn>,
    ) -> Result<bool, QueryError> {
        let mut ops = T::get_operators();
        for (op_name, op) in custom_ops {
            ops.insert(op_name.clone(), *op);
        }

        self.evaluate_with_ops(value, &ops)
    }
    fn evaluate_with_ops(
        &self,
        value: Option<&Value>,
        ops: &HashMap<String, &OperatorFn>,
    ) -> Result<bool, QueryError> {
        Ok(match self {
            Op::NullScalar => {
                if let Some(Value::Null) = value {
                    true
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::Null)
                } else {
                    false
                }
            }
            Op::NumericScalar(n) => {
                if let Some(Value::Number(input)) = value {
                    input == n
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::Number(n.clone()))
                } else {
                    false
                }
            }
            Op::BooleanScalar(b) => {
                if let Some(Value::Bool(input)) = value {
                    input == b
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::Bool(*b))
                } else {
                    false
                }
            }
            Op::StringScalar(s) => {
                if let Some(Value::String(input)) = value {
                    input == s
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::String(s.clone()))
                } else {
                    false
                }
            }
            Op::Sequence(seq) => {
                if let Some(Value::Array(v)) = value {
                    seq == v
                } else if let Some(v) = value {
                    seq.contains(v)
                } else {
                    false
                }
            }
            Op::Compound(compound) => {
                for cond in compound {
                    if cond.evaluate(value, ops)? == false {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            Op::_Marker(..) => unreachable!("marker variant will never be constructed"),
        })
    }
}

impl<T> Condition<T>
where
    T: OperatorProvider,
{
    fn from_map(map: &Map<String, Value>) -> Vec<Condition<T>> {
        let mut v = Vec::with_capacity(map.len());
        for (operator, condition) in map.iter() {
            match operator.as_str() {
                "$and" => {
                    v.push(Condition::And(compound_condition_from_value(condition)));
                }
                "$or" => {
                    v.push(Condition::Or(compound_condition_from_value(condition)));
                }
                "$nor" => {
                    v.push(Condition::Nor(compound_condition_from_value(condition)));
                }
                "$not" => v.push(Condition::Not {
                    op: Op::from_value(condition),
                }),
                op => {
                    if let Some(stripped) = op.strip_prefix("$") {
                        v.push(Condition::Operator {
                            operator: stripped.to_string(),
                            condition: condition.clone(),
                        })
                    } else {
                        v.push(Condition::Field {
                            field_name: op.to_string(),
                            op: Op::from_value(condition),
                        })
                    }
                }
            }
        }
        v
    }
    fn evaluate(
        &self,
        value: Option<&Value>,
        ops: &HashMap<String, &OperatorFn>,
    ) -> Result<bool, QueryError> {
        Ok(match self {
            Condition::And(operators) => {
                for op in operators {
                    if op.evaluate_with_ops(value, ops)? == false {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            Condition::Or(operators) => {
                for op in operators {
                    if op.evaluate_with_ops(value, ops)? == true {
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            Condition::Nor(operators) => {
                for op in operators {
                    if op.evaluate_with_ops(value, ops)? == true {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            Condition::Not { op } => !op.evaluate_with_ops(value, ops)?,
            Condition::Field { field_name, op } => {
                let field = extract(value, &field_name.split('.').collect::<Vec<_>>());
                op.evaluate_with_ops(field.as_ref(), ops)?
            }
            Condition::Operator {
                operator,
                condition,
            } => {
                let op = *ops
                    .get(operator)
                    .ok_or_else(|| QueryError::UnsupportedOperator {
                        operator: operator.clone(),
                    })?;
                op(value, condition)
            }
        })
    }
}

pub trait OperatorProvider: Debug {
    fn get_operators() -> HashMap<String, &'static dyn Fn(Option<&Value>, &Value) -> bool>;
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
    } else if let (Value::Number(lhs), Value::Bool(rhs)) = (lhs, rhs) {
        lhs.as_f64()?.partial_cmp(&1f64)
    } else {
        None
    }
}

#[derive(Debug)]
pub struct BaseOperators {}
impl BaseOperators {
    fn eq(evaluatee: Option<&Value>, condition: &Value) -> bool {
        evaluatee.map(|e| e == condition).unwrap_or(false)
    }
    fn gt(evaluatee: Option<&Value>, condition: &Value) -> bool {
        if let Some(evaluatee) = evaluatee {
            matches!(
                value_partial_cmp(evaluatee, condition),
                Some(Ordering::Greater)
            )
        } else {
            false
        }
    }
    fn gte(evaluatee: Option<&Value>, condition: &Value) -> bool {
        if let Some(evaluatee) = evaluatee {
            matches!(
                value_partial_cmp(evaluatee, condition),
                Some(Ordering::Greater | Ordering::Equal)
            )
        } else {
            false
        }
    }
    fn lt(evaluatee: Option<&Value>, condition: &Value) -> bool {
        if let Some(evaluatee) = evaluatee {
            matches!(
                value_partial_cmp(evaluatee, condition),
                Some(Ordering::Less)
            )
        } else {
            false
        }
    }
    fn lte(evaluatee: Option<&Value>, condition: &Value) -> bool {
        if let Some(evaluatee) = evaluatee {
            matches!(
                value_partial_cmp(evaluatee, condition),
                Some(Ordering::Less | Ordering::Equal)
            )
        } else {
            false
        }
    }
}
impl OperatorProvider for BaseOperators {
    fn get_operators() -> HashMap<String, &'static dyn Fn(Option<&Value>, &Value) -> bool> {
        let mut map: HashMap<String, &'static dyn Fn(Option<&Value>, &Value) -> bool> =
            HashMap::new();
        map.insert("eq".into(), &BaseOperators::eq);
        map.insert("gt".into(), &BaseOperators::gt);
        map.insert("gte".into(), &BaseOperators::gte);
        map.insert("lt".into(), &BaseOperators::lt);
        map.insert("lte".into(), &BaseOperators::lte);
        map
    }
}

pub struct BaseQuerier {}
impl Querier for BaseQuerier {
    type Provider = BaseOperators;
}

// TODO: maybe apply Cow?
fn extract(entry: Option<&Value>, path: &[&str]) -> Option<Value> {
    if path.is_empty() {
        return entry.map(|x| x.clone());
    }
    if let Some(value) = entry {
        match value {
            Value::Null => Some(Value::Null),
            Value::Array(arr) => {
                if let Ok(v) = i64::from_str(path[0]) {
                    // index-based indexing
                    extract(arr.get(v as usize), &path[1..])
                } else {
                    // key-based nested document parallel indexing
                    let mut v = Vec::with_capacity(arr.len());
                    for e in arr.iter() {
                        v.push(extract(Some(e), path)?);
                    }
                    Some(Value::Array(v))
                }
            }
            Value::Object(obj) => extract(obj.get(path[0]), &path[1..]),
            _ => None,
        }
    } else {
        None
    }
}
fn compound_condition_from_value<T>(v: &Value) -> Vec<Op<T>>
where
    T: OperatorProvider,
{
    match v {
        Value::Array(vec) => vec.iter().map(Op::from_value).collect(),
        _ => vec![],
    }
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
