use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::str::FromStr;
use thiserror::Error;

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

    pub fn evaluate(&self, value: &Value) -> Result<bool, QueryError> {
        self.evaluate_with_custom_ops(value, &HashMap::new())
    }
    pub fn evaluate_with_custom_ops(
        &self,
        value: &Value,
        custom_ops: &HashMap<String, &dyn Fn(&Value, &Value) -> bool>,
    ) -> Result<bool, QueryError> {
        let mut ops = T::get_operators();
        for (op_name, op) in custom_ops {
            ops.insert(op_name.clone(), *op);
        }

        self.evaluate_with_ops(value, &ops)
    }
    fn evaluate_with_ops(
        &self,
        value: &Value,
        ops: &HashMap<String, &dyn Fn(&Value, &Value) -> bool>, // evaluatee, condition -> bool
    ) -> Result<bool, QueryError> {
        Ok(match self {
            Op::NullScalar => value.is_null(),
            Op::NumericScalar(n) => {
                if let Value::Number(input) = value {
                    input == n
                } else {
                    false
                }
            }
            Op::BooleanScalar(b) => {
                if let Value::Bool(input) = value {
                    input == b
                } else {
                    false
                }
            }
            Op::StringScalar(s) => {
                if let Value::String(input) = value {
                    input == s
                } else {
                    false
                }
            }
            Op::Sequence(seq) => seq.contains(value),
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
        value: &Value,
        ops: &HashMap<String, &dyn (Fn(&Value, &Value) -> bool)>,
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
                if let Some(field) = field {
                    op.evaluate_with_ops(&field, ops)?
                } else {
                    false
                }
            }
            Condition::Operator {
                operator,
                condition,
            } => {
                // TODO: do better error handling than this
                let op: &dyn Fn(&Value, &Value) -> bool =
                    *ops.get(operator)
                        .ok_or_else(|| QueryError::UnsupportedOperator {
                            operator: operator.clone(),
                        })?;
                op(value, condition)
            }
        })
    }
}

pub trait OperatorProvider: Debug {
    fn get_operators() -> HashMap<String, &'static dyn Fn(&Value, &Value) -> bool>;
}

pub trait Querier {
    type Provider: OperatorProvider;

    fn new(query: &Value) -> Op<Self::Provider> {
        Op::from_value(query)
    }
}

#[derive(Debug)]
pub struct BaseOperators {}
impl BaseOperators {
    fn eq(evaluatee: &Value, condition: &Value) -> bool {
        evaluatee == condition
    }
}
impl OperatorProvider for BaseOperators {
    fn get_operators() -> HashMap<String, &'static dyn Fn(&Value, &Value) -> bool> {
        let mut map: HashMap<String, &'static dyn Fn(&Value, &Value) -> bool> = HashMap::new();
        map.insert("eq".into(), &BaseOperators::eq);
        map
    }
}

pub struct BaseQuerier {}
impl Querier for BaseQuerier {
    type Provider = BaseOperators;
}

fn extract(entry: &Value, path: &[&str]) -> Option<Value> {
    if path.is_empty() {
        return Some(entry.to_owned());
    }
    match entry {
        Value::Null => Some(Value::Null),
        Value::Array(arr) => {
            if let Ok(v) = i64::from_str(path[0]) {
                // index-based indexing
                extract(arr.get(v as usize)?, &path[1..])
            } else {
                // key-based nested document parallel indexing
                let mut v = Vec::with_capacity(arr.len());
                for e in arr.iter() {
                    v.push(extract(e, path)?);
                }
                Some(Value::Array(v))
            }
        }
        Value::Object(obj) => extract(obj.get(path[0])?, &path[1..]),
        _ => None,
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
        assert!(query.evaluate(&doc).unwrap());

        let query = BaseQuerier::new(&json!(
            {"size": {"h": 14}}
        ));
        assert!(query.evaluate(&doc).unwrap());
    }

    #[test]
    fn test_query_match_empty_values() {
        let doc = json!({ "item": "journal", "qty": 25, "size": { "h": 14, "w": 21, "uom": "cm" }, "status": "A" });
        let query = BaseQuerier::new(&json!({}));
        assert!(query.evaluate(&doc).unwrap());

        let doc = Value::Null;
        let query = BaseQuerier::new(&Value::Null);
        assert!(query.evaluate(&doc).unwrap());
    }
}
