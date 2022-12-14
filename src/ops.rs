use crate::{OperatorProvider, QueryError};
use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::str::FromStr;

/// A function type that represents specific MongoDB Query Operator.  
///
/// Each operator is passed an evaluatee (a data that this operand is operating on) and a condition (specified in the query),
/// and is expected to return a `Result<bool, QueryError>`.  
///
/// There are three possible variants of return value:  
/// - If the return value is `Ok(true)`, then the evaluatee matches the condition specified by this operator.  
/// - If the return value is `Ok(false)`, then the evaluatee does not match this operator's condition.  
/// - If the return value is `Err(QueryError)`, the entire query fails.
pub type OperatorFn = dyn Fn(Option<&Value>, &Value) -> Result<bool, QueryError>;

/// An object that represents MongoDB query.
#[derive(Debug)]
pub enum Query<T>
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
    And(Vec<Query<T>>),
    Or(Vec<Query<T>>),
    Nor(Vec<Query<T>>),
    Not {
        op: Query<T>,
    },
    /// Condition evaluation on Field
    Field {
        field_name: String,
        op: Query<T>,
    },
    /// Non-compound operators that start with $
    Operator {
        operator: String,
        condition: Value,
    },
}

impl<T> Query<T>
where
    T: OperatorProvider,
{
    pub(crate) fn from_value(v: &Value) -> Query<T> {
        match v {
            Value::Null => Query::NullScalar,
            Value::Bool(b) => Query::BooleanScalar(*b),
            Value::Number(n) => Query::NumericScalar(n.clone()),
            Value::String(s) => Query::StringScalar(s.clone()),
            Value::Array(a) => Query::Sequence(a.clone()),
            Value::Object(obj) => Query::Compound(Condition::from_map(obj)),
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
            Query::NullScalar => {
                if let Some(Value::Null) = value {
                    true
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::Null)
                } else {
                    false
                }
            }
            Query::NumericScalar(n) => {
                if let Some(Value::Number(input)) = value {
                    input == n
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::Number(n.clone()))
                } else {
                    false
                }
            }
            Query::BooleanScalar(b) => {
                if let Some(Value::Bool(input)) = value {
                    input == b
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::Bool(*b))
                } else {
                    false
                }
            }
            Query::StringScalar(s) => {
                if let Some(Value::String(input)) = value {
                    input == s
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::String(s.clone()))
                } else {
                    false
                }
            }
            Query::Sequence(seq) => {
                if let Some(Value::Array(v)) = value {
                    seq == v
                } else if let Some(v) = value {
                    seq.contains(v)
                } else {
                    false
                }
            }
            Query::Compound(compound) => {
                for cond in compound {
                    if cond.evaluate(value, ops)? == false {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            Query::_Marker(..) => unreachable!("marker variant will never be constructed"),
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
                    op: Query::from_value(condition),
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
                            op: Query::from_value(condition),
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
                op(value, condition)?
            }
        })
    }
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

fn compound_condition_from_value<T>(v: &Value) -> Vec<Query<T>>
where
    T: OperatorProvider,
{
    match v {
        Value::Array(vec) => vec.iter().map(Query::from_value).collect(),
        _ => vec![],
    }
}
