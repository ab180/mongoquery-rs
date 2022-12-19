use crate::async_operator::AsyncCustomOperator;
use crate::operator::StandardOperator;
use crate::query::extract;
use crate::{OperatorProvider, QueryError};
use async_recursion::async_recursion;
use serde_json::{Map, Number, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::marker::PhantomData;

/// An async variant of [Query](crate::Query)
#[derive(Debug)]
pub enum AsyncQuery<T>
where
    T: OperatorProvider,
{
    NullScalar,
    NumericScalar(Number),
    BooleanScalar(bool),
    StringScalar(String),
    Sequence(Vec<Value>),
    Compound(Vec<AsyncCondition<T>>),
    _Marker(Infallible, PhantomData<T>),
}

#[derive(Debug)]
pub enum AsyncCondition<T>
where
    T: OperatorProvider,
{
    And(Vec<AsyncQuery<T>>),
    Or(Vec<AsyncQuery<T>>),
    Nor(Vec<AsyncQuery<T>>),
    Not {
        op: AsyncQuery<T>,
    },
    /// Condition evaluation on Field
    Field {
        field_name: String,
        op: AsyncQuery<T>,
    },
    /// Non-compound operators that start with $
    Operator {
        operator: String,
        condition: Value,
    },
}

impl<T> AsyncQuery<T>
where
    T: OperatorProvider,
{
    pub(crate) fn from_value(v: &Value) -> AsyncQuery<T> {
        match v {
            Value::Null => AsyncQuery::NullScalar,
            Value::Bool(b) => AsyncQuery::BooleanScalar(*b),
            Value::Number(n) => AsyncQuery::NumericScalar(n.clone()),
            Value::String(s) => AsyncQuery::StringScalar(s.clone()),
            Value::Array(a) => AsyncQuery::Sequence(a.clone()),
            Value::Object(obj) => AsyncQuery::Compound(AsyncCondition::from_map(obj)),
        }
    }

    /// Evaluate this query on the specified value.
    pub async fn evaluate(&self, value: Option<&Value>) -> Result<bool, QueryError> {
        self.evaluate_with_custom_ops(value, &HashMap::new()).await
    }

    pub async fn evaluate_with_custom_ops(
        &self,
        value: Option<&Value>,
        custom_ops: &HashMap<String, Box<dyn AsyncCustomOperator>>,
    ) -> Result<bool, QueryError> {
        self.evaluate_with_ops(value, &T::get_operators(), custom_ops)
            .await
    }

    async fn evaluate_with_ops(
        &self,
        value: Option<&Value>,
        std_ops: &HashMap<String, StandardOperator>,
        custom_ops: &HashMap<String, Box<dyn AsyncCustomOperator>>,
    ) -> Result<bool, QueryError> {
        Ok(match self {
            AsyncQuery::NullScalar => {
                if let Some(Value::Null) = value {
                    true
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::Null)
                } else {
                    false
                }
            }
            AsyncQuery::NumericScalar(n) => {
                if let Some(Value::Number(input)) = value {
                    input == n
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::Number(n.clone()))
                } else {
                    false
                }
            }
            AsyncQuery::BooleanScalar(b) => {
                if let Some(Value::Bool(input)) = value {
                    input == b
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::Bool(*b))
                } else {
                    false
                }
            }
            AsyncQuery::StringScalar(s) => {
                if let Some(Value::String(input)) = value {
                    input == s
                } else if let Some(Value::Array(v)) = value {
                    v.contains(&Value::String(s.clone()))
                } else {
                    false
                }
            }
            AsyncQuery::Sequence(seq) => {
                if let Some(Value::Array(v)) = value {
                    seq == v
                } else if let Some(v) = value {
                    seq.contains(v)
                } else {
                    false
                }
            }
            AsyncQuery::Compound(compound) => {
                for cond in compound {
                    if cond.evaluate(value, std_ops, custom_ops).await? == false {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            AsyncQuery::_Marker(..) => unreachable!("marker variant will never be constructed"),
        })
    }
}

impl<T> AsyncCondition<T>
where
    T: OperatorProvider,
{
    fn from_map(map: &Map<String, Value>) -> Vec<AsyncCondition<T>> {
        let mut v = Vec::with_capacity(map.len());
        for (operator, condition) in map.iter() {
            match operator.as_str() {
                "$and" => {
                    v.push(AsyncCondition::And(compound_condition_from_value(
                        condition,
                    )));
                }
                "$or" => {
                    v.push(AsyncCondition::Or(compound_condition_from_value(condition)));
                }
                "$nor" => {
                    v.push(AsyncCondition::Nor(compound_condition_from_value(
                        condition,
                    )));
                }
                "$not" => v.push(AsyncCondition::Not {
                    op: AsyncQuery::from_value(condition),
                }),
                op => {
                    if let Some(stripped) = op.strip_prefix('$') {
                        v.push(AsyncCondition::Operator {
                            operator: stripped.to_string(),
                            condition: condition.clone(),
                        })
                    } else {
                        v.push(AsyncCondition::Field {
                            field_name: op.to_string(),
                            op: AsyncQuery::from_value(condition),
                        })
                    }
                }
            }
        }
        v
    }

    #[async_recursion]
    async fn evaluate(
        &self,
        value: Option<&'async_recursion Value>,
        std_ops: &HashMap<String, StandardOperator>,
        custom_ops: &HashMap<String, Box<dyn AsyncCustomOperator>>,
    ) -> Result<bool, QueryError> {
        Ok(match self {
            AsyncCondition::And(operators) => {
                for op in operators {
                    if op.evaluate_with_ops(value, std_ops, custom_ops).await? == false {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            AsyncCondition::Or(operators) => {
                for op in operators {
                    if op.evaluate_with_ops(value, std_ops, custom_ops).await? == true {
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            AsyncCondition::Nor(operators) => {
                for op in operators {
                    if op.evaluate_with_ops(value, std_ops, custom_ops).await? == true {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            AsyncCondition::Not { op } => !op.evaluate_with_ops(value, std_ops, custom_ops).await?,
            AsyncCondition::Field { field_name, op } => {
                let field = extract(value, &field_name.split('.').collect::<Vec<_>>());
                op.evaluate_with_ops(field.as_ref(), std_ops, custom_ops)
                    .await?
            }
            AsyncCondition::Operator {
                operator,
                condition,
            } => {
                if let Some(custom_op) = custom_ops.get(operator) {
                    custom_op.evaluate(value, condition).await?
                } else if let Some(std_op) = std_ops.get(operator) {
                    std_op(value, condition)?
                } else {
                    return Err(QueryError::UnsupportedOperator {
                        operator: operator.clone(),
                    });
                }
            }
        })
    }
}

fn compound_condition_from_value<T>(v: &Value) -> Vec<AsyncQuery<T>>
where
    T: OperatorProvider,
{
    match v {
        Value::Array(vec) => vec.iter().map(AsyncQuery::from_value).collect(),
        _ => vec![],
    }
}
