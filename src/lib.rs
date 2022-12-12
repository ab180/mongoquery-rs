use serde_json::{Map, Number, Value};
use std::str::FromStr;

#[derive(Debug)]
pub enum Op {
    NullScalar,
    NumericScalar(Number),
    BooleanScalar(bool),
    StringScalar(String),
    Sequence(Vec<Value>),
    Compound(Vec<Condition>),
}

#[derive(Debug)]
pub enum Condition {
    And(Vec<Op>),
    Or(Vec<Op>),
    Nor(Vec<Op>),
    Not {
        op: Op,
    },
    /// Conditional evaluation on Field
    Field {
        field_name: String,
        op: Op,
    },
    /// Non-compound operators that start with $
    Operator {
        operator: String,
        condition: Value,
    },
}

impl Op {
    pub fn from_value(v: &Value) -> Op {
        match v {
            Value::Null => Op::NullScalar,
            Value::Bool(b) => Op::BooleanScalar(*b),
            Value::Number(n) => Op::NumericScalar(n.clone()),
            Value::String(s) => Op::StringScalar(s.clone()),
            Value::Array(a) => Op::Sequence(a.clone()),
            Value::Object(obj) => Op::Compound(Condition::from_map(obj)),
        }
    }

    pub fn evaluate(&self, value: &Value) -> bool {
        match self {
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
            Op::Compound(compound) => compound
                .iter()
                .fold(true, |acc, x| acc && x.evaluate(value)),
        }
    }
}

impl Condition {
    pub fn from_map(map: &Map<String, Value>) -> Vec<Condition> {
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
                    if op.starts_with("$") {
                        v.push(Condition::Operator {
                            operator: op[1..].to_string(),
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
    pub fn evaluate(&self, value: &Value) -> bool {
        match self {
            Condition::And(operators) => operators
                .iter()
                .fold(true, |acc, x| acc && x.evaluate(value)),
            Condition::Or(operators) => operators
                .iter()
                .fold(false, |acc, x| acc || x.evaluate(value)),
            Condition::Nor(operators) => operators
                .iter()
                .fold(true, |acc, x| acc && !x.evaluate(value)),
            Condition::Not { op } => !op.evaluate(value),
            Condition::Field { field_name, op } => {
                let field = extract(value, &field_name.split(".").collect::<Vec<_>>());
                if let Some(field) = field {
                    op.evaluate(&field)
                } else {
                    false
                }
            }
            Condition::Operator {
                operator,
                condition,
            } => {
                unimplemented!("operator unimplemented")
            }
        }
    }
}

fn extract(entry: &Value, path: &[&str]) -> Option<Value> {
    if path.len() == 0 {
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
fn compound_condition_from_value(v: &Value) -> Vec<Op> {
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
        let op = Op::from_value(&v);
        println!("{:#?}", op);
    }

    #[test]
    fn test_parse_query_2() {
        let v = json!({ "status": "D"});
        let op = Op::from_value(&v);
        println!("{:#?}", op);
    }
    #[test]
    fn test_parse_query_3() {
        let v = json!({ "$or": [ { "status": "A" }, { "qty": { "$lt": 30 } } ] });
        let op = Op::from_value(&v);
        println!("{:#?}", op);
    }

    #[test]
    fn test_query_match_1() {
        let doc = json!({ "item": "journal", "qty": 25, "size": { "h": 14, "w": 21, "uom": "cm" }, "status": "A" });
        let query = Op::from_value(&json!(
            {"item": "journal"}
        ));
        assert!(query.evaluate(&doc));

        let query = Op::from_value(&json!(
            {"size": {"h": 14}}
        ));
        assert!(query.evaluate(&doc));
    }

    #[test]
    fn test_query_match_empty_values() {
        let doc = json!({ "item": "journal", "qty": 25, "size": { "h": 14, "w": 21, "uom": "cm" }, "status": "A" });
        let query = Op::from_value(&json!({}));
        assert!(query.evaluate(&doc));

        let doc = Value::Null;
        let query = Op::from_value(&Value::Null);
        assert!(query.evaluate(&doc));
    }
}
