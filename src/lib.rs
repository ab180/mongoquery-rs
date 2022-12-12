use serde_json::{Map, Number, Value};

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

pub fn from_value(v: &Value) -> Op {
    match v {
        Value::Null => Op::NullScalar,
        Value::Bool(b) => Op::BooleanScalar(*b),
        Value::Number(n) => Op::NumericScalar(n.clone()),
        Value::String(s) => Op::StringScalar(s.clone()),
        Value::Array(a) => Op::Sequence(a.clone()),
        Value::Object(obj) => Op::Compound(condition_from_map(obj)),
    }
}

pub fn condition_from_map(map: &Map<String, Value>) -> Vec<Condition> {
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
                op: from_value(condition),
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
                        op: from_value(condition),
                    })
                }
            }
        }
    }
    v
}

pub fn compound_condition_from_value(v: &Value) -> Vec<Op> {
    match v {
        Value::Array(vec) => vec.iter().map(from_value).collect(),
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
        let op = from_value(&v);
        println!("{:#?}", op);
    }

    #[test]
    fn test_parse_query_2() {
        let v = json!({ "status": "D"});
        let op = from_value(&v);
        println!("{:#?}", op);
    }
    #[test]
    fn test_parse_query_3() {
        let v = json!({ "$or": [ { "status": "A" }, { "qty": { "$lt": 30 } } ] });
        let op = from_value(&v);
        println!("{:#?}", op);
    }
}
