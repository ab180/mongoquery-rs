use lazy_static::lazy_static;
use mongoquery_rs::{BaseQuerier, Querier};
use serde_json::{json, Value};

lazy_static! {
    pub static ref FOOD: Value = json!({
        "_id": 100,
        "type": "food",
        "item": "xyz",
        "qty": 25,
        "price": 2.5,
        "ratings": [5, 8, 9],
        "memos": [
            {"memo": "on time", "by": "shipping"},
            {"memo": "approved", "by": "billing"}
        ]
    });
    pub static ref FRUIT: Value = json!({
        "_id": 101,
        "type": "fruit",
        "item": "jkl",
        "qty": 10,
        "price": 4.25,
        "ratings": [5, 9],
        "memos": [
            {"memo": "on time", "by": "payment"},
            {"memo": "delayed", "by": "shipping"}]
    });
}

fn query(query: Value, collection: Vec<&Value>) -> Vec<&Value> {
    let querier = BaseQuerier::new(&query);
    collection
        .into_iter()
        .filter(|e| querier.evaluate(Some(e)).unwrap())
        .collect()
}

fn all() -> Vec<&'static Value> {
    vec![&FOOD, &FRUIT]
}
fn empty() -> Vec<&'static Value> {
    vec![]
}

#[test]
fn test_simple_lookup() {
    assert_eq!(vec![&*FRUIT], query(json!({"type": "fruit"}), all()));
    assert_eq!(empty(), query(json!({"type": "ham"}), all()));
    assert_eq!(all(), query(json!({"memos.memo": "on time"}), all()));
    assert_eq!(vec![&*FRUIT], query(json!({"memos.by": "payment"}), all()));
    assert_eq!(
        vec![&*FOOD],
        query(json!({"memos.1.memo": "approved"}), all())
    );
}
