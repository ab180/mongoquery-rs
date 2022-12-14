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

#[test]
fn test_comparison() {
    assert_eq!(vec![&*FOOD], query(json!({"qty": {"$eq": 25}}), all()));
    assert_eq!(vec![&*FOOD], query(json!({"qty": {"$gt": 20}}), all()));
    assert_eq!(all(), query(json!({"qty": {"$gte": 10}}), all()));
    assert_eq!(vec![&*FRUIT], query(json!({"qty": {"$lt": 20}}), all()));
    assert_eq!(vec![&*FRUIT], query(json!({"qty": {"$lte": 10}}), all()));

    assert_eq!(all(), query(json!({"ratings": {"$in": [5, 6]}}), all()));
    assert_eq!(
        vec![&*FRUIT],
        query(json!({"qty": {"$in": [10, 42]}}), all())
    );
    assert_eq!(
        vec![&*FOOD],
        query(json!({"qty": {"$nin": [10, 42]}}), all())
    );

    assert_eq!(vec![&*FOOD], query(json!({"qty": {"$ne": 10}}), all()));
}
