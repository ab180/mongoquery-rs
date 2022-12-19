use lazy_static::lazy_static;
use mongoquery::AsyncCustomOperator;
use mongoquery::{AsyncBaseQuerier, AsyncQuerier, QueryError};
use serde_json::{json, Value};
use std::collections::HashMap;

use futures::stream;
use futures::stream::StreamExt;

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

async fn query(query: Value, collection: Vec<&Value>) -> Vec<&Value> {
    let querier = AsyncBaseQuerier::new(&query);
    stream::iter(collection)
        .filter(|e| async { querier.evaluate(Some(e)).await.unwrap() })
        .collect()
        .await
}

async fn query_custom<'a>(
    query: Value,
    collection: Vec<&'a Value>,
    custom_ops: &HashMap<String, Box<dyn AsyncCustomOperator>>,
) -> Vec<&'a Value> {
    let querier = AsyncBaseQuerier::new(&query);
    stream::iter(collection)
        .filter(|e| async {
            querier
                .evaluate_with_custom_ops(Some(e), custom_ops)
                .await
                .unwrap()
        })
        .collect()
        .await
}

fn all() -> Vec<&'static Value> {
    vec![&FOOD, &FRUIT]
}
fn empty() -> Vec<&'static Value> {
    vec![]
}

#[tokio::test]
async fn test_simple_lookup() {
    assert_eq!(vec![&*FRUIT], query(json!({"type": "fruit"}), all()).await);
    assert_eq!(empty(), query(json!({"type": "ham"}), all()).await);
    assert_eq!(all(), query(json!({"memos.memo": "on time"}), all()).await);
    assert_eq!(
        vec![&*FRUIT],
        query(json!({"memos.by": "payment"}), all()).await
    );
    assert_eq!(
        vec![&*FOOD],
        query(json!({"memos.1.memo": "approved"}), all()).await
    );
}

#[tokio::test]
async fn test_comparison() {
    assert_eq!(
        vec![&*FOOD],
        query(json!({"qty": {"$eq": 25}}), all()).await
    );
    assert_eq!(
        vec![&*FOOD],
        query(json!({"qty": {"$gt": 20}}), all()).await
    );
    assert_eq!(all(), query(json!({"qty": {"$gte": 10}}), all()).await);
    assert_eq!(
        vec![&*FRUIT],
        query(json!({"qty": {"$lt": 20}}), all()).await
    );
    assert_eq!(
        vec![&*FRUIT],
        query(json!({"qty": {"$lte": 10}}), all()).await
    );

    assert_eq!(
        all(),
        query(json!({"ratings": {"$in": [5, 6]}}), all()).await
    );
    assert_eq!(
        vec![&*FRUIT],
        query(json!({"qty": {"$in": [10, 42]}}), all()).await
    );
    assert_eq!(
        vec![&*FOOD],
        query(json!({"qty": {"$nin": [10, 42]}}), all()).await
    );

    assert_eq!(
        vec![&*FOOD],
        query(json!({"qty": {"$ne": 10}}), all()).await
    );
}

#[tokio::test]
async fn test_element() {
    assert_eq!(all(), query(json!({"qty": {"$exists": true}}), all()).await);
    assert_eq!(
        empty(),
        query(json!({"foo": {"$exists": true}}), all()).await
    );

    let records = vec![
        json!({"a": 5, "b": 5, "c": null}),
        json!({"a": 3, "b": null, "c": 8}),
        json!({"a": null, "b": 3, "c": 9}),
        json!({"a": 1, "b": 2, "c": 3} ),
        json!({"a": 2, "c": 5}),
        json!({"a": 3, "b": 2}),
        json!({"a": 4}),
        json!({"b": 2, "c": 4}),
        json!({"b": 2}),
        json!({"c": 6}),
    ];
    let records_ref: Vec<_> = records.iter().collect();

    assert_eq!(
        records_ref[..7],
        query(json!({"a": {"$exists": true}}), records_ref.clone()).await
    );
    assert_eq!(
        vec![records_ref[4], records_ref[6], records_ref[9]],
        query(json!({"b": {"$exists": false}}), records_ref.clone()).await
    );

    assert_eq!(
        vec![records_ref[5], records_ref[6], records_ref[8]],
        query(json!({"c": {"$exists": false}}), records_ref.clone()).await
    );
}

#[tokio::test]
async fn test_custom_ops() {
    use async_trait::async_trait;

    pub struct MyCustomOperator {
        evaluatee_greater_than: i64,
    }
    #[async_trait]
    impl AsyncCustomOperator for MyCustomOperator {
        async fn evaluate(
            &self,
            evaluatee: Option<&Value>,
            _condition: &Value,
        ) -> Result<bool, QueryError> {
            if let Some(Value::Number(n)) = evaluatee {
                Ok(n.as_f64().unwrap() > self.evaluatee_greater_than as f64)
            } else {
                Ok(false)
            }
        }
    }

    let records = vec![
        json!({"a": 5, "b": 5, "c": null}),
        json!({"a": 3, "b": null, "c": 8}),
        json!({"a": null, "b": 3, "c": 9}),
        json!({"a": 1, "b": 2, "c": 3} ),
        json!({"a": 2, "c": 5}),
        json!({"a": 3, "b": 2}),
        json!({"a": 4}),
        json!({"b": 2, "c": 4}),
        json!({"b": 2}),
        json!({"c": 6}),
    ];
    let records_ref: Vec<_> = records.iter().collect();

    let mut custom_ops: HashMap<String, Box<dyn AsyncCustomOperator>> = HashMap::new();
    custom_ops.insert(
        "custom_op".to_string(),
        Box::new(MyCustomOperator {
            evaluatee_greater_than: 4,
        }),
    );
    assert_eq!(
        vec![records_ref[0]],
        query_custom(
            json!({"a": { "$custom_op": true}}),
            records_ref,
            &custom_ops
        )
        .await
    );
}
