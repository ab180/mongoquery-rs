use crate::QueryError;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// Async version of [CustomOperator](crate::CustomOperator)
#[async_trait]
pub trait AsyncCustomOperator: Send + Sync {
    async fn evaluate(
        &self,
        evaluatee: Option<&Value>,
        condition: &Value,
    ) -> Result<bool, QueryError>;
}

/// Helper struct used to construct operator-containing HashMap.
///
/// Use [AsyncOperatorContainer::as_ref] to convert this object to a reference of HashMap.
pub struct AsyncOperatorContainer {
    hashmap: HashMap<String, Box<dyn AsyncCustomOperator>>,
}

impl AsyncOperatorContainer {
    pub fn new() -> Self {
        Self {
            hashmap: HashMap::new(),
        }
    }

    pub fn insert<Op: AsyncCustomOperator + 'static>(&mut self, name: impl ToString, operator: Op) {
        self.hashmap.insert(name.to_string(), Box::new(operator));
    }

    pub fn to_hashmap(self) -> HashMap<String, Box<dyn AsyncCustomOperator>> {
        self.hashmap
    }
}

impl AsRef<HashMap<String, Box<dyn AsyncCustomOperator>>> for AsyncOperatorContainer {
    fn as_ref(&self) -> &HashMap<String, Box<dyn AsyncCustomOperator>> {
        &self.hashmap
    }
}

impl Default for AsyncOperatorContainer {
    fn default() -> Self {
        Self::new()
    }
}
