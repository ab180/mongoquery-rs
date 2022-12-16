use crate::QueryError;
use serde_json::Value;

/// A function pointer that represents specific MongoDB Query Operator.  
///
/// See [CustomOperator::evaluate] for more details about the function signature and return value.
///
/// # Differences between StandardOperator and CustomOperator
/// While StandardOperator is a type alias for function pointer, CustomOperator is a trait.  
///
/// StandardOperator is meant for static operators that does not need to be built on the fly.
/// Hence, these operators are provided to the [Query](crate::Query) object via
/// [OperatorProvider::get_operators](crate::OperatorProvider::get_operators).  
///
/// CustomOperators, on the other hand, is meant for the operators that needs to be built dynamically.
/// One potential use case of CustomOperator is when you need an additional context to evaluate the expression.
/// Consider the following (contrived) example:
/// ```
/// use std::collections::HashMap;use serde_json::{json, Value};
/// use mongoquery::{CustomOperator, QueryError, BaseQuerier, Querier};
///
/// struct MyOperator {
///     evaluatee_greater_than: f64
/// }
/// impl CustomOperator for MyOperator {
///     fn evaluate(&self, evaluatee: Option<&Value>, _condition: &Value) -> Result<bool, QueryError> {
///         if let Some(Value::Number(n)) = evaluatee {
///             Ok(n.as_f64().unwrap() > self.evaluatee_greater_than as f64)
///         } else {
///             Ok(false)
///         }
///     }
/// }
///
/// let querier = BaseQuerier::new(&json!({"a": { "$custom_op": null}}));
/// let value = json!({"a": 5});
///
/// let mut my_op = MyOperator {
///     evaluatee_greater_than: 4.0
/// };
///
/// let mut ops: HashMap<String, Box<dyn CustomOperator>> = HashMap::new();
/// ops.insert("custom_op".to_string(), Box::new(my_op));
///
/// assert!(querier.evaluate_with_custom_ops(Some(&value), &ops));
/// ```
/// In this example, `my_op` stores an additional context (`evaluatee_greater_than`) that is
/// not present in the query.
pub type StandardOperator = fn(Option<&Value>, &Value) -> Result<bool, QueryError>;

/// A trait that represents custom operator.  
/// See [StandardOperator](crate::StandardOperator)'s documentation for differences between `StandardOperator` and `CustomOperator`.
pub trait CustomOperator {
    /// Evaluate this operator on a specified evaluatee with the condition.
    ///
    /// Each operator is passed an evaluatee (a data that this operand is operating on) and a condition (specified in the query),
    /// and is expected to return a `Result<bool, QueryError>`.  
    ///
    /// There are three possible variants of return value:  
    /// - If the return value is `Ok(true)`, then the evaluatee matches the condition specified by this operator.  
    /// - If the return value is `Ok(false)`, then the evaluatee does not match this operator's condition.  
    /// - If the return value is `Err(QueryError)`, the entire query fails.
    fn evaluate(&self, evaluatee: Option<&Value>, condition: &Value) -> Result<bool, QueryError>;
}
