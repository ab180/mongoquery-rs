# mongoquery-rs
An incomplete Rust port of Python's [mongoquery] library.

<hr>

## Example
```rust
use mongoquery::{BaseQuerier, Querier};
use serde_json::{Value, json};

let object = json!({
    "item": "journal",
    "qty": 25,
    "size": { "h": 14, "w": 21, "uom": "cm" },
    "status": "A"
});
let querier = BaseQuerier::new(&json!({ "item": "journal"}));

assert!(querier.evaluate(Some(&object)).unwrap());
```
[mongoquery]: https://github.com/kapouille/mongoquery
