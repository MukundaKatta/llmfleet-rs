//! Fleet-level batch dispatcher for LLM APIs.
//!
//! Pool requests from many tasks, route per-call to the provider's sync API
//! (low latency) or its batch API (50% discount), without rewriting your
//! agent loops.
//!
//! # Quick start
//!
//! ```no_run
//! use llmfleet::{Backend, BackendError, FleetDispatcher, RoutingPolicy};
//! use async_trait::async_trait;
//! use serde_json::{json, Value};
//! use std::collections::HashMap;
//!
//! struct MyBackend;
//!
//! #[async_trait]
//! impl Backend for MyBackend {
//!     async fn submit_sync(&self, req: Value) -> Result<Value, BackendError> {
//!         // call client.messages.create here
//!         Ok(json!({"echo": req}))
//!     }
//!     async fn submit_batch(
//!         &self,
//!         items: Vec<(String, Value)>,
//!     ) -> Result<HashMap<String, Result<Value, BackendError>>, BackendError> {
//!         // call client.messages.batches.create + poll + collect here
//!         Ok(items.into_iter().map(|(id, p)| (id, Ok(json!({"echo": p})))).collect())
//!     }
//! }
//!
//! # tokio_test::block_on(async {
//! let fleet = FleetDispatcher::new(MyBackend, RoutingPolicy::default());
//! let response = fleet
//!     .submit(json!({"messages": [{"role": "user", "content": "hi"}]}))
//!     .latency_budget_ms(1_000)
//!     .send()
//!     .await
//!     .unwrap();
//! fleet.shutdown().await;
//! # });
//! ```
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

mod backend;
mod dispatcher;

pub use crate::backend::{Backend, BackendError};
pub use crate::dispatcher::{
    DispatchStats, FleetDispatcher, RoutingPolicy, SubmitBuilder,
};
