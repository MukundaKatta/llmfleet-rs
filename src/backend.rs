use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;

/// Error raised by a [`Backend`] implementation.
#[derive(Debug, Error)]
pub enum BackendError {
    /// Underlying transport / HTTP error.
    #[error("backend transport error: {0}")]
    Transport(String),
    /// Provider returned an error payload.
    #[error("provider error: {0}")]
    Provider(String),
    /// Anything else.
    #[error("backend error: {0}")]
    Other(String),
}

/// Adapt your LLM client to the dispatcher.
///
/// Implementations call the provider's sync API in [`submit_sync`] and the
/// provider's batch API (submit + poll + fetch results) in [`submit_batch`].
/// `submit_batch` is expected to be blocking from the dispatcher's view: it
/// returns when all results are ready.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Send one request synchronously.
    async fn submit_sync(&self, req: Value) -> Result<Value, BackendError>;

    /// Submit `items` as one batch and return per-`custom_id` results.
    ///
    /// `items` is a list of `(custom_id, request_params)` pairs. The returned
    /// map must contain an entry for every `custom_id` (use `Err(...)` for
    /// per-item failures).
    async fn submit_batch(
        &self,
        items: Vec<(String, Value)>,
    ) -> Result<HashMap<String, Result<Value, BackendError>>, BackendError>;
}
