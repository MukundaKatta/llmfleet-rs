# llmfleet

[![crates.io](https://img.shields.io/crates/v/llmfleet.svg)](https://crates.io/crates/llmfleet)
[![docs.rs](https://docs.rs/llmfleet/badge.svg)](https://docs.rs/llmfleet)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Fleet-level batch dispatcher for LLM APIs. Pool requests across tasks, route to provider Batch APIs (50% discount on Anthropic, OpenAI), keep your sync agent loops working.

```toml
[dependencies]
llmfleet = "0.1"
```

## Why

Anthropic's Batch API saves 50% on input tokens, but it's [terrible for one agent](https://eran.sandler.co.il/post/2026-04-27-batch-api-is-terrible-for-one-agent/) — single requests poll for 90–120s. The right unit of batching isn't one user's turn; it's a fleet of agents' turns pooled together by a layer the user never sees. `llmfleet` is that layer.

## Quick start

Implement [`Backend`] for your LLM client (sync + batch paths), then:

```rust,no_run
use async_trait::async_trait;
use llmfleet::{Backend, BackendError, FleetDispatcher, RoutingPolicy};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

struct AnthropicBackend; // wraps your Anthropic Rust client

#[async_trait]
impl Backend for AnthropicBackend {
    async fn submit_sync(&self, req: Value) -> Result<Value, BackendError> {
        // call client.messages.create
        unimplemented!()
    }
    async fn submit_batch(
        &self,
        items: Vec<(String, Value)>,
    ) -> Result<HashMap<String, Result<Value, BackendError>>, BackendError> {
        // call client.messages.batches.create + poll + collect results
        unimplemented!()
    }
}

# tokio_test::block_on(async {
let policy = RoutingPolicy {
    sync_max_latency_ms: 5_000,
    batch_window: Duration::from_secs(30),
    batch_min_size: 10,
    batch_max_size: 100,
};
let fleet = FleetDispatcher::new(AnthropicBackend, policy);

// User-facing chat: sync
let chat = fleet
    .submit(json!({"messages": [{"role": "user", "content": "Hi"}]}))
    .latency_budget_ms(2_000)
    .send()
    .await;

// Background grading: batch
let graded = fleet
    .submit(json!({"messages": [{"role": "user", "content": "Grade this"}]}))
    .latency_budget_ms(600_000)
    .send()
    .await;

fleet.shutdown().await;
# });
```

Concurrent `submit().send()` calls from independent tasks share batches.

## API surface

- [`FleetDispatcher::new`] spawns a background flusher.
- [`FleetDispatcher::submit`] returns a [`SubmitBuilder`] that lets you set latency budget or force routing.
- [`FleetDispatcher::shutdown`] drains pending work and stops the flusher.
- [`Backend`] is the user-implemented trait that adapts your LLM client.
- [`RoutingPolicy`] controls window/min/max/threshold.

## What it doesn't do (v0.1)

- No built-in Anthropic / OpenAI client — bring your own and implement `Backend`.
- Process-local pooling. Cross-process pooling (Redis, NATS) is not in scope.
- Doesn't try to batch tool-call turns where the tool is on the critical path; use `.sync()` for those.

## Sibling: Python `llmfleet`

Python users: same dispatcher idea, asyncio-based — see [MukundaKatta/llmfleet](https://github.com/MukundaKatta/llmfleet).

## License

MIT
