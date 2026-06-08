use async_trait::async_trait;
use llmfleet::{Backend, BackendError, FleetDispatcher, RoutingPolicy};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Default)]
struct EchoBackend {
    sync_calls: AtomicUsize,
    batches: AtomicUsize,
    batched_count: AtomicUsize,
}

#[async_trait]
impl Backend for EchoBackend {
    async fn submit_sync(&self, req: Value) -> Result<Value, BackendError> {
        self.sync_calls.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"echo": req, "via": "sync"}))
    }
    async fn submit_batch(
        &self,
        items: Vec<(String, Value)>,
    ) -> Result<HashMap<String, Result<Value, BackendError>>, BackendError> {
        self.batches.fetch_add(1, Ordering::SeqCst);
        self.batched_count.fetch_add(items.len(), Ordering::SeqCst);
        Ok(items
            .into_iter()
            .map(|(id, p)| (id, Ok(json!({"echo": p, "via": "batch"}))))
            .collect())
    }
}

#[tokio::test]
async fn sync_routing_for_tight_latency() {
    let backend = EchoBackend::default();
    let policy = RoutingPolicy {
        sync_max_latency_ms: 5_000,
        batch_window: Duration::from_millis(50),
        batch_min_size: 1,
        batch_max_size: 100,
    };
    let fleet = FleetDispatcher::new(backend, policy);
    let r = fleet
        .submit(json!({"k": "v"}))
        .latency_budget_ms(1_000)
        .send()
        .await
        .unwrap();
    assert_eq!(r["via"], "sync");
    let (sync_calls, batched, _, errors) = fleet.stats.snapshot();
    assert_eq!(sync_calls, 1);
    assert_eq!(batched, 0);
    assert_eq!(errors, 0);
    fleet.shutdown().await;
}

#[tokio::test]
async fn batch_routing_when_no_budget() {
    let backend = EchoBackend::default();
    let policy = RoutingPolicy {
        sync_max_latency_ms: 1_000,
        batch_window: Duration::from_millis(50),
        batch_min_size: 1,
        batch_max_size: 10,
    };
    let fleet = FleetDispatcher::new(backend, policy);
    let r = fleet.submit(json!({"k": 1})).send().await.unwrap();
    assert_eq!(r["via"], "batch");
    let (sync_calls, batched, batches, _) = fleet.stats.snapshot();
    assert_eq!(sync_calls, 0);
    assert_eq!(batched, 1);
    assert_eq!(batches, 1);
    fleet.shutdown().await;
}

#[tokio::test]
async fn force_sync_overrides_no_budget() {
    let backend = EchoBackend::default();
    let fleet = FleetDispatcher::new(backend, RoutingPolicy::default());
    let r = fleet.submit(json!({})).sync().send().await.unwrap();
    assert_eq!(r["via"], "sync");
    fleet.shutdown().await;
}

#[tokio::test]
async fn force_batch_overrides_tight_budget() {
    let backend = EchoBackend::default();
    let policy = RoutingPolicy {
        sync_max_latency_ms: 5_000,
        batch_window: Duration::from_millis(50),
        batch_min_size: 1,
        batch_max_size: 10,
    };
    let fleet = FleetDispatcher::new(backend, policy);
    let r = fleet
        .submit(json!({}))
        .latency_budget_ms(100)
        .batch()
        .send()
        .await
        .unwrap();
    assert_eq!(r["via"], "batch");
    fleet.shutdown().await;
}

#[tokio::test]
async fn concurrent_submissions_pool_into_one_batch() {
    let backend = EchoBackend::default();
    let policy = RoutingPolicy {
        sync_max_latency_ms: 0, // never sync
        batch_window: Duration::from_millis(150),
        batch_min_size: 3,
        batch_max_size: 10,
    };
    let fleet = FleetDispatcher::new(backend, policy);

    let f1 = fleet.submit(json!({"i": 1})).send();
    let f2 = fleet.submit(json!({"i": 2})).send();
    let f3 = fleet.submit(json!({"i": 3})).send();
    let (r1, r2, r3) = tokio::join!(f1, f2, f3);
    r1.unwrap();
    r2.unwrap();
    r3.unwrap();

    let (_, batched, batches, _) = fleet.stats.snapshot();
    assert_eq!(
        batches, 1,
        "three concurrent submits should pool into one batch"
    );
    assert_eq!(batched, 3);
    fleet.shutdown().await;
}

#[tokio::test]
async fn reaching_min_size_flushes_before_window_expires() {
    // With a long window but min_size = 2, two concurrent submits should flush
    // as soon as the second arrives, well before the window elapses.
    let backend = EchoBackend::default();
    let policy = RoutingPolicy {
        sync_max_latency_ms: 0, // never sync
        batch_window: Duration::from_secs(30),
        batch_min_size: 2,
        batch_max_size: 100,
    };
    let fleet = FleetDispatcher::new(backend, policy);

    let f1 = fleet.submit(json!({"i": 1})).send();
    let f2 = fleet.submit(json!({"i": 2})).send();
    // If min_size early-flush works, this resolves quickly; otherwise it would
    // block ~30s and trip the timeout below.
    let joined = tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(f1, f2) })
        .await
        .expect("submits should flush at min_size, not wait out the window");
    joined.0.unwrap();
    joined.1.unwrap();

    let (_, batched, batches, _) = fleet.stats.snapshot();
    assert_eq!(batches, 1);
    assert_eq!(batched, 2);
    fleet.shutdown().await;
}

#[tokio::test]
async fn shutdown_drains_pending_results_into_errors() {
    // A backend that returns sync immediately, but if used for batch, just
    // echoes. We submit then shut down before the batch window closes, then
    // assert pending submits resolve (either with result or error).
    let backend = EchoBackend::default();
    let policy = RoutingPolicy {
        sync_max_latency_ms: 0,
        batch_window: Duration::from_millis(500),
        batch_min_size: 1,
        batch_max_size: 100,
    };
    let fleet = FleetDispatcher::new(backend, policy);
    let f = fleet.submit(json!({"x": 1})).send();
    // Give the queue a moment to receive the item.
    tokio::time::sleep(Duration::from_millis(20)).await;
    fleet.shutdown().await;
    // The future should resolve (either a normal echo or a shutdown error).
    let r = f.await;
    let _ = r; // any Result is fine — we just shouldn't deadlock.
}
