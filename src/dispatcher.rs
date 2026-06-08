use crate::backend::{Backend, BackendError};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Policy controlling sync vs batch routing decisions.
#[derive(Debug, Clone, Copy)]
pub struct RoutingPolicy {
    /// Latency budgets at or below this go through `submit_sync`.
    pub sync_max_latency_ms: u64,
    /// Window during which the dispatcher accumulates a batch.
    pub batch_window: Duration,
    /// Submit early once the queue has at least this many items.
    pub batch_min_size: usize,
    /// Hard cap; submit immediately when reached.
    pub batch_max_size: usize,
}

impl Default for RoutingPolicy {
    fn default() -> Self {
        Self {
            sync_max_latency_ms: 5_000,
            batch_window: Duration::from_secs(30),
            batch_min_size: 1,
            batch_max_size: 100,
        }
    }
}

impl RoutingPolicy {
    /// Cost-aware preset (alias for `default`).
    pub fn cost_aware() -> Self {
        Self::default()
    }
}

/// Counters; cheap to read across tasks.
#[derive(Debug, Default)]
pub struct DispatchStats {
    /// Calls routed synchronously.
    pub sync_calls: AtomicUsize,
    /// Calls routed via batch.
    pub batched_calls: AtomicUsize,
    /// Total batches submitted.
    pub batches_submitted: AtomicUsize,
    /// Errors observed.
    pub errors: AtomicUsize,
}

impl DispatchStats {
    /// Snapshot the counters as a tuple `(sync, batched, batches, errors)`.
    pub fn snapshot(&self) -> (usize, usize, usize, usize) {
        (
            self.sync_calls.load(Ordering::Relaxed),
            self.batched_calls.load(Ordering::Relaxed),
            self.batches_submitted.load(Ordering::Relaxed),
            self.errors.load(Ordering::Relaxed),
        )
    }
}

struct PendingItem {
    request_id: String,
    params: Value,
    tx: oneshot::Sender<Result<Value, BackendError>>,
}

enum Msg {
    Item(PendingItem),
    Shutdown,
}

/// Pools requests across tasks and dispatches via batch or sync.
pub struct FleetDispatcher<B: Backend> {
    backend: Arc<B>,
    policy: RoutingPolicy,
    /// Stats counters (sync/batched/batches/errors). Shared across tasks.
    pub stats: Arc<DispatchStats>,
    queue_tx: mpsc::UnboundedSender<Msg>,
    flusher: Mutex<Option<JoinHandle<()>>>,
}

impl<B: Backend> FleetDispatcher<B> {
    /// Spawn the dispatcher with `backend` and `policy`.
    pub fn new(backend: B, policy: RoutingPolicy) -> Arc<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<Msg>();
        let stats = Arc::new(DispatchStats::default());
        let backend = Arc::new(backend);

        // Spawn the flusher and store its handle synchronously, so a `shutdown`
        // racing right after `new` always has the handle available to await.
        let task = tokio::spawn(flush_loop(backend.clone(), rx, policy, stats.clone()));

        Arc::new(Self {
            backend,
            policy,
            stats,
            queue_tx: tx,
            flusher: Mutex::new(Some(task)),
        })
    }

    /// Begin a submission. Use the builder to set latency budget / forcing.
    pub fn submit(self: &Arc<Self>, params: Value) -> SubmitBuilder<B> {
        SubmitBuilder {
            disp: self.clone(),
            params,
            latency_budget_ms: None,
            force_sync: false,
            force_batch: false,
        }
    }

    /// Drain pending work and stop the flusher. Subsequent submits will fail.
    pub async fn shutdown(self: &Arc<Self>) {
        let _ = self.queue_tx.send(Msg::Shutdown);
        // Best-effort: wait for the flusher to exit if we have a handle.
        let handle_opt = {
            let mut g = self.flusher.lock().await;
            g.take()
        };
        if let Some(h) = handle_opt {
            let _ = h.await;
        }
    }
}

/// Builder returned by [`FleetDispatcher::submit`].
pub struct SubmitBuilder<B: Backend> {
    disp: Arc<FleetDispatcher<B>>,
    params: Value,
    latency_budget_ms: Option<u64>,
    force_sync: bool,
    force_batch: bool,
}

impl<B: Backend> SubmitBuilder<B> {
    /// Hint at a latency budget. Tighter than `policy.sync_max_latency_ms` ⇒
    /// route sync. No hint = route batch (subject to `force_*`).
    pub fn latency_budget_ms(mut self, ms: u64) -> Self {
        self.latency_budget_ms = Some(ms);
        self
    }

    /// Force synchronous routing.
    pub fn sync(mut self) -> Self {
        self.force_sync = true;
        self
    }

    /// Force batch routing.
    pub fn batch(mut self) -> Self {
        self.force_batch = true;
        self
    }

    /// Send the submission and await the response.
    pub async fn send(self) -> Result<Value, BackendError> {
        let routes_sync = self.force_sync
            || (!self.force_batch
                && match self.latency_budget_ms {
                    Some(ms) => ms <= self.disp.policy.sync_max_latency_ms,
                    None => false,
                });

        if routes_sync {
            let r = self.disp.backend.submit_sync(self.params).await;
            self.disp.stats.sync_calls.fetch_add(1, Ordering::Relaxed);
            if r.is_err() {
                self.disp.stats.errors.fetch_add(1, Ordering::Relaxed);
            }
            return r;
        }

        let (tx, rx) = oneshot::channel::<Result<Value, BackendError>>();
        let item = PendingItem {
            request_id: Uuid::new_v4().simple().to_string(),
            params: self.params,
            tx,
        };
        self.disp
            .queue_tx
            .send(Msg::Item(item))
            .map_err(|_| BackendError::Other("dispatcher closed".into()))?;
        rx.await
            .map_err(|_| BackendError::Other("dispatcher dropped result".into()))?
    }
}

async fn flush_loop<B: Backend>(
    backend: Arc<B>,
    mut rx: mpsc::UnboundedReceiver<Msg>,
    policy: RoutingPolicy,
    stats: Arc<DispatchStats>,
) {
    'outer: loop {
        // Wait for the first item or shutdown.
        let first = match rx.recv().await {
            Some(Msg::Item(i)) => i,
            Some(Msg::Shutdown) | None => break 'outer,
        };
        let mut batch: Vec<PendingItem> = vec![first];
        let deadline = tokio::time::Instant::now() + policy.batch_window;
        let mut shutdown_seen = false;

        // Fill the batch up to max size or until window expires / shutdown.
        // Flush early once we have at least `batch_min_size` items so callers
        // get their results without waiting out the whole window.
        while batch.len() < policy.batch_max_size && batch.len() < policy.batch_min_size {
            let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            if timeout.is_zero() {
                break;
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(Msg::Item(i))) => batch.push(i),
                Ok(Some(Msg::Shutdown)) | Ok(None) => {
                    shutdown_seen = true;
                    break;
                }
                Err(_) => break, // window timeout
            }
        }

        // Submit the batch.
        let items: Vec<(String, Value)> = batch
            .iter()
            .map(|p| (p.request_id.clone(), p.params.clone()))
            .collect();
        let n = batch.len();
        let result = backend.submit_batch(items).await;
        stats.batches_submitted.fetch_add(1, Ordering::Relaxed);

        match result {
            Ok(mut by_id) => {
                stats.batched_calls.fetch_add(n, Ordering::Relaxed);
                for p in batch {
                    let r = by_id
                        .remove(&p.request_id)
                        .unwrap_or_else(|| Err(BackendError::Other("no result".into())));
                    let _ = p.tx.send(r);
                }
            }
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                let msg = e.to_string();
                for p in batch {
                    let _ = p.tx.send(Err(BackendError::Provider(msg.clone())));
                }
            }
        }

        if shutdown_seen {
            break 'outer;
        }
    }

    // Drain anything left after shutdown signal.
    while let Ok(msg) = rx.try_recv() {
        if let Msg::Item(p) = msg {
            let _ =
                p.tx.send(Err(BackendError::Other("dispatcher shutdown".into())));
        }
    }
}
