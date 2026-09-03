use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tower_service::Service;

use crate::{
    diagnostics::{AccessEvent, AccessLogDecision, Diagnostics},
    logging::ACCESS_TARGET,
};

/// Per-IP token-bucket rate limiter.
///
/// Each IP gets a token bucket that refills at a constant rate.
/// A single burst up to `capacity` is allowed; sustained throughput
/// is limited to `capacity / window` requests per second.
pub struct RateLimiter {
    inner: Mutex<Inner>,
    capacity: u64,
    refill_rate: f64,
    window: Duration,
}

/// Maximum number of distinct IPs tracked before evicting the stalest.
const MAX_TRACKED_IPS: usize = 10_000;

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

struct Inner {
    buckets: HashMap<IpAddr, Bucket>,
    next_cleanup: Instant,
}

impl RateLimiter {
    /// Create a new limiter.
    ///
    /// * `max_per_window` — max requests allowed from one IP within `window`.
    ///   Also the burst size (bucket capacity).
    /// * `window` — time window for the rate calculation.
    pub fn new(max_per_window: u64, window: Duration) -> Self {
        let window_secs = window.as_secs_f64();
        Self {
            inner: Mutex::new(Inner {
                buckets: HashMap::new(),
                next_cleanup: Instant::now() + window,
            }),
            capacity: max_per_window,
            refill_rate: max_per_window as f64 / window_secs,
            window,
        }
    }

    /// Check whether `ip` is within the rate limit.
    ///
    /// Returns `true` if the request is allowed, `false` if rate-limited.
    pub async fn check(&self, ip: IpAddr) -> bool {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();

        if now >= inner.next_cleanup {
            let cutoff = now - self.window * 2;
            inner.buckets.retain(|_, b| b.last_refill > cutoff);
            inner.next_cleanup = now + self.window;
        }

        let bucket = inner.buckets.entry(ip).or_insert_with(|| Bucket {
            tokens: self.capacity as f64,
            last_refill: now,
        });

        let elapsed = now
            .saturating_duration_since(bucket.last_refill)
            .as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_rate).min(self.capacity as f64);
        bucket.last_refill = now;

        let allowed = if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        };

        if inner.buckets.len() > MAX_TRACKED_IPS {
            Self::evict_stalest(&mut inner.buckets, MAX_TRACKED_IPS);
        }

        allowed
    }

    /// Trim `buckets` down to `max_ips` entries, keeping the most-recently-used.
    fn evict_stalest(buckets: &mut HashMap<IpAddr, Bucket>, max_ips: usize) {
        if buckets.len() <= max_ips {
            return;
        }
        let mut entries: Vec<_> = buckets.drain().collect();
        entries.sort_by_key(|b| std::cmp::Reverse(b.1.last_refill));
        *buckets = entries.into_iter().take(max_ips).collect();
    }
}

/// Per-IP admission budget for unknown clients (requests per second).
/// Each unknown IP is limited to this rate individually, preventing a
/// single scanner from hogging the global pool.
pub const ADMISSION_PER_IP_BUDGET: u64 = 2;

/// Global admission cap for all unknown IPs combined (requests per second).
/// Total unknown traffic never exceeds this ceiling,
/// preserving CPU for authenticated clients.
pub const ADMISSION_GLOBAL_CAP: u64 = 20;

/// How often stale unknown-IP entries are purged from the admission gate.
const ADMISSION_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);

/// Maximum number of distinct unknown IPs tracked by the admission gate
/// before evicting the stalest entries.
const ADMISSION_MAX_UNKNOWN_IPS: usize = 10_000;

/// Maximum number of trusted IPs before evicting the oldest entry.
const ADMISSION_MAX_TRUSTED_IPS: usize = 10_000;

/// A bounded set of trusted IPs that supports FIFO eviction.
///
/// Once the capacity is exceeded, the oldest-trusted entry is evicted.
struct TrustedSet {
    /// Fast membership checks.
    by_ip: HashSet<IpAddr>,
    /// Insertion order for FIFO eviction.
    order: VecDeque<IpAddr>,
}

impl TrustedSet {
    fn new() -> Self {
        Self {
            by_ip: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    /// Check whether `ip` is in the set.
    fn contains(&self, ip: IpAddr) -> bool {
        self.by_ip.contains(&ip)
    }

    /// Insert `ip` and evict the oldest entry if over capacity.
    fn insert(&mut self, ip: IpAddr) {
        if self.by_ip.insert(ip) {
            self.order.push_back(ip);
            if self.by_ip.len() > ADMISSION_MAX_TRUSTED_IPS
                && let Some(oldest) = self.order.pop_front()
            {
                self.by_ip.remove(&oldest);
            }
        }
    }
}

struct GlobalBucket {
    tokens: f64,
    last_refill: Instant,
}

/// A two-tier admission gate that prioritizes known (authenticated) IPs.
///
/// **Tier 1 — Trusted IPs:** IPs that have previously authenticated
/// successfully **bypass the gate entirely**.  They go straight to per-IP
/// rate limiting and auth verification.
///
/// **Tier 2 — Unknown IPs (per-IP + global cap):** Each unknown IP has its
/// own tiny token bucket (default 2 req/s) and a hard global ceiling
/// (default 20 req/s) across all unknown IPs.
///
/// This ensures that a legitimate first-time user almost always gets through
/// (their IP has its own budget) while a massive coordinated scan still
/// cannot overwhelm the server.
pub struct AdmissionGate {
    /// IPs that have successfully authenticated (own lock — read on every
    /// fast path without contending on the gate's main lock).
    /// Automatically evicts the oldest entry when over capacity.
    trusted: Mutex<TrustedSet>,
    /// Per-IP buckets + global budget, behind a single lock.
    inner: Mutex<AdmissionInner>,
    /// Per-IP token refill rate (tokens/sec).
    per_ip_budget: f64,
    /// Global token refill rate (tokens/sec) for unknown IPs.
    global_cap: f64,
}

struct AdmissionInner {
    /// Per-IP token buckets for unknown IPs.
    per_ip: HashMap<IpAddr, Bucket>,
    /// Global budget shared across all unknown IPs.
    global: GlobalBucket,
    /// When the next periodic cleanup runs.
    next_cleanup: Instant,
}

impl AdmissionGate {
    /// Create a new gate.
    ///
    /// * `per_ip_budget`  — max requests per second per unknown IP.
    /// * `global_cap`     — max total requests per second from all unknown IPs.
    pub fn new(per_ip_budget: u64, global_cap: u64) -> Self {
        let now = Instant::now();
        Self {
            trusted: Mutex::new(TrustedSet::new()),
            inner: Mutex::new(AdmissionInner {
                per_ip: HashMap::new(),
                global: GlobalBucket {
                    tokens: global_cap as f64,
                    last_refill: now,
                },
                next_cleanup: now + ADMISSION_CLEANUP_INTERVAL,
            }),
            per_ip_budget: per_ip_budget as f64,
            global_cap: global_cap as f64,
        }
    }

    /// Check whether a request from `ip` should be admitted for processing.
    ///
    /// Returns `true` if the request may proceed (trusted IPs bypass the
    /// gate entirely; unknown IPs are subject to both per-IP and global
    /// budgets).
    pub async fn admit(&self, ip: IpAddr) -> bool {
        // The per-IP rate limiter (after auth) is their ceiling.
        if self.trusted.lock().await.contains(ip) {
            return true;
        }

        let mut inner = self.inner.lock().await;
        let now = Instant::now();

        // evict stale unknown IPs
        if now >= inner.next_cleanup {
            let cutoff = now - ADMISSION_CLEANUP_INTERVAL * 2;
            inner.per_ip.retain(|_, b| b.last_refill > cutoff);
            inner.next_cleanup = now + ADMISSION_CLEANUP_INTERVAL;
        }

        // Only track IPs that have previously passed admission — rejected
        // IPs never enter the map, preventing an attacker from filling it
        // with requests that were never admitted.
        let per_ip_ok = if let Some(bucket) = inner.per_ip.get_mut(&ip) {
            let elapsed = now
                .saturating_duration_since(bucket.last_refill)
                .as_secs_f64();
            bucket.tokens = (bucket.tokens + elapsed * self.per_ip_budget).min(self.per_ip_budget);
            bucket.last_refill = now;
            if bucket.tokens < 1.0 {
                false
            } else {
                bucket.tokens -= 1.0;
                true
            }
        } else {
            // New IP — don't insert yet; only after global check passes.
            true
        };

        if !per_ip_ok {
            return false;
        }

        // Global budget check (shared across all unknown IPs)
        let elapsed = now
            .saturating_duration_since(inner.global.last_refill)
            .as_secs_f64();
        inner.global.tokens =
            (inner.global.tokens + elapsed * self.global_cap).min(self.global_cap);
        inner.global.last_refill = now;

        if inner.global.tokens < 1.0 {
            // Refund the per-IP token consumed above (if the IP was
            // already tracked).
            if let Some(bucket) = inner.per_ip.get_mut(&ip) {
                bucket.tokens += 1.0;
            }
            return false;
        }
        inner.global.tokens -= 1.0;

        // Both checks passed — record this IP if first time through.
        inner.per_ip.entry(ip).or_insert_with(|| Bucket {
            tokens: self.per_ip_budget - 1.0, // 1 consumed for this request
            last_refill: now,
        });

        // Memory cap (evict stalest if we have too many IPs)
        if inner.per_ip.len() > ADMISSION_MAX_UNKNOWN_IPS {
            let mut entries: Vec<_> = inner.per_ip.drain().collect();
            entries.sort_by_key(|b| std::cmp::Reverse(b.1.last_refill));
            inner.per_ip = entries
                .into_iter()
                .take(ADMISSION_MAX_UNKNOWN_IPS)
                .collect();
        }

        true
    }

    /// Mark `ip` as trusted (called after a successful authentication).
    ///
    /// Future requests from this IP **bypass the admission gate entirely** —
    /// the per-IP rate limiter (after auth) is their only ceiling.
    /// If the trusted set has reached capacity, the oldest-trusted IP
    /// is evicted to make room.
    pub async fn mark_trusted(&self, ip: IpAddr) {
        self.trusted.lock().await.insert(ip);
    }
}

/// Maximum number of concurrent TCP connections across all IPs.
///
/// Once this ceiling is reached the server stops accepting new connections
/// (they see a TCP reset).  This prevents file‑descriptor / tokio‑task /
/// TLS‑handshake‑CPU exhaustion.
pub const MAX_CONCURRENT_CONNECTIONS: usize = 512;

// The inner acceptor (e.g. TLS handshake) runs while the permit
// is held — this is intentional so the handshake CPU burn is
// also capped.  A timeout ensures incomplete handshakes (slow-
// loris style) cannot permanently exhaust the connection pool.
const ACCEPT_TIMEOUT: Duration = Duration::from_secs(15);

/// A tiny wrapper around `tokio::sync::Semaphore` that caps concurrent
/// TCP connections.
///
/// Each accepted connection acquires an `OwnedSemaphorePermit` that is
/// held for the lifetime of the connection — the permit is released when
/// the `PermittedStream` (which wraps the I/O stream) is dropped.
#[derive(Clone)]
pub struct ConnectionLimiter {
    sem: Arc<Semaphore>,
}

impl ConnectionLimiter {
    pub fn new(max: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(max)),
        }
    }

    /// Try to acquire a permit.  Returns `None` when the server is at
    /// capacity — the caller should drop the connection immediately.
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        // Clone the Arc so we can move it into try_acquire_owned (which
        // takes ownership of the Arc).
        Arc::clone(&self.sem).try_acquire_owned().ok()
    }
}

/// Holds an `OwnedSemaphorePermit` behind an `Arc` so it can be cloned
/// (required by axum_server's service bounds).
///
/// The permit is released when the last `Arc` reference is dropped —
/// i.e. when the HTTP connection ends and the tokio task exits.
#[derive(Clone)]
#[allow(dead_code)]
struct SharedPermit(Arc<OwnedSemaphorePermit>);

/// Wraps a tower [`Service`] and holds a semaphore permit for the
/// connection's lifetime.
///
/// Every byte of HTTP traffic goes through this service, so the permit
/// is alive for the entire HTTP connection — from first request header
/// to last response byte.
#[derive(Clone)]
pub struct PermittedService<S> {
    inner: S,
    _permit: SharedPermit,
}

impl<S, T> Service<T> for PermittedService<S>
where
    S: Service<T>,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    #[inline]
    fn call(&mut self, req: T) -> Self::Future {
        self.inner.call(req)
    }
}

/// An `axum_server` acceptor that caps concurrent connections via a
/// [`ConnectionLimiter`].
#[derive(Clone)]
pub struct LimiterAcceptor<A> {
    pub inner: A,
    pub limiter: ConnectionLimiter,
    pub diagnostics: Arc<Diagnostics>,
}

impl<I, S, A> axum_server::accept::Accept<I, S> for LimiterAcceptor<A>
where
    A: axum_server::accept::Accept<I, S>,
    A::Stream: Unpin,
    A::Future: Send + 'static,
    A::Stream: Send + 'static,
    A::Service: Send + 'static,
    // We need the inner service to be clonable so PermittedService can
    // implement Clone (required by axum_server's SendService bound).
    A::Service: Clone,
{
    type Stream = A::Stream;
    type Service = PermittedService<A::Service>;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let Some(permit) = self.limiter.try_acquire() else {
            if matches!(
                self.diagnostics
                    .record_access(AccessEvent::ConnectionRejected),
                AccessLogDecision::Emit
            ) {
                tracing::warn!(
                    target: ACCESS_TARGET,
                    "Connection rejected: at capacity ({} max)",
                    MAX_CONCURRENT_CONNECTIONS,
                );
            }
            return Box::pin(async move {
                Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "server at capacity",
                ))
            });
        };

        let inner_fut = self.inner.accept(stream, service);
        let diagnostics = Arc::clone(&self.diagnostics);
        Box::pin(async move {
            match tokio::time::timeout(ACCEPT_TIMEOUT, inner_fut).await {
                Ok(Ok((stream, service))) => Ok((
                    stream,
                    PermittedService {
                        inner: service,
                        _permit: SharedPermit(Arc::new(permit)),
                    },
                )),
                Ok(Err(e)) => Err(e),
                Err(_elapsed) => {
                    if matches!(
                        diagnostics.record_access(AccessEvent::AcceptTimeout),
                        AccessLogDecision::Emit
                    ) {
                        tracing::warn!(
                            target: ACCESS_TARGET,
                            "Connection accept timed out after {ACCEPT_TIMEOUT:?}",
                        );
                    }
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "handshake timed out",
                    ))
                }
            }
        })
    }
}
