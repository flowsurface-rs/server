use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

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
    pub fn check(&self, ip: IpAddr) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
