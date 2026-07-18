use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::storage::Storage;

/// Floor on delay between cleanup passes; prevents tight retry loops
/// when `last_cleanup` is stale (first run or previous pass failed).
const MIN_CLEANUP_DELAY: Duration = Duration::from_secs(60);

/// Wake interval for the size-cap check.  Only matters when a size cap
/// is configured and the retention window is longer than this.
const SIZE_CHECK_INTERVAL: Duration = Duration::from_secs(300);

/// Safety ceiling for the size-based purge loop (200 × 100k = 20M rows).
const MAX_PURGE_ITERATIONS: usize = 200;

/// Conservative estimate of on-disk bytes per trade row.  Used only as
/// a fast-path threshold; the authoritative check is the real file
/// size after `CHECKPOINT`.
const EST_BYTES_PER_ROW: u64 = 50;

/// Target fraction of the cap to purge down to, leaving headroom for
/// incoming trades between periodic checks.  4/5 = 80 %.
const HEADROOM_FRACTION: u64 = 4;

/// Batch size for each purge iteration — ~10% of the cap, clamped to
/// [1 000, 100 000].
fn purge_batch_size(max_bytes: u64) -> i64 {
    const MIN_ROWS: i64 = 1_000;
    const MAX_ROWS: i64 = 100_000;
    let rows = (max_bytes / 10 / EST_BYTES_PER_ROW) as i64;
    rows.clamp(MIN_ROWS, MAX_ROWS)
}

#[derive(Debug, Clone, Copy)]
pub struct CleanupConfig {
    /// Trades older than this are purged.
    pub retention_hours: u64,
    /// Optional hard cap on total DB+WAL size in bytes.
    pub max_storage_bytes: Option<u64>,
}

impl CleanupConfig {
    /// Derive from user-facing settings.  `max_storage_mb` of `0` or
    /// `None` disables the cap.  Returns an error if
    /// `retention_hours` is zero.
    pub fn from_config(retention_hours: u64, max_storage_mb: Option<u64>) -> anyhow::Result<Self> {
        if retention_hours == 0 {
            anyhow::bail!(
                "data_retention_hours must be > 0 (got 0) — \
                 a zero retention period would purge all trades on every cleanup pass"
            );
        }
        let max_storage_bytes = max_storage_mb
            .filter(|&mb| mb > 0)
            .map(|mb| mb.saturating_mul(1024 * 1024));
        Ok(Self {
            retention_hours,
            max_storage_bytes,
        })
    }
}

#[derive(Clone)]
pub struct CleanupScheduler {
    storage: Storage,
    config: CleanupConfig,
}

impl CleanupScheduler {
    /// Create the scheduler and check the startup guard: if the database
    /// is > 2× the configured cap the process exits (this is a
    /// misconfiguration that would take too long to recover from at
    /// startup).
    ///
    /// Returns an error when the storage size cannot be read at all.
    pub fn new(storage: &Storage, config: CleanupConfig) -> anyhow::Result<Self> {
        if let Some(max) = config.max_storage_bytes {
            tracing::info!(
                "Storage hard cap enabled: {} MB (will purge oldest trades when exceeded)",
                max / (1024 * 1024)
            );

            match storage.current_storage_bytes() {
                Ok(current) if current > max.saturating_mul(2) => {
                    anyhow::bail!(
                        "Database is {} MB — more than 2x the configured \
                         max_storage_mb cap ({} MB).  Purging that much data \
                         at startup would take too long; this likely indicates \
                         a misconfiguration.  Either raise max_storage_mb \
                         (e.g. to {} MB or higher) or manually shrink the \
                         database and restart.",
                        current / (1024 * 1024),
                        max / (1024 * 1024),
                        (current / (1024 * 1024)).saturating_add(1),
                    );
                }
                Ok(current) if current > max => {
                    tracing::warn!(
                        "Database is {} MB — above the {} MB cap; \
                         startup cleanup will purge oldest trades.",
                        current / (1024 * 1024),
                        max / (1024 * 1024),
                    );
                }
                Err(e) => {
                    anyhow::bail!("Failed to check storage size at startup: {e:#}");
                }
                _ => {}
            }
        }

        Ok(Self {
            storage: storage.clone(),
            config,
        })
    }

    /// Run one cleanup pass: time-based retention, optional size-cap
    /// purge, checkpoint, and record `last_cleanup`.
    ///
    /// Returns `Some(last_cleanup_ts)` on success, or `None` if the
    /// pass failed (e.g. time-based purge errored or recording the
    /// timestamp failed).
    pub fn run_pass(&self) -> Option<i64> {
        let mut any_deleted = false;
        let mut time_cleanup_ok = false;

        match self.storage.purge_old_trades(self.config.retention_hours) {
            Ok(n) => {
                time_cleanup_ok = true;
                if n > 0 {
                    tracing::info!(
                        "Cleaned up {n} trade(s) older than {}h",
                        self.config.retention_hours
                    );
                    any_deleted = true;
                }
            }
            Err(e) => {
                tracing::error!("Data retention cleanup failed: {e:#}");
            }
        }

        if let Some(max_bytes) = self.config.max_storage_bytes {
            match self.purge_oldest_trades_until_below(max_bytes) {
                Ok(n) if n > 0 => any_deleted = true,
                Err(e) => tracing::error!("Storage-cap cleanup failed: {e:#}"),
                _ => {}
            }
        }

        if any_deleted && let Err(e) = self.storage.run_checkpoint() {
            tracing::warn!("Failed to checkpoint DuckDB WAL after cleanup: {e:#}");
        }

        if time_cleanup_ok {
            match self.storage.record_cleanup() {
                Ok(ts) => return Some(ts),
                Err(e) => tracing::warn!("Failed to record last_cleanup timestamp: {e:#}"),
            }
        }
        None
    }

    /// Delete oldest trades until on-disk size is estimated to be under the cap.
    ///
    /// To avoid per-iteration CHECKPOINT overhead we use a two-phase approach:
    /// 1. **Row-count estimate loop** — delete batches until the estimated row
    ///    count is below the headroom-adjusted threshold.  No CHECKPOINTs here.
    /// 2. **CHECKPOINT once**, then verify the real file size.  If still over
    ///    the absolute cap we log a warning — the next periodic pass will retry.
    ///
    /// Targets `HEADROOM_FRACTION / 5` of the cap so there is headroom for
    /// incoming trades between 5-minute checks.
    fn purge_oldest_trades_until_below(&self, max_bytes: u64) -> Result<u64> {
        let target_bytes = max_bytes.saturating_mul(HEADROOM_FRACTION) / 5;
        let max_est_rows = target_bytes.saturating_div(EST_BYTES_PER_ROW);
        let batch_size = purge_batch_size(target_bytes);
        let mut total_deleted = 0u64;
        let mut converged = false;

        for _ in 0..MAX_PURGE_ITERATIONS {
            // Cheap row-count check (no CHECKPOINT needed).
            if self.storage.count_trades()? <= max_est_rows {
                converged = true;
                break;
            }
            let deleted = self.storage.delete_oldest_trades_batch(batch_size)?;
            if deleted == 0 {
                converged = true;
                break;
            }
            total_deleted += deleted;
        }

        if total_deleted > 0 {
            self.storage.run_checkpoint()?;
            let remaining_rows = self.storage.count_trades()?;

            if converged {
                let current_bytes = self.storage.current_storage_bytes()?;
                if current_bytes <= max_bytes {
                    tracing::info!(
                        "Cleaned up {total_deleted} trade(s) to keep storage under \
                         {} MB cap (now ~{} MB)",
                        max_bytes / (1024 * 1024),
                        current_bytes / (1024 * 1024),
                    );
                } else {
                    tracing::warn!(
                        "Storage ({} MB) still exceeds {} MB cap after cleanup. \
                         The next periodic pass will retry.",
                        current_bytes / (1024 * 1024),
                        max_bytes / (1024 * 1024),
                    );
                }
            } else {
                tracing::warn!(
                    "Size-cap purge did not converge after \
                     {MAX_PURGE_ITERATIONS} iterations (deleted {total_deleted} \
                     rows); storage may still exceed the {} MB cap",
                    max_bytes / (1024 * 1024),
                );
            }

            if should_vacuum(total_deleted, remaining_rows) {
                if let Err(e) = self.storage.vacuum() {
                    tracing::warn!(
                        "VACUUM after size-cap purge failed (data is \
                         correct, but filesystem space wasn't reclaimed): \
                         {e:#}"
                    );
                } else {
                    tracing::debug!(
                        "VACUUMed database after purging {total_deleted} \
                         rows ({remaining_rows} remaining)"
                    );
                }
            }
        }

        Ok(total_deleted)
    }

    /// Spawn the periodic background cleanup task.
    ///
    /// A one-shot startup pass must have been run beforehand (via
    /// [`run_startup_pass`](Self::run_startup_pass)) so that
    /// `last_cleanup` is initialised.  The loop then computes the
    /// next wake-up from `last_cleanup` and only polls when work
    /// is actually due (or every `SIZE_CHECK_INTERVAL` when a size
    /// cap is configured).
    pub fn spawn(self, last_cleanup: Option<i64>, shutdown: CancellationToken) -> JoinHandle<()> {
        let storage = self.storage.clone();
        let config = self.config;

        let retention_ms = (config.retention_hours as i64) * 3_600_000;

        tokio::spawn(async move {
            let mut last_cleanup = last_cleanup;

            loop {
                let time_delay = Self::delay_until_next_cleanup(last_cleanup, retention_ms);
                let delay =
                    if config.max_storage_bytes.is_some() && SIZE_CHECK_INTERVAL < time_delay {
                        SIZE_CHECK_INTERVAL
                    } else {
                        time_delay
                    }
                    .max(MIN_CLEANUP_DELAY);

                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        tracing::info!("Periodic cleanup shut down.");
                        break;
                    }
                    _ = tokio::time::sleep(delay) => {
                        let result = tokio::task::spawn_blocking({
                            let storage = storage.clone();
                            move || {
                                CleanupScheduler { storage, config }.run_pass()
                            }
                        })
                        .await
                        .unwrap_or(None);
                        if let Some(ts) = result {
                            last_cleanup = Some(ts);
                        }
                    }
                }
            }
        })
    }

    fn delay_until_next_cleanup(last_cleanup_ms: Option<i64>, retention_ms: i64) -> Duration {
        let now_ms = Storage::now_ms();

        let Some(anchor_ms) = last_cleanup_ms else {
            // No recorded timestamp — retry quickly (caller applies
            // `MIN_CLEANUP_DELAY`).
            return Duration::ZERO;
        };

        let next_ms = anchor_ms + retention_ms;
        if next_ms <= now_ms {
            Duration::ZERO
        } else {
            Duration::from_millis((next_ms - now_ms) as u64)
        }
    }
}

/// Whether VACUUM is worth running after a purge — only when we've
/// freed at least ~25% of the remaining data.
const fn should_vacuum(total_deleted: u64, remaining_rows: u64) -> bool {
    remaining_rows == 0 || total_deleted >= remaining_rows / 4
}
