use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::storage::Storage;

/// Minimum delay between cleanup attempts, regardless of schedule.
///
/// This prevents a tight retry loop when `delay_until_next_cleanup`
/// returns zero (e.g. because the last time-based cleanup failed and
/// `last_cleanup` is stale).
const MIN_CLEANUP_DELAY: Duration = Duration::from_secs(60);

/// How often the size-based cleanup task wakes up to check whether the
/// database has exceeded the `max_storage_mb` cap.
///
/// This interval only matters when a size cap is configured *and* the
/// time-based retention window is longer than 5 minutes — otherwise the
/// time-based schedule dominates.
const SIZE_CHECK_INTERVAL: Duration = Duration::from_secs(300);

/// Estimated on-disk bytes per trade row.
///
/// DuckDB's columnar storage compresses the schema well: exchange
/// names dictionary-compress to ~1–2 bytes, symbols to ~2–5 bytes,
/// timestamps benefit from delta encoding, and `DOUBLE` columns
/// compress modestly.  Empirically ~40–60 bytes per row including
/// row-group overhead.
///
/// Used only as a fast-path threshold in the size-based purge loop —
/// the authoritative check is the actual filesystem file size after
/// `CHECKPOINT`.  A conservative (lower) value here means we more
/// often verify with the real file size, which is safer.
const EST_BYTES_PER_ROW: u64 = 50;

/// Choose a batch size proportional to `max_bytes` so that each purge
/// iteration deletes ~10% of the cap.
///
/// Clamped to **[1 000, 100 000]** so tiny caps still make progress
/// and huge caps don't create giant transactions.
fn purge_batch_size(max_bytes: u64) -> i64 {
    const MIN_ROWS: i64 = 1_000;
    const MAX_ROWS: i64 = 100_000;

    // Target ~10% of the cap per iteration.
    let rows = (max_bytes / 10 / EST_BYTES_PER_ROW) as i64;
    rows.clamp(MIN_ROWS, MAX_ROWS)
}

/// Groups the two retention parameters that are always passed together.
#[derive(Debug, Clone, Copy)]
pub struct CleanupConfig {
    /// Time-based retention: trades older than this are purged.
    pub retention_hours: u64,
    /// Optional hard cap on total DB+WAL size in bytes.  When set, the
    /// oldest trades are purged even if within the retention window.
    pub max_storage_bytes: Option<u64>,
}

impl CleanupConfig {
    /// Derive the config from the user-facing `max_storage_mb` setting.
    /// A value of `0` or `None` means no cap.
    pub fn from_config(retention_hours: u64, max_storage_mb: Option<u64>) -> Self {
        let max_storage_bytes = max_storage_mb
            .filter(|&mb| mb > 0)
            .map(|mb| mb.saturating_mul(1024 * 1024));
        Self {
            retention_hours,
            max_storage_bytes,
        }
    }
}

/// Owns the cleanup lifecycle: startup guard, startup pass, and the
/// periodic background task.
///
/// Created in [`CleanupScheduler::new`] which runs the startup guard.
/// Call [`spawn`](Self::spawn) to start the periodic task — this also
/// runs the startup cleanup pass on a blocking thread and feeds its
/// result into the scheduler so the first periodic delay is accurate.
pub struct CleanupScheduler {
    storage: Storage,
    config: CleanupConfig,
}

impl CleanupScheduler {
    /// Create the scheduler and run the startup guard.
    pub fn new(storage: &Storage, config: CleanupConfig) -> Self {
        if let Some(max) = config.max_storage_bytes {
            tracing::info!(
                "Storage hard cap enabled: {} MB (will purge oldest trades when exceeded)",
                max / (1024 * 1024)
            );

            match storage.current_storage_bytes() {
                Ok(current) if current > max.saturating_mul(2) => {
                    tracing::error!(
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
                    std::process::exit(1);
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
                    tracing::error!("Failed to check storage size at startup: {e:#}");
                    std::process::exit(1);
                }
                _ => {}
            }
        }

        Self {
            storage: storage.clone(),
            config,
        }
    }

    /// Run a single cleanup pass (time-based retention + optional size
    /// cap + checkpoint + record timestamp).
    ///
    /// Returns `Some(ts)` when the time-based purge succeeded and the
    /// `last_cleanup` timestamp was recorded, or `None` on failure.
    fn run_pass(&self) -> Option<i64> {
        let mut cleaned_anything = false;
        let mut time_cleanup_ok = false;

        match self.storage.purge_old_trades(self.config.retention_hours) {
            Ok(n) => {
                time_cleanup_ok = true;
                if n > 0 {
                    tracing::info!(
                        "Cleaned up {n} trade(s) older than {}h",
                        self.config.retention_hours
                    );
                    cleaned_anything = true;
                }
            }
            Err(e) => {
                tracing::error!("Data retention cleanup failed: {e:#}");
            }
        }

        if let Some(max_bytes) = self.config.max_storage_bytes {
            match self.purge_oldest_trades_until_below(max_bytes) {
                Ok(n) => {
                    if n > 0 {
                        let current_mb = self
                            .storage
                            .current_storage_bytes()
                            .map(|b| b / (1024 * 1024))
                            .unwrap_or(0);
                        tracing::info!(
                            "Cleaned up {n} trade(s) to keep storage under cap \
                             (max {} MB, now ~{current_mb} MB)",
                            max_bytes / (1024 * 1024),
                        );
                        cleaned_anything = true;
                    }
                }
                Err(e) => {
                    tracing::error!("Storage-cap cleanup failed: {e:#}");
                }
            }
        }

        if cleaned_anything && let Err(e) = self.storage.run_checkpoint() {
            tracing::warn!("Failed to checkpoint DuckDB WAL after cleanup: {e:#}");
        }

        let mut last_cleanup_ts: Option<i64> = None;
        if time_cleanup_ok {
            match self.storage.record_cleanup() {
                Ok(ts) => last_cleanup_ts = Some(ts),
                Err(e) => tracing::warn!("Failed to record last_cleanup timestamp: {e:#}"),
            }
        }
        last_cleanup_ts
    }

    /// Delete the oldest trades until the on-disk size drops below
    /// `max_bytes`.  Returns the total number of deleted rows.
    ///
    /// The authoritative convergence criterion is the actual filesystem
    /// size after `CHECKPOINT`.  A row-count estimate
    /// (`COUNT(*) × EST_BYTES_PER_ROW`) is used as a fast path: when
    /// the row count is clearly above the estimated target we purge
    /// without an expensive `CHECKPOINT`; when it's below we verify
    /// with the real file size to guard against estimation errors.
    ///
    /// After purging, if a significant fraction of the data was freed,
    /// a `VACUUM` is run to reclaim filesystem space — `CHECKPOINT`
    /// alone doesn't shrink the main database file.
    fn purge_oldest_trades_until_below(&self, max_bytes: u64) -> Result<u64> {
        /// Safety ceiling: if the purge hasn't converged after this
        /// many iterations, something is wrong.  200 × 100k batch =
        /// 20M rows — far beyond any plausible cap.  Bail out rather
        /// than looping forever.
        const MAX_ITERATIONS: usize = 200;

        let max_est_rows = max_bytes.saturating_div(EST_BYTES_PER_ROW);
        let batch_size = purge_batch_size(max_bytes);
        let mut total_deleted = 0u64;

        for _iteration in 0..MAX_ITERATIONS {
            // ── Fast path: row count ───────────────────────────
            // COUNT(*) in DuckDB is a metadata operation on
            // row-group headers — cheap even on large tables.
            //
            // If the row count is clearly above the estimated
            // target we purge without an expensive CHECKPOINT.
            // If it's below, the estimate may be wrong so we
            // fall through to the authoritative file-size check.
            let current_rows = self.storage.count_trades()?;

            if current_rows <= max_est_rows {
                // ── Authoritative criterion: file size ──────
                // CHECKPOINT first so the WAL doesn't inflate
                // the filesystem size check.
                self.storage.run_checkpoint()?;

                let file_size = self.storage.current_storage_bytes()?;
                if file_size <= max_bytes {
                    tracing::debug!(
                        "Size-based purge: file size {file_size} ≤ \
                         cap {max_bytes}, stopping"
                    );
                    break;
                }
                // Row count says we're under, but the file is
                // still over the cap — our estimate was too
                // optimistic.  Continue purging.
            }

            let deleted = self.storage.delete_oldest_trades_batch(batch_size)?;
            if deleted == 0 {
                break;
            }
            total_deleted += deleted;
        }

        if total_deleted > 0 {
            // CHECKPOINT to merge the WAL, then VACUUM if we've
            // freed a significant fraction of the data — DuckDB's
            // CHECKPOINT only merges the WAL, it doesn't shrink the
            // main file.  VACUUM rewrites the database to reclaim
            // filesystem space, but is O(n) in remaining rows so
            // we only run it when the payoff is worth it.
            self.storage.run_checkpoint()?;

            let remaining_rows = self.storage.count_trades()?;

            // VACUUM when we've freed at least ~20% of the
            // remaining data — enough to reclaim meaningful space
            // without running an expensive rewrite on every pass.
            if remaining_rows == 0 || total_deleted >= remaining_rows / 4 {
                self.storage.vacuum()?;
                tracing::debug!(
                    "VACUUMed database after purging {total_deleted} rows \
                     ({remaining_rows} remaining)"
                );
            }
        }

        Ok(total_deleted)
    }

    /// Spawn the startup cleanup and the periodic background task.
    ///
    /// Returns the `JoinHandle` for the periodic task (and the startup
    /// task is awaited internally by the periodic task).
    pub fn spawn(self, shutdown: CancellationToken) -> JoinHandle<()> {
        let Self { storage, config } = self;

        let (startup_tx, startup_rx) = tokio::sync::oneshot::channel();

        // Startup cleanup on a blocking thread.
        let startup_handle = {
            let scheduler = CleanupScheduler {
                storage: storage.clone(),
                config,
            };
            tokio::task::spawn_blocking(move || {
                let result = scheduler.run_pass();
                let _ = startup_tx.send(result);
            })
        };

        let retention_ms = (config.retention_hours as i64) * 3_600_000;

        tokio::spawn(async move {
            // Await the startup cleanup so we have an accurate
            // `last_cleanup` timestamp before scheduling the next
            // pass.
            let mut last_cleanup = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::info!("Periodic cleanup shut down.");
                    return;
                }
                result = startup_rx => result.ok().flatten(),
            };

            // Also await the startup task's JoinHandle so it is
            // properly joined on shutdown (not detached).
            let _ = startup_handle.await;

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
                        let scheduler = CleanupScheduler {
                            storage: storage.clone(),
                            config,
                        };
                        let result = tokio::task::spawn_blocking(move || {
                            scheduler.run_pass()
                        })
                        .await
                        .ok()
                        .flatten();
                        if let Some(ts) = result {
                            last_cleanup = Some(ts);
                        }
                    }
                }
            }
        })
    }

    fn delay_until_next_cleanup(last_cleanup_ms: Option<i64>, retention_ms: i64) -> Duration {
        let now_ms = Self::now_ms();

        let anchor_ms = last_cleanup_ms.unwrap_or_else(|| {
            tracing::debug!("No last_cleanup recorded; anchoring at now");
            now_ms
        });

        let next_ms = anchor_ms + retention_ms;
        if next_ms <= now_ms {
            Duration::ZERO
        } else {
            Duration::from_millis((next_ms - now_ms) as u64)
        }
    }

    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}
