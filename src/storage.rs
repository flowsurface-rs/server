use anyhow::{Context, Result};
use duckdb::{Appender, Connection};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use flowsurface_exchange::adapter::Exchange;
use flowsurface_exchange::unit::{price::Price, qty::Qty};
use flowsurface_exchange::{Ticker, TickerInfo, UnixMs};

use crate::api::{AnnotatedTrade, TradeQuery};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Information about a tracked pair with the timestamp range stored.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct PairInfo {
    pub ticker: Ticker,
    pub earliest: Option<UnixMs>,
    pub latest: Option<UnixMs>,
}

/// Manages the DuckDB database lifecycle and provides high-level read access.
///
/// Write access is handled through a separate [`BatchWriter`] that uses
/// DuckDB's `Appender` API for efficient bulk insertion.
///
/// Internally holds a single root `duckdb_database` handle.  All
/// sub-connections (readers, writer) are created via `try_clone()`,
/// ensuring they share the same buffer pool, catalog cache and WAL —
/// so writes are immediately visible to subsequent reads.
#[derive(Clone)]
pub struct Storage {
    db: Arc<parking_lot::Mutex<duckdb::Connection>>,
}

impl Storage {
    /// Open (or create) the database at `data_dir / trades.duckdb` and ensure
    /// the schema exists.  Creates `data_dir` if it does not exist.
    ///
    /// The returned `Storage` keeps a single `duckdb_database` handle alive;
    /// all sub-connections (readers and the batch writer) must be obtained
    /// via [`open_writer`](Self::open_writer) / [`connection`](Self::connection)
    /// so they share the same database instance.
    pub fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("creating data directory {}", data_dir.display()))?;

        let db_path = data_dir.join("trades.duckdb");

        // Open the *first* connection – this creates the underlying
        // duckdb_database handle.  All later connections MUST use
        // try_clone() on this root connection to share the same
        // database instance (and thus the same WAL / buffer pool).
        let root = Connection::open(&db_path)
            .with_context(|| format!("opening DuckDB at {}", db_path.display()))?;

        root.execute_batch(
            "CREATE TABLE IF NOT EXISTS trades (
                exchange   VARCHAR NOT NULL,
                symbol     VARCHAR NOT NULL,
                ts  BIGINT  NOT NULL,
                price      DOUBLE  NOT NULL,
                qty   DOUBLE  NOT NULL,
                is_sell    BOOLEAN NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_trades_ex_sym_t
                ON trades (exchange, symbol, ts);

            CREATE TABLE IF NOT EXISTS ticker_info (
                exchange        VARCHAR NOT NULL,
                symbol          VARCHAR NOT NULL,
                min_ticksize    INTEGER NOT NULL,
                min_qty         INTEGER NOT NULL,
                contract_size   INTEGER,
                PRIMARY KEY (exchange, symbol)
            );

            CREATE TABLE IF NOT EXISTS metadata (
                key   VARCHAR NOT NULL PRIMARY KEY,
                value VARCHAR NOT NULL
            );",
        )
        .context("creating DuckDB schema")?;

        Ok(Self {
            db: Arc::new(parking_lot::Mutex::new(root)),
        })
    }

    /// Open a dedicated connection for the batch writer (shares the
    /// underlying `duckdb_database`).
    pub fn open_writer(&self) -> Result<BatchWriter> {
        let locked = self.db.lock();
        let conn = locked
            .try_clone()
            .context("cloning root connection for writer")?;
        Ok(BatchWriter { conn })
    }

    /// Obtain a fresh read-only connection that shares the same
    /// `duckdb_database` as the root connection.
    fn connection(&self) -> Result<duckdb::Connection> {
        let locked = self.db.lock();
        locked
            .try_clone()
            .context("cloning root connection for query")
    }

    /// Query trades matching the given filter.
    pub fn query_trades(&self, q: &TradeQuery) -> Result<Vec<AnnotatedTrade>> {
        let conn = self.connection()?;

        let limit = q.limit.unwrap_or(1000).min(10_000);
        let exchange = q.exchange_filter();

        // Build the SQL with positional ? placeholders.
        let mut sql = String::from(
            "SELECT exchange, symbol, ts, price, qty, is_sell
             FROM trades
             WHERE symbol = ?
             AND exchange = ?",
        );

        if q.from.is_some() {
            sql.push_str(" AND ts >= ?");
        }
        if q.to.is_some() {
            sql.push_str(" AND ts <= ?");
        }

        if q.from.is_some() {
            sql.push_str(" ORDER BY ts ASC");
        } else {
            sql.push_str(" ORDER BY ts DESC");
        }
        sql.push_str(&format!(" LIMIT {}", limit));

        let mut stmt = conn.prepare(&sql).context("preparing trade query")?;

        let symbol_lower = q.symbol.to_lowercase();
        let mut params: Vec<&dyn duckdb::ToSql> = vec![&symbol_lower, &exchange];
        if let Some(ref from) = q.from {
            params.push(from);
        }
        if let Some(ref to) = q.to {
            params.push(to);
        }

        let mut rows = stmt.query(&params[..]).context("querying trades")?;

        let mut trades = Vec::new();
        while let Some(row) = rows.next()? {
            let exchange_str: String = row.get(0)?;
            let symbol_str: String = row.get(1)?;
            let exchange: Exchange = exchange_str.parse().map_err(|e: String| {
                anyhow::anyhow!("cannot parse exchange '{exchange_str}': {e}")
            })?;

            trades.push(AnnotatedTrade {
                ticker: Ticker::new(&symbol_str, exchange),
                trade: flowsurface_exchange::Trade {
                    time: UnixMs::new(row.get::<_, i64>(2)? as u64),
                    is_sell: row.get(5)?,
                    price: Price::from_f64(row.get::<_, f64>(3)?),
                    qty: Qty::from_f64(row.get::<_, f64>(4)?),
                },
            });
        }
        Ok(trades)
    }

    /// Export matching trades as an **Arrow IPC stream**.
    ///
    /// Returns the complete Arrow IPC streaming format payload suitable
    /// for HTTP response with `Content-Type: application/vnd.apache.arrow.stream`.
    pub fn query_trades_arrow_ipc(&self, q: &TradeQuery) -> Result<Vec<u8>> {
        let conn = self.connection()?;

        let limit = q.limit.unwrap_or(100_000).min(1_000_000);
        let exchange = q.exchange_filter();
        let symbol_lower = q.symbol.to_lowercase();

        let mut sql = String::from(
            "SELECT ts, price, qty, is_sell FROM trades \
             WHERE symbol = ? AND exchange = ?",
        );

        if q.from.is_some() {
            sql.push_str(" AND ts >= ?");
        }
        if q.to.is_some() {
            sql.push_str(" AND ts <= ?");
        }

        if q.from.is_some() {
            sql.push_str(" ORDER BY ts ASC");
        } else {
            sql.push_str(" ORDER BY ts DESC");
        }
        sql.push_str(&format!(" LIMIT {limit}"));

        let mut params: Vec<&dyn duckdb::ToSql> = vec![&symbol_lower, &exchange];
        if let Some(ref from) = q.from {
            params.push(from);
        }
        if let Some(ref to) = q.to {
            params.push(to);
        }

        let mut stmt = conn.prepare(&sql).context("preparing Arrow IPC query")?;

        let batches: Vec<arrow::record_batch::RecordBatch> = stmt
            .query_arrow(&params[..])
            .context("executing Arrow query")?
            .collect();

        // Drop the statement so the connection is free for the IPC writer.
        drop(stmt);

        let mut buf = Vec::new();
        {
            use arrow::datatypes::{DataType, Field, Schema};
            use arrow::ipc::writer::StreamWriter;
            use std::sync::Arc;

            let schema: Arc<Schema> = if batches.is_empty() {
                Arc::new(Schema::new(vec![
                    Field::new("ts", DataType::Int64, false),
                    Field::new("price", DataType::Float64, false),
                    Field::new("qty", DataType::Float64, false),
                    Field::new("is_sell", DataType::Boolean, false),
                ]))
            } else {
                batches[0].schema()
            };

            let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref())
                .context("creating Arrow IPC stream writer")?;

            for batch in &batches {
                writer.write(batch).context("writing Arrow record batch")?;
            }

            writer.finish().context("finishing Arrow IPC stream")?;
        }

        tracing::debug!(
            "Exported {} bytes of Arrow IPC data ({} batch(es))",
            buf.len(),
            batches.len()
        );
        Ok(buf)
    }

    /// Persist ticker metadata for every resolved pair so the API can
    /// look up tick sizes at query time.
    pub fn store_ticker_infos(&self, infos: &[TickerInfo]) -> Result<()> {
        let conn = self.connection()?;

        let mut stmt = conn
            .prepare(
                "INSERT OR REPLACE INTO ticker_info
                    (exchange, symbol, min_ticksize, min_qty, contract_size)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .context("preparing ticker_info upsert")?;

        for info in infos {
            stmt.execute(duckdb::params![
                info.exchange().to_string(),
                info.ticker
                    .display_symbol()
                    .map(|s| s.to_lowercase())
                    .unwrap_or_else(|| info.ticker.to_string().to_lowercase()),
                info.min_ticksize.power,
                info.min_qty.power,
                info.contract_size.map(|cs| cs.power),
            ])
            .with_context(|| {
                format!(
                    "inserting ticker_info for {}/{}",
                    info.exchange(),
                    info.ticker
                )
            })?;
        }
        Ok(())
    }

    /// Return every (venue, symbol) that has at least one stored trade,
    /// along with the earliest and latest timestamps.
    pub fn pairs_with_bounds(&self) -> Result<Vec<PairInfo>> {
        let conn = self.connection()?;

        let mut stmt = conn.prepare(
            "SELECT exchange, symbol,
                    MIN(ts) AS earliest,
                    MAX(ts) AS latest
             FROM trades
             GROUP BY exchange, symbol
             ORDER BY exchange, symbol",
        )?;

        let rows = stmt.query_map([], |row| {
            let exchange_str: String = row.get(0)?;
            let symbol_str: String = row.get(1)?;
            let exchange: Exchange = exchange_str.parse().map_err(|e: String| {
                duckdb::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("cannot parse exchange '{exchange_str}': {e}"),
                )))
            })?;

            Ok(PairInfo {
                ticker: Ticker::new(&symbol_str, exchange),
                earliest: row.get::<_, Option<i64>>(2)?.map(|v| UnixMs::new(v as u64)),
                latest: row.get::<_, Option<i64>>(3)?.map(|v| UnixMs::new(v as u64)),
            })
        })?;

        let mut pairs = Vec::new();
        for row in rows {
            pairs.push(row?);
        }
        Ok(pairs)
    }

    /// Count of tracked pairs in the database.
    pub fn pair_count(&self) -> Result<u64> {
        let conn = self.connection()?;
        let count: u64 = conn.query_row(
            "SELECT COUNT(DISTINCT exchange || ':' || symbol) FROM trades",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Set a metadata key-value pair.
    pub fn set_metadata(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
            duckdb::params![key, value],
        )
        .context("setting metadata")?;
        Ok(())
    }

    /// Get a metadata value by key, returning `None` if absent.
    pub fn get_metadata(&self, key: &str) -> Result<Option<String>> {
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare("SELECT value FROM metadata WHERE key = ?1")
            .context("preparing metadata lookup")?;
        let mut rows = stmt.query_map(duckdb::params![key], |row| row.get::<_, String>(0))?;
        match rows.next() {
            Some(Ok(v)) => Ok(Some(v)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Delete every trade row whose `ts` (milliseconds since epoch) is
    /// older than `retention_hours`.  Returns the number of deleted rows.
    pub fn purge_old_trades(&self, retention_hours: u64) -> Result<u64> {
        let conn = self.connection()?;
        let cutoff_ms = Self::now_ms() - (retention_hours as i64 * 3_600_000);
        let deleted = conn
            .execute(
                "DELETE FROM trades WHERE ts < ?1",
                duckdb::params![cutoff_ms],
            )
            .context("purging old trades")?;
        Ok(deleted as u64)
    }

    /// Convenience: return the stored `last_cleanup` timestamp (milliseconds
    /// since epoch), or `None` if cleanup has never run.
    pub fn last_cleanup_ms(&self) -> Result<Option<i64>> {
        self.get_metadata("last_cleanup")?
            .map(|v| v.parse::<i64>().context("parsing last_cleanup metadata"))
            .transpose()
    }

    pub fn record_cleanup(&self) -> Result<()> {
        let now_ms = Self::now_ms();
        self.set_metadata("last_cleanup", &now_ms.to_string())
    }

    /// Run a single data-retention cleanup pass.
    ///
    /// Deletes trades older than `retention_hours` and records the
    /// `last_cleanup` timestamp so callers can avoid running it again too soon.
    pub fn run_cleanup(&self, retention_hours: u64) {
        match self.purge_old_trades(retention_hours) {
            Ok(n) => {
                if n > 0 {
                    tracing::info!("Cleaned up {n} trade(s) older than {retention_hours}h");
                }
                if let Err(e) = self.record_cleanup() {
                    tracing::warn!("Failed to record last_cleanup timestamp: {e:#}");
                }
            }
            Err(e) => {
                tracing::error!("Data cleanup failed: {e:#}");
            }
        }
    }

    /// Compute how long to sleep before the next cleanup is needed.
    fn next_cleanup_delay(&self, retention_ms: i64) -> Duration {
        let now_ms = Self::now_ms();

        let anchor_ms = match self.last_cleanup_ms() {
            Ok(Some(ts)) => ts,
            Ok(None) => {
                tracing::debug!("No last_cleanup recorded; anchoring at now");
                now_ms
            }
            Err(e) => {
                tracing::warn!("Failed to read last_cleanup: {e:#}; retrying in 10 min");
                return Duration::from_secs(600);
            }
        };

        let next_ms = anchor_ms + retention_ms;
        if next_ms <= now_ms {
            Duration::ZERO
        } else {
            Duration::from_millis((next_ms - now_ms) as u64)
        }
    }

    /// Spawn a background task that schedules the next cleanup pass based
    /// on the `last_cleanup` metadata, without polling.
    ///
    /// After each cleanup pass (which writes `last_cleanup`), the task
    /// computes `last_cleanup + retention_hours` and sleeps exactly until
    /// that moment.  This means wakeups only happen when data is actually
    /// due for expiry — there is no periodic polling.
    ///
    /// A startup [`run_cleanup`](Self::run_cleanup) is expected to have been
    /// called by the caller before this task is spawned so that `last_cleanup`
    /// is initialised.
    pub fn spawn_periodic_cleanup(
        &self,
        retention_hours: u64,
        shutdown: CancellationToken,
    ) -> JoinHandle<()> {
        let storage = self.clone();
        tokio::spawn(async move {
            let retention_ms = (retention_hours as i64) * 3_600_000;

            loop {
                let delay = storage.next_cleanup_delay(retention_ms);

                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        tracing::info!("Periodic cleanup shut down.");
                        break;
                    }
                    _ = tokio::time::sleep(delay) => {
                        storage.run_cleanup(retention_hours);
                    }
                }
            }
        })
    }

    /// Return the current UTC timestamp in milliseconds since the Unix epoch.
    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    /// Spawn a background task that receives trades on `rx`, buffers them,
    /// and flushes to DuckDB via the Appender API every `flush_interval`.
    ///
    /// The flusher uses an interval-first biased select so that flushes
    /// always get a turn regardless of incoming trade volume.  A
    /// `max_buffered_trades` cap prevents runaway memory growth on
    /// constrained hosts — once the buffer exceeds this threshold
    /// incoming trades are silently dropped and a rate-limited warning
    /// is emitted.  The warning clears automatically when the buffer is
    /// flushed below capacity.
    ///
    /// DuckDB I/O runs on a **blocking thread** via `spawn_blocking` so
    /// that a stalled disk or fsync cannot stall the async task.  The
    /// channel continues to be drained into our capped buffer while the
    /// blocking flush is in-flight, ensuring the buffer cap is always
    /// effective.
    ///
    /// Uses the shared `duckdb_database` from `storage` so that flushed
    /// trades are immediately visible to reader connections.
    pub fn spawn_batch_flusher(
        &self,
        mut rx: mpsc::UnboundedReceiver<AnnotatedTrade>,
        flush_interval: Duration,
        max_buffered_trades: usize,
    ) -> JoinHandle<()> {
        let store = self.clone();

        tokio::spawn(async move {
            let mut buf = TradeBuffer::new(max_buffered_trades);
            let mut flush_handle: Option<JoinHandle<anyhow::Result<()>>> = None;
            let mut interval = tokio::time::interval(flush_interval);
            interval.reset_immediately();

            loop {
                // Check if an in-flight blocking flush has finished.
                if let Some(ref h) = flush_handle
                    && h.is_finished()
                {
                    let handle = flush_handle.take().unwrap();
                    match handle.await {
                        Ok(Ok(())) => buf.flush_succeeded(),
                        Ok(Err(e)) => {
                            tracing::error!(
                                "Batch flush failed ({} trades in new buffer, \
                                 failed batch dropped): {e:#}",
                                buf.len()
                            );
                        }
                        Err(_) => {
                            tracing::error!("Batch flush task panicked or cancelled");
                        }
                    }
                }

                tokio::select! {
                    biased;
                    _ = interval.tick() => {
                        // Non-blocking drain of everything the channel has
                        // queued since the last tick.
                        while let Ok(trade) = rx.try_recv() {
                            buf.push(trade);
                        }

                        // Start a blocking flush if we have data and none
                        // is currently in-flight.
                        if !buf.is_empty() && flush_handle.is_none() {
                            let batch = buf.take();
                            let store = store.clone();
                            flush_handle = Some(tokio::task::spawn_blocking(move || {
                                let mut writer = store.open_writer()?;
                                writer.flush(&batch)
                            }));
                        }
                    }
                    maybe = rx.recv() => {
                        match maybe {
                            Some(trade) => buf.push(trade),
                            None => {
                                // Channel closed: wait for in-flight flush,
                                // then flush whatever remains in the buffer.
                                if let Some(handle) = flush_handle.take() && let Err(e) = handle.await {
                                    tracing::error!("Final flush task failed: {e:#}");
                                }
                                if !buf.is_empty() {
                                    let batch = buf.take();
                                    let store = store.clone();
                                    if let Err(e) = tokio::task::spawn_blocking(move || {
                                        let mut writer = store.open_writer()?;
                                        writer.flush(&batch)
                                    })
                                    .await
                                    {
                                        tracing::error!("Final flush failed: {e:#}");
                                    }
                                }
                                tracing::info!("Batch flusher channel closed.");
                                break;
                            }
                        }
                    }
                }
            }
        })
    }
}

/// Writes trades to DuckDB using the `Appender` API, with in-memory
/// buffering so that flushes happen in bulk.
pub struct BatchWriter {
    conn: Connection,
}

impl BatchWriter {
    /// Flush a batch of trades to the database using the DuckDB Appender.
    pub fn flush(&mut self, trades: &[AnnotatedTrade]) -> Result<()> {
        if trades.is_empty() {
            return Ok(());
        }

        let mut appender: Appender<'_> = self
            .conn
            .appender("trades")
            .context("creating DuckDB appender")?;

        for t in trades {
            appender
                .append_row((
                    &t.ticker.exchange.to_string(),
                    &t.ticker
                        .display_symbol()
                        .map(|s| s.to_lowercase())
                        .unwrap_or_else(|| t.ticker.to_string().to_lowercase()),
                    t.trade.time.as_u64() as i64,
                    t.trade.price.to_f64(),
                    t.trade.qty.to_f64(),
                    t.trade.is_sell,
                ))
                .context("appending row via DuckDB appender")?;
        }

        appender.flush().context("flushing DuckDB appender")?;
        Ok(())
    }
}

/// A cap-limited trade buffer with overflow warnings.
///
/// Drops incoming trades when the buffer exceeds `max` and emits a
/// rate-limited warning.  The warning is automatically cleared on the
/// next successful flush.
struct TradeBuffer {
    trades: Vec<AnnotatedTrade>,
    max: usize,
    warned: bool,
}

impl TradeBuffer {
    fn new(max: usize) -> Self {
        Self {
            trades: Vec::new(),
            max,
            warned: false,
        }
    }

    /// Try to append a trade, dropping it if the buffer is full.
    fn push(&mut self, trade: AnnotatedTrade) {
        if self.trades.len() < self.max {
            self.trades.push(trade);
        } else if !self.warned {
            tracing::warn!(
                "Trade buffer exceeded {} — dropping trades to protect against OOM. \
                 This warning is rate-limited.",
                self.max
            );
            self.warned = true;
        }
    }

    /// Clear the over-capacity warning (call after a successful flush).
    fn flush_succeeded(&mut self) {
        if self.warned {
            tracing::info!(
                "Trade buffer flushed; back within capacity (max {}).",
                self.max
            );
            self.warned = false;
        }
    }

    fn len(&self) -> usize {
        self.trades.len()
    }

    fn is_empty(&self) -> bool {
        self.trades.is_empty()
    }

    /// Drain the buffer, returning all accumulated trades.
    fn take(&mut self) -> Vec<AnnotatedTrade> {
        std::mem::take(&mut self.trades)
    }
}
