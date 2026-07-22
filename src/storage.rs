use anyhow::{Context, Result};
use axum::http::StatusCode;
use duckdb::Connection;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use flowsurface_exchange::adapter::{Event, Exchange};
use flowsurface_exchange::unit::{price::Price, qty::Qty};
use flowsurface_exchange::{Ticker, TickerInfo, UnixMs};

use crate::api::{AnnotatedTrade, TradeQuery};
use crate::config::{RetentionHours, StorageBytes};
use crate::stream;
use tokio::task::JoinHandle;

/// Tracked pair with the timestamp range stored.
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
    data_dir: PathBuf,
    start_instant: std::time::Instant,
    start_wall_ms: i64,
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
            data_dir: data_dir.to_path_buf(),
            start_instant: std::time::Instant::now(),
            start_wall_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
        })
    }

    /// Return the size (in bytes) of the main database file plus the
    /// WAL file.  If a file does not (yet) exist its size is counted as 0.
    pub fn current_storage_bytes(&self) -> Result<StorageBytes> {
        let db_path = self.data_dir.join("trades.duckdb");
        let wal_path = self.data_dir.join("trades.duckdb.wal");

        let db_size = match std::fs::metadata(&db_path) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => {
                return Err(e).with_context(|| format!("checking size of {}", db_path.display()));
            }
        };

        let wal_size = match std::fs::metadata(&wal_path) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => {
                return Err(e).with_context(|| format!("checking size of {}", wal_path.display()));
            }
        };

        Ok(StorageBytes::from_bytes(db_size + wal_size))
    }

    /// Return the total number of rows in the `trades` table.
    pub fn count_trades(&self) -> Result<u64> {
        let conn = self.connection()?;
        conn.query_row("SELECT COUNT(*) FROM trades", [], |r| r.get(0))
            .context("counting trades")
    }

    /// Delete the `batch_size` oldest trades (by `ts`) and return the
    /// number of rows actually deleted.
    pub fn delete_oldest_trades_batch(&self, batch_size: i64) -> Result<u64> {
        let conn = self.connection()?;
        let deleted = conn
            .execute(
                "DELETE FROM trades WHERE rowid IN (\
                     SELECT rowid FROM trades ORDER BY ts ASC LIMIT ?\
                 )",
                duckdb::params![batch_size],
            )
            .context("deleting oldest trades batch")?;
        Ok(deleted as u64)
    }

    /// Rewrite the database file to reclaim filesystem space freed by
    /// prior `DELETE` operations.  `CHECKPOINT` alone only merges the
    /// WAL — it does not shrink the main file.  `VACUUM` is O(n) in
    /// remaining rows, so callers should gate it behind a threshold.
    ///
    /// `VACUUM` requires exclusive table access, so it can fail with a
    /// transaction conflict if the batch flusher is mid-append.  This
    /// method retries a few times with short sleeps — the flusher's
    /// appender is only held open for a few milliseconds per flush, so
    /// a brief wait is almost always enough.
    pub fn vacuum(&self) -> Result<()> {
        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 0..MAX_RETRIES {
            let conn = self.connection()?;
            match conn.execute_batch("VACUUM;") {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_err = Some(anyhow::Error::new(e));
                    if attempt + 1 < MAX_RETRIES {
                        tracing::debug!(
                            "VACUUM attempt {}/{} failed (likely batch flusher \
                             holding appender); retrying in {RETRY_DELAY:?}",
                            attempt + 1,
                            MAX_RETRIES,
                        );
                        std::thread::sleep(RETRY_DELAY);
                    }
                }
            }
        }
        // last_err is always Some when MAX_RETRIES > 0 and the loop reaches
        // this point; the unwrap_or_else branch covers the edge case where
        // MAX_RETRIES is zero (the loop body never executes).
        Err(last_err.unwrap_or_else(|| {
            anyhow::anyhow!("VACUUM never attempted (MAX_RETRIES = {MAX_RETRIES})")
        }))
        .with_context(|| format!("vacuuming DuckDB database after {MAX_RETRIES} attempts"))
    }

    /// Merge the DuckDB WAL into the main database file, then truncate
    /// the WAL.  This prevents the `.wal` file from doubling the on-disk
    /// footprint after a bulk delete.
    pub fn run_checkpoint(&self) -> Result<()> {
        let conn = self.connection()?;
        conn.execute_batch("CHECKPOINT;")
            .context("checkpointing DuckDB WAL")?;
        Ok(())
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

    pub fn query_trades(&self, q: &TradeQuery) -> Result<Vec<AnnotatedTrade>> {
        let conn = self.connection()?;

        let limit = q.limit.unwrap_or(1000).min(10_000);
        let exchange = q.exchange_filter();

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

    /// Run a blocking DuckDB query on the blocking thread pool with a timeout.
    ///
    /// # Errors
    ///
    /// Returns `Err((StatusCode, &'static str))` suitable for `json_err` on:
    /// - Query failure (the closure returned `Err`)
    /// - Task panic (`spawn_blocking` panicked)
    /// - Timeout (the deadline elapsed)
    pub async fn run_blocking_query<T: Send + 'static>(
        self,
        deadline: Duration,
        label: &str,
        f: impl FnOnce(Storage) -> anyhow::Result<T> + Send + 'static,
    ) -> Result<T, (StatusCode, &'static str)> {
        match tokio::time::timeout(deadline, tokio::task::spawn_blocking(move || f(self))).await {
            Ok(Ok(Ok(data))) => Ok(data),
            Ok(Ok(Err(e))) => {
                tracing::error!("{label} query failed: {e:#}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, "internal error"))
            }
            Ok(Err(join_err)) => {
                tracing::error!("{label} query task panicked: {join_err:#}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, "internal error"))
            }
            Err(_) => {
                tracing::warn!("{label} query timed out after {deadline:?}");
                Err((StatusCode::SERVICE_UNAVAILABLE, "query timed out"))
            }
        }
    }

    /// Return a monotonically-increasing timestamp (ms since Unix epoch)
    /// that is immune to NTP jumps and manual clock changes after startup.
    ///
    /// Wall-clock time is captured once in [`open`](Self::open); all
    /// subsequent calls derive the timestamp from [`Instant::now`],
    /// which never regresses.
    pub(crate) fn now_ms(&self) -> i64 {
        self.start_wall_ms + self.start_instant.elapsed().as_millis() as i64
    }

    /// Delete every trade row whose `ts` (milliseconds since epoch) is
    /// older than `retention_hours`.  Returns the number of deleted rows.
    ///
    /// When `retention_hours` is `0` (unlimited) the purge is skipped
    /// and `Ok(0)` is returned immediately.
    pub fn purge_old_trades(&self, retention_hours: RetentionHours) -> Result<u64> {
        if retention_hours.as_hours() == 0 {
            return Ok(0);
        }
        let conn = self.connection()?;
        let cutoff_ms = self.now_ms() - retention_hours.as_millis();
        let deleted = conn
            .execute(
                "DELETE FROM trades WHERE ts < ?1",
                duckdb::params![cutoff_ms],
            )
            .context("purging old trades")?;
        Ok(deleted as u64)
    }

    pub fn record_cleanup(&self) -> Result<i64> {
        let now_ms = self.now_ms();
        self.set_metadata("last_cleanup", &now_ms.to_string())?;
        Ok(now_ms)
    }

    /// Spawn a background task that receives [`Event`]s on the
    /// persist channel, matches on variant, and persists each
    /// data type to its DuckDB table.
    ///
    /// - `TradesReceived` → buffered and flushed via `Appender` API.
    /// - `DepthReceived` → logged (future tables).
    pub fn spawn_batch_flusher(
        &self,
        rx: &mut stream::StreamReceivers,
        flush_interval: Duration,
        max_buffered_trades: usize,
    ) -> JoinHandle<()> {
        let mut rx = rx
            .persist
            .take()
            .expect("spawn_batch_flusher needs the persist receiver");
        let store = self.clone();

        tokio::spawn(async move {
            let mut flusher = BatchFlusher::new(max_buffered_trades);
            let mut flush_handle: Option<JoinHandle<anyhow::Result<()>>> = None;
            let mut interval = tokio::time::interval(flush_interval);
            interval.reset_immediately();

            loop {
                if flush_handle.as_ref().is_some_and(|h| h.is_finished()) {
                    match flush_handle.take() {
                        Some(handle) => match handle.await {
                            Ok(Ok(())) => flusher.flush_succeeded(),
                            Ok(Err(e)) => {
                                tracing::error!(
                                    "Batch flush failed ({} trades in new buffer, \
                                         failed batch dropped): {e:#}",
                                    flusher.pending_count()
                                );
                            }
                            Err(_) => {
                                tracing::error!("Batch flush task panicked or cancelled");
                            }
                        },
                        None => {
                            tracing::warn!(
                                "Flush handle was unexpectedly None after is_finished check"
                            );
                        }
                    }
                }

                tokio::select! {
                    biased;
                    _ = interval.tick() => {
                        // Non-blocking drain of everything the channel has
                        // queued since the last tick.
                        while let Ok(event) = rx.try_recv() {
                            flusher.ingest(event);
                        }

                        // Start a blocking flush if we have data and none
                        // is currently in-flight.
                        if flusher.has_pending() && flush_handle.is_none() {
                            let batch = flusher.take_all();
                            let store = store.clone();
                            flush_handle = Some(tokio::task::spawn_blocking(move || {
                                let mut writer = store.open_writer()?;
                                writer.flush_all(&batch)
                            }));
                        }
                    }
                    maybe = rx.recv() => {
                        match maybe {
                            Some(event) => {
                                flusher.ingest(event);
                            }
                            None => {
                                // Channel closed: wait for in-flight flush,
                                // then flush whatever remains in the buffer.
                                if let Some(handle) = flush_handle.take() && let Err(e) = handle.await {
                                    tracing::error!("Final flush task failed: {e:#}");
                                }
                                if flusher.has_pending() {
                                    let batch = flusher.take_all();
                                    let store = store.clone();
                                    if let Err(e) = tokio::task::spawn_blocking(move || {
                                        let mut writer = store.open_writer()?;
                                        writer.flush_all(&batch)
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

/// All market-data types that the flusher can persist to DuckDB.
#[derive(Clone)]
enum PersistableData {
    Trade(AnnotatedTrade),
}

/// Routes [`Event`]s into a single [`DataBuffer`], separating the
/// concern of "does this event get persisted?" from *how* it's buffered.
///
/// Events that don't need persistence (depth, kline, connect/disconnect)
/// are logged or ignored.
struct BatchFlusher {
    buf: DataBuffer,
}

impl BatchFlusher {
    fn new(max_items: usize) -> Self {
        Self {
            buf: DataBuffer::new(max_items),
        }
    }

    /// Route an [`Event`] into the buffer or log it.
    fn ingest(&mut self, event: Event) {
        match event {
            Event::TradesReceived(stream_kind, _tss, trades) => {
                let ticker_info = stream_kind.ticker_info();
                for ft_trade in trades.iter() {
                    self.buf.push(PersistableData::Trade(AnnotatedTrade::new(
                        ticker_info.ticker,
                        *ft_trade,
                    )));
                }
            }
            Event::DepthReceived(stream_kind, _update_t, _depth) => {
                tracing::trace!(?stream_kind, "Depth update received");
            }
            Event::KlineReceived(stream_kind, _kline) => {
                tracing::trace!(?stream_kind, "Kline update received");
            }
            Event::Connected(_) | Event::Disconnected(..) => {
                // Already logged upstream in EventOutlets::route.
            }
        }
    }

    fn has_pending(&self) -> bool {
        !self.buf.is_empty()
    }

    fn pending_count(&self) -> usize {
        self.buf.len()
    }

    fn take_all(&mut self) -> Vec<PersistableData> {
        self.buf.take()
    }

    fn flush_succeeded(&mut self) {
        self.buf.flush_succeeded();
    }
}

/// A cap-limited buffer for any [`PersistableData`],
/// with overflow warnings.
struct DataBuffer {
    items: Vec<PersistableData>,
    max: usize,
    warned: bool,
}

impl DataBuffer {
    fn new(max: usize) -> Self {
        Self {
            items: Vec::new(),
            max,
            warned: false,
        }
    }

    fn push(&mut self, item: PersistableData) {
        if self.items.len() < self.max {
            self.items.push(item);
        } else if !self.warned {
            tracing::warn!(
                "Data buffer exceeded {} — dropping items to protect against OOM. \
                 This warning is rate-limited.",
                self.max
            );
            self.warned = true;
        }
    }

    fn flush_succeeded(&mut self) {
        if self.warned {
            tracing::info!(
                "Data buffer flushed; back within capacity (max {}).",
                self.max
            );
            self.warned = false;
        }
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn take(&mut self) -> Vec<PersistableData> {
        std::mem::take(&mut self.items)
    }
}

/// Writes buffered market data to DuckDB using the `Appender` API.
///
/// Each [`PersistableData`] variant is dispatched to its own table.
pub struct BatchWriter {
    conn: Connection,
}

impl BatchWriter {
    /// Flush a batch of persistable items to their respective tables.
    fn flush_all(&mut self, items: &[PersistableData]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let mut trade_appender = self
            .conn
            .appender("trades")
            .context("creating DuckDB appender for trades")?;

        for item in items {
            match item {
                PersistableData::Trade(t) => {
                    trade_appender
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
                        .context("appending trade row via DuckDB appender")?;
                }
            }
        }

        trade_appender
            .flush()
            .context("flushing DuckDB trade appender")?;

        Ok(())
    }
}
