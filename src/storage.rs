use anyhow::{Context, Result};
use duckdb::{Appender, Connection};
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use flowsurface_exchange::UnixMs;
use flowsurface_exchange::unit::price::Price;
use flowsurface_exchange::unit::qty::Qty;

use crate::api::{AnnotatedTrade, TradeQuery};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// A trade aggregated to a price level (tick-aligned bucket).
#[derive(Debug, Clone, Serialize)]
pub struct GroupedTrade {
    pub price_level: f64,
    pub buy_volume: f64,
    pub sell_volume: f64,
    pub buy_count: i64,
    pub sell_count: i64,
    pub first_ts: i64,
    pub last_ts: i64,
}

/// A record stored in the `ticker_info` table, mirroring
/// `flowsurface_exchange::TickerInfo`'s tick-size / min-qty data.
#[derive(Debug, Clone)]
pub struct TickerInfoRecord {
    pub exchange: String,
    pub symbol: String,
    pub min_ticksize: i8,
    pub min_qty: i8,
    pub contract_size: Option<i8>,
}

/// Information about a tracked pair with the timestamp range stored.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PairInfo {
    pub exchange: String,
    pub symbol: String,
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

    // ── queries ──────────────────────────────────────────────────────

    /// Derive the canonical exchange string from raw `venue` + `market` strings.
    pub fn exchange_from_venue_market(venue: &str, market: &str) -> Option<String> {
        let venue_enum: flowsurface_exchange::adapter::Venue = venue.parse().ok()?;
        let market_enum: flowsurface_exchange::adapter::MarketKind = market.parse().ok()?;
        flowsurface_exchange::adapter::Exchange::from_venue_and_market(venue_enum, market_enum)
            .map(|ex| ex.to_string())
    }

    /// Derive an exchange filter string from a `TradeQuery`'s `venue` + `market`.
    fn exchange_filter(q: &TradeQuery) -> String {
        Self::exchange_from_venue_market(&q.venue, &q.market)
            .unwrap_or_else(|| format!("{}/{}", q.venue, q.market))
    }

    /// Query trades matching the given filter.
    pub fn query_trades(&self, q: &TradeQuery) -> Result<Vec<AnnotatedTrade>> {
        let conn = self.connection()?;

        let limit = q.limit.unwrap_or(1000).min(10_000);
        let exchange = Self::exchange_filter(q);

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

        sql.push_str(" ORDER BY ts ASC");
        sql.push_str(&format!(" LIMIT {}", limit));

        let mut stmt = conn.prepare(&sql).context("preparing trade query")?;

        // Collect parameters in order.
        let mut params: Vec<&dyn duckdb::ToSql> = vec![
            &q.symbol as &dyn duckdb::ToSql,
            &exchange as &dyn duckdb::ToSql,
        ];
        if let Some(ref from) = q.from {
            params.push(from as &dyn duckdb::ToSql);
        }
        if let Some(ref to) = q.to {
            params.push(to as &dyn duckdb::ToSql);
        }

        let rows = stmt.query_map(&params[..] as &[&dyn duckdb::ToSql], |row| {
            Ok(AnnotatedTrade {
                exchange: row.get(0)?,
                symbol: row.get(1)?,
                trade: flowsurface_exchange::Trade {
                    time: UnixMs::new(row.get::<_, i64>(2)? as u64),
                    is_sell: row.get(5)?,
                    price: Price::from_f64(row.get::<_, f64>(3)?),
                    qty: Qty::from_f64(row.get::<_, f64>(4)?),
                },
            })
        })?;

        let mut trades = Vec::new();
        for row in rows {
            trades.push(row?);
        }
        Ok(trades)
    }

    // ── ticker metadata ──────────────────────────────────────────────

    /// Persist ticker metadata for every resolved pair so the API can
    /// look up tick sizes at query time.
    pub fn store_ticker_infos(&self, infos: &[TickerInfoRecord]) -> Result<()> {
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
                info.exchange,
                info.symbol,
                info.min_ticksize,
                info.min_qty,
                info.contract_size,
            ])
            .with_context(|| {
                format!(
                    "inserting ticker_info for {}/{}",
                    info.exchange, info.symbol
                )
            })?;
        }
        Ok(())
    }

    /// Look up the stored ticker metadata for an exchange + symbol.
    pub fn get_ticker_info(
        &self,
        exchange: &str,
        symbol: &str,
    ) -> Result<Option<TickerInfoRecord>> {
        let conn = self.connection()?;

        let mut stmt = conn
            .prepare(
                "SELECT exchange, symbol, min_ticksize, min_qty, contract_size
                 FROM ticker_info
                 WHERE exchange = ? AND symbol = ?",
            )
            .context("preparing ticker_info lookup")?;

        let mut rows = stmt.query_map(duckdb::params![exchange, symbol], |row| {
            Ok(TickerInfoRecord {
                exchange: row.get(0)?,
                symbol: row.get(1)?,
                min_ticksize: row.get(2)?,
                min_qty: row.get(3)?,
                contract_size: row.get(4)?,
            })
        })?;

        match rows.next() {
            Some(Ok(record)) => Ok(Some(record)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    // ── grouped query ────────────────────────────────────────────────

    /// Query trades aggregated by price level (tick-aligned buckets).
    ///
    /// `step_size` is the bucket width in price units (e.g. `0.01` for a
    /// 1-tick group of a USDT pair with tick size 0.01).  The caller is
    /// responsible for deriving this from `TickerInfo.min_ticksize` and
    /// any desired step multiplier.
    ///
    /// `price_precision` is the number of decimal places to round the
    /// resulting `price_level` to, derived from `TickerInfo.min_ticksize`
    /// (e.g. `1` for a tick size of `0.1`).  This prevents floating-point
    /// noise in the bucket label.
    ///
    /// `qty_precision` is the number of decimal places to round volume
    /// aggregates to, derived from `TickerInfo.min_qty` (e.g. `3` for a
    /// pair whose minimum qty step is `0.001`).  This eliminates the
    /// floating-point noise that accumulates when summing f64 quantities.
    pub fn query_grouped_trades(
        &self,
        q: &TradeQuery,
        step_size: f64,
        price_precision: u32,
        qty_precision: u32,
    ) -> Result<Vec<GroupedTrade>> {
        let conn = self.connection()?;

        let limit = q.limit.unwrap_or(1000).min(10_000);
        let exchange = Self::exchange_filter(q);

        let mut sql = String::from(
            "SELECT
                ROUND(FLOOR(price / ?) * ?, ?) AS price_level,
                ROUND(COALESCE(SUM(CASE WHEN NOT is_sell THEN qty ELSE 0 END), 0.0), ?) AS buy_volume,
                ROUND(COALESCE(SUM(CASE WHEN is_sell THEN qty ELSE 0 END), 0.0), ?)     AS sell_volume,
                SUM(CASE WHEN NOT is_sell THEN 1 ELSE 0 END) AS buy_count,
                SUM(CASE WHEN is_sell THEN 1 ELSE 0 END)     AS sell_count,
                MIN(ts)                        AS first_ts,
                MAX(ts)                        AS last_ts
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

        sql.push_str(" GROUP BY price_level ORDER BY last_ts DESC, price_level ASC");
        sql.push_str(&format!(" LIMIT {}", limit));

        let mut stmt = conn
            .prepare(&sql)
            .context("preparing grouped trade query")?;

        let step = step_size;
        let price_prec = price_precision as i32;
        let qty_prec = qty_precision as i32;
        let mut params: Vec<&dyn duckdb::ToSql> = vec![
            &step as &dyn duckdb::ToSql,
            &step as &dyn duckdb::ToSql,
            &price_prec as &dyn duckdb::ToSql,
            &qty_prec as &dyn duckdb::ToSql,
            &qty_prec as &dyn duckdb::ToSql,
            &q.symbol as &dyn duckdb::ToSql,
            &exchange as &dyn duckdb::ToSql,
        ];
        if let Some(ref from) = q.from {
            params.push(from);
        }
        if let Some(ref to) = q.to {
            params.push(to);
        }

        let rows = stmt.query_map(&params[..], |row| {
            Ok(GroupedTrade {
                price_level: row.get(0)?,
                buy_volume: row.get(1)?,
                sell_volume: row.get(2)?,
                buy_count: row.get(3)?,
                sell_count: row.get(4)?,
                first_ts: row.get(5)?,
                last_ts: row.get(6)?,
            })
        })?;

        let mut trades = Vec::new();
        for row in rows {
            trades.push(row?);
        }
        Ok(trades)
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
            Ok(PairInfo {
                exchange: row.get(0)?,
                symbol: row.get(1)?,
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
        let cutoff_ms = chrono_now_ms() - (retention_hours as i64 * 3_600_000);
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

    /// Record that cleanup ran just now.
    pub fn record_cleanup(&self) -> Result<()> {
        let now_ms = chrono_now_ms();
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
        let now_ms = chrono_now_ms();

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

    /// Spawn a background task that receives trades on `rx`, buffers them,
    /// and flushes to DuckDB via the Appender API every `flush_interval`.
    ///
    /// Uses the shared `duckdb_database` from `storage` so that flushed
    /// trades are immediately visible to reader connections.
    pub fn spawn_batch_flusher(
        &self,
        mut rx: mpsc::Receiver<AnnotatedTrade>,
        shutdown: CancellationToken,
        flush_interval: Duration,
    ) -> JoinHandle<()> {
        let storage = self.clone();
        tokio::spawn(async move {
            let mut buffer: Vec<AnnotatedTrade> = Vec::new();

            let mut interval = tokio::time::interval(flush_interval);
            interval.reset_immediately();

            let mut writer = match storage.open_writer() {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!("Failed to open batch writer: {e:#}");
                    return;
                }
            };

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        if !buffer.is_empty() && let Err(e) = writer.flush(&buffer) {
                            tracing::error!("Final flush failed: {e:#}");
                        }
                        tracing::info!("Batch flusher shut down.");
                        break;
                    }
                    maybe = rx.recv() => {
                        match maybe {
                            Some(trade) => buffer.push(trade),
                            None => {
                                if !buffer.is_empty() && let Err(e) = writer.flush(&buffer) {
                                    tracing::error!("Final flush failed: {e:#}");
                                }
                                tracing::info!("Batch flusher channel closed.");
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        if !buffer.is_empty() {
                            if let Err(e) = writer.flush(&buffer) {
                                tracing::error!("Batch flush failed: {e:#}");
                            }
                            buffer.clear();
                        }
                    }
                }
            }
        })
    }
}

/// Millisecond timestamp suitable for retention calculations (UTC-based,
/// monotonic-adjacent — uses `std::time::SystemTime`).
fn chrono_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Writes trades to DuckDB using the `Appender` API, with in-memory
/// buffering so that flushes happen in bulk.
pub struct BatchWriter {
    conn: Connection,
}

impl BatchWriter {
    /// Flush a batch of trades to the database using the DuckDB Appender.
    /// The Appender is created fresh for each flush so we don't fight
    /// Rust's borrow checker (Appender borrows Connection).
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
                .append_row([
                    &t.exchange as &dyn duckdb::ToSql,
                    &t.symbol as &dyn duckdb::ToSql,
                    &(t.trade.time.as_u64() as i64) as &dyn duckdb::ToSql,
                    &t.trade.price.to_f64() as &dyn duckdb::ToSql,
                    &t.trade.qty.to_f64() as &dyn duckdb::ToSql,
                    &t.trade.is_sell as &dyn duckdb::ToSql,
                ])
                .context("appending row via DuckDB appender")?;
        }

        appender.flush().context("flushing DuckDB appender")?;
        // Appender drops here, implicitly committing.
        Ok(())
    }
}
