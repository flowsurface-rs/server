use parking_lot::Mutex;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use axum_server::Handle;
use axum_server::tls_rustls::RustlsConfig;
use flowsurface_exchange::{
    Ticker, Trade,
    adapter::{Exchange, MarketKind, Venue},
};
use hyper_util::rt::TokioTimer;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{Semaphore, mpsc};

use crate::{
    config::BearerToken,
    diagnostics::{Diagnostics, DiagnosticsSnapshot},
    limiter::{
        AdmissionGate, ConnectionLimiter, LimiterAcceptor, MAX_CONCURRENT_CONNECTIONS, RateLimiter,
    },
    storage::{PairInfo, QueryCancellation, Storage},
};

/// Static body for 503 responses from the admission gate.
static GATE_BUDGET_EXHAUSTED: &str = r#"{"error":"server busy, try again later"}"#;

/// Static body for 429 responses
static RATE_LIMITED: &str = r#"{"error":"rate limit exceeded, slow down"}"#;

/// Static body for 503 responses when the query concurrency cap is hit.
static QUERY_BUSY: &str = r#"{"error":"server busy, too many concurrent requests"}"#;

#[derive(Serialize)]
#[serde(untagged)]
enum Response {
    Pairs {
        pairs: Vec<PairInfo>,
        tracked_count: usize,
    },
    Trades {
        trades: Vec<AnnotatedTrade>,
    },
}

/// Pre-serialised response bodies for endpoints whose output is either
/// immutable or infrequently changing.
pub struct CachedResponses {
    /// `/exchanges` — computed once at startup, never changes.
    exchanges: String,
}

const DATABASE_HEALTH_CACHE_TTL: Duration = Duration::from_secs(1);
const DATABASE_HEALTH_TIMEOUT: Duration = Duration::from_secs(1);

struct DatabaseHealth {
    last_result: Mutex<Option<DatabaseHealthResult>>,
    probe_semaphore: Arc<Semaphore>,
    cache_ttl: Duration,
    probe_timeout: Duration,
}

struct DatabaseHealthResult {
    checked_at: Instant,
    ok: bool,
}

impl DatabaseHealth {
    fn new() -> Self {
        Self::with_limits(DATABASE_HEALTH_CACHE_TTL, DATABASE_HEALTH_TIMEOUT)
    }

    fn with_limits(cache_ttl: Duration, probe_timeout: Duration) -> Self {
        Self {
            last_result: Mutex::new(None),
            probe_semaphore: Arc::new(Semaphore::new(1)),
            cache_ttl,
            probe_timeout,
        }
    }

    async fn check<F>(&self, probe: F) -> bool
    where
        F: FnOnce() -> bool + Send + 'static,
    {
        let deadline = Instant::now() + self.probe_timeout;
        if let Some(result) = self.fresh_result() {
            return result;
        }

        let permit = match tokio::time::timeout(
            remaining_time(deadline),
            Arc::clone(&self.probe_semaphore).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            _ => return self.fresh_result().unwrap_or(false),
        };

        if let Some(result) = self.fresh_result() {
            drop(permit);
            return result;
        }

        let task = tokio::task::spawn_blocking(move || {
            let result = probe();
            // Keep the permit in the blocking task until the result is ready.
            (permit, result)
        });
        let result = match tokio::time::timeout(remaining_time(deadline), task).await {
            Ok(Ok((permit, result))) => {
                self.last_result.lock().replace(DatabaseHealthResult {
                    checked_at: Instant::now(),
                    ok: result,
                });
                drop(permit);
                return result;
            }
            Ok(Err(error)) => {
                tracing::warn!("Database health check task failed: {error:#}");
                false
            }
            Err(_) => false,
        };

        self.last_result.lock().replace(DatabaseHealthResult {
            checked_at: Instant::now(),
            ok: result,
        });
        result
    }

    fn fresh_result(&self) -> Option<bool> {
        let result = self.last_result.lock();
        result
            .as_ref()
            .and_then(|result| (result.checked_at.elapsed() < self.cache_ttl).then_some(result.ok))
    }
}

fn remaining_time(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

/// A normalized trade record, used both in-memory and serialized to JSON.
#[derive(Debug, Clone)]
pub struct AnnotatedTrade {
    pub ticker: Ticker,
    pub trade: Trade,
}

pub(crate) const DEFAULT_ARROW_LIMIT: usize = 50_000;
pub(crate) const MAX_ARROW_LIMIT: usize = 1_000_000;
const MIN_ARROW_LIMIT: usize = 50_000;
const ROWS_PER_MEMORY_MB: u64 = 2_500;
const MEMORY_MB_PER_QUERY: u64 = 512;
const DEFAULT_QUERY_CONCURRENCY: usize = 4;
const MAX_QUERY_CONCURRENCY: usize = 4;
const ARROW_CHUNK_SIZE: usize = 256 * 1024;
const ARROW_CHANNEL_CAPACITY: usize = 2;
const ARROW_START_TIMEOUT: Duration = Duration::from_secs(20);

type ArrowBodyItem = Result<Vec<u8>, io::Error>;

struct ArrowChunkWriter {
    sender: mpsc::Sender<ArrowBodyItem>,
    buffer: Vec<u8>,
    sent_bytes: usize,
}

impl ArrowChunkWriter {
    fn new(sender: mpsc::Sender<ArrowBodyItem>) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(ARROW_CHUNK_SIZE),
            sent_bytes: 0,
        }
    }

    fn send_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let chunk = std::mem::replace(&mut self.buffer, Vec::with_capacity(ARROW_CHUNK_SIZE));
        self.sent_bytes += chunk.len();
        self.sender
            .blocking_send(Ok(chunk))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Arrow client disconnected"))
    }
}

impl Write for ArrowChunkWriter {
    fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
        let written = bytes.len();
        while !bytes.is_empty() {
            let available = ARROW_CHUNK_SIZE - self.buffer.len();
            let copied = available.min(bytes.len());
            self.buffer.extend_from_slice(&bytes[..copied]);
            bytes = &bytes[copied..];

            if self.buffer.len() == ARROW_CHUNK_SIZE {
                self.send_buffer()?;
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.send_buffer()
    }
}

struct ArrowBodyState {
    first: Option<ArrowBodyItem>,
    receiver: mpsc::Receiver<ArrowBodyItem>,
    cancellation: Arc<QueryCancellation>,
}

impl ArrowBodyState {
    fn new(receiver: mpsc::Receiver<ArrowBodyItem>, cancellation: Arc<QueryCancellation>) -> Self {
        Self {
            first: None,
            receiver,
            cancellation,
        }
    }

    async fn receive(&mut self) -> Option<ArrowBodyItem> {
        self.receiver.recv().await
    }

    fn start_with(&mut self, first: Vec<u8>) {
        self.first = Some(Ok(first));
    }

    fn into_stream(self) -> impl futures::Stream<Item = ArrowBodyItem> + Send + 'static {
        futures::stream::unfold(self, |mut state| async move {
            let item = match state.first.take() {
                Some(item) => Some(item),
                None => state.receiver.recv().await,
            };
            item.map(|item| (item, state))
        })
    }
}

impl Drop for ArrowBodyState {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryBudget {
    pub max_arrow_rows: usize,
    pub concurrency: usize,
}

/// Derive response and concurrency limits from the configured DuckDB budget.
/// An unset budget keeps the high-memory defaults for compatibility.
pub fn query_budget(memory_limit_mb: u64) -> QueryBudget {
    if memory_limit_mb == 0 {
        return QueryBudget {
            max_arrow_rows: MAX_ARROW_LIMIT,
            concurrency: DEFAULT_QUERY_CONCURRENCY,
        };
    }

    let max_arrow_rows = memory_limit_mb
        .saturating_mul(ROWS_PER_MEMORY_MB)
        .clamp(MIN_ARROW_LIMIT as u64, MAX_ARROW_LIMIT as u64) as usize;
    let concurrency = memory_limit_mb
        .saturating_div(MEMORY_MB_PER_QUERY)
        .clamp(1, MAX_QUERY_CONCURRENCY as u64) as usize;

    QueryBudget {
        max_arrow_rows,
        concurrency,
    }
}

impl AnnotatedTrade {
    pub fn new(ticker: Ticker, trade: Trade) -> Self {
        AnnotatedTrade { ticker, trade }
    }
}

impl Serialize for AnnotatedTrade {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("Trade", 4)?;
        s.serialize_field("ts", &self.trade.time)?;
        s.serialize_field("price", &self.trade.price.to_f64())?;
        s.serialize_field("qty", &self.trade.qty.to_f64())?;
        s.serialize_field("is_sell", &self.trade.is_sell)?;
        s.end()
    }
}

/// Query parameters shared by the `GET /trades` (JSON) and
/// `GET /trades.arrow` endpoints.
#[derive(Debug, Deserialize)]
pub struct TradeQuery {
    /// Venue filter, e.g. "binance" (used with `market` to derive exchange).
    pub venue: String,
    /// Symbol filter.
    pub symbol: String,
    /// Market filter: "spot", "linear", or "inverse" (used with `venue`).
    pub market: String,
    /// Inclusive lower bound (milliseconds since epoch). Optional.
    pub from: Option<i64>,
    /// Inclusive upper bound (milliseconds since epoch). Optional.
    pub to: Option<i64>,
    /// Maximum number of records to return.
    ///
    /// Endpoint-dependent caps:
    /// - `/trades` (JSON): default `1000`, max `10_000`.
    /// - `/trades.arrow`: default `50_000`, max depends on the memory budget
    ///   and is capped at `1_000_000`.
    pub limit: Option<usize>,
}

impl TradeQuery {
    /// Validate that `venue` and `market` parse into known enum variants.
    /// Returns a human-readable error string on failure.
    pub fn validate(&self) -> Result<(), String> {
        let _venue: Venue = self
            .venue
            .parse()
            .map_err(|_| format!("unknown venue: '{}'", self.venue))?;
        let _market: MarketKind = self.market.parse().map_err(|_| {
            format!(
                "unknown market kind: '{}' (expected spot, linear, or inverse)",
                self.market
            )
        })?;
        Ok(())
    }

    /// Derive an exchange filter string from a `TradeQuery`'s `venue` + `market`.
    pub fn exchange_filter(self: &TradeQuery) -> String {
        exchange_from_venue_market(&self.venue, &self.market)
            .unwrap_or_else(|| format!("{}/{}", self.venue, self.market))
    }
}

/// Derive the canonical exchange string from raw `venue` + `market` strings.
pub fn exchange_from_venue_market(venue: &str, market: &str) -> Option<String> {
    let venue_enum: Venue = venue.parse().ok()?;
    let market_enum: MarketKind = market.parse().ok()?;

    Exchange::from_venue_and_market(venue_enum, market_enum).map(|ex| ex.to_string())
}

pub struct Server {
    pub storage: Storage,
    pub auth_token: Option<BearerToken>,
    pub diagnostics: Arc<Diagnostics>,
    /// The tickers configured at startup.
    /// Used by `/pairs` to include pairs that have not yet received trades.
    pub configured_pairs: Vec<Ticker>,
    /// TLS configuration for the HTTPS server (self-signed).
    /// `None` on loopback addresses (plain HTTP), `Some` for remote binds.
    pub tls_config: Option<RustlsConfig>,
    /// Per-IP rate limiter.  `None` when rate limiting is disabled.
    pub rate_limiter: Option<RateLimiter>,
    /// Admission gate that throttles unknown IPs to a small global budget.
    pub admission_gate: AdmissionGate,
    /// Connection‑level cap that prevents TCP/TLS connection‑storm.
    pub connection_limiter: ConnectionLimiter,
    /// Semaphore capping concurrent in-flight DB-backed response queries
    /// (`/trades`, `/trades.arrow`). JSON output is materialised in the Rust
    /// heap, while Arrow output uses a bounded stream outside DuckDB's
    /// `memory_limit`. The permit remains held for the complete query.
    pub query_semaphore: Arc<Semaphore>,
    /// Memory-derived limits for database-backed HTTP queries.
    pub query_budget: QueryBudget,
    /// Cached pre-serialised response body for `/exchanges`.
    pub cached: CachedResponses,
    database_health: DatabaseHealth,
}

impl Server {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Storage,
        auth_token: Option<BearerToken>,
        configured_pairs: Vec<Ticker>,
        available_tickers: &FxHashMap<String, Vec<String>>,
        diagnostics: Arc<Diagnostics>,
        tls_config: Option<RustlsConfig>,
        rate_limiter: Option<RateLimiter>,
        admission_gate: AdmissionGate,
        query_budget: QueryBudget,
    ) -> Self {
        // Pre-serialise the /exchanges response — available tickers are
        // immutable after startup, so we compute this once and avoid
        // cloning + serialising the HashMap on every request.
        let cached_exchanges_json =
            serde_json::to_string(&serde_json::json!({ "exchanges": available_tickers }))
                .unwrap_or_else(|_| r#"{"exchanges":{}}"#.to_string());

        let connection_limiter = ConnectionLimiter::new(MAX_CONCURRENT_CONNECTIONS);

        Self {
            storage,
            auth_token,
            diagnostics,
            configured_pairs,
            tls_config,
            rate_limiter,
            admission_gate,
            connection_limiter,
            query_semaphore: Arc::new(Semaphore::new(query_budget.concurrency)),
            query_budget,
            cached: CachedResponses {
                exchanges: cached_exchanges_json,
            },
            database_health: DatabaseHealth::new(),
        }
    }

    /// Check whether the request carries a valid Bearer token.
    /// Returns `Ok(())` if no auth is configured or the token matches.
    fn check_auth(
        &self,
        headers: &HeaderMap,
        peer_addr: SocketAddr,
    ) -> Result<(), (StatusCode, &'static str)> {
        let Some(ref expected_token) = self.auth_token else {
            return Ok(());
        };

        let provided = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if !expected_token.is_valid_authorization(provided) {
            let token_len = provided.len();
            tracing::warn!(
                "Auth failure from {}: expected valid Bearer token (received header of {token_len} bytes)",
                peer_addr.ip(),
            );
            return Err((StatusCode::UNAUTHORIZED, "missing or invalid auth token"));
        }

        Ok(())
    }

    /// Build a JSON `200 OK` response from any serializable value.
    fn json_ok<T: Serialize>(data: &T) -> axum::response::Response {
        match serde_json::to_string(data) {
            Ok(json) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                json,
            )
                .into_response(),
            Err(e) => {
                tracing::error!("Failed to serialise response: {e:#}");
                Self::json_err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to serialise response",
                )
            }
        }
    }

    /// Build a JSON error response with the given status code.
    fn json_err(status: StatusCode, msg: &str) -> axum::response::Response {
        (
            status,
            [(header::CONTENT_TYPE, "application/json")],
            format!("{{\"error\":\"{msg}\"}}"),
        )
            .into_response()
    }

    /// Acquire a permit for a DB-backed query handler, or `None` when the
    /// concurrency cap is saturated. Non-blocking: refuse rather than queue.
    /// Callers move the permit into the blocking task so timeouts do not
    /// release capacity while the query is still running.
    fn try_acquire_query(state: &Arc<Self>) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&state.query_semaphore).try_acquire_owned().ok()
    }

    /// `503` response used when the query concurrency cap is hit.
    fn query_busy() -> axum::response::Response {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            QUERY_BUSY,
        )
            .into_response()
    }

    async fn database_is_healthy(&self) -> bool {
        let storage = self.storage.clone();
        self.database_health
            .check(move || match storage.health_check() {
                Ok(()) => true,
                Err(error) => {
                    tracing::warn!("Database health check failed: {error:#}");
                    false
                }
            })
            .await
    }

    fn start_arrow_export(
        storage: Storage,
        query: TradeQuery,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> ArrowBodyState {
        let (sender, receiver) = mpsc::channel(ARROW_CHANNEL_CAPACITY);
        let cancellation = Arc::new(QueryCancellation::new());
        let worker_cancellation = Arc::clone(&cancellation);
        let error_sender = sender.clone();

        let worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut output = ArrowChunkWriter::new(sender);
            let result =
                storage.write_trades_arrow_ipc(&query, &mut output, worker_cancellation.as_ref());
            result.map(|stats| (stats, output.sent_bytes))
        });

        let completion = Arc::clone(&cancellation);
        tokio::spawn(async move {
            match worker.await {
                Ok(Ok((stats, bytes))) => {
                    tracing::debug!(
                        "Streamed {bytes} bytes of Arrow IPC data ({} rows in {} batches)",
                        stats.rows,
                        stats.batches,
                    );
                }
                Ok(Err(error)) if completion.is_cancelled() => {
                    tracing::debug!("Arrow export cancelled: {error:#}");
                }
                Ok(Err(error)) => {
                    tracing::error!("Arrow export failed: {error:#}");
                    let _ = error_sender
                        .send(Err(io::Error::other("Arrow export failed")))
                        .await;
                }
                Err(error) if completion.is_cancelled() => {
                    tracing::debug!("Arrow export task cancelled: {error:#}");
                }
                Err(error) => {
                    tracing::error!("Arrow export task panicked: {error:#}");
                    let _ = error_sender
                        .send(Err(io::Error::other("Arrow export task failed")))
                        .await;
                }
            }
            completion.complete();
        });

        ArrowBodyState::new(receiver, cancellation)
    }

    /// GET /status  (public — no auth required)
    ///
    /// Returns server uptime and a basic DB connectivity check.
    /// Suitable for load-balancer / container health probes.
    async fn status(State(state): State<Arc<Self>>) -> impl IntoResponse {
        let uptime = state.diagnostics.uptime().as_secs();
        let db_ok = state.database_is_healthy().await;
        let body = match serde_json::to_string(&serde_json::json!({
            "status": "ok",
            "uptime_secs": uptime,
            "db_ok": db_ok,
        })) {
            Ok(json) => json,
            Err(e) => {
                tracing::error!("Failed to serialise status: {e:#}");
                return Self::json_err(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
        };

        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response()
    }

    /// GET /diagnostics (authenticated)
    ///
    /// Returns the current feed and ingestion pipeline state. The endpoint
    /// remains available while the server is degraded so its response can
    /// explain the failure.
    async fn diagnostics(State(state): State<Arc<Self>>) -> impl IntoResponse {
        let database_ok = state.database_is_healthy().await;
        let snapshot: DiagnosticsSnapshot = state.diagnostics.snapshot(database_ok);
        Self::json_ok(&snapshot)
    }

    /// GET /exchanges
    ///
    /// Returns all available ticker symbols per exchange, as discovered from
    /// the exchange APIs at startup.  Useful for discovering the correct suffix
    /// patterns when configuring `config.toml`.
    ///
    /// The response body is pre-serialised once at startup (the data is
    /// immutable), so this handler only does a cheap `String` clone.
    async fn exchanges(State(state): State<Arc<Self>>) -> impl IntoResponse {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            state.cached.exchanges.clone(),
        )
            .into_response()
    }

    /// GET /pairs
    ///
    /// Returns all configured pairs (from the startup config), enriched with
    /// earliest/latest timestamps from the database when data exists.
    /// Pairs that have been configured but have not yet received any trades
    /// appear with `earliest: null` / `latest: null`.
    async fn pairs(State(state): State<Arc<Self>>) -> impl IntoResponse {
        let storage = state.storage.clone();

        let db_pairs = match storage
            .run_blocking_query(Duration::from_secs(10), "pairs", |s| s.pairs_with_bounds())
            .await
        {
            Ok(pairs) => pairs,
            Err((status, msg)) => return Self::json_err(status, msg),
        };

        // Build a lookup keyed by "exchange:symbol" from DB results.
        let mut by_key: FxHashMap<String, &PairInfo> = FxHashMap::default();
        for p in &db_pairs {
            let ex_str = p.ticker.exchange.to_string();
            let sym_str = p.ticker.to_string().to_lowercase();
            by_key.insert(format!("{ex_str}:{sym_str}"), p);
        }

        // Merge: every configured pair gets a PairInfo; fill in DB bounds when available.
        let mut merged: Vec<PairInfo> = Vec::with_capacity(state.configured_pairs.len());
        for ticker in &state.configured_pairs {
            let ex_str = ticker.exchange.to_string();
            let sym_str = ticker
                .display_symbol()
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| ticker.to_string().to_lowercase());
            let key = format!("{ex_str}:{sym_str}");
            match by_key.get(&key) {
                Some(found) => merged.push(*(*found)),
                None => merged.push(PairInfo {
                    ticker: *ticker,
                    earliest: None,
                    latest: None,
                }),
            }
        }

        let count = merged.len();
        Self::json_ok(&Response::Pairs {
            pairs: merged,
            tracked_count: count,
        })
    }

    /// GET /trades
    async fn trades(State(state): State<Arc<Self>>, query: Query<TradeQuery>) -> impl IntoResponse {
        if let Err(msg) = query.validate() {
            return Self::json_err(StatusCode::BAD_REQUEST, &msg);
        }

        let storage = state.storage.clone();

        let Some(permit) = Self::try_acquire_query(&state) else {
            return Self::query_busy();
        };

        let q = query.0;
        match storage
            .run_blocking_query(Duration::from_secs(10), "trades", move |s| {
                let _permit = permit;
                s.query_trades(&q)
            })
            .await
        {
            Ok(trades) => Self::json_ok(&Response::Trades { trades }),
            Err((status, msg)) => Self::json_err(status, msg),
        }
    }

    /// GET /trades.arrow
    ///
    /// Returns trades as an **Arrow IPC stream**, exported directly from
    /// DuckDB via Arrow export.
    ///
    /// The Arrow IPC streaming format uses 4 columns:
    /// `ts (int64)`, `price (float64)`,
    /// `qty (float64)`, `is_sell (bool)`.
    ///
    /// This endpoint uses parameterised queries internally (safe from SQL
    /// injection).
    async fn trades_arrow(
        State(state): State<Arc<Self>>,
        query: Query<TradeQuery>,
    ) -> axum::response::Response {
        if let Err(msg) = query.validate() {
            return Server::json_err(StatusCode::BAD_REQUEST, &msg);
        }

        let limit = query
            .limit
            .unwrap_or(DEFAULT_ARROW_LIMIT)
            .min(state.query_budget.max_arrow_rows);
        let mut bounded = query.0;
        bounded.limit = Some(limit);

        let storage = state.storage.clone();

        let Some(permit) = Self::try_acquire_query(&state) else {
            return Self::query_busy();
        };

        let mut body_state = Self::start_arrow_export(storage, bounded, permit);
        let first_chunk =
            match tokio::time::timeout(ARROW_START_TIMEOUT, body_state.receive()).await {
                Ok(Some(Ok(chunk))) => chunk,
                Ok(Some(Err(error))) => {
                    tracing::error!("Arrow export failed before response started: {error:#}");
                    return Server::json_err(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
                }
                Ok(None) => {
                    tracing::error!("Arrow export ended before producing an IPC stream");
                    return Server::json_err(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
                }
                Err(_) => {
                    tracing::warn!("Arrow export did not start within {ARROW_START_TIMEOUT:?}");
                    return Server::json_err(StatusCode::SERVICE_UNAVAILABLE, "query timed out");
                }
            };
        body_state.start_with(first_chunk);

        (
            StatusCode::OK,
            [
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/vnd.apache.arrow.stream",
                ),
                (
                    axum::http::header::CONTENT_DISPOSITION,
                    "attachment; filename=\"trades.arrow\"",
                ),
            ],
            Body::from_stream(body_state.into_stream()),
        )
            .into_response()
    }

    /// Bind to `bind_address` and spawn the axum HTTP(S) server.
    ///
    /// Uses plain HTTP for loopback addresses, HTTPS with a self-signed
    /// certificate for non-loopback (remote) binds.  Exits on bind failure.
    ///
    /// Middleware execution order (outermost → innermost):
    ///
    ///   1. **Priority gate** — throttles unknown IPs to a tiny budget
    ///      ([`crate::limiter::ADMISSION_PER_IP_BUDGET`] req/s per IP,
    ///      [`crate::limiter::ADMISSION_GLOBAL_CAP`] req/s total) so scanners
    ///      consume almost no CPU.
    ///   2. **Auth** — Bearer token verification for protected routes.
    ///   3. **Per-IP rate limiter** — token-bucket per IP (default 500 req/10s).
    ///      Only reached by authenticated requests — unauthenticated requests are
    ///      rejected by auth first, so they never consume rate-limiter bookkeeping.
    ///
    /// `/status` is public (no auth) but still goes through the priority gate.
    /// Its database probe is cached and bounded so health checks stay cheap.
    ///
    /// **Connection‑level DoS protection:** both the TLS and plain‑HTTP paths
    /// use `axum_server` with a [`LimiterAcceptor`] that caps concurrent
    /// connections at [`MAX_CONCURRENT_CONNECTIONS`] (default 512).  Once the
    /// ceiling is hit, new connections are dropped immediately (TCP reset).
    pub async fn serve(
        self: Arc<Self>,
        bind_address: SocketAddr,
    ) -> (tokio::task::JoinHandle<()>, Handle<std::net::SocketAddr>) {
        let tls_config = self.tls_config.clone();
        let limiter = self.connection_limiter.clone();

        // Public route — /status is rate-gated but NOT auth-gated.
        let public = Router::new()
            .route("/status", get(Server::status))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                priority_gate_middleware,
            ))
            .with_state(self.clone());

        // Protected routes — require auth.
        // Layer order: outermost = priority gate, then auth,
        // then rate limiter (innermost).  Rate limiter is after auth
        // so unauthenticated requests never consume rate-limit state.
        let protected = Router::new()
            .route("/exchanges", get(Server::exchanges))
            .route("/pairs", get(Server::pairs))
            .route("/diagnostics", get(Server::diagnostics))
            .route("/trades", get(Server::trades))
            .route("/trades.arrow", get(Server::trades_arrow))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                rate_limit_middleware,
            ))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                auth_middleware,
            ))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                priority_gate_middleware,
            ))
            .with_state(self);

        let router = public.merge(protected);

        let handle = Handle::new();
        let handle_for_server = handle.clone();

        let join_handle = tokio::spawn(async move {
            // Bind synchronously for both paths — axum_server needs a
            // std::net::TcpListener regardless of TLS.
            let std_listener = match std::net::TcpListener::bind(bind_address) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(
                        "Failed to bind {bind_address}: {e:#}. \
                         Server will not accept connections."
                    );
                    return;
                }
            };
            let _ = std_listener.set_nonblocking(true);

            let make_svc = router.into_make_service_with_connect_info::<SocketAddr>();

            let result = if let Some(cfg) = tls_config {
                tracing::info!("Starting HTTPS API on {bind_address}");
                let mut server = axum_server::tls_rustls::from_tcp_rustls(std_listener, cfg)
                    .expect("failed to create TLS server from listener")
                    .handle(handle_for_server.clone())
                    .map(|acceptor| LimiterAcceptor {
                        inner: acceptor,
                        limiter: limiter.clone(),
                    });

                server
                    .http_builder()
                    .http1()
                    .timer(TokioTimer::new())
                    .header_read_timeout(Duration::from_secs(10))
                    .max_headers(100)
                    .keep_alive(false);

                server
                    .http_builder()
                    .http2()
                    .timer(TokioTimer::new())
                    .keep_alive_interval(Some(Duration::from_secs(30)))
                    .keep_alive_timeout(Duration::from_secs(5));

                server.serve(make_svc).await
            } else {
                tracing::info!("Starting HTTP API on {bind_address}");
                let mut server = axum_server::from_tcp(std_listener)
                    .expect("failed to create HTTP server from listener")
                    .handle(handle_for_server.clone())
                    .map(|acceptor| LimiterAcceptor {
                        inner: acceptor,
                        limiter: limiter.clone(),
                    });

                server
                    .http_builder()
                    .http1()
                    .timer(TokioTimer::new())
                    .header_read_timeout(Duration::from_secs(10))
                    .max_headers(100)
                    .keep_alive(false);

                server
                    .http_builder()
                    .http2()
                    .timer(TokioTimer::new())
                    .keep_alive_interval(Some(Duration::from_secs(30)))
                    .keep_alive_timeout(Duration::from_secs(5));

                server.serve(make_svc).await
            };

            if let Err(e) = result {
                tracing::error!("Server error on {bind_address}: {e:#}");
            }
        });

        (join_handle, handle)
    }
}

/// Axum middleware that enforces per-IP rate limits.
async fn rate_limit_middleware(
    State(state): State<Arc<Server>>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if peer_addr.ip().is_loopback() {
        return next.run(req).await;
    }

    if let Some(ref limiter) = state.rate_limiter
        && !limiter.check(peer_addr.ip()).await
    {
        tracing::warn!(
            "Rate limit exceeded for {} on {}",
            peer_addr.ip(),
            req.uri().path(),
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::CONTENT_TYPE, "application/json")],
            RATE_LIMITED,
        )
            .into_response();
    }
    next.run(req).await
}

/// Axum middleware that delegates auth checking to `Server::check_auth`.
/// Returns a JSON error body on auth failure for consistency with the rest of the API.
///
/// On success the peer's IP is **marked as trusted** so future requests from
/// that IP skip the priority admission gate entirely.
async fn auth_middleware(
    State(state): State<Arc<Server>>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if state.auth_token.is_some() {
        if let Err((status, msg)) = state.check_auth(&headers, peer_addr) {
            return Server::json_err(status, msg);
        }
        state.admission_gate.mark_trusted(peer_addr.ip()).await;
    }
    next.run(req).await
}

/// Axum middleware that applies the priority admission gate.
///
/// IPs that have previously authenticated successfully bypass the gate
/// entirely, the per-IP rate limiter (inner layer) is their ceiling.
async fn priority_gate_middleware(
    State(state): State<Arc<Server>>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if peer_addr.ip().is_loopback() {
        return next.run(req).await;
    }

    if !state.admission_gate.admit(peer_addr.ip()).await {
        tracing::warn!(
            "Admission gate blocked {} on {} (global budget exhausted)",
            peer_addr.ip(),
            req.uri().path(),
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            GATE_BUDGET_EXHAUSTED,
        )
            .into_response();
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limiter::{ADMISSION_GLOBAL_CAP, ADMISSION_PER_IP_BUDGET};
    use arrow::ipc::reader::StreamReader;
    use futures::StreamExt;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DATABASE_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        directory: PathBuf,
    }

    impl TestDatabase {
        fn with_trades(rows: usize) -> (Self, Arc<Server>) {
            let id = TEST_DATABASE_ID.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "flowsurface-arrow-stream-test-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&directory).unwrap();

            let connection = duckdb::Connection::open(directory.join("trades.duckdb")).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE trades (
                        exchange VARCHAR NOT NULL,
                        symbol VARCHAR NOT NULL,
                        ts BIGINT NOT NULL,
                        price DOUBLE NOT NULL,
                        qty DOUBLE NOT NULL,
                        is_sell BOOLEAN NOT NULL
                    );",
                )
                .unwrap();
            let exchange = exchange_from_venue_market("binance", "spot").unwrap();
            connection
                .execute(
                    &format!(
                        "INSERT INTO trades
                         SELECT ?, 'btcusdt', i::BIGINT, i::DOUBLE, 1.0::DOUBLE, i % 2 = 0
                         FROM range({rows}) AS r(i)"
                    ),
                    duckdb::params![exchange],
                )
                .unwrap();
            drop(connection);

            let storage = Storage::open(&directory, 400, 1, 4096).unwrap();
            let available_tickers = FxHashMap::default();
            let server = Arc::new(Server::new(
                storage,
                None,
                Vec::new(),
                &available_tickers,
                Arc::new(Diagnostics::new()),
                None,
                None,
                AdmissionGate::new(ADMISSION_PER_IP_BUDGET, ADMISSION_GLOBAL_CAP),
                query_budget(400),
            ));

            (Self { directory }, server)
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    fn arrow_query(limit: usize) -> TradeQuery {
        TradeQuery {
            venue: "binance".to_string(),
            symbol: "btcusdt".to_string(),
            market: "spot".to_string(),
            from: None,
            to: None,
            limit: Some(limit),
        }
    }

    #[test]
    fn query_budget_preserves_unset_defaults() {
        assert_eq!(
            query_budget(0),
            QueryBudget {
                max_arrow_rows: 1_000_000,
                concurrency: 4,
            }
        );
    }

    #[test]
    fn query_budget_scales_arrow_limit_and_concurrency() {
        assert_eq!(query_budget(256).max_arrow_rows, 640_000);
        assert_eq!(
            query_budget(400),
            QueryBudget {
                max_arrow_rows: 1_000_000,
                concurrency: 1,
            }
        );
        assert_eq!(
            query_budget(1024),
            QueryBudget {
                max_arrow_rows: 1_000_000,
                concurrency: 2,
            }
        );
        assert_eq!(
            query_budget(2048),
            QueryBudget {
                max_arrow_rows: 1_000_000,
                concurrency: 4,
            }
        );
    }

    #[test]
    fn query_budget_clamps_small_and_large_limits() {
        assert_eq!(query_budget(1).max_arrow_rows, MIN_ARROW_LIMIT);
        assert_eq!(query_budget(u64::MAX).max_arrow_rows, MAX_ARROW_LIMIT);
        assert_eq!(query_budget(1).concurrency, 1);
        assert_eq!(query_budget(u64::MAX).concurrency, MAX_QUERY_CONCURRENCY);
    }

    #[tokio::test]
    async fn arrow_endpoint_streams_valid_ipc_without_content_length() {
        let (database, state) = TestDatabase::with_trades(25_000);
        let response =
            Server::trades_arrow(State(Arc::clone(&state)), Query(arrow_query(25_000))).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::CONTENT_LENGTH).is_none());

        let mut bytes = Vec::new();
        let mut chunk_count = 0usize;
        let mut body = response.into_body().into_data_stream();
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
            chunk_count += 1;
        }

        let batches = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        let rows: usize = batches.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(rows, 25_000);
        assert!(chunk_count > 1);
        assert_eq!(state.query_semaphore.available_permits(), 1);

        drop(state);
        drop(database);
    }

    #[tokio::test]
    async fn dropping_arrow_body_cancels_producer_and_releases_permit() {
        let (database, state) = TestDatabase::with_trades(100_000);
        let response =
            Server::trades_arrow(State(Arc::clone(&state)), Query(arrow_query(100_000))).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.query_semaphore.available_permits(), 0);
        drop(response);

        tokio::time::timeout(Duration::from_secs(2), async {
            while state.query_semaphore.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Arrow producer did not stop after response body was dropped");

        drop(state);
        drop(database);
    }
}
