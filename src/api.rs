use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::{
    Router,
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
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use crate::{
    config::BearerToken,
    limiter::{
        AdmissionGate, ConnectionLimiter, LimiterAcceptor, MAX_CONCURRENT_CONNECTIONS, RateLimiter,
    },
    storage::{PairInfo, Storage},
};

/// Static body for 503 responses from the admission gate.
static GATE_BUDGET_EXHAUSTED: &str = r#"{"error":"server busy, try again later"}"#;

/// Static body for 429 responses
static RATE_LIMITED: &str = r#"{"error":"rate limit exceeded, slow down"}"#;

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
    /// `/status` — cached JSON body with a ~1 second TTL.  Wrapped in a
    /// `Mutex` because the cache is refreshed on expiry.
    status: Mutex<StatusCache>,
    /// `/exchanges` — computed once at startup, never changes.
    exchanges: String,
}

/// Inner state for the cached `/status` response.
struct StatusCache {
    body: String,
    refreshed_at: Instant,
}

/// A normalized trade record, used both in-memory and serialized to JSON.
#[derive(Debug, Clone)]
pub struct AnnotatedTrade {
    pub ticker: Ticker,
    pub trade: Trade,
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

/// Query parameters for the GET /trades endpoint.
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
    /// Maximum number of records to return (default 1000, max 10_000).
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
    pub startup: Instant,
    pub auth_token: Option<BearerToken>,
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
    /// Cached pre-serialised response bodies for `/status` and `/exchanges`.
    pub cached: CachedResponses,
}

impl Server {
    pub fn new(
        storage: Storage,
        auth_token: Option<BearerToken>,
        configured_pairs: Vec<Ticker>,
        available_tickers: &FxHashMap<String, Vec<String>>,
        tls_config: Option<RustlsConfig>,
        rate_limiter: Option<RateLimiter>,
        admission_gate: AdmissionGate,
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
            startup: Instant::now(),
            auth_token,
            configured_pairs,
            tls_config,
            rate_limiter,
            admission_gate,
            connection_limiter,
            cached: CachedResponses {
                status: Mutex::new(StatusCache {
                    body: String::new(),
                    refreshed_at: Instant::now(),
                }),
                exchanges: cached_exchanges_json,
            },
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

    /// GET /status  (public — no auth required)
    ///
    /// Returns server uptime and a basic DB connectivity check.
    /// Suitable for load-balancer / container health probes.
    ///
    /// The response is cached for ~1 second to avoid a DB query (and full
    /// serialization) on every health-check request.
    async fn status(State(state): State<Arc<Self>>) -> impl IntoResponse {
        // Fast path: serve from cache if fresh (< 1 second old).
        {
            let cache = state.cached.status.lock().unwrap();
            if !cache.body.is_empty()
                && Instant::now()
                    .saturating_duration_since(cache.refreshed_at)
                    .as_secs()
                    < 1
            {
                return (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/json")],
                    cache.body.clone(),
                )
                    .into_response();
            }
        }

        // Slow path: query the DB, build response, update cache.
        let uptime = state.startup.elapsed().as_secs();
        let db_ok = state.storage.pair_count().is_ok();
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

        {
            let mut cache = state.cached.status.lock().unwrap();
            cache.body = body.clone();
            cache.refreshed_at = Instant::now();
        }

        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response()
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
            .run_blocking_query(Duration::from_secs(30), "pairs", |s| s.pairs_with_bounds())
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

        let q = query.0;
        match storage
            .run_blocking_query(Duration::from_secs(30), "trades", move |s| {
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

        let limit = query.limit.unwrap_or(50_000).min(400_000);
        let mut bounded = query.0;
        bounded.limit = Some(limit);

        let storage = state.storage.clone();

        let arrow_bytes = match storage
            .run_blocking_query(Duration::from_secs(30), "arrow", move |s| {
                s.query_trades_arrow_ipc(&bounded)
            })
            .await
        {
            Ok(bytes) => bytes,
            Err((status, msg)) => return Server::json_err(status, msg),
        };

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
            arrow_bytes,
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
    ///   1. **Priority gate** — throttles unknown IPs to a tiny global budget
    ///      (10 req/s) so scanners consume almost no CPU.
    ///   2. **Auth** — Bearer token verification for protected routes.
    ///   3. **Per-IP rate limiter** — token-bucket per IP (default 500 req/10s).
    ///      Only reached by authenticated requests — unauthenticated requests are
    ///      rejected by auth first, so they never consume rate-limiter bookkeeping.
    ///
    /// `/status` is public (no auth) but still goes through the priority gate
    /// and its response is cached to avoid a DB hit on every health check.
    ///
    /// **Connection‑level DoS protection:** both the TLS and plain‑HTTP paths
    /// use `axum_server` with a [`LimiterAcceptor`] that caps concurrent
    /// connections at [`MAX_CONCURRENT_CONNECTIONS`] (default 512).  Once the
    /// ceiling is hit, new connections are dropped immediately (TCP reset).
    pub async fn serve(
        self: Arc<Self>,
        bind_address: SocketAddr,
    ) -> (tokio::task::JoinHandle<()>, Handle) {
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
                    .handle(handle_for_server.clone())
                    .map(|acceptor| LimiterAcceptor {
                        inner: acceptor,
                        limiter: limiter.clone(),
                    });

                server
                    .http_builder()
                    .http1()
                    .header_read_timeout(Duration::from_secs(10))
                    .max_headers(100)
                    .keep_alive(false);

                server
                    .http_builder()
                    .http2()
                    .keep_alive_interval(Some(Duration::from_secs(30)))
                    .keep_alive_timeout(Duration::from_secs(5));

                server.serve(make_svc).await
            } else {
                tracing::info!("Starting HTTP API on {bind_address}");
                let mut server = axum_server::from_tcp(std_listener)
                    .handle(handle_for_server.clone())
                    .map(|acceptor| LimiterAcceptor {
                        inner: acceptor,
                        limiter: limiter.clone(),
                    });

                server
                    .http_builder()
                    .http1()
                    .header_read_timeout(Duration::from_secs(10))
                    .max_headers(100)
                    .keep_alive(false);

                server
                    .http_builder()
                    .http2()
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
