use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    Router,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use axum_server::tls_rustls::RustlsConfig;
use flowsurface_exchange::{
    Ticker, Trade,
    adapter::{Exchange, MarketKind, Venue},
};
use serde::{Deserialize, Serialize};

use crate::{
    config::BearerToken,
    limiter::RateLimiter,
    storage::{PairInfo, Storage},
};

#[derive(Serialize)]
#[serde(untagged)]
enum Response {
    Status {
        status: &'static str,
        uptime_secs: u64,
        db_ok: bool,
    },
    Pairs {
        pairs: Vec<PairInfo>,
        tracked_count: usize,
    },
    Trades {
        trades: Vec<AnnotatedTrade>,
    },
    Exchanges {
        exchanges: HashMap<String, Vec<String>>,
    },
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
    /// All available ticker symbols per exchange, from the metadata cache.
    /// Used by `/exchanges` to help users discover correct suffix patterns.
    pub available_tickers: HashMap<String, Vec<String>>,
    /// TLS configuration for the HTTPS server (self-signed).
    /// `None` on loopback addresses (plain HTTP), `Some` for remote binds.
    pub tls_config: Option<RustlsConfig>,
    /// Per-IP rate limiter.  `None` when rate limiting is disabled.
    pub rate_limiter: Option<RateLimiter>,
}

impl Server {
    pub fn new(
        storage: Storage,
        auth_token: Option<BearerToken>,
        configured_pairs: Vec<Ticker>,
        available_tickers: HashMap<String, Vec<String>>,
        tls_config: Option<RustlsConfig>,
        rate_limiter: Option<RateLimiter>,
    ) -> Self {
        Self {
            storage,
            startup: Instant::now(),
            auth_token,
            configured_pairs,
            available_tickers,
            tls_config,
            rate_limiter,
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
    async fn status(State(state): State<Arc<Self>>) -> impl IntoResponse {
        let uptime = state.startup.elapsed().as_secs();
        let db_ok = state.storage.pair_count().is_ok();

        Self::json_ok(&Response::Status {
            status: "ok",
            uptime_secs: uptime,
            db_ok,
        })
    }

    /// GET /exchanges
    ///
    /// Returns all available ticker symbols per exchange, as discovered from
    /// the exchange APIs at startup.  Useful for discovering the correct suffix
    /// patterns when configuring `config.toml`.
    async fn exchanges(State(state): State<Arc<Self>>) -> impl IntoResponse {
        Self::json_ok(&Response::Exchanges {
            exchanges: state.available_tickers.clone(),
        })
    }

    /// GET /pairs
    ///
    /// Returns all configured pairs (from the startup config), enriched with
    /// earliest/latest timestamps from the database when data exists.
    /// Pairs that have been configured but have not yet received any trades
    /// appear with `earliest: null` / `latest: null`.
    async fn pairs(State(state): State<Arc<Self>>) -> impl IntoResponse {
        let db_pairs = match state.storage.pairs_with_bounds() {
            Ok(pairs) => pairs,
            Err(e) => {
                tracing::error!("Failed to query pairs: {e:#}");
                return Self::json_err(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
        };

        // Build a lookup keyed by "exchange:symbol" from DB results.
        let mut by_key: std::collections::HashMap<String, &PairInfo> =
            std::collections::HashMap::new();
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
        match state.storage.query_trades(&query) {
            Ok(trades) => Self::json_ok(&Response::Trades { trades }),
            Err(e) => {
                tracing::error!("Failed to query trades: {e:#}");
                Self::json_err(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
            }
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

        let limit = query.limit.unwrap_or(100_000).min(1_000_000);
        let mut bounded = query.0;
        bounded.limit = Some(limit);

        let arrow_bytes = match state.storage.query_trades_arrow_ipc(&bounded) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!("Arrow export failed: {e:#}");
                return Server::json_err(StatusCode::INTERNAL_SERVER_ERROR, "arrow export failed");
            }
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
    pub async fn serve(self: Arc<Self>, bind_address: SocketAddr) -> tokio::task::JoinHandle<()> {
        let tls_config = self.tls_config.clone();

        // Public routes — no auth required
        let public = Router::new()
            .route("/status", get(Server::status))
            .with_state(self.clone());

        // Protected routes — require Bearer token when auth is configured.
        // Rate limiting is the outermost layer so abusive clients are dropped
        // before we spend cycles verifying their token.
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
            .with_state(self);

        let router = public.merge(protected);

        tokio::spawn(async move {
            if let Some(cfg) = tls_config {
                tracing::info!("Starting HTTPS API on {bind_address}");
                if let Err(e) = axum_server::bind_rustls(bind_address, cfg)
                    .serve(router.into_make_service_with_connect_info::<SocketAddr>())
                    .await
                {
                    tracing::error!("HTTPS server error on {bind_address}: {e:#}");
                }
            } else {
                tracing::info!("Starting HTTP API on {bind_address}");
                let listener = match tokio::net::TcpListener::bind(bind_address).await {
                    Ok(listener) => listener,
                    Err(e) => {
                        tracing::error!(
                            "Failed to bind to {bind_address}: {e:#}. \
                             Server will not accept connections."
                        );
                        return;
                    }
                };
                if let Err(e) = axum::serve(
                    listener,
                    router.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .await
                {
                    tracing::error!("HTTP server error on {bind_address}: {e:#}");
                }
            }
        })
    }
}

/// Static body for 429 responses
static RATE_LIMIT_BODY: &str = r#"{"error":"rate limit exceeded, slow down"}"#;

/// Axum middleware that enforces per-IP rate limits.
/// Applied before auth so abusive clients are dropped without
/// spending cycles on token verification.
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
            RATE_LIMIT_BODY,
        )
            .into_response();
    }
    next.run(req).await
}

/// Thin axum middleware that delegates auth checking to `Server::check_auth`.
/// Returns a JSON error body on auth failure for consistency with the rest of the API.
async fn auth_middleware(
    State(state): State<Arc<Server>>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Err((status, msg)) = state.check_auth(&headers, peer_addr) {
        return Server::json_err(status, msg);
    }
    next.run(req).await
}
