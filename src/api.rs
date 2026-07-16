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
    Ticker, Timeframe, Trade,
    adapter::{Exchange, MarketKind, Venue},
};
use serde::{Deserialize, Serialize};

use crate::storage::{GroupedBucket, PairInfo, Storage};

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
    GroupedTrades {
        buckets: Vec<GroupedBucket>,
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

/// Query parameters for the GET /trades/grouped endpoint
///
/// Returns consecutive time buckets with price-level trade aggregations.
/// Each bucket is `timeframe` wide.
#[derive(Debug, Deserialize)]
pub struct GroupedTradeQuery {
    /// Venue filter, e.g. "binance" (used with `market` to derive exchange).
    venue: String,
    /// Symbol filter.
    symbol: String,
    /// Market filter: "spot", "linear", or "inverse" (used with `venue`).
    market: String,
    /// Start of the first time bucket (milliseconds since epoch).
    /// When omitted, defaults to `now - timeframe * limit`.
    from: Option<i64>,
    /// Number of consecutive time buckets to return (default 100, max 500).
    limit: Option<usize>,
    /// Integer multiplier applied to the exchange's minimum tick size
    /// to produce the price bucket width (default 1).
    step: Option<u16>,
    /// Time-bucket width, e.g. "5m", "15m", "1h", "1d".
    /// Required — see table above for usage patterns.
    timeframe: Option<String>,
}

pub struct Server {
    pub storage: Storage,
    pub startup: Instant,
    pub auth_token: Option<String>,
    /// The tickers configured at startup.
    /// Used by `/pairs` to include pairs that have not yet received trades.
    pub configured_pairs: Vec<Ticker>,
    /// All available ticker symbols per exchange, from the metadata cache.
    /// Used by `/exchanges` to help users discover correct suffix patterns.
    pub available_tickers: HashMap<String, Vec<String>>,
    /// TLS configuration for the HTTPS server (self-signed).
    /// `None` on loopback addresses (plain HTTP), `Some` for remote binds.
    pub tls_config: Option<RustlsConfig>,
}

impl Server {
    pub fn new(
        storage: Storage,
        auth_token: Option<String>,
        configured_pairs: Vec<Ticker>,
        available_tickers: HashMap<String, Vec<String>>,
        tls_config: Option<RustlsConfig>,
    ) -> Self {
        Self {
            storage,
            startup: Instant::now(),
            auth_token,
            configured_pairs,
            available_tickers,
            tls_config,
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

        let expected = format!("Bearer {expected_token}");

        if !provided.eq_ignore_ascii_case(&expected) {
            let truncated: String = provided.chars().take(20).collect();
            tracing::warn!(
                "Auth failure from {}: expected valid Bearer token, got '{truncated}'",
                peer_addr.ip(),
            );
            return Err((StatusCode::UNAUTHORIZED, "missing or invalid auth token"));
        }

        Ok(())
    }

    /// Build a JSON `200 OK` response from any serializable value.
    fn json_ok<T: Serialize>(data: &T) -> axum::response::Response {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::to_string(data).unwrap(),
        )
            .into_response()
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
            Err(e) => return Self::json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
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
        match state.storage.query_trades(&query) {
            Ok(trades) => Self::json_ok(&Response::Trades { trades }),
            Err(e) => Self::json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    /// GET /trades.parquet
    ///
    /// Returns trades as a **real Parquet** file, exported directly from
    /// DuckDB via `COPY (SELECT ...) TO 'file.parquet'`.  The Parquet
    /// schema uses the same 4 columns the Rust client expects:
    /// `ts (int64)`, `price (double)`, `qty (double)`, `is_sell (bool)`.
    async fn trades_parquet(
        State(state): State<Arc<Self>>,
        query: Query<TradeQuery>,
    ) -> axum::response::Response {
        let limit = query.limit.unwrap_or(10_000).min(100_000);

        let mut bounded = query.0;
        bounded.limit = Some(limit);

        let parquet_bytes = match state.storage.query_trades_parquet(&bounded) {
            Ok(bytes) => bytes,
            Err(e) => {
                return Server::json_err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("parquet export: {e:#}"),
                );
            }
        };

        (
            StatusCode::OK,
            [
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/vnd.apache.parquet",
                ),
                (
                    axum::http::header::CONTENT_DISPOSITION,
                    "attachment; filename=\"trades.parquet\"",
                ),
            ],
            parquet_bytes,
        )
            .into_response()
    }

    /// GET /trades/grouped
    ///
    /// Returns up to `limit` consecutive time buckets, each `timeframe` wide.
    /// Within each bucket, trades are grouped by price level (tick-aligned via `step`).
    async fn grouped_trades(
        State(state): State<Arc<Self>>,
        query: Query<GroupedTradeQuery>,
    ) -> impl IntoResponse {
        let tf_str = match &query.timeframe {
            Some(tf) => tf,
            None => {
                return Self::json_err(
                    StatusCode::BAD_REQUEST,
                    "`timeframe` is required — e.g. ?timeframe=15m",
                );
            }
        };

        let bucket_tf = match parse_timeframe(tf_str) {
            Some(tf) => tf,
            None => {
                return Self::json_err(
                    StatusCode::BAD_REQUEST,
                    "invalid timeframe — expected one of: \
                     1m,3m,5m,15m,30m,1h,2h,4h,12h,1d",
                );
            }
        };
        let bucket_ms = bucket_tf.to_milliseconds() as i64;

        let limit = query.limit.unwrap_or(100).min(500) as i64;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let from = match query.from {
            Some(f) => f,
            None => now_ms - bucket_ms * limit,
        };

        let ex_str = match exchange_from_venue_market(&query.venue, &query.market) {
            Some(ex) => ex,
            None => {
                return Self::json_err(
                    StatusCode::BAD_REQUEST,
                    "could not determine exchange from venue/market",
                );
            }
        };

        let info = match state.storage.get_ticker_info(&ex_str, &query.symbol) {
            Ok(Some(info)) => info,
            Ok(None) => {
                return Self::json_err(
                    StatusCode::NOT_FOUND,
                    &format!("no ticker metadata for {}/{}", ex_str, query.symbol),
                );
            }
            Err(e) => {
                return Self::json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
            }
        };

        let multiplier = query.step.unwrap_or(1).max(1) as f64;
        let step_size = 10.0_f64.powi(info.min_ticksize.power as i32) * multiplier;

        let price_precision = if info.min_ticksize.power < 0 {
            -info.min_ticksize.power as u32
        } else {
            0
        };

        let qty_precision = if info.min_qty.power < 0 {
            -info.min_qty.power as u32
        } else {
            0
        };

        let trade_query = TradeQuery {
            venue: query.venue.clone(),
            symbol: query.symbol.clone(),
            market: query.market.clone(),
            from: Some(from),
            to: None,
            limit: Some(limit as usize),
        };

        match state.storage.query_grouped_trades(
            &trade_query,
            bucket_ms,
            step_size,
            price_precision,
            qty_precision,
        ) {
            Ok(buckets) => Self::json_ok(&Response::GroupedTrades { buckets }),
            Err(e) => Self::json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    /// Bind to `bind_address` and spawn the axum HTTP(S) server.
    ///
    /// Uses plain HTTP for loopback addresses, HTTPS with a self-signed
    /// certificate for non-loopback (remote) binds.  Exits on bind failure.
    pub async fn serve(self: Arc<Self>, bind_address: &str) -> tokio::task::JoinHandle<()> {
        let addr: SocketAddr = bind_address.parse().unwrap_or_else(|e| {
            tracing::error!("Invalid bind_address '{bind_address}': {e}");
            std::process::exit(1);
        });

        let tls_config = self.tls_config.clone();

        // Public routes — no auth required
        let public = Router::new()
            .route("/status", get(Server::status))
            .with_state(self.clone());

        // Protected routes — require Bearer token when auth is configured
        let protected = Router::new()
            .route("/exchanges", get(Server::exchanges))
            .route("/pairs", get(Server::pairs))
            .route("/trades", get(Server::trades))
            .route("/trades.parquet", get(Server::trades_parquet))
            .route("/trades/grouped", get(Server::grouped_trades))
            .layer(axum::middleware::from_fn_with_state(
                self.clone(),
                auth_middleware,
            ))
            .with_state(self);

        let router = public.merge(protected);

        tokio::spawn(async move {
            if let Some(cfg) = tls_config {
                tracing::info!("Starting HTTPS API on {addr}");
                axum_server::bind_rustls(addr, cfg)
                    .serve(router.into_make_service_with_connect_info::<SocketAddr>())
                    .await
                    .unwrap();
            } else {
                tracing::info!("Starting HTTP API on {addr}");
                let listener = tokio::net::TcpListener::bind(addr)
                    .await
                    .unwrap_or_else(|e| {
                        tracing::error!("Failed to bind to {addr}: {e}");
                        std::process::exit(1);
                    });
                axum::serve(
                    listener,
                    router.into_make_service_with_connect_info::<SocketAddr>(),
                )
                .await
                .unwrap();
            }
        })
    }
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

fn parse_timeframe(s: &str) -> Option<Timeframe> {
    match s {
        "1m" => Some(Timeframe::M1),
        "3m" => Some(Timeframe::M3),
        "5m" => Some(Timeframe::M5),
        "15m" => Some(Timeframe::M15),
        "30m" => Some(Timeframe::M30),
        "1h" => Some(Timeframe::H1),
        "2h" => Some(Timeframe::H2),
        "4h" => Some(Timeframe::H4),
        "12h" => Some(Timeframe::H12),
        "1d" => Some(Timeframe::D1),
        _ => None,
    }
}
