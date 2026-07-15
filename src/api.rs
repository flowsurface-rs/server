use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use axum_server::tls_rustls::RustlsConfig;
use flowsurface_exchange::{TickerInfo, Trade};
use serde::{Deserialize, Serialize};

use crate::storage::{GroupedTrade, PairInfo, Storage};

pub struct Server {
    pub storage: Storage,
    pub startup: Instant,
    pub auth_token: Option<String>,
    /// The set of (exchange, symbol) pairs configured at startup.
    /// Used by `/pairs` to include pairs that have not yet received trades.
    pub configured_pairs: Vec<(String, String)>,
    /// All available ticker symbols per exchange, from the metadata cache.
    /// Used by `/exchanges` to help users discover correct suffix patterns.
    pub available_tickers: HashMap<String, Vec<String>>,
    /// TLS configuration for the HTTPS server (self-signed).
    /// `None` on loopback addresses (plain HTTP), `Some` for remote binds.
    pub tls_config: Option<RustlsConfig>,
}

#[derive(Serialize)]
struct StatusResponse {
    status: &'static str,
    uptime_secs: u64,
    db_ok: bool,
}

#[derive(Serialize)]
struct PairsResponse {
    pairs: Vec<PairInfo>,
    tracked_count: usize,
}

#[derive(Serialize)]
struct TradesResponse {
    trades: Vec<AnnotatedTrade>,
}

#[derive(Serialize)]
struct GroupedTradesResponse {
    trades: Vec<GroupedTrade>,
}

/// A normalized trade record, used both in-memory and serialized to JSON.
#[derive(Debug, Clone)]
pub struct AnnotatedTrade {
    pub exchange: String,
    pub symbol: String,
    pub trade: Trade,
}

impl AnnotatedTrade {
    pub fn new(ticker_info: TickerInfo, trade: Trade) -> Self {
        let symbol = ticker_info.ticker.to_string().to_lowercase();
        let exchange = ticker_info.exchange();

        AnnotatedTrade {
            exchange: exchange.to_string(),
            symbol: symbol.clone(),
            trade,
        }
    }
}

impl Serialize for AnnotatedTrade {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("Trade", 6)?;
        s.serialize_field("exchange", &self.exchange)?;
        s.serialize_field("symbol", &self.symbol)?;
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

/// Query parameters for the GET /trades/grouped endpoint.
#[derive(Debug, Deserialize)]
pub struct GroupedTradeQuery {
    /// Venue filter, e.g. "binance" (used with `market` to derive exchange).
    venue: String,
    /// Symbol filter.
    symbol: String,
    /// Market filter: "spot", "linear", or "inverse" (used with `venue`).
    market: String,
    /// Inclusive lower bound (milliseconds since epoch). Optional.
    from: Option<i64>,
    /// Inclusive upper bound (milliseconds since epoch). Optional.
    to: Option<i64>,
    /// Maximum number of records to return (default 1000, max 10_000).
    limit: Option<usize>,
    /// Integer multiplier applied to the exchange's minimum tick size
    /// to produce the price bucket width (default 1).
    step: Option<u16>,
}

#[derive(Serialize)]
struct ExchangesResponse {
    exchanges: HashMap<String, Vec<String>>,
}

impl Server {
    pub fn new(
        storage: Storage,
        auth_token: Option<String>,
        configured_pairs: Vec<(String, String)>,
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
    fn check_auth(&self, headers: &HeaderMap) -> Result<(), (StatusCode, &'static str)> {
        let Some(ref expected_token) = self.auth_token else {
            return Ok(());
        };

        let provided = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        let expected = format!("Bearer {expected_token}");

        if provided != expected {
            let client_ip = headers
                .get("X-Forwarded-For")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("unknown");

            let truncated: String = provided.chars().take(20).collect();
            tracing::warn!(
                "Auth failure from {client_ip}: expected valid Bearer token, got '{truncated}'"
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

        Self::json_ok(&StatusResponse {
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
        Self::json_ok(&ExchangesResponse {
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
            by_key.insert(format!("{}:{}", p.exchange, p.symbol), p);
        }

        // Merge: every configured pair gets a PairInfo; fill in DB bounds when available.
        let mut merged: Vec<PairInfo> = Vec::with_capacity(state.configured_pairs.len());
        for (ex, sym) in &state.configured_pairs {
            let key = format!("{ex}:{sym}");
            match by_key.get(&key) {
                Some(found) => merged.push((*found).clone()),
                None => merged.push(PairInfo {
                    exchange: ex.clone(),
                    symbol: sym.clone(),
                    earliest: None,
                    latest: None,
                }),
            }
        }

        let count = merged.len();
        Self::json_ok(&PairsResponse {
            pairs: merged,
            tracked_count: count,
        })
    }

    /// GET /trades
    async fn trades(State(state): State<Arc<Self>>, query: Query<TradeQuery>) -> impl IntoResponse {
        match state.storage.query_trades(&query) {
            Ok(trades) => Self::json_ok(&TradesResponse { trades }),
            Err(e) => Self::json_err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    }

    /// GET /trades/grouped
    async fn grouped_trades(
        State(state): State<Arc<Self>>,
        query: Query<GroupedTradeQuery>,
    ) -> impl IntoResponse {
        // Derive the canonical exchange string from venue + market.
        let ex_str = match Storage::exchange_from_venue_market(&query.venue, &query.market) {
            Some(ex) => ex,
            None => {
                return Self::json_err(
                    StatusCode::BAD_REQUEST,
                    "could not determine exchange from venue/market",
                );
            }
        };

        // Look up ticker metadata to get the tick size.
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

        // Compute the bucket width: min_ticksize × step multiplier.
        // `step` is an integer (e.g. 1, 2, 5, 50) — never a raw decimal.
        let multiplier = query.step.unwrap_or(1).max(1) as f64;
        let step_size = 10.0_f64.powi(info.min_ticksize as i32) * multiplier;

        // Derive price precision from min_ticksize (e.g. power -1 → 1 decimal place).
        let price_precision = if info.min_ticksize < 0 {
            -info.min_ticksize as u32
        } else {
            0
        };

        // Derive quantity precision from min_qty (e.g. power -3 → 3 decimal places).
        let qty_precision = if info.min_qty < 0 {
            -info.min_qty as u32
        } else {
            0
        };

        // Reuse the same filter params as a regular TradeQuery.
        let trade_query = TradeQuery {
            venue: query.venue.clone(),
            symbol: query.symbol.clone(),
            market: query.market.clone(),
            from: query.from,
            to: query.to,
            limit: query.limit,
        };

        match state.storage.query_grouped_trades(
            &trade_query,
            step_size,
            price_precision,
            qty_precision,
        ) {
            Ok(trades) => Self::json_ok(&GroupedTradesResponse { trades }),
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
                    .serve(router.into_make_service())
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
                axum::serve(listener, router).await.unwrap();
            }
        })
    }
}

/// Thin axum middleware that delegates auth checking to `Server::check_auth`.
async fn auth_middleware(
    State(state): State<Arc<Server>>,
    headers: HeaderMap,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, (StatusCode, &'static str)> {
    state.check_auth(&headers)?;
    Ok(next.run(req).await)
}
