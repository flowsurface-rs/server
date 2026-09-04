use crate::config::WhitelistTemplates;
use anyhow::{Context, Result};
use flowsurface_exchange::adapter::{AdapterHandles, Exchange, MarketKind, Venue};
use flowsurface_exchange::{Ticker, TickerInfo};
use futures::StreamExt;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

const EXCHANGE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const EXCHANGE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const METADATA_FETCH_TIMEOUT: Duration = Duration::from_secs(35);

/// Bound on concurrent metadata fetches during startup discovery, so a
/// growing exchange list cannot fire an unbounded request burst.
const METADATA_FETCH_CONCURRENCY: usize = 8;

type MetadataMap = FxHashMap<Exchange, FxHashMap<Ticker, Option<TickerInfo>>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiscoveryMode {
    AllExchanges,
    WhitelistOnly,
}

pub(crate) struct Discovery {
    adapter_handles: AdapterHandles,
    metadata: MetadataCatalog,
    resolved_pairs: Vec<TickerInfo>,
}

struct DiscoveryPlan<'a> {
    base_assets: &'a [String],
    mode: DiscoveryMode,
    whitelist: &'a WhitelistTemplates,
}

#[derive(Default)]
struct MetadataCatalog(MetadataMap);

async fn with_timeout<T, E, F>(
    operation: F,
    timeout: Duration,
    timeout_context: String,
) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    tokio::time::timeout(timeout, operation)
        .await
        .with_context(|| timeout_context)?
        .context("metadata operation failed")
}

impl Discovery {
    /// Run the full pair discovery pipeline and construct its result.
    pub(crate) async fn run(
        base_assets: &[String],
        mode: DiscoveryMode,
        whitelist: &WhitelistTemplates,
    ) -> Result<Self> {
        let plan = DiscoveryPlan::new(base_assets, mode, whitelist);
        let venues = plan.venues();
        tracing::info!("Spawning venue adapters: {venues:?}");
        let client = reqwest::Client::builder()
            .connect_timeout(EXCHANGE_CONNECT_TIMEOUT)
            .timeout(EXCHANGE_REQUEST_TIMEOUT)
            .build()
            .context("building exchange HTTP client")?;
        let adapter_handles = AdapterHandles::spawn_venues(&client, venues, None);

        tracing::info!("Fetching ticker metadata from exchanges…");
        let metadata = MetadataCatalog::fetch(&adapter_handles, &plan).await;
        let resolved_pairs = plan.resolve_pairs(&metadata);

        Ok(Self {
            adapter_handles,
            metadata,
            resolved_pairs,
        })
    }

    pub(crate) fn resolved_pairs(&self) -> &[TickerInfo] {
        &self.resolved_pairs
    }

    pub(crate) fn available_tickers(&self) -> FxHashMap<String, Vec<String>> {
        self.metadata.tickers_per_exchange()
    }

    pub(crate) fn into_streaming_parts(self) -> (AdapterHandles, Vec<TickerInfo>) {
        (self.adapter_handles, self.resolved_pairs)
    }
}

impl<'a> DiscoveryPlan<'a> {
    fn new(
        base_assets: &'a [String],
        mode: DiscoveryMode,
        whitelist: &'a WhitelistTemplates,
    ) -> Self {
        Self {
            base_assets,
            mode,
            whitelist,
        }
    }

    fn venues(&self) -> Vec<Venue> {
        match self.mode {
            DiscoveryMode::AllExchanges => Venue::ALL.to_vec(),
            DiscoveryMode::WhitelistOnly => self
                .whitelist
                .keys()
                .filter_map(|v| v.parse::<Venue>().ok())
                .collect(),
        }
    }

    fn exchanges_to_fetch(&self) -> Vec<Exchange> {
        if matches!(self.mode, DiscoveryMode::AllExchanges) {
            return Exchange::ALL.to_vec();
        }

        let mut exchanges = Vec::new();
        let mut seen = FxHashSet::default();

        for (venue_str, markets) in self.whitelist {
            let Ok(venue) = venue_str.parse::<Venue>() else {
                tracing::warn!("Unknown venue in whitelist: {venue_str}, skipping");
                continue;
            };

            for &market_kind in markets.keys() {
                let market: MarketKind = market_kind.into();
                let Some(exchange) = Exchange::from_venue_and_market(venue, market) else {
                    tracing::warn!(
                        "Unsupported venue+market combination: {venue_str} {market_kind}"
                    );
                    continue;
                };

                if seen.insert(exchange) {
                    exchanges.push(exchange);
                }
            }
        }

        exchanges
    }

    fn exchange_groups(&self) -> Vec<(Venue, Vec<Exchange>)> {
        let mut grouped = FxHashMap::<Venue, Vec<Exchange>>::default();
        for exchange in self.exchanges_to_fetch() {
            grouped.entry(exchange.venue()).or_default().push(exchange);
        }
        grouped.into_iter().collect()
    }

    /// Resolve base assets against whitelist templates and metadata into
    /// concrete ticker infos.
    fn resolve_pairs(&self, metadata: &MetadataCatalog) -> Vec<TickerInfo> {
        let mut pairs = Vec::new();
        let mut seen: FxHashSet<(Exchange, Ticker)> = FxHashSet::default();

        for base in self.base_assets {
            let base_upper = base.to_uppercase();

            for (venue_str, markets) in self.whitelist {
                for (&market_kind, quotes) in markets {
                    let Ok(venue) = venue_str.parse::<Venue>() else {
                        continue;
                    };
                    let market: MarketKind = market_kind.into();
                    let Some(exchange) = Exchange::from_venue_and_market(venue, market) else {
                        continue;
                    };

                    let Some(exchange_metadata) = metadata.for_exchange(exchange) else {
                        continue;
                    };

                    for quote in quotes {
                        if quote.is_empty() {
                            // Empty quote acts as a wildcard: include every ticker
                            // on this exchange whose base matches.
                            for (ticker, opt_ti) in exchange_metadata {
                                let Some(ti) = opt_ti else {
                                    continue;
                                };

                                // Check both the internal ticker string and the
                                // display symbol (e.g. Hyperliquid spot uses opaque
                                // internal IDs like "@107" with display "PURR/USDC").
                                let raw = ticker.to_string().to_uppercase();
                                let display = ticker.display_symbol().map(|s| s.to_uppercase());
                                let matches_base = raw.starts_with(&base_upper)
                                    || display
                                        .as_deref()
                                        .is_some_and(|d| d.starts_with(&base_upper));

                                if matches_base && seen.insert((exchange, *ticker)) {
                                    pairs.push(*ti);
                                }
                            }
                            continue;
                        }

                        let candidate = format_ticker(&base_upper, quote, exchange);
                        let search_ticker = Ticker::new(&candidate, exchange);

                        let found = exchange_metadata
                            .get(&search_ticker)
                            .copied()
                            .flatten()
                            // Fallback: case-insensitive search across all metadata keys for this exchange.
                            .or_else(|| {
                                exchange_metadata.iter().find_map(|(t, ti)| {
                                    let ti = (*ti)?;
                                    let (internal, _) = t.to_full_symbol_and_type();
                                    if internal.eq_ignore_ascii_case(&candidate)
                                        || t.display_symbol()
                                            .is_some_and(|d| d.eq_ignore_ascii_case(&candidate))
                                    {
                                        Some(ti)
                                    } else {
                                        None
                                    }
                                })
                            });

                        match found {
                            Some(ti) => {
                                if seen.insert((exchange, ti.ticker)) {
                                    pairs.push(ti);
                                }
                            }
                            None => {
                                tracing::debug!(
                                    "Ticker not found on {venue_str} {market_kind}: {candidate}"
                                );
                            }
                        }
                    }
                }
            }
        }

        pairs
    }
}

impl MetadataCatalog {
    async fn fetch(handles: &AdapterHandles, plan: &DiscoveryPlan<'_>) -> Self {
        let mut catalog = Self::default();
        let mut results = futures::stream::iter(
            plan.exchange_groups()
                .into_iter()
                .map(|(venue, exchanges)| Self::fetch_venue(handles, venue, exchanges)),
        )
        .buffer_unordered(METADATA_FETCH_CONCURRENCY);

        while let Some(venue_results) = results.next().await {
            for (exchange, result) in venue_results {
                catalog.record(exchange, result);
            }
        }

        catalog
    }

    async fn fetch_venue(
        handles: &AdapterHandles,
        venue: Venue,
        exchanges: Vec<Exchange>,
    ) -> Vec<(Exchange, Result<HashMap<Ticker, Option<TickerInfo>>>)> {
        let mut results = Vec::with_capacity(exchanges.len());

        for exchange in exchanges {
            let market = exchange.market_type();
            let result = Self::fetch_ticker_metadata(handles, venue, &[market]).await;
            results.push((exchange, result));
        }

        results
    }

    async fn fetch_ticker_metadata(
        handles: &AdapterHandles,
        venue: Venue,
        markets: &[MarketKind],
    ) -> Result<HashMap<Ticker, Option<TickerInfo>>> {
        with_timeout(
            handles.fetch_ticker_metadata(venue, markets),
            METADATA_FETCH_TIMEOUT,
            format!("metadata request for {venue} timed out"),
        )
        .await
        .with_context(|| format!("fetching metadata for {venue}"))
    }

    fn record(&mut self, exchange: Exchange, result: Result<HashMap<Ticker, Option<TickerInfo>>>) {
        match result {
            Ok(meta) => {
                tracing::info!("Fetched metadata for {exchange}: {} tickers", meta.len());
                self.0.insert(exchange, meta.into_iter().collect());
            }
            Err(e) => {
                tracing::warn!("Failed to fetch metadata for {exchange}: {e:#}");
            }
        }
    }

    fn for_exchange(&self, exchange: Exchange) -> Option<&FxHashMap<Ticker, Option<TickerInfo>>> {
        self.0.get(&exchange)
    }

    /// Flatten metadata into exchange → available ticker symbols.
    fn tickers_per_exchange(&self) -> FxHashMap<String, Vec<String>> {
        self.0
            .iter()
            .map(|(exchange, tickers)| {
                let mut symbols: Vec<String> = tickers
                    .keys()
                    .map(|t| {
                        t.display_symbol()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| t.to_string())
                            .to_uppercase()
                    })
                    .collect();
                symbols.sort();
                (exchange.to_string(), symbols)
            })
            .collect()
    }
}

/// Generate the canonical ticker string for a given base asset, quote asset,
/// and exchange, based on the exchange's known ticker format.
///
/// Users only provide quote assets (e.g. `"USDT"`, `"USDC"`, `"USD"`) and
/// this function handles exchange-specific separators, perpetual suffixes,
/// and other formatting quirks.
fn format_ticker(base: &str, quote: &str, exchange: Exchange) -> String {
    let b = base.to_uppercase();
    let q = quote.to_uppercase();

    use Exchange::*;

    match exchange {
        // {base}{quote} — direct concatenation
        BinanceSpot | BinanceLinear | BybitSpot | BybitLinear | BybitInverse | MexcSpot => {
            format!("{b}{q}")
        }
        // {base}{quote}_PERP — inverse perpetuals with _PERP suffix (Binance only)
        BinanceInverse => {
            format!("{b}{q}_PERP")
        }
        // {base}_{quote} — underscore separator (MEXC linear/inverse)
        MexcLinear | MexcInverse => {
            format!("{b}_{q}")
        }
        // {base} — Hyperliquid linear (quote is implicit; just use the base asset)
        HyperliquidLinear => b,
        // Hyperliquid spot uses opaque internal IDs — matched via wildcard only
        HyperliquidSpot => b,
        // {base}-{quote} — OKX spot (dash separator)
        OkexSpot => format!("{b}-{q}"),
        // {base}-{quote}-SWAP — OKX linear & inverse perpetuals
        OkexLinear | OkexInverse => {
            format!("{b}-{q}-SWAP")
        }
    }
}
