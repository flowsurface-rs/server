use anyhow::{Context, Result};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use flowsurface_exchange::adapter::{AdapterHandles, Exchange, MarketKind, Venue};
use flowsurface_exchange::{Ticker, TickerInfo};

use crate::config::WhitelistTemplates;

pub type MetadataCache = FxHashMap<Exchange, FxHashMap<Ticker, Option<TickerInfo>>>;

const EXCHANGE_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const EXCHANGE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const METADATA_FETCH_TIMEOUT: Duration = Duration::from_secs(35);

/// Orchestrate the full pair discovery pipeline: determine which venues to
/// use based on discovery mode, spawn adapter handles, fetch ticker metadata
/// from exchanges, and resolve concrete pair infos from the whitelist.
///
/// Returns the adapter handles, the metadata cache, and the resolved pairs.
pub async fn setup_pairs(
    base_assets: &[String],
    discovery_mode: bool,
    whitelist: &WhitelistTemplates,
) -> Result<(AdapterHandles, MetadataCache, Vec<TickerInfo>)> {
    let venues: Vec<Venue> = if discovery_mode {
        Venue::ALL.to_vec()
    } else {
        whitelist
            .keys()
            .filter_map(|v| v.parse::<Venue>().ok())
            .collect()
    };
    tracing::info!("Spawning venue adapters: {venues:?}");
    let client = reqwest::Client::builder()
        .connect_timeout(EXCHANGE_CONNECT_TIMEOUT)
        .timeout(EXCHANGE_REQUEST_TIMEOUT)
        .build()
        .context("building exchange HTTP client")?;
    let adapter_handles = AdapterHandles::spawn_venues(&client, venues, None);

    tracing::info!("Fetching ticker metadata from exchanges…");
    let metadata_cache = build_metadata_cache(&adapter_handles, whitelist, discovery_mode).await;

    let resolved_pairs = resolve_pairs(base_assets, whitelist, &metadata_cache);

    Ok((adapter_handles, metadata_cache, resolved_pairs))
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

/// Fetch metadata for exchanges and return a cache.
///
/// When `discovery_mode` is `true`, this ignores the whitelist templates
/// and fetches metadata for **all** supported exchange variants, so that
/// `/exchanges` is fully populated for discovery purposes.
async fn build_metadata_cache(
    handles: &AdapterHandles,
    templates: &WhitelistTemplates,
    discovery_mode: bool,
) -> MetadataCache {
    let mut cache = MetadataCache::default();

    if discovery_mode {
        for exchange in Exchange::ALL {
            let venue = exchange.venue();
            let market = exchange.market_type();

            match fetch_ticker_metadata(handles, venue, &[market]).await {
                Ok(meta) => {
                    tracing::info!("Fetched metadata for {exchange}: {} tickers", meta.len());
                    cache.insert(exchange, meta.into_iter().collect());
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch metadata for {exchange}: {e:#}");
                }
            }
        }
    } else {
        for (venue_str, markets) in templates {
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

                match fetch_ticker_metadata(handles, venue, &[market]).await {
                    Ok(meta) => {
                        tracing::info!("Fetched metadata for {exchange}: {} tickers", meta.len());
                        cache.insert(exchange, meta.into_iter().collect());
                    }
                    Err(e) => {
                        tracing::warn!("Failed to fetch metadata for {exchange}: {e:#}");
                    }
                }
            }
        }
    }

    cache
}

/// Flatten the metadata cache into a map of exchange → available ticker symbols.
///
/// Uses the display symbol when available (e.g. `"PURR/USDC"` instead of
/// Hyperliquid's opaque internal ID `"@107"`).
///
/// Returns symbols in UPPERCASE for consistent display in the `/exchanges` API.
pub fn tickers_per_exchange(cache: &MetadataCache) -> FxHashMap<String, Vec<String>> {
    cache
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

/// Resolve base assets against whitelist templates and metadata cache
/// into a list of concrete `ResolvedPair`s.
///
/// The whitelist provides **quote assets** per venue+market; this function
/// constructs the correct ticker string per exchange using [`format_ticker`]
/// and looks it up in the pre-fetched metadata cache.
fn resolve_pairs(
    base_assets: &[String],
    templates: &WhitelistTemplates,
    cache: &MetadataCache,
) -> Vec<TickerInfo> {
    let mut pairs = Vec::new();
    let mut seen: FxHashSet<(Exchange, Ticker)> = FxHashSet::default();

    for base in base_assets {
        let base_upper = base.to_uppercase();

        for (venue_str, markets) in templates {
            for (&market_kind, quotes) in markets {
                let Ok(venue) = venue_str.parse::<Venue>() else {
                    continue;
                };
                let market: MarketKind = market_kind.into();
                let Some(exchange) = Exchange::from_venue_and_market(venue, market) else {
                    continue;
                };

                let Some(metadata) = cache.get(&exchange) else {
                    continue;
                };

                for quote in quotes {
                    if quote.is_empty() {
                        // Empty quote acts as a wildcard: include every ticker
                        // on this exchange whose base matches.
                        for (ticker, opt_ti) in metadata {
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

                    let found = metadata
                        .get(&search_ticker)
                        .copied()
                        .flatten()
                        // Fallback: case-insensitive search across all metadata keys for this exchange.
                        .or_else(|| {
                            metadata.iter().find_map(|(t, ti)| {
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
