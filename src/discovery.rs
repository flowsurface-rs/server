use std::collections::HashMap;

use flowsurface_exchange::adapter::{AdapterHandles, Exchange, MarketKind, Venue};
use flowsurface_exchange::{Ticker, TickerInfo};

use crate::config::WhitelistTemplates;

/// A fully-resolved tracked pair that the ingestion layer can use.
#[derive(Debug, Clone)]
pub struct ResolvedPair {
    pub exchange: Exchange,
    pub ticker_info: TickerInfo,
}

type MetadataCache = HashMap<Exchange, HashMap<Ticker, Option<TickerInfo>>>;

/// Collect the set of venues needed from the whitelist templates.
pub fn venues_from_whitelist(templates: &WhitelistTemplates) -> Vec<Venue> {
    templates
        .keys()
        .filter_map(|v| v.parse::<Venue>().ok())
        .collect()
}

/// Fetch metadata for every combination of venue + market in the
/// whitelist templates and return a cache.
pub async fn build_metadata_cache(
    handles: &AdapterHandles,
    templates: &WhitelistTemplates,
) -> MetadataCache {
    let mut cache = MetadataCache::new();

    for (venue_str, markets) in templates {
        let Ok(venue) = venue_str.parse::<Venue>() else {
            tracing::warn!("Unknown venue in whitelist: {venue_str}, skipping");
            continue;
        };

        for market_str in markets.keys() {
            let Ok(market) = market_str.parse::<MarketKind>() else {
                tracing::warn!(
                    "Unknown market kind '{market_str}' for venue {venue_str}, skipping"
                );
                continue;
            };

            let Some(exchange) = Exchange::from_venue_and_market(venue, market) else {
                tracing::warn!("Unsupported venue+market combination: {venue_str} {market_str}");
                continue;
            };

            match handles.fetch_ticker_metadata(venue, &[market]).await {
                Ok(meta) => {
                    tracing::info!("Fetched metadata for {exchange}: {} tickers", meta.len());
                    cache.insert(exchange, meta);
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch metadata for {exchange}: {e:#}");
                }
            }
        }
    }

    cache
}

/// Resolve base assets against whitelist templates and metadata cache
/// into a list of concrete `ResolvedPair`s.
pub fn resolve_pairs(
    base_assets: &[String],
    templates: &WhitelistTemplates,
    cache: &MetadataCache,
) -> Vec<ResolvedPair> {
    let mut pairs = Vec::new();

    for base in base_assets {
        let base_upper = base.to_uppercase();

        for (venue_str, markets) in templates {
            for (market_str, suffixes) in markets {
                let Ok(venue) = venue_str.parse::<Venue>() else {
                    continue;
                };
                let Ok(market) = market_str.parse::<MarketKind>() else {
                    continue;
                };
                let Some(exchange) = Exchange::from_venue_and_market(venue, market) else {
                    continue;
                };

                let Some(metadata) = cache.get(&exchange) else {
                    continue;
                };

                for suffix in suffixes {
                    let candidate = format!("{base_upper}{}", suffix.to_uppercase());
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
                            pairs.push(ResolvedPair {
                                exchange,
                                ticker_info: ti,
                            });
                        }
                        None => {
                            tracing::debug!(
                                "Ticker not found on {venue_str} {market_str}: {candidate}"
                            );
                        }
                    }
                }
            }
        }
    }

    pairs
}
