use std::collections::HashMap;

use anyhow::Result;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use flowsurface_exchange::adapter::{AdapterHandles, Event, Exchange, StreamConfig};
use flowsurface_exchange::{PushFrequency, TickerInfo};

use crate::api::AnnotatedTrade;

/// Start ingest tasks for every resolved pair, grouped by exchange.
///
/// Each exchange gets one task that subscribes a single trade stream
/// for all its tickers.
pub async fn start_all_ingest_tasks(
    pairs: &[TickerInfo],
    handles: AdapterHandles,
    tx: mpsc::UnboundedSender<AnnotatedTrade>,
    shutdown: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut by_exchange: HashMap<Exchange, Vec<TickerInfo>> = HashMap::new();
    for ti in pairs {
        by_exchange.entry(ti.exchange()).or_default().push(*ti);
    }

    let mut tasks = Vec::new();
    for (fl_exchange, ticker_infos) in by_exchange {
        let tx = tx.clone();
        let shutdown = shutdown.child_token();
        let handles = handles.clone();

        tasks.push(tokio::spawn(async move {
            if let Err(e) =
                run_exchange_ingestion(handles, fl_exchange, ticker_infos, tx, shutdown).await
            {
                tracing::error!(
                    "Ingestion for {exchange} exited: {e:#}",
                    exchange = fl_exchange
                );
            }
        }));
    }

    tasks
}

/// Subscribe to the trade stream for `exchange` with the given tickers
/// and forward normalised trades into `tx` until cancelled.
async fn run_exchange_ingestion(
    handles: AdapterHandles,
    exchange: Exchange,
    ticker_infos: Vec<TickerInfo>,
    tx: mpsc::UnboundedSender<AnnotatedTrade>,
    shutdown: CancellationToken,
) -> Result<()> {
    let symbols: Vec<String> = ticker_infos
        .iter()
        .map(|ti| ti.ticker.to_string().to_lowercase())
        .collect();

    let stream_cfg = StreamConfig::new(ticker_infos, exchange, None, PushFrequency::ServerDefault);
    let mut stream = handles.trade_stream(&stream_cfg);

    tracing::info!(%exchange, symbols = ?symbols, "Subscribed to trade stream");

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(%exchange, "Ingestion cancelled, shutting down");
                break;
            }
            event = stream.next() => {
                match event {
                    Some(event) => {
                        match event {
                            Event::TradesReceived(stream_kind, _tss, trades) => {
                                let ticker_info = stream_kind.ticker_info();

                                for ft_trade in trades.iter() {
                                    let normalized = AnnotatedTrade::new(ticker_info.ticker, *ft_trade);

                                    if tx.send(normalized).is_err() {
                                        tracing::info!(%exchange, "Trade channel closed, stopping ingest");
                                        return Ok(());
                                    }
                                }
                            }
                            Event::Connected(streams) => {
                                tracing::info!(%exchange, streams = ?streams, "Trade stream connected");
                            }
                            Event::Disconnected(streams, reason) => {
                                tracing::warn!(%exchange, streams = ?streams, %reason, "Trade stream disconnected");
                            }
                            _ => {}
                        }
                    }
                    None => {
                        tracing::info!(%exchange, "Trade stream ended");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
