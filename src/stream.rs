use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use flowsurface_exchange::TickerInfo;
use flowsurface_exchange::adapter::{AdapterHandles, Event, StreamKind};

use std::collections::HashMap;

use flowsurface_exchange::adapter::{
    Exchange, MAX_KLINE_STREAMS_PER_STREAM, StreamConfig, StreamTicksize,
};
use flowsurface_exchange::{PushFrequency, TickMultiplier, Timeframe};
use futures::{StreamExt, stream::BoxStream, stream::select_all};

/// How many events are buffered for best-effort consumers before
/// the slowest lagged receiver starts dropping messages.
const BROADCAST_CAPACITY: usize = 1024;

/// Aggregates all outbound channels for market-data events.
///
/// Both channels carry raw [`Event`]s — no variant-specific typing
/// until the final consumer.
#[derive(Default, Clone)]
struct EventOutlets {
    /// Persist channel (never drops).  Single consumer — the
    /// persister in `storage.rs`.
    persist: Option<mpsc::UnboundedSender<Event>>,
    /// Best-effort fan-out to WebSocket API, alerters, etc.
    broadcast: Option<broadcast::Sender<Event>>,
}

impl EventOutlets {
    fn new() -> Self {
        Self {
            persist: None,
            broadcast: None,
        }
    }

    fn with_persist(mut self, tx: mpsc::UnboundedSender<Event>) -> Self {
        self.persist = Some(tx);
        self
    }

    fn with_broadcast(mut self, tx: broadcast::Sender<Event>) -> Self {
        self.broadcast = Some(tx);
        self
    }

    /// Handle every [`Event`] from the stream engine and route it to
    /// whichever outlets are configured.
    async fn route(
        self,
        mut event_rx: mpsc::UnboundedReceiver<Event>,
        shutdown: CancellationToken,
    ) {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::info!("Event handler cancelled");
                    break;
                }
                maybe = event_rx.recv() => {
                    match maybe {
                        Some(event) => {
                            match &event {
                                Event::Connected(exchange) => tracing::info!(%exchange, "Stream connected"),
                                Event::Disconnected(exchange, reason) => tracing::warn!(%exchange, %reason, "Stream disconnected"),
                                _ => {}
                            }

                            if let Some(ref tx) = self.broadcast
                                && tx.receiver_count() > 0
                            {
                                let _ = tx.send(event.clone());
                            }

                            if let Some(ref tx) = self.persist && tx.send(event).is_err() {
                                tracing::info!("Persist channel closed, stopping handler");
                                return;
                            }
                        }
                        None => {
                            tracing::info!("Event channel closed, handler ending");
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[allow(dead_code)]
pub struct StreamManager {
    /// All outbound channels.  The event handler gets its own clone.
    outlets: EventOutlets,
    /// Per-exchange stream engine tasks.
    stream_tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Event handler task.
    event_task: Option<tokio::task::JoinHandle<()>>,
}

/// Receiving end of the persist channel created by
/// [`StreamManager::start_streams`].  The caller hands this to the persister
/// in `storage.rs`, which is the first place that matches on [`Event`]
/// variants.
#[derive(Default)]
pub struct StreamReceivers {
    /// Persist channel — carries every [`Event`] as-is.
    pub persist: Option<mpsc::UnboundedReceiver<Event>>,
}

impl StreamManager {
    /// Build the full streaming pipeline.
    ///
    /// Returns the manager plus a [`StreamReceivers`] containing the
    /// persist receiver.
    pub fn start_streams(
        handles: AdapterHandles,
        pairs: &[TickerInfo],
        shutdown: CancellationToken,
    ) -> (Self, StreamReceivers) {
        let (persist_tx, persist_rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(BROADCAST_CAPACITY);

        let mut all_streams: Vec<StreamKind> = Vec::with_capacity(pairs.len());
        for ti in pairs {
            all_streams.push(StreamKind::Trades { ticker_info: *ti });
        }

        // Low-level per-exchange WebSocket tasks.
        let (event_rx, stream_tasks) = spawn(handles, all_streams, shutdown.child_token());

        let outlets = EventOutlets::new()
            .with_persist(persist_tx)
            .with_broadcast(broadcast_tx);

        let event_task = tokio::spawn(outlets.clone().route(event_rx, shutdown.child_token()));

        (
            Self {
                outlets,
                stream_tasks,
                event_task: Some(event_task),
            },
            StreamReceivers {
                persist: Some(persist_rx),
            },
        )
    }

    /// Drop the persist sender so the persister's receiver
    /// gets `None` and drains its buffers.  Call **before** joining
    /// the persister during shutdown.
    pub fn drop_persist_sender(&mut self) {
        self.outlets.persist.take();
    }

    /// Return a new broadcast receiver for downstream consumers.
    #[allow(dead_code)]
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.outlets
            .broadcast
            .as_ref()
            .expect("StreamManager always has broadcast set")
            .subscribe()
    }

    /// Join all streaming tasks.  The caller must have cancelled the
    /// shutdown token and dropped the trade sender first.
    pub async fn shutdown(mut self) {
        for h in self.stream_tasks {
            let _ = h.await;
        }
        if let Some(h) = self.event_task.take() {
            let _ = h.await;
        }
        // outlets (and all its senders) are dropped here.
    }
}

/// Spawn per-exchange stream tasks for every provided [`StreamKind`].
///
/// Each exchange gets one task that subscribes to **all** requested stream
/// kinds (trades, depth, kline) for its tickers, merges the [`BoxStream`]s
/// with `select_all`, and forwards every [`Event`] into a unified channel.
fn spawn(
    handles: AdapterHandles,
    streams: Vec<StreamKind>,
    shutdown: CancellationToken,
) -> (
    mpsc::UnboundedReceiver<Event>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let (event_tx, event_rx) = mpsc::unbounded_channel();

    let mut by_exchange: HashMap<Exchange, Vec<StreamKind>> = HashMap::new();
    for stream in &streams {
        let exchange = stream.ticker_info().exchange();
        by_exchange.entry(exchange).or_default().push(*stream);
    }

    let mut tasks = Vec::new();
    for (exchange, exchange_streams) in by_exchange {
        let tx = event_tx.clone();
        let shutdown = shutdown.child_token();
        let handles = handles.clone();

        tasks.push(tokio::spawn(async move {
            if let Err(e) =
                run_exchange_streams(handles, exchange, exchange_streams, tx, shutdown).await
            {
                tracing::error!("Stream engine for {exchange} exited: {e:#}");
            }
        }));
    }

    // Drop the original sender so all clones live inside the tasks.
    // When the tasks finish (on cancellation) the receiver gets None.
    drop(event_tx);

    (event_rx, tasks)
}

/// Subscribe to **all** stream kinds for a single `exchange` and forward
/// every [`Event`] into `tx` until cancelled or all streams end.
async fn run_exchange_streams(
    handles: AdapterHandles,
    exchange: Exchange,
    streams: Vec<StreamKind>,
    tx: mpsc::UnboundedSender<Event>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    // Separate streams by kind so we can build the right StreamConfig.
    let mut trade_tickers: Vec<TickerInfo> = Vec::new();
    let mut depth_specs: Vec<(TickerInfo, StreamTicksize, PushFrequency)> = Vec::new();
    let mut kline_specs: Vec<(TickerInfo, Timeframe)> = Vec::new();

    for stream in &streams {
        match stream {
            StreamKind::Trades { ticker_info } => trade_tickers.push(*ticker_info),
            StreamKind::Depth {
                ticker_info,
                depth_aggr,
                push_freq,
            } => depth_specs.push((*ticker_info, *depth_aggr, *push_freq)),
            StreamKind::Kline {
                ticker_info,
                timeframe,
            } => kline_specs.push((*ticker_info, *timeframe)),
        }
    }

    let mut box_streams: Vec<BoxStream<'static, Event>> = Vec::new();

    // ── Trade stream ─────────────────────────────────────────────
    // All tickers batched into one WebSocket connection per exchange.
    if !trade_tickers.is_empty() {
        let symbols: Vec<String> = trade_tickers
            .iter()
            .map(|ti| ti.ticker.to_string().to_lowercase())
            .collect();
        let cfg = StreamConfig::new(trade_tickers, exchange, None, PushFrequency::ServerDefault);
        box_streams.push(handles.trade_stream(&cfg));
        tracing::info!(%exchange, symbols = ?symbols, "Subscribed to trade stream");
    }

    // ── Depth streams ────────────────────────────────────────────
    // One BoxStream per ticker because depth uses StreamConfig<TickerInfo>.
    for (ticker, aggr, push_freq) in &depth_specs {
        let tick_mltp: Option<TickMultiplier> = match aggr {
            StreamTicksize::Client => None,
            StreamTicksize::ServerSide(m) => Some(*m),
        };
        let cfg = StreamConfig::new(*ticker, exchange, tick_mltp, *push_freq);
        box_streams.push(handles.depth_stream(&cfg));
        tracing::info!(%exchange, ticker = %ticker.ticker, "Subscribed to depth stream");
    }

    // ── Kline streams ────────────────────────────────────────────
    // Chunked so each connection stays within MAX_KLINE_STREAMS_PER_STREAM.
    if !kline_specs.is_empty() {
        for chunk in kline_specs.chunks(MAX_KLINE_STREAMS_PER_STREAM) {
            let cfg =
                StreamConfig::new(chunk.to_vec(), exchange, None, PushFrequency::ServerDefault);
            box_streams.push(handles.kline_stream(&cfg));
            tracing::info!(%exchange, count = chunk.len(), "Subscribed to kline chunk");
        }
    }

    let mut merged = select_all(box_streams);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(%exchange, "Stream engine cancelled, shutting down");
                break;
            }
            event = merged.next() => {
                match event {
                    Some(event) => {
                        if tx.send(event).is_err() {
                            tracing::info!(%exchange, "Event channel closed, stopping stream engine");
                            break;
                        }
                    }
                    None => {
                        tracing::info!(%exchange, "All streams ended");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
