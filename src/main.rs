mod api;
mod config;
mod discovery;
mod ingestion;
mod storage;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use flowsurface_exchange::adapter::{AdapterHandles, Venue};

use crate::api::Server;
use crate::config::{Args, Config};
use crate::discovery::ResolvedPair;
use crate::storage::Storage;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,flowsurface_exchange=warn")),
        )
        .init();

    let args = Args::parse();
    let config_path = Config::resolve_path(args.config);
    let config = Config::load_or_write_template(&config_path);

    if let Err(e) = config.validate_auth() {
        tracing::error!("{e:#}");
        std::process::exit(1);
    }

    let app = App::new(&config).await;
    let handles = app.serve().await;
    handles.shutdown().await;
}

struct App {
    storage: Storage,
    adapter_handles: AdapterHandles,
    resolved_pairs: Vec<ResolvedPair>,
    bind_address: String,
    auth_token: Option<String>,
    flush_interval: std::time::Duration,
    data_retention_hours: u64,
}

impl App {
    /// Open storage, resolve configured pairs, persist ticker metadata.
    async fn new(config: &Config) -> Self {
        let data_dir = PathBuf::from(&config.data_dir);
        let storage = Storage::open(&data_dir).unwrap_or_else(|e| {
            tracing::error!("Failed to initialise storage: {e:#}");
            std::process::exit(1);
        });

        storage.run_cleanup(config.data_retention_hours);

        let whitelist = config.resolve_whitelist();
        if whitelist.is_empty() || config.base_assets.is_empty() {
            tracing::error!("No pairs configured. Set base_assets and whitelist in config.toml");
            std::process::exit(1);
        }

        let venues: Vec<Venue> = discovery::venues_from_whitelist(&whitelist);
        tracing::info!("Spawning venue adapters: {venues:?}");
        let adapter_handles = AdapterHandles::spawn_venues(venues, None);

        tracing::info!("Fetching ticker metadata from exchanges…");
        let metadata_cache = discovery::build_metadata_cache(&adapter_handles, &whitelist).await;

        let resolved_pairs =
            discovery::resolve_pairs(&config.base_assets, &whitelist, &metadata_cache);

        if resolved_pairs.is_empty() {
            tracing::error!(
                "No matching pairs found for base_assets {:?} with current whitelist",
                config.base_assets
            );
            std::process::exit(1);
        }

        tracing::info!(
            "Tracking {} pair(s) across {} exchange(s)",
            resolved_pairs.len(),
            resolved_pairs
                .iter()
                .map(|p| p.exchange)
                .collect::<std::collections::HashSet<_>>()
                .len()
        );

        // Persist ticker metadata for the API layer.
        {
            let records = resolved_pairs
                .iter()
                .map(|p| storage::TickerInfoRecord {
                    exchange: p.exchange.to_string(),
                    symbol: p.ticker_info.ticker.to_string().to_lowercase(),
                    min_ticksize: p.ticker_info.min_ticksize.power,
                    min_qty: p.ticker_info.min_qty.power,
                    contract_size: p.ticker_info.contract_size.map(|cs| cs.power),
                })
                .collect::<Vec<_>>();

            if let Err(e) = storage.store_ticker_infos(&records) {
                tracing::warn!("Failed to persist ticker metadata: {e:#}");
            }
        }

        Self {
            storage,
            adapter_handles,
            resolved_pairs,
            bind_address: config.bind_address.clone(),
            auth_token: config.auth_token.clone(),
            flush_interval: std::time::Duration::from_millis(config.flush_interval_ms),
            data_retention_hours: config.data_retention_hours,
        }
    }

    /// Start the pipeline (flusher, cleanup, ingest) and the HTTP server.
    async fn serve(self) -> AppHandles {
        let (trade_tx, trade_rx) = mpsc::channel::<api::AnnotatedTrade>(1024);
        let shutdown = CancellationToken::new();

        let flusher =
            self.storage
                .spawn_batch_flusher(trade_rx, shutdown.child_token(), self.flush_interval);

        let _cleanup = self
            .storage
            .spawn_periodic_cleanup(self.data_retention_hours, shutdown.child_token());

        let ingest = ingestion::start_all_ingest_tasks(
            &self.resolved_pairs,
            self.adapter_handles,
            trade_tx.clone(),
            shutdown.child_token(),
        )
        .await;

        let configured_pairs: Vec<(String, String)> = self
            .resolved_pairs
            .iter()
            .map(|p| {
                (
                    p.exchange.to_string(),
                    p.ticker_info.ticker.to_string().to_lowercase(),
                )
            })
            .collect();

        let server = Arc::new(Server::new(self.storage, self.auth_token, configured_pairs));
        let server_handle = server.serve(&self.bind_address).await;

        AppHandles {
            shutdown,
            _trade_tx: trade_tx,
            flusher,
            _cleanup,
            ingest,
            _server: server_handle,
        }
    }
}

/// Runtime handles for the active pipeline — provides ordered shutdown.
struct AppHandles {
    shutdown: CancellationToken,
    _trade_tx: mpsc::Sender<api::AnnotatedTrade>,
    flusher: tokio::task::JoinHandle<()>,
    _cleanup: tokio::task::JoinHandle<()>,
    ingest: Vec<tokio::task::JoinHandle<()>>,
    _server: tokio::task::JoinHandle<()>,
}

impl AppHandles {
    const SHUTDOWN_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

    /// Wait for SIGINT/SIGTERM, then drain in-flight trades
    /// and join all background tasks within the grace period.
    async fn shutdown(self) {
        let mut sigint =
            signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }

        tracing::info!("Shutting down…");
        self.shutdown.cancel();
        drop(self._trade_tx); // drop the extra sender so the flusher can drain

        tokio::time::timeout(Self::SHUTDOWN_GRACE_PERIOD, async {
            let _ = self.flusher.await;
            for h in self.ingest {
                let _ = h.await;
            }
        })
        .await
        .ok();
    }
}
