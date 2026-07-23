mod api;
mod cleanup;
mod config;
mod discovery;
mod limiter;
mod storage;
mod stream;
mod tls;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use flowsurface_exchange::adapter::AdapterHandles;
use flowsurface_exchange::{Ticker, TickerInfo};

use crate::api::Server;
use crate::config::{Args, BearerToken, Config};
use crate::limiter::{ADMISSION_GLOBAL_CAP, ADMISSION_PER_IP_BUDGET, AdmissionGate, RateLimiter};
use crate::storage::Storage;

#[tokio::main]
async fn main() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs as the default rustls CryptoProvider");

    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,flowsurface_exchange=warn")),
        )
        .init();

    let args = Args::parse();
    let config_path = Config::resolve_path(args.config);
    let mut config = Config::load_or_write_template(&config_path);

    let data_dir = if Path::new(&config.storage.data_dir).is_relative() {
        config_path
            .parent()
            .expect("config path has no parent")
            .join(&config.storage.data_dir)
    } else {
        PathBuf::from(&config.storage.data_dir)
    };

    if let Err(e) = config.resolve_auth_token(&data_dir) {
        tracing::error!("{e:#}");
        std::process::exit(1);
    }

    let app = App::new(&config, &data_dir).await;
    let handles = app.serve().await;
    handles.shutdown().await;
}

struct App {
    storage: Storage,
    adapter_handles: AdapterHandles,
    resolved_pairs: Vec<TickerInfo>,
    metadata_cache: discovery::MetadataCache,
    bind_address: SocketAddr,
    auth_token: Option<BearerToken>,
    flush_interval: std::time::Duration,
    max_buffered_trades: usize,
    cleanup_scheduler: cleanup::CleanupScheduler,
    tls_config: Option<axum_server::tls_rustls::RustlsConfig>,
    rate_limiter: Option<RateLimiter>,
}

impl App {
    /// Open storage, resolve configured pairs, persist ticker metadata.
    async fn new(config: &Config, data_dir: &Path) -> Self {
        let storage = Storage::open(
            data_dir,
            config.storage.memory_limit_mb,
            config.storage.threads,
            config.storage.max_storage_mb,
        )
        .unwrap_or_else(|e| {
            tracing::error!("Failed to initialise storage: {e:#}");
            std::process::exit(1);
        });

        let cleanup_config = cleanup::CleanupConfig::from_config(
            config.storage.data_retention_hours,
            config.storage.max_storage_mb,
        )
        .unwrap_or_else(|e| {
            tracing::error!("Invalid cleanup configuration: {e:#}");
            std::process::exit(1);
        });

        let cleanup_scheduler = cleanup::CleanupScheduler::new(&storage, cleanup_config)
            .unwrap_or_else(|e| {
                tracing::error!("{e:#}");
                std::process::exit(1);
            });

        let whitelist = config.resolve_whitelist();
        if !config.pairs.discovery_mode
            && (whitelist.is_empty() || config.pairs.base_assets.is_empty())
        {
            tracing::error!("No pairs configured. Set base_assets and whitelist in config.toml");
            std::process::exit(1);
        }

        let (adapter_handles, metadata_cache, resolved_pairs) = discovery::setup_pairs(
            &config.pairs.base_assets,
            config.pairs.discovery_mode,
            &whitelist,
        )
        .await;

        if resolved_pairs.is_empty() {
            if config.pairs.discovery_mode {
                tracing::warn!(
                    "No matching pairs for base_assets {:?} with current whitelist \
                     - discovery mode is on, so /exchanges is still populated.",
                    config.pairs.base_assets
                );
            } else {
                tracing::error!(
                    "No matching pairs found for base_assets {:?} with current whitelist",
                    config.pairs.base_assets
                );
                std::process::exit(1);
            }
        }

        tracing::info!(
            "Tracking {} pair(s) across {} exchange(s)",
            resolved_pairs.len(),
            resolved_pairs
                .iter()
                .map(|ti| ti.exchange())
                .collect::<rustc_hash::FxHashSet<_>>()
                .len()
        );

        // Persist ticker metadata for the API layer.
        if let Err(e) = storage.store_ticker_infos(&resolved_pairs) {
            tracing::warn!("Failed to persist ticker metadata: {e:#}");
        }

        let tls_config = tls::setup_tls_config(
            data_dir,
            &storage,
            config.network.bind_address,
            &config.network.tls_domain,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::error!("{e:#}");
            std::process::exit(1);
        });

        let rate_limiter = config.network.rate_limit_max().map(|max| {
            tracing::info!(
                "Rate limiting enabled: max {max} req/10s per IP (≈ {} req/s)",
                max / 10
            );
            RateLimiter::new(max, std::time::Duration::from_secs(10))
        });

        Self {
            storage,
            adapter_handles,
            resolved_pairs,
            metadata_cache,
            bind_address: config.network.bind_address,
            auth_token: config.auth_token.clone(),
            flush_interval: std::time::Duration::from_millis(config.storage.flush_interval_ms),
            max_buffered_trades: config.storage.max_buffered_trades,
            tls_config,
            cleanup_scheduler,
            rate_limiter,
        }
    }

    /// Start the pipeline (stream engine, flusher, cleanup) and the HTTP server.
    async fn serve(self) -> AppHandles {
        let shutdown = CancellationToken::new();

        let (stream_mgr, mut rx) = stream::StreamManager::start_streams(
            self.adapter_handles,
            &self.resolved_pairs,
            shutdown.child_token(),
        );

        let cleanup_last_run = tokio::task::spawn_blocking({
            let scheduler = self.cleanup_scheduler.clone();
            move || scheduler.run_pass()
        })
        .await
        .unwrap_or(None);

        let _cleanup = self
            .cleanup_scheduler
            .spawn(cleanup_last_run, shutdown.child_token());

        let flusher = self.storage.spawn_batch_flusher(
            &mut rx,
            self.flush_interval,
            self.max_buffered_trades,
        );

        let configured_pairs: Vec<Ticker> =
            self.resolved_pairs.iter().map(|ti| ti.ticker).collect();

        let available_tickers = discovery::tickers_per_exchange(&self.metadata_cache);
        let admission_gate = AdmissionGate::new(ADMISSION_PER_IP_BUDGET, ADMISSION_GLOBAL_CAP);
        if self.auth_token.is_some() {
            tracing::info!(
                "Admission gate active: {ADMISSION_PER_IP_BUDGET} req/s per unknown IP, \
                 max {ADMISSION_GLOBAL_CAP} req/s total; authenticated IPs bypass the gate",
            );
        }
        let server = Arc::new(Server::new(
            self.storage,
            self.auth_token,
            configured_pairs,
            &available_tickers,
            self.tls_config,
            self.rate_limiter,
            admission_gate,
        ));
        let (server_task, server_shutdown_handle) = server.serve(self.bind_address).await;

        AppHandles {
            shutdown,
            stream_mgr: Some(stream_mgr),
            flusher,
            _cleanup,
            _server: server_task,
            server_shutdown_handle,
        }
    }
}

/// Runtime handles for the active pipeline - provides ordered shutdown.
struct AppHandles {
    shutdown: CancellationToken,
    stream_mgr: Option<stream::StreamManager>,
    flusher: tokio::task::JoinHandle<()>,
    _cleanup: tokio::task::JoinHandle<()>,
    _server: tokio::task::JoinHandle<()>,
    server_shutdown_handle: axum_server::Handle,
}

impl AppHandles {
    const SHUTDOWN_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

    /// Wait for SIGINT/SIGTERM, then cancel all streams, drain in-flight
    /// trades, and join every background task within the grace period.
    async fn shutdown(mut self) {
        let mut sigint =
            signal(SignalKind::interrupt()).expect("failed to register SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }

        tracing::info!("Shutting down…");

        // 1. Cancel all stream tasks (they share the root token).
        self.shutdown.cancel();

        // 2. Drop persist sender so downstream consumers drain.
        if let Some(ref mut mgr) = self.stream_mgr {
            mgr.drop_persist_sender();
        }

        // 3. Join everything within the grace period.
        tokio::time::timeout(Self::SHUTDOWN_GRACE_PERIOD, async {
            // Flusher first - it drains the trade buffer to disk.
            let _ = self.flusher.await;

            // Streaming tasks (engine + handler).
            if let Some(mgr) = self.stream_mgr.take() {
                mgr.shutdown().await;
            }

            // Background cleanup (may be mid-VACUUM - let it finish).
            let _ = self._cleanup.await;

            // Axum server - signal graceful shutdown first so it stops
            // accepting new connections and drains in-flight requests.
            self.server_shutdown_handle
                .graceful_shutdown(Some(std::time::Duration::from_secs(3)));
            let _ = self._server.await;
        })
        .await
        .ok();

        tracing::info!("Shutdown complete.");
    }
}
