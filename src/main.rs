mod api;
mod config;
mod discovery;
mod ingestion;
mod storage;
mod tls;

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

    if config.auth_token.is_none() {
        let addr: std::net::SocketAddr = match config.bind_address.parse() {
            Ok(a) => a,
            Err(_) => {
                tracing::error!("Invalid bind_address '{}'", config.bind_address);
                std::process::exit(1);
            }
        };
        if !addr.ip().is_loopback() {
            let token_dir = std::path::PathBuf::from(&config.data_dir);
            let token_file = token_dir.join(".auth_token");

            let token = if token_file.exists() {
                std::fs::read_to_string(&token_file)
                    .unwrap_or_default()
                    .trim()
                    .to_string()
            } else {
                String::new()
            };

            let token = if token.is_empty() {
                let mut buf = [0u8; 32];
                getrandom::getrandom(&mut buf).expect("failed to get random bytes");
                let t: String = buf.iter().map(|b| format!("{b:02x}")).collect();
                std::fs::create_dir_all(&token_dir).ok();
                std::fs::write(&token_file, &t).ok();
                tracing::info!("Auth token generated: {}", token_file.display());
                tracing::info!("Token: {t}");
                t
            } else {
                token
            };

            config.auth_token = Some(token);
        }
    }

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
    metadata_cache: discovery::MetadataCache,
    bind_address: String,
    auth_token: Option<String>,
    flush_interval: std::time::Duration,
    data_retention_hours: u64,
    tls_config: Option<axum_server::tls_rustls::RustlsConfig>,
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
        if !config.discovery_mode && (whitelist.is_empty() || config.base_assets.is_empty()) {
            tracing::error!("No pairs configured. Set base_assets and whitelist in config.toml");
            std::process::exit(1);
        }

        let venues: Vec<Venue> = if config.discovery_mode {
            Venue::ALL.to_vec()
        } else {
            discovery::venues_from_whitelist(&whitelist)
        };
        tracing::info!("Spawning venue adapters: {venues:?}");
        let adapter_handles = AdapterHandles::spawn_venues(venues, None);

        tracing::info!("Fetching ticker metadata from exchanges…");
        let metadata_cache =
            discovery::build_metadata_cache(&adapter_handles, &whitelist, config.discovery_mode)
                .await;

        let resolved_pairs =
            discovery::resolve_pairs(&config.base_assets, &whitelist, &metadata_cache);

        if resolved_pairs.is_empty() {
            if config.discovery_mode {
                tracing::warn!(
                    "No matching pairs for base_assets {:?} with current whitelist \
                     — discovery mode is on, so /exchanges is still populated.",
                    config.base_assets
                );
            } else {
                tracing::error!(
                    "No matching pairs found for base_assets {:?} with current whitelist",
                    config.base_assets
                );
                std::process::exit(1);
            }
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

        // Only generate TLS cert for non-loopback addresses.
        // On localhost plain HTTP is used — no overhead, no cert needed.
        let addr: std::net::SocketAddr = config
            .bind_address
            .parse()
            .expect("bind_address already validated");

        let tls_config = if addr.ip().is_loopback() {
            None
        } else {
            let tls_domain = config.tls_domain.clone();
            let bind_ip = (!addr.ip().is_unspecified()).then_some(addr.ip());
            let tls_cert =
                tls::load_or_generate(&data_dir, &tls_domain, bind_ip).unwrap_or_else(|e| {
                    tracing::error!("Failed to load/generate TLS certificate: {e:#}");
                    std::process::exit(1);
                });

            tracing::info!(
                "TLS certificate fingerprint (SHA-256): {}",
                tls_cert.fingerprint
            );
            tracing::info!(
                "Use this fingerprint for cert pinning: sha256${}",
                tls_cert.fingerprint
            );

            Some(
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(
                        axum_server::tls_rustls::RustlsConfig::from_pem(
                            tls_cert.cert_pem.as_bytes().to_vec(),
                            tls_cert.key_pem.as_bytes().to_vec(),
                        ),
                    )
                })
                .unwrap_or_else(|e| {
                    tracing::error!("Failed to build TLS config: {e:#}");
                    std::process::exit(1);
                }),
            )
        };

        Self {
            storage,
            adapter_handles,
            resolved_pairs,
            metadata_cache,
            bind_address: config.bind_address.clone(),
            auth_token: config.auth_token.clone(),
            flush_interval: std::time::Duration::from_millis(config.flush_interval_ms),
            data_retention_hours: config.data_retention_hours,
            tls_config,
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

        let available_tickers = discovery::tickers_per_exchange(&self.metadata_cache);
        let server = Arc::new(Server::new(
            self.storage,
            self.auth_token,
            configured_pairs,
            available_tickers,
            self.tls_config,
        ));
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
