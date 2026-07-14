use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Whitelist templates: venue → market_kind → list of quote assets.
///
/// For example `{"binance": {"spot": ["USDT"], "linear": ["USDT", "USDC"]}}`.
/// The server constructs the correct ticker string per exchange
/// (handling separators, _PERP, -SWAP suffixes, etc.).
pub type WhitelistTemplates = HashMap<String, HashMap<String, Vec<String>>>;

#[derive(Parser)]
#[command(name = "flowsurface-server", about = "Trade data store daemon")]
pub struct Args {
    /// Path to the configuration file.
    #[arg(short, long, default_value = None)]
    pub config: Option<PathBuf>,
}

/// Top-level application configuration, mirroring `config.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Socket address to bind the HTTP API (e.g. `127.0.0.1:8080`).
    pub bind_address: String,
    /// Directory where the DuckDB database file will be stored.
    pub data_dir: String,
    /// Optional bearer-token required on all API requests.
    /// Mandatory when `bind_address` is not a loopback address.
    pub auth_token: Option<String>,

    // ── Pair tracking ───────────────────────────────────────────
    /// Base assets to expand via the whitelist templates (e.g. `["btc", "eth"]`).
    #[serde(default)]
    pub base_assets: Vec<String>,

    /// Whitelist templates (venue → market → suffixes).
    #[serde(default)]
    pub whitelist: Option<WhitelistTemplates>,

    /// Trade batch-flush interval in milliseconds.
    /// Trades are buffered in memory and flushed to DuckDB in bulk
    /// on this interval.  Lower values reduce data-loss on crash but
    /// increase fsync pressure; higher values are more I/O-efficient.
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,

    /// Data retention period in hours.  Trades older than this are
    /// deleted on startup (and periodically while running).
    #[serde(default = "default_data_retention_hours")]
    pub data_retention_hours: u64,

    /// When `true`, fetch metadata for **all** supported exchange variants
    /// on startup so `/exchanges` is fully populated, regardless of the
    /// whitelist.  Useful for discovering available tickers before deciding
    /// what to track.  Default: `true`.
    #[serde(default = "default_true")]
    pub discovery_mode: bool,
}

const fn default_flush_interval() -> u64 {
    2000
}

const fn default_data_retention_hours() -> u64 {
    48
}

const fn default_true() -> bool {
    true
}

impl Config {
    /// Return the default config template as a commented TOML string.
    pub fn template() -> &'static str {
        include_str!("../config.example.toml")
    }

    /// Load and parse a TOML config file from the given path.
    pub fn load(path: &Path) -> Result<Self> {
        let content =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&content).with_context(|| format!("parsing {}", path.display()))
    }

    /// Resolve the whitelist templates, falling back to an empty map.
    pub fn resolve_whitelist(&self) -> WhitelistTemplates {
        self.whitelist.clone().unwrap_or_default()
    }

    /// Resolve the configuration file path.
    pub fn resolve_path(override_path: Option<PathBuf>) -> PathBuf {
        if let Some(path) = override_path {
            return path;
        }
        if let Ok(exe) = std::env::current_exe()
            && let Some(parent) = exe.parent()
        {
            let candidate = parent.join("config.toml");
            if candidate.exists() {
                return candidate;
            }
        }
        PathBuf::from("config.toml")
    }

    /// Attempt to load config.
    ///
    /// - If the file is **missing**, writes the default template to disk and
    ///   exits with code 2 (so systemd / supervisors can distinguish "not yet
    ///   configured" from a runtime crash).
    /// - If the file **exists** but is invalid, reports the parse error and
    ///   exits with code 1 — it never overwrites a broken user file.
    pub fn load_or_write_template(path: &Path) -> Self {
        if !path.exists() {
            tracing::info!(
                "Config file not found at {}. Writing default template.",
                path.display()
            );

            if let Err(write_err) = std::fs::write(path, Self::template()) {
                tracing::error!("Failed to write default config: {write_err:#}");
                std::process::exit(1);
            }
            tracing::info!(
                "Template written to {}. Edit it to suit your needs, then re-run.",
                path.display()
            );
            std::process::exit(2);
        }

        match Self::load(path) {
            Ok(cfg) => cfg,
            Err(e) => {
                tracing::error!(
                    "Failed to parse config at {}:\n  {e:#}\n\
                     Fix or remove the file and re-run.",
                    path.display()
                );
                std::process::exit(1);
            }
        }
    }

    /// Validate that auth_token is set when bind_address is not a loopback
    /// address.
    pub fn validate_auth(&self) -> Result<()> {
        let addr: SocketAddr = self
            .bind_address
            .parse()
            .with_context(|| format!("invalid bind_address '{}'", self.bind_address))?;
        if !addr.ip().is_loopback() && self.auth_token.is_none() {
            anyhow::bail!(
                "auth_token is required when bind_address is not a loopback address \
                 (binding to {})",
                addr.ip()
            );
        }
        Ok(())
    }
}
