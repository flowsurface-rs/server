use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};

use std::fmt;
use std::str::FromStr;

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
    pub bind_address: SocketAddr,
    /// Directory where the DuckDB database file will be stored.
    pub data_dir: String,
    /// Optional bearer-token required on all API requests.
    /// Mandatory when `bind_address` is not a loopback address.
    ///
    /// The server uses HTTPS with a self-signed certificate (generated
    /// on first boot), so the token is always encrypted in transit.
    ///
    /// When generated automatically the token is stored in
    /// `data_dir / .auth_token` so that restarts reuse the same token.
    /// Not settable via `config.toml` — use the `AUTH_TOKEN` env var instead.
    #[serde(skip)]
    pub auth_token: Option<BearerToken>,

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

    /// Domain name inserted into the self-signed TLS certificate's SAN
    /// (Subject Alternative Names).
    ///
    /// Ignored when `bind_address` is a loopback address (plain HTTP).
    /// Default: `"flowsurface-server"`.
    #[serde(default = "default_tls_domain")]
    pub tls_domain: String,

    /// Maximum number of trades to buffer in memory before dropping
    /// incoming trades to prevent OOM on constrained hosts.
    /// Trades are still received from the WebSocket (WS reader never
    /// blocks), but once this ceiling is reached new trades are
    /// silently dropped until the buffer is flushed to DuckDB.
    /// Default: 200_000 (~20–40 MB depending on symbol length).
    #[serde(default = "default_max_buffered_trades")]
    pub max_buffered_trades: usize,

    /// Optional hard cap on total DuckDB storage (main DB + WAL) in
    /// megabytes.  When the combined file size exceeds this value the
    /// oldest trades are purged during cleanup — even if they're within
    /// the time-based retention window.
    ///
    /// Use this to prevent the database from filling the disk on
    /// constrained hosts.  Default: `None` (no size cap).
    ///
    /// Tip: set this to ~50-80 % of your available disk space so the
    /// server leaves room for system files, logs, and burst.
    /// Default: `4096` (4 GiB).
    #[serde(default = "default_max_storage_mb")]
    pub max_storage_mb: Option<u64>,
}

const fn default_flush_interval() -> u64 {
    2000
}

const fn default_data_retention_hours() -> u64 {
    168
}

const fn default_true() -> bool {
    true
}

fn default_tls_domain() -> String {
    "flowsurface-server".to_string()
}

const fn default_max_buffered_trades() -> usize {
    200_000
}

const fn default_max_storage_mb() -> Option<u64> {
    Some(4096)
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
        let mut cfg: Self =
            toml::from_str(&content).with_context(|| format!("parsing {}", path.display()))?;

        if let Ok(token) = std::env::var("AUTH_TOKEN")
            && let Some(token) = BearerToken::new(token)
        {
            cfg.auth_token = Some(token);
        }

        Ok(cfg)
    }

    /// Resolve the whitelist templates, falling back to an empty map.
    pub fn resolve_whitelist(&self) -> WhitelistTemplates {
        self.whitelist.clone().unwrap_or_default()
    }

    /// Resolve the configuration file path.
    ///
    /// Defaults to next to the binary so the entire app is portable in
    /// a single directory.
    pub fn resolve_path(override_path: Option<PathBuf>) -> PathBuf {
        if let Some(path) = override_path {
            return path;
        }
        if let Ok(exe) = std::env::current_exe() {
            if let Some(parent) = exe.parent() {
                return parent.join("config.toml");
            }
        }
        PathBuf::from("config.toml")
    }

    pub fn resolve_auth_token(&mut self) -> anyhow::Result<()> {
        if self.bind_address.ip().is_loopback() {
            return Ok(());
        }

        let token_file = std::path::PathBuf::from(&self.data_dir).join(".auth_token");
        let on_disk_raw = std::fs::read_to_string(&token_file)
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty());
        let on_disk_token = on_disk_raw
            .as_deref()
            .and_then(|s| BearerToken::new(s.to_owned()));

        let token = match (self.auth_token.clone(), on_disk_token) {
            (Some(explicit), _) => explicit,    // env/config wins
            (None, Some(existing)) => existing, // reuse what's on disk
            (None, None) => generate_token(),   // nothing anywhere — mint one
        };

        if on_disk_raw.as_deref() != Some(token.as_str()) {
            std::fs::create_dir_all(&self.data_dir)
                .with_context(|| format!("creating data dir '{}'", self.data_dir))?;
            std::fs::write(&token_file, token.as_str())
                .with_context(|| format!("writing {}", token_file.display()))?;
            crate::tls::restrict_permissions(&token_file);
            tracing::info!(
                "Auth token → {}\n  Token starts with: {}…  (run `cat {}` to view full token)",
                token_file.display(),
                &token.as_str()[..4.min(token.as_str().len())],
                token_file.display(),
            );
        }

        self.auth_token = Some(token);
        Ok(())
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
}

/// A Bearer token used to authenticate API requests.
///
/// Constructed via [`FromStr`] (or [`BearerToken::new`]) which rejects
/// empty strings.  The [`Display`] implementation outputs `[REDACTED]`
/// to prevent accidental leakage in logs.
///
/// # Example
///
/// ```ignore
/// let token: BearerToken = "my-secret-token".parse()?;
/// assert!(token.is_valid_authorization("Bearer my-secret-token"));
/// assert!(!token.is_valid_authorization("Bearer wrong-token"));
/// ```
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(try_from = "String")]
pub struct BearerToken(String);

impl BearerToken {
    /// Create a new `BearerToken`, returning `None` if `raw` is empty.
    pub fn new(raw: String) -> Option<Self> {
        if raw.is_empty() {
            None
        } else {
            Some(Self(raw))
        }
    }

    /// Check whether `authorization_header` matches `"Bearer {token}"`
    /// (case-insensitive).
    pub fn is_valid_authorization(&self, authorization_header: &str) -> bool {
        let expected = format!("Bearer {}", self.0);
        authorization_header.eq_ignore_ascii_case(&expected)
    }

    /// Return the raw token string (for writing to disk, etc.).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for BearerToken {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err("Bearer token must not be empty")
        } else {
            Ok(Self(s.to_owned()))
        }
    }
}

impl TryFrom<String> for BearerToken {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl fmt::Display for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[REDACTED]")
    }
}

impl AsRef<str> for BearerToken {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq for BearerToken {
    fn eq(&self, other: &Self) -> bool {
        // Constant-time comparison would be better, but for a self-hosted
        // internal API the timing leak is negligible.
        self.0 == other.0
    }
}

/// Retention period expressed in hours.  A value of `0` means
/// **unlimited** — no time-based purges.
#[derive(Debug, Clone, Copy)]
pub struct RetentionHours(u64);

impl RetentionHours {
    /// Create a `RetentionHours`.  `0` is accepted and means unlimited
    /// (time-based purges are skipped).
    pub fn new(hours: u64) -> anyhow::Result<Self> {
        Ok(Self(hours))
    }

    /// The raw hour count.
    pub fn as_hours(self) -> u64 {
        self.0
    }

    /// The equivalent span in milliseconds (as a signed value for SQL).
    pub fn as_millis(self) -> i64 {
        (self.0 as i64) * 3_600_000
    }
}

/// A storage size expressed in bytes.  Provides convenience conversions
/// to megabytes to avoid sprinkling `(1024 * 1024)` throughout the code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StorageBytes(u64);

impl StorageBytes {
    /// Construct from a byte count.
    pub const fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Construct from a megabyte count (clamped to `u64::MAX` on overflow).
    pub fn from_mb(mb: u64) -> Self {
        Self(mb.saturating_mul(1024 * 1024))
    }

    /// The raw byte count.
    pub const fn as_bytes(self) -> u64 {
        self.0
    }

    /// The size in whole megabytes (truncated).
    pub fn as_mb(self) -> u64 {
        self.0 / (1024 * 1024)
    }

    /// Saturating multiplication (returns `StorageBytes`).
    pub fn saturating_mul(self, rhs: u64) -> Self {
        Self(self.0.saturating_mul(rhs))
    }
}

/// Generate a random 256-bit hex token.
fn generate_token() -> BearerToken {
    let mut buf = [0u8; 32];
    getrandom::getrandom(&mut buf).expect("failed to get random bytes");
    let hex: String = buf.iter().map(|b| format!("{b:02x}")).collect();
    BearerToken::new(hex).expect("generated hex token is never empty")
}
