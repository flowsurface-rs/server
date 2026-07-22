use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use flowsurface_exchange::adapter::MarketKind;
use serde::{Deserialize, Serialize};

use std::fmt;
use std::str::FromStr;

/// Market kind as it appears in the whitelist config section.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigMarketKind {
    Spot,
    Linear,
    Inverse,
}

impl From<ConfigMarketKind> for MarketKind {
    fn from(k: ConfigMarketKind) -> Self {
        match k {
            ConfigMarketKind::Spot => MarketKind::Spot,
            ConfigMarketKind::Linear => MarketKind::LinearPerps,
            ConfigMarketKind::Inverse => MarketKind::InversePerps,
        }
    }
}

impl fmt::Display for ConfigMarketKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigMarketKind::Spot => write!(f, "spot"),
            ConfigMarketKind::Linear => write!(f, "linear"),
            ConfigMarketKind::Inverse => write!(f, "inverse"),
        }
    }
}

/// Whitelist templates: venue → market_kind → list of quote assets.
///
/// For example `{"binance": {"spot": ["USDT"], "linear": ["USDT", "USDC"]}}`.
/// The server constructs the correct ticker string per exchange
/// (handling separators, _PERP, -SWAP suffixes, etc.).
pub type WhitelistTemplates = HashMap<String, HashMap<ConfigMarketKind, Vec<String>>>;

#[derive(Parser)]
#[command(name = "flowsurface-server", about = "Trade data store daemon")]
pub struct Args {
    /// Path to the configuration file.
    #[arg(short, long, default_value = None)]
    pub config: Option<PathBuf>,
}

/// Top-level application configuration, mirroring `config.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub pairs: PairsConfig,
    #[serde(default)]
    pub whitelist: Option<WhitelistTemplates>,
    #[serde(skip)]
    pub auth_token: Option<BearerToken>,
}

/// `[network]` section — bind address, rate limiting, and TLS domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    /// Socket address to bind the HTTP API (e.g. `127.0.0.1:8080`).
    /// Defaults to `127.0.0.1:8080`.
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    /// Per-IP token-bucket rate limit: max requests per 10-second window.
    /// `0` disables rate limiting entirely (not recommended for internet-facing binds).
    /// Default: `500` (burst 500, sustained ≈ 50 req/s).
    #[serde(default = "default_rate_limit_max_requests")]
    pub max_requests: u64,
    /// Domain name inserted into the self-signed TLS certificate's SAN.
    /// Default: `"flowsurface-server"`.
    #[serde(default = "default_tls_domain")]
    pub tls_domain: String,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            max_requests: default_rate_limit_max_requests(),
            tls_domain: default_tls_domain(),
        }
    }
}

impl NetworkConfig {
    /// Returns the max requests per window, or `None` when disabled.
    pub fn rate_limit_max(&self) -> Option<u64> {
        if self.max_requests == 0 {
            None
        } else {
            Some(self.max_requests)
        }
    }
}

/// `[storage]` section — data directory, flush interval, retention, buffering caps.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Directory where the DuckDB database file will be stored.
    /// Defaults to `"data"` (relative to the config file's directory).
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Trade batch-flush interval in milliseconds.
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
    /// Data retention period in hours.  Trades older than this are
    /// deleted on startup (and periodically while running).
    #[serde(default = "default_data_retention_hours")]
    pub data_retention_hours: u64,
    /// Maximum number of trades to buffer in memory before dropping.
    #[serde(default = "default_max_buffered_trades")]
    pub max_buffered_trades: usize,
    /// Hard cap on total DuckDB storage in megabytes.
    /// `0` disables the cap (no size-based purging).
    /// Default: `4096` (4 GiB).
    #[serde(default = "default_max_storage_mb")]
    pub max_storage_mb: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            flush_interval_ms: default_flush_interval(),
            data_retention_hours: default_data_retention_hours(),
            max_buffered_trades: default_max_buffered_trades(),
            max_storage_mb: default_max_storage_mb(),
        }
    }
}

/// `[pairs]` section — base assets and discovery mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairsConfig {
    /// Base assets to expand via the whitelist templates (e.g. `["btc", "eth"]`).
    #[serde(default)]
    pub base_assets: Vec<String>,
    /// When `true`, fetch metadata for **all** supported exchange variants
    /// on startup so `/exchanges` is fully populated, regardless of the
    /// whitelist.  Default: `true`.
    #[serde(default = "default_true")]
    pub discovery_mode: bool,
}

impl Default for PairsConfig {
    fn default() -> Self {
        Self {
            base_assets: Vec::new(),
            discovery_mode: default_true(),
        }
    }
}

const fn default_rate_limit_max_requests() -> u64 {
    500
}

fn default_data_dir() -> String {
    "data".to_string()
}

fn default_bind_address() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8080))
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

const fn default_max_storage_mb() -> u64 {
    4096
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

    /// Project root directory, baked in at compile time.
    ///
    /// Used as the anchor when running via `cargo run` so that persistent
    /// files land next to `Cargo.toml` rather than inside `target/`.
    const CARGO_MANIFEST_DIR: &'static str = env!("CARGO_MANIFEST_DIR");

    /// Resolve the configuration file path.
    ///
    /// - **Distributed binary**: defaults to next to the executable (portable).
    /// - **`cargo run` / `cargo test`**: defaults to the project root
    ///   (next to `Cargo.toml`), keeping persistent files out of `target/`.
    /// - **Explicit `--config` flag**: always honoured.
    pub fn resolve_path(override_path: Option<PathBuf>) -> PathBuf {
        if let Some(path) = override_path {
            return path;
        }

        let running_via_cargo = std::env::var_os("CARGO").is_some()
            || std::env::current_exe()
                .ok()
                .and_then(|p| {
                    p.to_str()
                        .map(|s| s.contains("/target/") || s.contains("\\target\\"))
                })
                .unwrap_or(false);

        if running_via_cargo {
            return Path::new(Self::CARGO_MANIFEST_DIR).join("config.toml");
        }

        if let Ok(exe) = std::env::current_exe()
            && let Some(parent) = exe.parent()
        {
            return parent.join("config.toml");
        }
        PathBuf::from("config.toml")
    }

    pub fn resolve_auth_token(&mut self, data_dir: &Path) -> anyhow::Result<()> {
        if self.network.bind_address.ip().is_loopback() {
            return Ok(());
        }

        let token_file = data_dir.join(".auth_token");
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
            std::fs::create_dir_all(data_dir)
                .with_context(|| format!("creating data dir '{}'", data_dir.display()))?;
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
/// empty strings.  The [`Display`] implementation outputs `[REDACTED]`.
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

/// The expected prefix before the token value.
const BEARER_PREFIX: &str = "Bearer ";

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
        let header = authorization_header.as_bytes();
        let expected_len = BEARER_PREFIX.len() + self.0.len();

        // Length is not a secret (the token length is always 64 hex chars).
        if header.len() != expected_len {
            return false;
        }

        // "Bearer " prefix is public knowledge — we can short-circuit safely.
        if !header[..BEARER_PREFIX.len()].eq_ignore_ascii_case(BEARER_PREFIX.as_bytes()) {
            return false;
        }

        // Constant-time comparison of the token itself.
        let token_start = BEARER_PREFIX.len();
        constant_time_eq_ignore_ascii_case(&header[token_start..], self.0.as_bytes())
    }

    /// Return the raw token string (for writing to disk, etc.).
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Compare two byte slices in **constant time** (case-insensitive ASCII).
///
/// Every byte position is always compared — the function does **not**
/// short-circuit on the first mismatch.
fn constant_time_eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    debug_assert_eq!(
        a.len(),
        b.len(),
        "constant_time_eq_ignore_ascii_case requires equal-length slices"
    );
    let mut result: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x.to_ascii_lowercase() ^ y.to_ascii_lowercase();
    }
    result == 0
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
