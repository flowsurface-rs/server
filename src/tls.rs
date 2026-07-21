use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use base64::Engine;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
use sha2::{Digest, Sha256};

use crate::storage::Storage;

/// A self-signed certificate and its private key, both in PEM format,
/// along with the SHA-256 fingerprint for client-side pinning.
struct SelfSignedCert {
    cert_pem: String,
    key_pem: String,
    /// Hex-encoded SHA-256 fingerprint of the DER-encoded certificate.
    fingerprint: String,
}

impl SelfSignedCert {
    /// Load a self-signed cert from `data_dir/cert.pem` + `data_dir/key.pem`,
    /// or generate and persist them if they don't exist.
    ///
    /// `domain` is inserted as a DNS SAN (Subject Alternative Name) so that
    /// clients connecting via that DNS name do not get a hostname-verification
    /// error.  `bind_ip`, when provided, is also added as an IP SAN so that
    /// direct IP connections pass hostname checks as well.
    fn load_or_generate(
        data_dir: &Path,
        domain: &str,
        bind_ip: Option<std::net::IpAddr>,
    ) -> Result<Self> {
        let cert_path = data_dir.join("cert.pem");
        let key_path = data_dir.join("key.pem");

        if cert_path.exists() && key_path.exists() {
            let cert_pem = std::fs::read_to_string(&cert_path)
                .with_context(|| format!("reading {}", cert_path.display()))?;
            let key_pem = std::fs::read_to_string(&key_path)
                .with_context(|| format!("reading {}", key_path.display()))?;

            let fingerprint = fingerprint_from_pem(&cert_pem)
                .context("failed to compute fingerprint from loaded certificate")?;

            tracing::info!("Loaded existing TLS certificate");
            return Ok(SelfSignedCert {
                cert_pem,
                key_pem,
                fingerprint,
            });
        }

        tracing::info!("Generating self-signed TLS certificate…");

        let key_pair = KeyPair::generate().context("generating ECDSA P-256 key pair")?;
        let mut params = CertificateParams::new(vec![domain.to_string()])
            .context("creating certificate parameters")?;
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, domain);

        // If binding to a specific (non-wildcard) IP, include it as an IP SAN.
        if let Some(ip) = bind_ip {
            params.subject_alt_names.push(SanType::IpAddress(ip));
        }

        let cert = params
            .self_signed(&key_pair)
            .context("self-signing certificate")?;

        let cert_pem = cert.pem();
        let key_pem = key_pair.serialize_pem();

        // Compute the SHA-256 fingerprint of the DER-encoded certificate.
        let fingerprint = fingerprint_from_pem(&cert_pem)
            .context("failed to compute fingerprint from generated certificate")?;

        // Persist so we reuse the same cert across restarts.
        std::fs::write(&cert_path, &cert_pem)
            .with_context(|| format!("writing {}", cert_path.display()))?;
        restrict_permissions(&cert_path);

        std::fs::write(&key_path, &key_pem)
            .with_context(|| format!("writing {}", key_path.display()))?;
        restrict_permissions(&key_path);

        tracing::info!("TLS certificate written to {}", cert_path.display());

        Ok(SelfSignedCert {
            cert_pem,
            key_pem,
            fingerprint,
        })
    }

    async fn into_rustl_config(self) -> Result<RustlsConfig> {
        RustlsConfig::from_pem(
            self.cert_pem.as_bytes().to_vec(),
            self.key_pem.as_bytes().to_vec(),
        )
        .await
        .context("building TLS config from PEM")
    }
}

/// Set up TLS for the HTTP server.
///
/// - Returns `Ok(None)` when the bind address is loopback (plain HTTP).
/// - Detects `tls_domain` changes and regenerates the certificate.
/// - Loads or generates a self-signed certificate, persists the domain to
///   storage metadata, and builds the `RustlsConfig`.
///
/// # Errors
///
/// Propagates errors from certificate loading/generation and from building
/// the `RustlsConfig`.
pub async fn setup_tls_config(
    data_dir: &Path,
    storage: &Storage,
    bind_address: SocketAddr,
    tls_domain: &str,
) -> Result<Option<RustlsConfig>> {
    if bind_address.ip().is_loopback() {
        return Ok(None);
    }

    let bind_ip = (!bind_address.ip().is_unspecified()).then_some(bind_address.ip());

    // If the domain changed, remove old cert/key so load_or_generate
    // creates new ones for the new domain.
    let cert_path = data_dir.join("cert.pem");
    let key_path = data_dir.join("key.pem");
    if cert_path.exists() && key_path.exists() {
        let stored_domain = storage.get_metadata("tls_domain").ok().flatten();
        if stored_domain.as_deref() != Some(tls_domain) {
            tracing::warn!(
                "tls_domain changed ({:?} → {:?}), regenerating TLS certificate",
                stored_domain,
                tls_domain,
            );
            std::fs::remove_file(&cert_path).ok();
            std::fs::remove_file(&key_path).ok();
        }
    }

    let tls_cert = SelfSignedCert::load_or_generate(data_dir, tls_domain, bind_ip)?;

    if let Err(e) = storage.set_metadata("tls_domain", tls_domain) {
        tracing::warn!("Failed to persist tls_domain metadata: {e:#}");
    }

    tracing::info!(
        "TLS certificate fingerprint (SHA-256): {}",
        tls_cert.fingerprint
    );
    tracing::info!(
        "Use this fingerprint for cert pinning: sha256${}",
        tls_cert.fingerprint
    );

    let config = tls_cert.into_rustl_config().await?;
    Ok(Some(config))
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Extract the DER bytes from the first PEM-encoded X.509 certificate
/// by stripping the PEM armour and base64-decoding the body.
fn pem_to_der(pem: &str) -> Option<Vec<u8>> {
    let mut lines = pem.lines();
    // Skip the `-----BEGIN CERTIFICATE-----` header.
    lines.next()?;
    let mut b64 = String::new();
    for line in lines {
        if line.starts_with("-----") {
            break;
        }
        b64.push_str(line.trim());
    }
    base64::engine::general_purpose::STANDARD.decode(&b64).ok()
}

/// Compute the SHA-256 fingerprint of a PEM-encoded certificate.
fn fingerprint_from_pem(pem: &str) -> Option<String> {
    let der = pem_to_der(pem)?;
    Some(sha256_hex(&der))
}

/// Set Unix permissions to 0o600 (owner read/write only) on `path`.
/// Silently ignored on non-Unix platforms.
pub(crate) fn restrict_permissions(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            tracing::warn!("Failed to set permissions on {}: {e}", path.display());
        }
    }
    let _ = path; // suppress unused warning on non-Unix
}
