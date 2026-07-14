use std::path::Path;

use anyhow::{Context, Result};
use base64::Engine;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use sha2::{Digest, Sha256};

/// A self-signed certificate and its private key, both in PEM format,
/// along with the SHA-256 fingerprint for client-side pinning.
pub struct SelfSignedCert {
    pub cert_pem: String,
    pub key_pem: String,
    /// Hex-encoded SHA-256 fingerprint of the DER-encoded certificate.
    pub fingerprint: String,
}

/// Compute the lowercase hex SHA-256 digest of `data`.
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
fn restrict_permissions(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            tracing::warn!("Failed to set permissions on {}: {e}", path.display());
        }
    }
    let _ = path; // suppress unused warning on non-Unix
}

/// Load a self-signed cert from `data_dir/cert.pem` + `data_dir/key.pem`,
/// or generate and persist them if they don't exist.
pub fn load_or_generate(data_dir: &Path) -> Result<SelfSignedCert> {
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
    let mut params = CertificateParams::new(vec!["flowsurface-server".to_string()])
        .context("creating certificate parameters")?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "flowsurface-server");

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
