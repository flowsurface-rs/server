use std::path::Path;

use anyhow::{Context, Result};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};

/// A self-signed certificate and its private key, both in PEM format.
pub struct SelfSignedCert {
    pub cert_pem: String,
    pub key_pem: String,
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

        tracing::info!("Loaded existing TLS certificate");
        return Ok(SelfSignedCert { cert_pem, key_pem });
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

    // Persist so we reuse the same cert across restarts.
    std::fs::write(&cert_path, &cert_pem)
        .with_context(|| format!("writing {}", cert_path.display()))?;
    std::fs::write(&key_path, &key_pem)
        .with_context(|| format!("writing {}", key_path.display()))?;

    tracing::info!("TLS certificate written to {}", cert_path.display());

    Ok(SelfSignedCert { cert_pem, key_pem })
}
