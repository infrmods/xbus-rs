use crate::error::Error;
use rustls::internal::pemfile;
use rustls::{Certificate, PrivateKey, RootCertStore};
use std::fs::File;
use std::io;
use std::time::Duration;

const DEFAULT_REQUEST_TIMEOUT: u64 = 5;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct Config {
    pub endpoint: String,
    pub insecure: bool,
    pub dev_app: Option<String>,
    pub ca_file: Option<String>,
    pub cert_key_file: Option<(String, String)>,
    pub max_idle_connections: Option<usize>,
    pub request_timeout: Duration,
}

impl Config {
    pub fn new(endpoint: &str) -> Config {
        Config {
            endpoint: endpoint.to_owned(),
            insecure: false,
            dev_app: None,
            ca_file: None,
            cert_key_file: None,
            max_idle_connections: None,
            request_timeout: Duration::from_secs(DEFAULT_REQUEST_TIMEOUT),
        }
    }

    pub fn ca_file(mut self, file: &str) -> Config {
        self.ca_file = Some(file.to_owned());
        self
    }

    pub fn cert_key_file(mut self, cert: &str, key: &str) -> Config {
        self.cert_key_file = Some((cert.to_owned(), key.to_owned()));
        self
    }

    pub fn add_ca(&self, store: &mut RootCertStore) -> Result<(), Error> {
        if let Some(path) = &self.ca_file {
            let f = File::open(path)?;
            if store.add_pem_file(&mut io::BufReader::new(f)).is_err() {
                return Err(Error::Other("add cacert to rootstore fail".to_string()));
            }
        }
        Ok(())
    }

    pub fn load_cert_key(&self) -> Result<Option<(Vec<Certificate>, PrivateKey)>, Error> {
        if let Some((cert_path, key_path)) = &self.cert_key_file {
            let certs = load_pem_certs(cert_path)?;
            let keys = load_pem_keys(key_path)?;
            if let Some(key) = keys.into_iter().next() {
                return Ok(Some((certs, key)));
            }
        }
        Ok(None)
    }
}

fn load_pem_certs(cert_path: &str) -> Result<Vec<Certificate>, Error> {
    let mut file = io::BufReader::new(
        File::open(cert_path)
            .map_err(|e| Error::Other(format!("open cert file({}) fail: {}", cert_path, e)))?,
    );
    match pemfile::certs(&mut file) {
        Ok(certs) => {
            if certs.is_empty() {
                return Err(Error::Other(format!("empty cert file: {}", cert_path)));
            }
            Ok(certs)
        }
        Err(_) => {
            return Err(Error::Other(format!("invalid cert file: {}", cert_path)));
        }
    }
}

fn load_pem_keys(key_path: &str) -> Result<Vec<PrivateKey>, Error> {
    let mut file = io::BufReader::new(
        File::open(key_path)
            .map_err(|e| Error::Other(format!("open key file({}) fail: {}", key_path, e)))?,
    );
    match pemfile::rsa_private_keys(&mut file) {
        Ok(keys) => {
            if keys.is_empty() {
                return Err(Error::Other(format!("empty key file: {}", key_path)));
            }
            Ok(keys)
        }
        Err(_) => {
            return Err(Error::Other(format!("invalid key file: {}", key_path)));
        }
    }
}
