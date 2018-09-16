use error::Error;
use futures::prelude::*;
use hyper::client::connect::{Connect, Connected, Destination};
use rustls;
use rustls::internal::pemfile;
use rustls::ClientConfig;
use std::fs::File;
use std::io;
use std::sync::Arc;
use tokio_rustls::{ClientConfigExt, TlsStream};
use webpki;
use webpki_roots;

pub trait ClientConfigPemExt {
    fn set_insecure(&mut self);
    fn add_root_cert(&mut self, pem_path: &str) -> Result<(), Error>;
    fn add_cert_key(&mut self, cert_path: &str, key_path: &str) -> Result<(), Error>;
}

impl ClientConfigPemExt for ClientConfig {
    fn set_insecure(&mut self) {
        warn!("using insecure https client");
        self.dangerous()
            .set_certificate_verifier(Arc::new(DangerServerVerier));
    }

    fn add_root_cert(&mut self, pem_path: &str) -> Result<(), Error> {
        let mut file = io::BufReader::new(File::open(pem_path)?);
        if let Err(_) = self.root_store.add_pem_file(&mut file) {
            Err(Error::Other(format!("add root cert fail: {}", pem_path)))
        } else {
            Ok(())
        }
    }

    fn add_cert_key(&mut self, cert_path: &str, key_path: &str) -> Result<(), Error> {
        let certs = {
            let mut file = io::BufReader::new(File::open(cert_path)?);
            match pemfile::certs(&mut file) {
                Ok(certs) => {
                    if certs.len() == 0 {
                        return Err(Error::Other(format!("empty cert file: {}", cert_path)));
                    }
                    certs
                }
                Err(_) => {
                    return Err(Error::Other(format!("invalid cert file: {}", cert_path)));
                }
            }
        };
        let key = {
            let mut file = io::BufReader::new(File::open(key_path)?);
            match pemfile::rsa_private_keys(&mut file) {
                Ok(keys) => {
                    if keys.len() == 0 {
                        return Err(Error::Other(format!("empty key file: {}", key_path)));
                    }
                    keys.into_iter().next().unwrap()
                }
                Err(_) => {
                    return Err(Error::Other(format!("invalid key file: {}", key_path)));
                }
            }
        };
        self.set_single_client_cert(certs, key);
        Ok(())
    }
}

pub struct HttpsConnector<C: Connect> {
    config: Arc<ClientConfig>,
    connector: C,
}

impl<C: Connect> HttpsConnector<C> {
    pub fn new(mut config: ClientConfig, connector: C) -> HttpsConnector<C> {
        config
            .root_store
            .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
        HttpsConnector {
            config: Arc::new(config),
            connector,
        }
    }
}

impl<C> Connect for HttpsConnector<C>
where
    C: Connect<Error = io::Error>,
    C::Transport: 'static,
    C::Future: 'static,
{
    type Transport = TlsStream<C::Transport, rustls::ClientSession>;
    type Error = io::Error;
    type Future = Box<Future<Item = (Self::Transport, Connected), Error = Self::Error> + Send>;

    fn connect(&self, dst: Destination) -> Self::Future {
        let host = dst.host().to_string();
        let config = self.config.clone();
        Box::new(
            self.connector
                .connect(dst)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                .and_then(move |(stream, connected)| {
                    let domain = webpki::DNSNameRef::try_from_ascii_str(&host).unwrap();
                    config
                        .connect_async(domain, stream)
                        .map(move |stream| (stream, connected))
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                }),
        )
    }
}

struct DangerServerVerier;

impl rustls::ServerCertVerifier for DangerServerVerier {
    fn verify_server_cert(
        &self,
        _roots: &rustls::RootCertStore,
        _presented_certs: &[rustls::Certificate],
        _dns_name: webpki::DNSNameRef,
        _ocsp: &[u8],
    ) -> Result<rustls::ServerCertVerified, rustls::TLSError> {
        Ok(rustls::ServerCertVerified::assertion())
    }
}
