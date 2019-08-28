use crate::cert::get_cert_cn;
use crate::error::Error;
use futures::prelude::*;
use hyper::client::connect::{Connect, Connected, Destination};
use rustls;
use rustls::internal::pemfile;
use rustls::ClientConfig;
use std::fs::File;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use webpki;
use webpki_roots;

pub trait ClientConfigPemExt {
    fn set_insecure(&mut self);
    fn add_root_cert(&mut self, pem_path: &str) -> Result<(), Error>;
    fn add_cert_key(&mut self, cert_path: &str, key_path: &str) -> Result<String, Error>;
}

impl ClientConfigPemExt for ClientConfig {
    fn set_insecure(&mut self) {
        warn!("using insecure https client");
        self.dangerous()
            .set_certificate_verifier(Arc::new(DangerServerVerier));
    }

    fn add_root_cert(&mut self, pem_path: &str) -> Result<(), Error> {
        let mut file =
            io::BufReader::new(File::open(pem_path).map_err(|e| {
                Error::Other(format!("open cacert file({}) fail: {}", pem_path, e))
            })?);
        if self.root_store.add_pem_file(&mut file).is_err() {
            Err(Error::Other(format!("add root cert fail: {}", pem_path)))
        } else {
            Ok(())
        }
    }

    fn add_cert_key(&mut self, cert_path: &str, key_path: &str) -> Result<String, Error> {
        let certs = {
            let mut file =
                io::BufReader::new(File::open(cert_path).map_err(|e| {
                    Error::Other(format!("open cert file({}) fail: {}", cert_path, e))
                })?);
            match pemfile::certs(&mut file) {
                Ok(certs) => {
                    if certs.is_empty() {
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
            let mut file =
                io::BufReader::new(File::open(key_path).map_err(|e| {
                    Error::Other(format!("open key file({}) fail: {}", key_path, e))
                })?);
            match pemfile::rsa_private_keys(&mut file) {
                Ok(keys) => {
                    if keys.is_empty() {
                        return Err(Error::Other(format!("empty key file: {}", key_path)));
                    }
                    keys.into_iter().next().unwrap()
                }
                Err(_) => {
                    return Err(Error::Other(format!("invalid key file: {}", key_path)));
                }
            }
        };
        let cn =
            get_cert_cn(&certs[0].0).ok_or_else(|| Error::Other("get cert cn fail".to_string()))?;
        self.set_single_client_cert(certs, key);
        Ok(cn)
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
    type Transport = TlsStream<C::Transport>;
    type Error = io::Error;
    type Future = HttpsConnecting<C>;

    fn connect(&self, dst: Destination) -> Self::Future {
        HttpsConnecting {
            domain: dst.host().to_string(),
            http_conn_fut: Some(self.connector.connect(dst)),
            connected: None,
            tls_connector: TlsConnector::from(self.config.clone()),
            tls_conn_fut: None,
        }
    }
}

pub struct HttpsConnecting<C: Connect> {
    domain: String,
    http_conn_fut: Option<C::Future>,
    connected: Option<Connected>,
    tls_connector: TlsConnector,
    tls_conn_fut: Option<tokio_rustls::Connect<C::Transport>>,
}

impl<C: Connect<Error = io::Error>> Future for HttpsConnecting<C> {
    type Output = Result<(TlsStream<C::Transport>, Connected), io::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Some(http_conn_fut) = self.http_conn_fut.as_mut() {
            match ready!(Pin::new(http_conn_fut).poll(cx)) {
                Ok((stream, connected)) => {
                    self.connected = Some(connected);
                    let domain = webpki::DNSNameRef::try_from_ascii_str(&self.domain).unwrap();
                    self.tls_conn_fut = Some(self.tls_connector.connect(domain, stream));
                    self.http_conn_fut = None;
                }
                Err(e) => {
                    return Poll::Ready(Err(e));
                }
            }
        }
        if let Some(tls_conn_fut) = self.tls_conn_fut.as_mut() {
            let stream = ready!(Pin::new(tls_conn_fut).poll(cx))?;
            return Poll::Ready(Ok((stream, self.connected.take().unwrap())));
        }
        Poll::Pending
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
