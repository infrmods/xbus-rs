use crate::cert::get_cert_cn;
use crate::error::Error;
use futures::prelude::*;
use hyper::client::connect::{Connected, Connection};
use hyper::service::Service;
use hyper::Uri;
use tokio_rustls::rustls::{self, Certificate, ClientConfig, PrivateKey};
use std::io::{Error as IoErr, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::webpki;
use tokio_rustls::webpki::DNSNameRef;

pub trait TlsClientConfigExt {
    fn set_insecure(&mut self);
    fn add_cert_key(&mut self, certs: Vec<Certificate>, key: PrivateKey) -> Result<String, Error>;
}

impl TlsClientConfigExt for ClientConfig {
    fn set_insecure(&mut self) {
        self.dangerous()
            .set_certificate_verifier(Arc::new(DangerServerVerier));
    }

    fn add_cert_key(&mut self, certs: Vec<Certificate>, key: PrivateKey) -> Result<String, Error> {
        let cn =
            get_cert_cn(&certs[0].0).ok_or_else(|| Error::Other("get cert cn fail".to_string()))?;
        if let Err(e) = self.set_single_client_cert(certs, key) {
            return Err(Error::Ssl(format!("add client cert fail: {}", e)));
        }
        Ok(cn)
    }
}

#[derive(Clone)]
pub struct HttpsConnector<T> {
    http: T,
    tls: TlsConnector,
}

impl<T> HttpsConnector<T> {
    pub fn new(mut config: ClientConfig, http: T) -> HttpsConnector<T> {
        config
            .root_store
            .add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
        HttpsConnector {
            http,
            tls: Arc::new(config).into(),
        }
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl<T> Service<Uri> for HttpsConnector<T>
where
    T: Service<Uri>,
    T::Response: AsyncRead + AsyncWrite + Send + Unpin,
    T::Future: Send + 'static,
    T::Error: Into<BoxError>,
{
    type Response = MaybeHttpsStream<T::Response>;
    type Error = BoxError;
    type Future = HttpsConnecting<T::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.http.poll_ready(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let is_https = dst.scheme_str() == Some("https");
        let host = dst.host().unwrap_or("").to_owned();
        let connecting = self.http.call(dst);
        let tls = self.tls.clone();
        let fut = async move {
            let tcp = connecting.await.map_err(Into::into)?;
            let maybe = if is_https {
                let domain = DNSNameRef::try_from_ascii_str(&host).unwrap();
                let tls = tls.connect(domain, tcp).await?;
                MaybeHttpsStream::Tls(tls)
            } else {
                MaybeHttpsStream::Http(tcp)
            };
            Ok(maybe)
        };
        HttpsConnecting(Box::pin(fut))
    }
}

pub enum MaybeHttpsStream<T> {
    Http(T),
    Tls(TlsStream<T>),
}

impl<T: AsyncRead + AsyncWrite + Unpin> AsyncRead for MaybeHttpsStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<(), IoErr>> {
        match self.get_mut() {
            MaybeHttpsStream::Http(http) => Pin::new(http).poll_read(cx, buf),
            MaybeHttpsStream::Tls(tls) => Pin::new(tls).poll_read(cx, buf),
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> AsyncWrite for MaybeHttpsStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, IoErr>> {
        match self.get_mut() {
            MaybeHttpsStream::Http(http) => Pin::new(http).poll_write(cx, buf),
            MaybeHttpsStream::Tls(tls) => Pin::new(tls).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), IoErr>> {
        match self.get_mut() {
            MaybeHttpsStream::Http(http) => Pin::new(http).poll_flush(cx),
            MaybeHttpsStream::Tls(tls) => Pin::new(tls).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), IoErr>> {
        match self.get_mut() {
            MaybeHttpsStream::Http(http) => Pin::new(http).poll_shutdown(cx),
            MaybeHttpsStream::Tls(tls) => Pin::new(tls).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, IoErr>> {
        match self.get_mut() {
            MaybeHttpsStream::Http(http) => Pin::new(http).poll_write_vectored(cx, bufs),
            MaybeHttpsStream::Tls(tls) => Pin::new(tls).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            MaybeHttpsStream::Http(http) => http.is_write_vectored(),
            MaybeHttpsStream::Tls(tls) => tls.is_write_vectored(),
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Connection + Unpin> Connection for MaybeHttpsStream<T> {
    fn connected(&self) -> Connected {
        match self {
            MaybeHttpsStream::Http(s) => s.connected(),
            MaybeHttpsStream::Tls(s) => s.get_ref().0.connected(),
        }
    }
}

type BoxedFut<T> = Pin<Box<dyn Future<Output = Result<MaybeHttpsStream<T>, BoxError>> + Send>>;

pub struct HttpsConnecting<T>(BoxedFut<T>);

impl<T: AsyncRead + AsyncWrite + Unpin> Future for HttpsConnecting<T> {
    type Output = Result<MaybeHttpsStream<T>, BoxError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
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
