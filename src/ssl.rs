use std::fmt::Debug;
use std::io::{Read, Write, Error as IoError};
use std::net::{SocketAddr, Shutdown};
use std::time::Duration;
use openssl::ssl::{SslStream, SslContext, Ssl, SslMethod};
use openssl::x509::X509_FILETYPE_PEM;
use openssl::ssl::SSL_VERIFY_PEER;
use openssl::error::ErrorStack;
use hyper::net::{NetworkStream, SslClient, HttpsConnector};
use hyper::Error as HError;


pub struct WrapStream<T>(SslStream<T>);

impl<T> Clone for WrapStream<T> {
    fn clone(&self) -> WrapStream<T> {
        unimplemented!()
    }
}

impl<T: Read + Write> Read for WrapStream<T> {
    #[inline]
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, IoError> {
        self.0.read(&mut buf)
    }
}

impl<T: Read + Write> Write for WrapStream<T> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize, IoError> {
        self.0.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<(), IoError> {
        self.0.flush()
    }
}

impl<T: NetworkStream> NetworkStream for WrapStream<T> {
    #[inline]
    fn peer_addr(&mut self) -> Result<SocketAddr, IoError> {
        self.0.get_mut().peer_addr()
    }

    #[inline]
    fn set_read_timeout(&self, dur: Option<Duration>) -> Result<(), IoError> {
        self.0.get_ref().set_read_timeout(dur)
    }

    #[inline]
    fn set_write_timeout(&self, dur: Option<Duration>) -> Result<(), IoError> {
        self.0.get_ref().set_write_timeout(dur)
    }

    fn close(&mut self, how: Shutdown) -> Result<(), IoError> {
        self.0.get_mut().close(how)
    }
}

#[derive(Debug, Clone)]
pub struct OpensslClient(SslContext);

impl OpensslClient {
    pub fn new(ctx: SslContext) -> OpensslClient {
        OpensslClient(ctx)
    }
}

fn err_stack_to_hyper(e: ErrorStack) -> HError {
    HError::Ssl(Box::new(::Error::from(e)))
}

impl<T: NetworkStream + Send + Clone + Debug> SslClient<T> for OpensslClient {
    type Stream = WrapStream<T>;

    fn wrap_client(&self, stream: T, host: &str) -> Result<Self::Stream, HError> {
        let mut ssl = Ssl::new(&self.0).map_err(err_stack_to_hyper)?;
        ssl.set_hostname(host).map_err(err_stack_to_hyper)?;
        let host = host.to_owned();
        ssl.set_verify_callback(SSL_VERIFY_PEER,
                                move |p, x| ::openssl_verify::verify_callback(&host, p, x));
        Ok(WrapStream(ssl.connect(stream)
            .map_err(|e| HError::Ssl(Box::new(::Error::Ssl(format!("connect fail: {}", e)))))?))
    }
}

pub fn new_https_connector(ca_cert: Option<&String>,
                           cert_key: Option<&(String, String)>)
                           -> Result<HttpsConnector<OpensslClient>, ::Error> {
    let mut builder = SslContext::builder(SslMethod::tls()).unwrap();
    if let Some(path) = ca_cert {
        builder.set_ca_file(path).unwrap();
    }
    if let Some(&(ref cert, ref key)) = cert_key {
        builder.set_certificate_file(cert, X509_FILETYPE_PEM).unwrap();
        builder.set_private_key_file(key, X509_FILETYPE_PEM).unwrap();
    }
    Ok(HttpsConnector::new(OpensslClient::new(builder.build())))
}
