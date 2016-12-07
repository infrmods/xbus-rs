use std::io::{Read, Write, Error as IoError};
use std::process::exit;
use std::net::{SocketAddr, Shutdown};
use std::time::Duration;
use openssl::ssl::{SslStream, SslContext, Ssl, SslMethod};
use openssl::x509::X509_FILETYPE_PEM;
use hyper::net::{NetworkStream, SslClient, HttpsConnector, HttpStream};
use hyper::Error as HError;


pub struct WrapStream<T>(SslStream<T>);

impl<T> Clone for WrapStream<T> {
    fn clone(&self) -> WrapStream<T> {
        unimplemented!()
    }
}

impl<T: Read + Write> Read for WrapStream<T> {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, IoError> {
        println!("read stream");
        let r = self.0.read(&mut buf);
        println!("read ok");
        r
    }
}

impl<T: Read + Write> Write for WrapStream<T> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, IoError> {
        println!("write stream");
        let r = self.0.write(buf);
        println!("--write finished");
        r
    }

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
    /// Creates a new OpensslClient with a custom SslContext
    pub fn new(ctx: SslContext) -> OpensslClient {
        OpensslClient(ctx)
    }
}

impl<T: NetworkStream + Send + Clone> SslClient<T> for OpensslClient {
    type Stream = WrapStream<T>;

    #[cfg(not(windows))]
    fn wrap_client(&self, stream: T, host: &str) -> Result<Self::Stream, HError> {
        // TODO
        let mut ssl = Ssl::new(&self.0).unwrap();
        ssl.set_hostname(host).unwrap();
        let host = host.to_owned();
        // ssl.set_verify_callback(SSL_VERIFY_PEER,
        // move |p, x| ::openssl_verify::verify_callback(&host, p, x));
        Ok(WrapStream(ssl.connect(stream).unwrap_or_else(|e| {
            println!("ssl connect fail");
            exit(-1);
        })))
    }

    // #[cfg(windows)]
    // fn wrap_client(&self, stream: T, host: &str) -> Result<Self::Stream, HError> {
    // let mut ssl = try!(Ssl::new(&self.0));
    // try!(ssl.set_hostname(host));
    // SslStream::connect(ssl, stream).map_err(From::from)
    // }
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
