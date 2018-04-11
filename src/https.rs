use std::io::{self, Read, Write};
use futures::prelude::*;
use hyper::client::connect::{Connect, Connected, Destination};
use native_tls::{Error as TlsError, HandshakeError, TlsConnector, TlsStream};
use tokio_io::io::AllowStdIo;

pub struct HttpsConnector<C: Connect> {
    tls_connector: TlsConnector,
    connector: C,
}

impl<C: Connect> HttpsConnector<C> {
    pub fn new(tls_connector: TlsConnector, connector: C) -> HttpsConnector<C> {
        HttpsConnector {
            tls_connector,
            connector,
        }
    }
}

impl<C: Connect + 'static> Connect for HttpsConnector<C> {
    type Transport = AllowStdIo<TlsStream<C::Transport>>;
    type Error = io::Error;
    type Future = Box<Future<Item = (Self::Transport, Connected), Error = Self::Error> + Send>;

    fn connect(&self, dst: Destination) -> Self::Future {
        let tls_connector = self.tls_connector.clone();
        let domain = dst.host().to_string();
        Box::new(
            self.connector
                .connect(dst)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                .and_then(move |(stream, connected)| {
                    tls_connector
                        .connect_async(&domain, stream)
                        .map(move |stream| (AllowStdIo::new(stream), connected))
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                }),
        )
    }
}

pub trait ConnectAsync {
    fn connect_async<S: Read + Write>(&self, domain: &str, stream: S) -> HandShadke<S>;
}

impl ConnectAsync for TlsConnector {
    fn connect_async<S: Read + Write>(&self, domain: &str, stream: S) -> HandShadke<S> {
        HandShadke(Some(self.connect(domain, stream)))
    }
}

pub struct HandShadke<S: Read + Write>(Option<Result<TlsStream<S>, HandshakeError<S>>>);

impl<S: Read + Write> Future for HandShadke<S> {
    type Item = TlsStream<S>;
    type Error = TlsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.take() {
            None => {
                panic!("repoll");
            }
            Some(Ok(stream)) => Ok(Async::Ready(stream)),
            Some(Err(e)) => match e {
                HandshakeError::Failure(e) => Err(e),
                HandshakeError::Interrupted(mid) => match mid.handshake() {
                    Ok(stream) => Ok(Async::Ready(stream)),
                    Err(HandshakeError::Failure(e)) => Err(e),
                    e @ Err(HandshakeError::Interrupted(_)) => {
                        self.0 = Some(e);
                        Ok(Async::NotReady)
                    }
                },
            },
        }
    }
}
