use super::client::{AppNodes, Client, WatchHandle};
use super::error::Error;
use futures::prelude::*;
use futures::sync::{mpsc, oneshot};
use std::time::{Duration, Instant};
use tokio::spawn;
use tokio::timer::Delay;

const WATCH_DELAY: u64 = 5;

pub struct WatchTask {
    app: String,
    label: Option<String>,
    timeout: u64,
    close_rx: oneshot::Receiver<()>,
    tx: mpsc::UnboundedSender<AppNodes>,

    client: Client,
    last_revision: u64,
    watch_future: Box<Future<Item = Option<AppNodes>, Error = Error> + Send>,
}

impl WatchTask {
    pub fn spawn(
        client: &Client,
        app: &str,
        label: Option<&str>,
        timeout: u64,
    ) -> (WatchHandle, mpsc::UnboundedReceiver<AppNodes>) {
        let (tx, rx) = mpsc::unbounded();
        let (close_rx, handle) = WatchHandle::pair();
        spawn(WatchTask {
            app: app.to_string(),
            label: label.map(|s| s.to_string()),
            timeout,
            close_rx,
            tx,
            client: client.clone(),
            last_revision: 0,
            watch_future: client.query_app_nodes(app, label),
        });
        (handle, rx)
    }

    fn watch_once(&mut self, delay: bool) {
        if delay {
            self.watch_future = Box::new(
                Delay::new(Instant::now() + Duration::from_secs(WATCH_DELAY))
                    .map(|_| None)
                    .map_err(|e| Error::Other(format!("watch app sleep fail: {}", e))),
            );
        } else {
            self.watch_future = self.client.watch_app_nodes_once(
                &self.app,
                self.label.as_ref().map(|s| s.as_str()),
                self.last_revision,
                self.timeout,
            );
        }
    }
}

impl Future for WatchTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.close_rx.poll() {
            Ok(Async::NotReady) => {}
            Ok(Async::Ready(_)) | Err(_) => {
                return Ok(Async::Ready(()));
            }
        }
        match self.watch_future.poll() {
            Ok(Async::NotReady) => {}
            Ok(Async::Ready(Some(result))) => {
                self.last_revision = result.revision;
                if self.tx.unbounded_send(result).is_err() {
                    return Ok(Async::Ready(()));
                }
                self.watch_once(false);
            }
            Ok(Async::Ready(None)) => {
                self.watch_once(false);
            }
            Err(e) => {
                error!("watch app nodes fail: {}", e);
                self.watch_once(true);
            }
        }
        Ok(Async::NotReady)
    }
}
