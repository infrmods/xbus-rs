use super::client::RevisionResult;
use super::error::Error;
use futures::prelude::*;
use futures::sync::{mpsc, oneshot};
use std::time::{Duration, Instant};
use tokio::spawn;
use tokio::timer::Delay;

const WATCH_DELAY: u64 = 5;

pub struct WatchHandle {
    tx: Option<oneshot::Sender<()>>,
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        self.close();
    }
}

impl WatchHandle {
    pub(crate) fn pair() -> (oneshot::Receiver<()>, WatchHandle) {
        let (tx, rx) = oneshot::channel();
        (rx, WatchHandle { tx: Some(tx) })
    }

    fn close(&mut self) {
        drop(self.tx.take());
    }
}

pub(crate) struct WatchTask<T, WF> {
    close_rx: oneshot::Receiver<()>,
    tx: mpsc::UnboundedSender<T>,

    last_revision: Option<u64>,
    watch: WF,
    watch_future: Box<Future<Item = Option<T>, Error = Error> + Send>,
}

impl<T, WF> WatchTask<T, WF>
where
    T: RevisionResult + Send + 'static,
    WF: Fn(Option<u64>) -> Box<Future<Item = Option<T>, Error = Error> + Send> + Send + 'static,
{
    pub fn spawn(revision: Option<u64>, watch: WF) -> (WatchHandle, mpsc::UnboundedReceiver<T>) {
        let (tx, rx) = mpsc::unbounded();
        let (close_rx, handle) = WatchHandle::pair();
        let watch_future = watch(revision.clone());
        spawn(WatchTask {
            close_rx,
            tx,
            last_revision: revision,
            watch,
            watch_future,
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
            self.watch_future = (self.watch)(self.last_revision.clone());
        }
    }
}

impl<T, WF> Future for WatchTask<T, WF>
where
    T: RevisionResult + Send + 'static,
    WF: Fn(Option<u64>) -> Box<Future<Item = Option<T>, Error = Error> + Send> + Send + 'static,
{
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
                self.last_revision = Some(result.get_revision());
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
