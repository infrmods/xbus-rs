use super::client::RevisionResult;
use super::error::Error;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;

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
    watch_future: Pin<Box<dyn Future<Output = Result<Option<T>, Error>> + Send>>,
}

impl<T, WF> WatchTask<T, WF>
where
    T: RevisionResult + Send + 'static,
    WF: Fn(Option<u64>) -> Pin<Box<dyn Future<Output = Result<Option<T>, Error>> + Send>>
        + Send
        + Unpin
        + 'static,
{
    pub fn spawn(revision: Option<u64>, watch: WF) -> WatchStream<T> {
        let (tx, rx) = mpsc::unbounded();
        let (close_rx, handle) = WatchHandle::pair();
        let watch_future = watch(revision);
        spawn(WatchTask {
            close_rx,
            tx,
            last_revision: revision,
            watch,
            watch_future,
        });
        WatchStream::new(handle, rx)
    }

    fn watch_once(&mut self, to_delay: bool) {
        if to_delay {
            self.watch_future = sleep(Duration::from_secs(WATCH_DELAY))
                .map(|_| Ok(None))
                .boxed();
        } else {
            self.watch_future = (self.watch)(self.last_revision);
        }
    }
}

impl<T, WF> Future for WatchTask<T, WF>
where
    T: RevisionResult + Send + 'static,
    WF: Fn(Option<u64>) -> Pin<Box<dyn Future<Output = Result<Option<T>, Error>> + Send>>
        + Send
        + Unpin
        + 'static,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        loop {
            match Pin::new(&mut self.close_rx).poll(cx) {
                Poll::Pending => {}
                Poll::Ready(_) => {
                    return Poll::Ready(());
                }
            }
            match Pin::new(&mut self.watch_future).poll(cx) {
                Poll::Pending => {
                    break;
                }
                Poll::Ready(Ok(Some(result))) => {
                    let revision = result.get_revision();
                    if revision > 0 {
                        self.last_revision = Some(revision);
                    }
                    if self.tx.unbounded_send(result).is_err() {
                        return Poll::Ready(());
                    }
                    self.watch_once(false);
                }
                Poll::Ready(Ok(None)) => {
                    self.watch_once(false);
                }
                Poll::Ready(Err(e)) => {
                    error!("watch fail: {}", e);
                    self.watch_once(true);
                }
            }
        }
        Poll::Pending
    }
}

pub struct WatchStream<T> {
    handle: WatchHandle,
    rx: mpsc::UnboundedReceiver<T>,
}

impl<T> WatchStream<T> {
    fn new(handle: WatchHandle, rx: mpsc::UnboundedReceiver<T>) -> Self {
        WatchStream { handle, rx }
    }

    pub fn split(self) -> (WatchHandle, mpsc::UnboundedReceiver<T>) {
        (self.handle, self.rx)
    }
}

impl<T> Stream for WatchStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}
