use std::rc::Rc;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::time::Duration;
use futures::prelude::*;
use futures::sync::{mpsc, oneshot};
use tokio_core::reactor::Handle;
use tokio_timer::Timer;
use client::{Client, LeaseGrantResult, PlugResult, Service, ServiceEndpoint};
use error::Error;

const GRANT_RETRY_INTERVAL: u64 = 5;

enum Cmd {
    Plug(Service, oneshot::Sender<Result<(), Error>>),
    Unplug(String, String),
    Cancel(String, String),
}

pub struct ServiceKeeper {
    cmd_tx: mpsc::UnboundedSender<Cmd>,
}

impl ServiceKeeper {
    pub fn new(
        client: &Client,
        handle: &Handle,
        ttl: Option<i64>,
        endpoint: ServiceEndpoint,
    ) -> ServiceKeeper {
        let (tx, rx) = mpsc::unbounded();
        handle.spawn(KeepTask::new(client, handle, rx, ttl, endpoint));
        ServiceKeeper { cmd_tx: tx }
    }

    pub fn plug(&self, service: &Service) -> Box<Future<Item = (), Error = Error>> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self.cmd_tx.unbounded_send(Cmd::Plug(service.clone(), tx)) {
            return Box::new(Err(Error::Other("keep task failed".to_string())).into_future());
        }
        Box::new(rx.then(|r| match r {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::Other("keep task failed".to_string())),
        }))
    }

    pub fn unplug<S: Into<String>>(&self, name: S, version: S) {
        let _ = self.cmd_tx
            .unbounded_send(Cmd::Unplug(name.into(), version.into()));
    }
}

struct KeepTask {
    client: Rc<Client>,
    ttl: Option<i64>,
    endpoint: ServiceEndpoint,
    handle: Handle,
    cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    services: HashMap<(String, String), Service>,
    lease_result: Option<LeaseGrantResult>,
    lease_future: Option<Box<Future<Item = LeaseGrantResult, Error = Error>>>,
    replug_future: Option<Box<Future<Item = PlugResult, Error = Error>>>,
    lease_keep_future: Option<Box<Future<Item = (), Error = Error>>>,
}

impl KeepTask {
    fn new(
        client: &Client,
        handle: &Handle,
        cmd_rx: mpsc::UnboundedReceiver<Cmd>,
        ttl: Option<i64>,
        endpoint: ServiceEndpoint,
    ) -> KeepTask {
        KeepTask {
            client: Rc::new(client.clone()),
            ttl: ttl,
            endpoint: endpoint,
            handle: handle.clone(),
            services: HashMap::new(),
            cmd_rx: cmd_rx,
            lease_result: None,
            lease_future: None,
            replug_future: None,
            lease_keep_future: None,
        }
    }

    fn new_lease(&mut self, retry: bool) {
        if retry {
            let timer = Timer::default()
                .sleep(Duration::from_secs(GRANT_RETRY_INTERVAL))
                .map_err(|e| Error::Other(format!("create keep timer fail: {}", e)));
            let (client, ttl) = (self.client.clone(), self.ttl.clone());
            self.lease_future = Some(Box::new(timer.then(move |_| client.grant_lease(ttl))));
        } else {
            self.lease_future = Some(Box::new(self.client.grant_lease(self.ttl.clone())));
        }
        self.lease_keep_future = None;
    }

    fn keep_lease(&mut self) {
        if let Some(ref mut lease_result) = self.lease_result {
            let timer = Timer::default()
                .sleep(Duration::from_secs(lease_result.ttl as u64 / 2))
                .map_err(|e| Error::Other(format!("create keep timer fail: {}", e)));
            let (client, lease_id) = (self.client.clone(), lease_result.lease_id);
            self.lease_keep_future = Some(Box::new(
                timer.then(move |_| client.keepalive_lease(lease_id)),
            ));
        } else {
            error!("missing lease result");
        }
    }

    fn replug_all(&mut self, retry: bool) {
        if let Some(ref lease_result) = self.lease_result {
            let services: Vec<Service> = self.services.values().map(|s| s.clone()).collect();
            if retry {
                let timer = Timer::default()
                    .sleep(Duration::from_secs(GRANT_RETRY_INTERVAL))
                    .map_err(|e| Error::Other(format!("create keep timer fail: {}", e)));
                let (client, endpoint, lease_id) = (
                    self.client.clone(),
                    self.endpoint.clone(),
                    lease_result.lease_id,
                );
                self.replug_future = Some(Box::new(timer.then(move |_| {
                    client.plug_all_services(&services, &endpoint, Some(lease_id), None)
                })));
            } else {
                self.replug_future = Some(Box::new(self.client.plug_all_services(
                    &services,
                    &self.endpoint,
                    Some(lease_result.lease_id),
                    None,
                )));
            }
        } else {
            error!("missing lease result");
        }
    }
}

impl Future for KeepTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        if let Some(r) = self.lease_future.as_mut().map(|f| f.poll()) {
            match r {
                Ok(Async::Ready(result)) => {
                    self.lease_future = None;
                    self.lease_result = Some(result);
                    self.replug_all(false);
                }
                Err(e) => {
                    error!("grant lease fail: {}", e);
                    self.new_lease(true);
                }
                _ => {}
            }
        }
        if let Some(r) = self.replug_future.as_mut().map(|f| f.poll()) {
            match r {
                Ok(Async::Ready(_)) => {
                    self.replug_future = None;
                }
                Err(Error::NotPermitted(_, names)) => {
                    let set: HashSet<String> = HashSet::from_iter(names);
                    self.services.retain(|k, _| {
                        if set.contains(&k.0) {
                            error!("plug service not permitted: {}:{}", k.0, k.1);
                            false
                        } else {
                            true
                        }
                    });
                    self.replug_all(false);
                }
                Err(e) => {
                    error!("replug fail: {}", e);
                    self.replug_all(true);
                }
                _ => {}
            }
        }
        if let Some(r) = self.lease_keep_future.as_mut().map(|f| f.poll()) {
            match r {
                Ok(Async::Ready(_)) => {
                    self.lease_keep_future = None;
                    self.keep_lease();
                }
                Err(e) => {
                    error!("keep lease fail: {}", e);
                    self.new_lease(false);
                }
                _ => {}
            }
        }
        loop {
            if let Some(cmd) = try_ready!(self.cmd_rx.poll()) {
                match cmd {
                    Cmd::Plug(service, tx) => {
                        //
                    }
                    Cmd::Unplug(name, version) => {
                        //
                    }
                    Cmd::Cancel(name, version) => {
                        //
                    }
                }
            } else {
                return Ok(Async::Ready(()));
            }
        }
    }
}
