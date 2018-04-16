use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::time::{Duration, Instant};
use futures::prelude::*;
use futures::sync::{mpsc, oneshot};
use client::{Client, LeaseGrantResult, PlugResult, Service, ServiceEndpoint};
use tokio::timer::Delay;
use tokio::spawn;
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
    pub fn new(client: &Client, ttl: Option<i64>, endpoint: ServiceEndpoint) -> ServiceKeeper {
        let (tx, rx) = mpsc::unbounded();
        spawn(KeepTask::new(client, tx.clone(), rx, ttl, endpoint));
        ServiceKeeper { cmd_tx: tx }
    }

    pub fn plug(&self, service: &Service) -> Box<Future<Item = (), Error = Error> + Send> {
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
    client: Client,
    ttl: Option<i64>,
    endpoint: ServiceEndpoint,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
    cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    services: HashMap<(String, String), Service>,
    lease_result: Option<LeaseGrantResult>,
    lease_future: Option<Box<Future<Item = LeaseGrantResult, Error = Error> + Send>>,
    replug_future: Option<Box<Future<Item = PlugResult, Error = Error> + Send>>,
    replug_backs: HashMap<(String, String), oneshot::Sender<Result<(), Error>>>,
    lease_keep_future: Option<Box<Future<Item = (), Error = Error> + Send>>,
}

impl KeepTask {
    fn new(
        client: &Client,
        cmd_tx: mpsc::UnboundedSender<Cmd>,
        cmd_rx: mpsc::UnboundedReceiver<Cmd>,
        ttl: Option<i64>,
        endpoint: ServiceEndpoint,
    ) -> KeepTask {
        KeepTask {
            client: client.clone(),
            ttl: ttl,
            endpoint: endpoint,
            services: HashMap::new(),
            cmd_tx: cmd_tx,
            cmd_rx: cmd_rx,
            lease_result: None,
            lease_future: None,
            replug_future: None,
            replug_backs: HashMap::new(),
            lease_keep_future: None,
        }
    }

    fn new_lease(&mut self, retry: bool) {
        if retry {
            let delay = Delay::new(Instant::now() + Duration::from_secs(GRANT_RETRY_INTERVAL))
                .map_err(|e| Error::Other(format!("lease keep sleep fail: {}", e)));
            let (client, ttl) = (self.client.clone(), self.ttl.clone());
            self.lease_future = Some(Box::new(delay.then(move |_| client.grant_lease(ttl))));
        } else {
            self.lease_future = Some(Box::new(self.client.grant_lease(self.ttl.clone())));
        }
        self.lease_keep_future = None;
    }

    fn keep_lease(&mut self) {
        if let Some(ref mut lease_result) = self.lease_result {
            let delay = Delay::new(
                Instant::now() + Duration::from_secs(lease_result.ttl as u64 / 2),
            ).map_err(|e| Error::Other(format!("keep lease sleep fail: {}", e)));
            let (client, lease_id) = (self.client.clone(), lease_result.lease_id);
            self.lease_keep_future = Some(Box::new(
                delay.then(move |_| client.keepalive_lease(lease_id)),
            ));
        } else {
            error!("missing lease result");
        }
    }

    fn replug_all(&mut self, retry: bool) {
        if let Some(ref lease_result) = self.lease_result {
            let services: Vec<Service> = self.services.values().map(|s| s.clone()).collect();
            if retry {
                let delay = Delay::new(Instant::now() + Duration::from_secs(GRANT_RETRY_INTERVAL))
                    .map_err(|e| Error::Other(format!("replug sleep fail: {}", e)));
                let (client, endpoint, lease_id) = (
                    self.client.clone(),
                    self.endpoint.clone(),
                    lease_result.lease_id,
                );
                self.replug_future = Some(Box::new(delay.then(move |_| {
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

    fn plug_one(&mut self, service: Service, tx: oneshot::Sender<Result<(), Error>>) {
        if let Some(ref lease_result) = self.lease_result {
            let cmd_tx = self.cmd_tx.clone();
            spawn(
                self.client
                    .plug_service(&service, &self.endpoint, None, Some(lease_result.lease_id))
                    .then(move |r| {
                        Ok(match r {
                            Ok(_) => {
                                let _ = tx.send(Ok(()));
                            }
                            Err(e) => {
                                if !e.can_retry() {
                                    let _ = cmd_tx.send(Cmd::Cancel(service.name, service.version));
                                }
                                let _ = tx.send(Err(e));
                            }
                        })
                    }),
            );
        } else {
            error!("missing lease result");
        }
    }
}

impl Future for KeepTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            match self.cmd_rx.poll() {
                Ok(Async::Ready(Some(cmd))) => match cmd {
                    Cmd::Plug(service, tx) => {
                        let key = (service.name.clone(), service.version.clone());
                        if self.services.contains_key(&key) {
                            let _ = tx.send(Err(Error::Other(format!(
                                "{}:{} has beed plugged",
                                key.0, key.1
                            ))));
                        } else if self.lease_result.is_some() {
                            self.services.insert(key, service.clone());
                            self.plug_one(service, tx);
                        } else {
                            self.services.insert(key.clone(), service.clone());
                            self.replug_backs.insert(key, tx);
                            if self.lease_future.is_none() {
                                self.new_lease(false)
                            }
                        }
                    }
                    Cmd::Unplug(name, version) => {
                        let key = (name, version);
                        self.services.remove(&key);
                        self.replug_backs.remove(&key);
                    }
                    Cmd::Cancel(name, version) => {
                        let key = (name, version);
                        self.services.remove(&key);
                        self.replug_backs.remove(&key);
                    }
                },
                Ok(Async::NotReady) => {
                    break;
                }
                _ => {
                    return Ok(Async::Ready(()));
                }
            }
        }
        let mut ct = true;
        while ct {
            ct = false;
            if let Some(r) = self.lease_future.as_mut().map(|f| f.poll()) {
                match r {
                    Ok(Async::Ready(result)) => {
                        self.lease_future = None;
                        self.lease_result = Some(result);
                        self.keep_lease();
                        self.replug_all(false);
                        ct = true;
                    }
                    Err(e) => {
                        self.new_lease(true);
                        ct = true;
                        error!("grant lease fail: {}", e);
                    }
                    _ => {}
                }
            }
            if let Some(r) = self.replug_future.as_mut().map(|f| f.poll()) {
                match r {
                    Ok(Async::Ready(_)) => {
                        self.replug_future = None;
                        for (_, sender) in self.replug_backs.drain() {
                            let _ = sender.send(Ok(()));
                        }
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
                        let keys: Vec<(String, String)> = self.replug_backs
                            .keys()
                            .filter(|k| set.contains(&k.0))
                            .map(|k| k.clone())
                            .collect();
                        for k in keys {
                            let _ = self.replug_backs.remove(&k).unwrap().send(Err(
                                Error::NotPermitted("not permitted".to_owned(), vec![k.0]),
                            ));
                        }
                        self.replug_all(false);
                        ct = true;
                    }
                    Err(e) => {
                        error!("replug fail: {}", e);
                        self.replug_all(true);
                        ct = true;
                    }
                    _ => {}
                }
            }
            if let Some(r) = self.lease_keep_future.as_mut().map(|f| f.poll()) {
                match r {
                    Ok(Async::Ready(_)) => {
                        self.lease_keep_future = None;
                        self.keep_lease();
                        ct = true;
                    }
                    Err(e) => {
                        error!("keep lease fail: {}", e);
                        self.new_lease(false);
                        ct = true;
                    }
                    _ => {}
                }
            }
        }
        Ok(Async::NotReady)
    }
}
