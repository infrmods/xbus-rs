use super::client::{Client, LeaseGrantResult, PlugResult, ServiceEndpoint, ZoneService};
use error::Error;
use futures::prelude::*;
use futures::sync::{mpsc, oneshot};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::time::{Duration, Instant};
use tokio::spawn;
use tokio::timer::Delay;

const GRANT_RETRY_INTERVAL: u64 = 5;

enum Cmd {
    Start,
    UpdateEndpoint(ServiceEndpoint),
    Plug(ZoneService, oneshot::Sender<Result<(), Error>>),
    Unplug(String, String),
    Cancel(String, String),
    Clear(oneshot::Sender<()>),
    RevokeAndClose(oneshot::Sender<()>),
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

    pub fn start(&self) {
        let _ = self.cmd_tx.unbounded_send(Cmd::Start);
    }

    pub fn update_endpoint(&self, endpoint: ServiceEndpoint) {
        let _ = self.cmd_tx.unbounded_send(Cmd::UpdateEndpoint(endpoint));
    }

    pub fn plug(&self, service: &ZoneService) -> Box<Future<Item = (), Error = Error> + Send> {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .unbounded_send(Cmd::Plug(service.clone(), tx))
            .is_err()
        {
            return Box::new(Err(Error::Other("keep task closed".to_string())).into_future());
        }
        Box::new(rx.then(|r| match r {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::Other("keep task closed".to_string())),
        }))
    }

    pub fn unplug<S: Into<String>>(&self, service: S, zone: S) {
        let _ = self
            .cmd_tx
            .unbounded_send(Cmd::Unplug(service.into(), zone.into()));
    }

    pub fn clear(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        if let Err(r) = self.cmd_tx.unbounded_send(Cmd::Clear(tx)) {
            if let Cmd::Clear(tx) = r.into_inner() {
                let _ = tx.send(());
            } else {
                unreachable!();
            }
        }
        rx
    }

    pub fn close(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        if let Err(r) = self.cmd_tx.unbounded_send(Cmd::RevokeAndClose(tx)) {
            if let Cmd::RevokeAndClose(tx) = r.into_inner() {
                let _ = tx.send(());
            } else {
                unreachable!();
            }
        }
        rx
    }
}

struct KeepTask {
    client: Client,
    started: bool,
    ttl: Option<i64>,
    endpoint: ServiceEndpoint,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
    cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    services: HashMap<(String, String), ZoneService>,
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
            started: false,
            ttl,
            endpoint,
            services: HashMap::new(),
            cmd_tx,
            cmd_rx,
            lease_result: None,
            lease_future: None,
            replug_future: None,
            replug_backs: HashMap::new(),
            lease_keep_future: None,
        }
    }

    fn new_lease(&mut self, delay_new: bool) {
        if delay_new {
            let delay = Delay::new(Instant::now() + Duration::from_secs(GRANT_RETRY_INTERVAL))
                .map_err(|e| Error::Other(format!("lease keep sleep fail: {}", e)));
            let (client, ttl) = (self.client.clone(), self.ttl);
            self.lease_future = Some(Box::new(delay.and_then(move |_| client.grant_lease(ttl))));
        } else {
            self.lease_future = Some(Box::new(self.client.grant_lease(self.ttl)));
        }
        self.lease_keep_future = None;
        self.lease_result = None;
        self.replug_future = None;
    }

    fn keep_lease(&mut self) {
        if let Some(ref mut lease_result) = self.lease_result {
            let delay =
                Delay::new(Instant::now() + Duration::from_secs(lease_result.ttl as u64 / 2))
                    .map_err(|e| Error::Other(format!("keep lease sleep fail: {}", e)));
            let (client, lease_id) = (self.client.clone(), lease_result.lease_id);
            self.lease_keep_future = Some(Box::new(
                delay.and_then(move |_| client.keepalive_lease(lease_id)),
            ));
        } else {
            error!("missing lease result");
        }
    }

    fn replug_all(&mut self, delay_plug: bool) {
        if let Some(ref lease_result) = self.lease_result {
            let services: Vec<ZoneService> = self.services.values().cloned().collect();
            if delay_plug {
                let delay = Delay::new(Instant::now() + Duration::from_secs(GRANT_RETRY_INTERVAL))
                    .map_err(|e| Error::Other(format!("replug sleep fail: {}", e)));
                let (client, endpoint, lease_id) = (
                    self.client.clone(),
                    self.endpoint.clone(),
                    lease_result.lease_id,
                );
                self.replug_future = Some(Box::new(delay.and_then(move |_| {
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

    fn plug_one(&mut self, service: ZoneService, tx: oneshot::Sender<Result<(), Error>>) {
        if let Some(ref lease_result) = self.lease_result {
            let cmd_tx = self.cmd_tx.clone();
            spawn(
                self.client
                    .plug_service(&service, &self.endpoint, None, Some(lease_result.lease_id))
                    .then(move |r| {
                        match r {
                            Ok(_) => {
                                let _ = tx.send(Ok(()));
                            }
                            Err(e) => {
                                if !e.can_retry() {
                                    let _ = cmd_tx.send(Cmd::Cancel(service.service, service.zone));
                                }
                                let _ = tx.send(Err(e));
                            }
                        };
                        Ok(())
                    }),
            );
        } else {
            error!("missing lease result");
        }
    }

    #[allow(clippy::map_entry)]
    fn process_cmds(&mut self) -> bool {
        loop {
            match self.cmd_rx.poll() {
                Ok(Async::Ready(Some(cmd))) => match cmd {
                    Cmd::Start => {
                        if !self.started {
                            self.started = true;
                            if self.lease_result.is_none() {
                                if self.lease_future.is_none() {
                                    self.new_lease(false);
                                }
                            } else {
                                self.replug_all(false);
                            }
                        }
                    }
                    Cmd::UpdateEndpoint(endpoint) => {
                        self.endpoint = endpoint;
                        if !self.services.is_empty() && self.started {
                            self.new_lease(false);
                        }
                    }
                    Cmd::Plug(service, tx) => {
                        let key = (service.service.clone(), service.zone.clone());
                        if self.services.contains_key(&key) {
                            let _ = tx.send(Err(Error::Other(format!(
                                "{}:{} has been plugged",
                                key.0, key.1
                            ))));
                        } else {
                            self.services.insert(key.clone(), service.clone());
                            if self.started {
                                if self.lease_result.is_some() {
                                    self.plug_one(service, tx);
                                } else {
                                    self.replug_backs.insert(key, tx);
                                    if self.lease_future.is_none() {
                                        self.new_lease(false);
                                    }
                                }
                            } else {
                                self.replug_backs.insert(key, tx);
                            }
                        }
                    }
                    Cmd::Unplug(service, zone) => {
                        let key = (service, zone);
                        self.replug_backs.remove(&key);
                        if self.services.remove(&key).is_some() && self.started {
                            spawn(
                                self.client
                                    .unplug_service(
                                        &key.0,
                                        &key.1,
                                        &self.endpoint.address.to_string(),
                                    )
                                    .then(move |r| {
                                        if let Err(e) = r {
                                            error!(
                                                "unplug service {}:{} fail: {}",
                                                key.0, key.1, e
                                            );
                                        }
                                        Ok(())
                                    }),
                            );
                        }
                    }
                    Cmd::Cancel(service, zone) => {
                        let key = (service, zone);
                        self.services.remove(&key);
                        self.replug_backs.remove(&key);
                    }
                    Cmd::Clear(tx) => {
                        if let Some(ref lease_result) = self.lease_result.take() {
                            let lease_id = lease_result.lease_id;
                            spawn(self.client.revoke_lease(lease_id).then(move |r| {
                                if let Err(e) = r {
                                    error!("revoke lease {} fail: {}", lease_id, e);
                                }
                                let _ = tx.send(());
                                Ok(())
                            }));
                        }
                        self.lease_keep_future = None;
                        self.replug_future = None;
                        self.replug_backs.clear();
                        self.services.clear();
                    }
                    Cmd::RevokeAndClose(tx) => {
                        if let Some(ref lease_result) = self.lease_result {
                            let lease_id = lease_result.lease_id;
                            spawn(self.client.revoke_lease(lease_id).then(move |r| {
                                if let Err(e) = r {
                                    error!("revoke lease {} fail: {}", lease_id, e);
                                }
                                let _ = tx.send(());
                                Ok(())
                            }));
                        }
                        return false;
                    }
                },
                Ok(Async::NotReady) => {
                    break;
                }
                _ => {
                    return false;
                }
            }
        }
        true
    }

    fn process_futures(&mut self) {
        let mut ct = true;
        while ct {
            ct = false;
            if let Some(r) = self.lease_future.as_mut().map(|f| f.poll()) {
                match r {
                    Ok(Async::Ready(result)) => {
                        info!("grant lease ok: {}", result.lease_id);
                        self.lease_future = None;
                        self.lease_result = Some(result);
                        self.keep_lease();
                        self.replug_all(false);
                        ct = true;
                    }
                    Err(e) => {
                        self.new_lease(!e.is_timeout());
                        ct = true;
                        error!("grant lease fail: {}", e);
                    }
                    Ok(Async::NotReady) => {}
                }
            }
            if let Some(r) = self.replug_future.as_mut().map(|f| f.poll()) {
                match r {
                    Ok(Async::Ready(_)) => {
                        info!("services replugged ok");
                        self.replug_future = None;
                        for (_, sender) in self.replug_backs.drain() {
                            let _ = sender.send(Ok(()));
                        }
                    }
                    Err(Error::NotPermitted(_, services)) => {
                        warn!("not permitted services: {}", services.join(", "));
                        let set: HashSet<String> = HashSet::from_iter(services);
                        self.services.retain(|k, _| {
                            if set.contains(&k.0) {
                                error!("plug service not permitted: {}:{}", k.0, k.1);
                                false
                            } else {
                                true
                            }
                        });
                        let keys: Vec<(String, String)> = self
                            .replug_backs
                            .keys()
                            .filter(|k| set.contains(&k.0))
                            .cloned()
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
                        error!("services replug failed: {}", e);
                        self.replug_all(!e.is_timeout());
                        ct = true;
                    }
                    Ok(Async::NotReady) => {}
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
                    Ok(Async::NotReady) => {}
                }
            }
        }
    }
}

impl Future for KeepTask {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        if !self.process_cmds() {
            warn!("service keeper exit");
            return Ok(Async::Ready(()));
        }
        self.process_futures();
        Ok(Async::NotReady)
    }
}
