use super::client::{AppNode, Client, LeaseGrantResult, PlugResult, ServiceDesc, ServiceEndpoint};
use crate::error::Error;
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::spawn;
use tokio::time::sleep;

const GRANT_RETRY_INTERVAL: u64 = 5;

enum Cmd {
    Start,
    UpdateEndpoint(ServiceEndpoint),
    Plug(ServiceDesc, oneshot::Sender<Result<(), Error>>, bool),
    Unplug(String, String),
    Cancel(String, String),
    Clear(oneshot::Sender<()>),
    RevokeAndClose(oneshot::Sender<()>),
    NotifyNodeOnline(mpsc::UnboundedSender<bool>),
}

pub struct ServiceKeeper {
    cmd_tx: mpsc::UnboundedSender<Cmd>,
}

impl ServiceKeeper {
    pub fn new(
        client: &Client,
        ttl: Option<i64>,
        app_node: Option<AppNode>,
        endpoint: ServiceEndpoint,
    ) -> ServiceKeeper {
        let (tx, rx) = mpsc::unbounded();
        spawn(KeepTask::new(
            client,
            tx.clone(),
            rx,
            ttl,
            app_node,
            endpoint,
        ));
        ServiceKeeper { cmd_tx: tx }
    }

    pub fn start(&self) {
        let _ = self.cmd_tx.unbounded_send(Cmd::Start);
    }

    pub fn update_endpoint(&self, endpoint: ServiceEndpoint) {
        let _ = self.cmd_tx.unbounded_send(Cmd::UpdateEndpoint(endpoint));
    }

    pub fn plug(&self, service: &ServiceDesc) -> impl Future<Output = Result<(), Error>> {
        self.plug_replaceable(service, false)
    }

    pub fn plug_replaceable(
        &self,
        service: &ServiceDesc,
        replaceable: bool,
    ) -> impl Future<Output = Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        if self
            .cmd_tx
            .unbounded_send(Cmd::Plug(service.clone(), tx, replaceable))
            .is_err()
        {
            return future::err(Error::Other("keep task closed".to_string())).boxed();
        }
        rx.map(|r| match r {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::Other("keep task closed".to_string())),
        })
        .boxed()
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

    pub fn notify_node_online(&self, tx: mpsc::UnboundedSender<bool>) {
        let _ = self.cmd_tx.unbounded_send(Cmd::NotifyNodeOnline(tx));
    }
}

#[allow(clippy::type_complexity)]
struct KeepTask {
    client: Client,
    started: bool,
    closing: bool,
    ttl: Option<i64>,
    endpoint: ServiceEndpoint,
    app_node: Option<AppNode>,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
    cmd_rx: mpsc::UnboundedReceiver<Cmd>,
    services: HashMap<(String, String), ServiceDesc>,
    lease_result: Option<LeaseGrantResult>,
    lease_future: Option<Pin<Box<dyn Future<Output = Result<LeaseGrantResult, Error>> + Send>>>,
    replug_future: Option<Pin<Box<dyn Future<Output = Result<PlugResult, Error>> + Send>>>,
    replug_backs: HashMap<(String, String), oneshot::Sender<Result<(), Error>>>,
    lease_keep_future: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
    is_first_online: bool,
    online_notifiers: Vec<mpsc::UnboundedSender<bool>>,
}

impl KeepTask {
    fn new(
        client: &Client,
        cmd_tx: mpsc::UnboundedSender<Cmd>,
        cmd_rx: mpsc::UnboundedReceiver<Cmd>,
        ttl: Option<i64>,
        app_node: Option<AppNode>,
        endpoint: ServiceEndpoint,
    ) -> KeepTask {
        KeepTask {
            client: client.clone(),
            started: false,
            closing: false,
            ttl,
            app_node,
            endpoint,
            services: HashMap::new(),
            cmd_tx,
            cmd_rx,
            lease_result: None,
            lease_future: None,
            replug_future: None,
            replug_backs: HashMap::new(),
            lease_keep_future: None,
            is_first_online: true,
            online_notifiers: Vec::new(),
        }
    }

    fn new_lease(&mut self, delay_new: bool) {
        let app_node = self.app_node.clone();
        if delay_new {
            let (client, ttl) = (self.client.clone(), self.ttl);
            self.lease_future = Some(
                sleep(Duration::from_secs(GRANT_RETRY_INTERVAL))
                    .then(move |_| client.grant_lease(ttl, app_node.as_ref()))
                    .boxed(),
            );
        } else {
            self.lease_future = Some(self.client.grant_lease(self.ttl, app_node.as_ref()).boxed());
        }
        self.lease_keep_future = None;
        self.lease_result = None;
        self.replug_future = None;
    }

    fn keep_lease(&mut self) {
        self.lease_keep_future = None;
        if let Some(ref mut lease_result) = self.lease_result {
            let (client, lease_id) = (self.client.clone(), lease_result.lease_id);
            self.lease_keep_future = Some(
                sleep(Duration::from_secs(lease_result.ttl as u64 / 2))
                    .then(move |_| client.keepalive_lease(lease_id))
                    .boxed(),
            );
        } else {
            error!("missing lease result");
        }
    }

    fn replug_all(&mut self, delay_plug: bool) {
        self.replug_future = None;
        if let Some(ref lease_result) = self.lease_result {
            if self.services.is_empty() {
                info!("empty services, replug ignored");
                return;
            }

            let services: Vec<ServiceDesc> = self.services.values().cloned().collect();
            if delay_plug {
                let (client, endpoint, lease_id) = (
                    self.client.clone(),
                    self.endpoint.clone(),
                    lease_result.lease_id,
                );
                self.replug_future = Some(
                    sleep(Duration::from_secs(GRANT_RETRY_INTERVAL))
                        .then(move |_| {
                            client.plug_all_services(&services, &endpoint, Some(lease_id), None)
                        })
                        .boxed(),
                );
            } else {
                self.replug_future = Some(
                    self.client
                        .plug_all_services(
                            &services,
                            &self.endpoint,
                            Some(lease_result.lease_id),
                            None,
                        )
                        .boxed(),
                );
            }
        } else {
            error!("missing lease result");
        }
    }

    fn plug_one(&mut self, service: ServiceDesc, tx: oneshot::Sender<Result<(), Error>>) {
        if let Some(ref lease_result) = self.lease_result {
            let mut cmd_tx = self.cmd_tx.clone();
            spawn(
                self.client
                    .plug_service(&service, &self.endpoint, None, Some(lease_result.lease_id))
                    .map(move |r| match r {
                        Ok(_) => {
                            let _ = tx.send(Ok(()));
                        }
                        Err(e) => {
                            if !e.can_retry() {
                                let _ = cmd_tx.send(Cmd::Cancel(service.service, service.zone));
                            }
                            let _ = tx.send(Err(e));
                        }
                    }),
            );
        } else {
            error!("missing lease result");
        }
    }

    fn process_cmd(&mut self, cmd: Cmd) {
        match cmd {
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
            Cmd::Plug(service, tx, replaceable) => {
                let key = (service.service.clone(), service.zone.clone());
                if self.services.contains_key(&key) && !replaceable {
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
                            .unplug_service(&key.0, &key.1, &self.endpoint.address.to_string())
                            .map(move |r| {
                                if let Err(e) = r {
                                    error!("unplug service {}:{} fail: {}", key.0, key.1, e);
                                }
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
                self.revoke_lease(tx);
                self.lease_keep_future = None;
                self.replug_future = None;
                self.replug_backs.clear();
                self.services.clear();
            }
            Cmd::RevokeAndClose(tx) => {
                self.revoke_lease(tx);
                self.closing = true;
            }
            Cmd::NotifyNodeOnline(tx) => {
                self.online_notifiers.push(tx);
            }
        }
    }

    fn revoke_lease(&self, tx: oneshot::Sender<()>) {
        if let Some(ref lease_result) = self.lease_result {
            let lease_id = lease_result.lease_id;
            let fut = match &self.app_node {
                Some(node) => self
                    .client
                    .revoke_lease_with_node(lease_id, &node.key, node.label.as_deref())
                    .boxed(),
                None => self.client.revoke_lease(lease_id).boxed(),
            };
            spawn(fut.map(move |r| {
                if let Err(e) = r {
                    if !e.is_not_found() {
                        error!("revoke lease {} fail: {}", lease_id, e);
                    }
                }
                let _ = tx.send(());
            }));
        }
    }

    fn process_futures(&mut self, cx: &mut Context) {
        let mut ct = true;
        while ct {
            ct = false;
            if let Some(r) = self.lease_future.as_mut().map(|f| Pin::new(f).poll(cx)) {
                match r {
                    Poll::Ready(Ok(result)) => {
                        info!("grant lease ok: {:x}", result.lease_id);
                        if result.new_app_node == Some(true) {
                            let is_first_online = self.is_first_online;
                            self.online_notifiers
                                .retain(|tx| tx.unbounded_send(is_first_online).is_ok());
                            self.is_first_online = false;
                        }
                        self.lease_future = None;
                        self.lease_result = Some(result);
                        self.keep_lease();
                        self.replug_all(false);
                        ct = true;
                    }
                    Poll::Ready(Err(e)) => {
                        self.new_lease(!e.is_timeout());
                        ct = true;
                        error!("grant lease fail: {}", e);
                    }
                    Poll::Pending => {}
                }
            }
            if let Some(r) = self.replug_future.as_mut().map(|f| Pin::new(f).poll(cx)) {
                match r {
                    Poll::Ready(Ok(result)) => {
                        info!("services replugged ok");
                        self.replug_future = None;
                        for (_, sender) in self.replug_backs.drain() {
                            let _ = sender.send(Ok(()));
                        }
                        if let Some(lease_result) = &mut self.lease_result {
                            if lease_result.lease_id != result.lease_id {
                                warn!("lease_id changed: {}", result.lease_id);
                                lease_result.lease_id = result.lease_id
                            }
                        }
                    }
                    Poll::Ready(Err(Error::NotPermitted(_, services))) => {
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
                    Poll::Ready(Err(e)) => {
                        error!("services replug failed: {}", e);
                        self.replug_all(!e.is_timeout());
                        ct = true;
                    }
                    Poll::Pending => {}
                }
            }
            if let Some(r) = self
                .lease_keep_future
                .as_mut()
                .map(|f| Pin::new(f).poll(cx))
            {
                match r {
                    Poll::Ready(Ok(_)) => {
                        self.keep_lease();
                        ct = true;
                    }
                    Poll::Ready(Err(e)) => {
                        error!("keep lease fail: {}", e);
                        if e.is_timeout() {
                            self.keep_lease();
                        } else {
                            self.new_lease(false);
                        }
                        ct = true;
                    }
                    Poll::Pending => {}
                }
            }
        }
    }
}

impl Future for KeepTask {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        while !self.closing {
            match Pin::new(&mut self.cmd_rx).poll_next(cx) {
                Poll::Ready(Some(cmd)) => {
                    self.process_cmd(cmd);
                }
                Poll::Ready(None) => {
                    self.closing = true;
                    break;
                }
                Poll::Pending => {
                    break;
                }
            }
        }
        if !self.closing {
            self.process_futures(cx);
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
