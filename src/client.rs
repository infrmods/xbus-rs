use crate::https::{HttpsConnector, TlsClientConfigExt};
use crate::request::{Form, RequestBuilder};
use crate::service::{AppNode, AppNodes, Service, ServiceDesc, ServiceEndpoint};
use crate::service_keeper::ServiceKeeper;
use crate::watcher::{WatchStream, WatchTask};
use crate::{config::Config, service::ServiceDescEvent};
use crate::{error::Error, RevisionResult};
use futures::prelude::*;
use hyper::client::{Client as HttpClient, HttpConnector};
use hyper::Method;
use serde::Deserialize;
use serde_json;
use serde_yaml;
use std::time::Duration;

#[derive(Clone)]
pub struct Client {
    app_name: Option<String>,
    config: Config,
    client: HttpClient<HttpsConnector<HttpConnector>>,
}

impl Client {
    pub const DEFAULT_MAX_IDLE_PER_HOST: usize = 20;

    fn build_https_connector(
        config: &Config,
    ) -> Result<(HttpsConnector<HttpConnector>, Option<String>), Error> {
        let mut tls_config = ::rustls::ClientConfig::new();
        if config.insecure {
            warn!("using insecure https client");
            tls_config.set_insecure();
        }
        config.add_ca(&mut tls_config.root_store)?;
        let app_name = if let Some((cert, key)) = config.load_cert_key()? {
            Some(tls_config.add_cert_key(cert, key)?)
        } else {
            None
        };

        let mut http_connector = HttpConnector::new();
        http_connector.enforce_http(false);
        let https_connector = HttpsConnector::new(tls_config, http_connector);
        Ok((https_connector, app_name))
    }

    pub fn new(config: Config) -> Result<Client, Error> {
        if config.dev_app.is_some() && config.cert_key_file.is_some() {
            return Err(Error::Other("dev_app & config duplicated".to_string()));
        }
        let (https_connector, mut app_name) = Self::build_https_connector(&config)?;
        if config.dev_app.is_some() {
            app_name = config.dev_app.clone();
        }
        let max_idle_per_host = config
            .max_idle_connections
            .unwrap_or(Self::DEFAULT_MAX_IDLE_PER_HOST);
        let client = HttpClient::builder()
            .pool_max_idle_per_host(max_idle_per_host)
            .build(https_connector);
        Ok(Client {
            app_name,
            config,
            client,
        })
    }

    pub fn get_app_name(&self) -> Option<&str> {
        self.app_name.as_deref()
    }

    pub fn request<'a>(
        &'a self,
        method: Method,
        path: &'a str,
    ) -> RequestBuilder<'a, HttpsConnector<HttpConnector>> {
        self.request_timeout(method, path, self.config.request_timeout)
    }

    pub fn request_timeout<'a>(
        &'a self,
        method: Method,
        path: &'a str,
        timeout: Duration,
    ) -> RequestBuilder<'a, HttpsConnector<HttpConnector>> {
        let mut builder = RequestBuilder::new(
            &self.client,
            &self.config.endpoint,
            method,
            path,
            Some(timeout),
        );
        if let Some(ref dev_app) = self.config.dev_app {
            builder = builder.header("Dev-App", dev_app);
        }
        builder
    }

    pub fn get(&self, key: &str) -> impl Future<Output = Result<Item, Error>> {
        self.request(Method::GET, &format!("/api/configs/{}", key))
            .send::<ItemResult>()
            .map(|result| result.map(|r| r.config))
    }

    pub async fn get_all(&self, keys: &[String]) -> Result<Vec<Item>, Error> {
        let val = serde_json::to_string(keys).map_err(Error::from)?;
        let result = self
            .request(Method::GET, "/api/configs")
            .param("keys", &val)
            .send::<ItemsResult>()
            .await?;
        Ok(result.configs)
    }

    pub fn get_service(&self, service: &str) -> impl Future<Output = Result<ServiceResult, Error>> {
        self.request(Method::GET, &format!("/api/v1/services/{}", service))
            .send()
    }

    pub fn get_service_only_zones(
        &self,
        service: &str,
    ) -> impl Future<Output = Result<ServiceWithRawZoneResult, Error>> {
        self.request(
            Method::GET,
            &format!("/api/v1/services/{}?only_zone=true", service),
        )
        .send()
    }

    pub fn get_service_by_zone(
        &self,
        service: &str,
        zone: &str,
    ) -> impl Future<Output = Result<ServiceResult, Error>> {
        self.request(
            Method::GET,
            &format!("/api/v1/services/{}/{}", service, zone),
        )
        .send()
    }

    pub fn plug_service(
        &self,
        service: &ServiceDesc,
        endpoint: &ServiceEndpoint,
        ttl: Option<i64>,
        lease_id: Option<i64>,
    ) -> impl Future<Output = Result<PlugResult, Error>> {
        let form = form!("ttl" => ttl, "lease_id" => lease_id,
                         "desc" => service, "endpoint" => endpoint);
        self.request(
            Method::POST,
            &format!("/api/v1/services/{}", &service.service),
        )
        .form_result(form)
        .send()
    }

    pub fn plug_all_services(
        &self,
        services: &[ServiceDesc],
        endpoint: &ServiceEndpoint,
        lease_id: Option<i64>,
        ttl: Option<i64>,
    ) -> impl Future<Output = Result<PlugResult, Error>> {
        let form = form!("ttl" => ttl, "lease_id" => lease_id,
                         "descs" => services, "endpoint" => endpoint);
        self.request(Method::POST, "/api/v1/services")
            .form_result(form)
            .send()
    }

    pub fn unplug_service(
        &self,
        service: &str,
        zone: &str,
        addr: &str,
    ) -> impl Future<Output = Result<(), Error>> {
        self.request(
            Method::DELETE,
            &format!("/api/v1/services/{}/{}/{}", service, zone, addr),
        )
        .get_ok()
    }

    pub fn grant_lease(
        &self,
        ttl: Option<i64>,
        app_node: Option<&AppNode>,
    ) -> impl Future<Output = Result<LeaseGrantResult, Error>> {
        let ttl = ttl.map(|n| format!("{}", n));
        let ttl = ttl.as_ref();
        let mut req = self.request(Method::POST, "/api/leases");
        if let Some(ttl) = ttl {
            req = req.param("ttl", ttl);
        }
        if let Some(app_node) = app_node {
            req = req.form_result(form!("app_node" => app_node));
        }
        req.send().boxed()
    }

    pub fn keepalive_lease(&self, lease_id: i64) -> impl Future<Output = Result<(), Error>> {
        self.request(Method::POST, &format!("/api/leases/{}", lease_id))
            .get_ok()
    }

    pub fn revoke_lease(&self, lease_id: i64) -> impl Future<Output = Result<(), Error>> {
        self.request(Method::DELETE, &format!("/api/leases/{}", lease_id))
            .get_ok()
    }

    pub fn revoke_lease_with_node(
        &self,
        lease_id: i64,
        key: &str,
        label: Option<&str>,
    ) -> impl Future<Output = Result<(), Error>> {
        self.request(Method::DELETE, &format!("/api/leases/{}", lease_id))
            .param("rm_node_key", key)
            .param_opt("app_node_label", label)
            .get_ok()
    }

    pub fn get_app_nodes(
        &self,
        name: &str,
        label: Option<&str>,
    ) -> impl Future<Output = Result<AppNodes, Error>> {
        self.request(Method::GET, &format!("/api/apps/{}/nodes", name))
            .param_opt("label", label)
            .send()
    }

    pub fn watch_app_nodes_once(
        &self,
        app: &str,
        label: Option<&str>,
        revision: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<Option<AppNodes>, Error>> {
        self.request_timeout(
            Method::GET,
            &format!("/api/apps/{}/nodes", app),
            timeout + self.config.request_timeout,
        )
        .param_opt("label", label)
        .param("revision", &format!("{}", revision))
        .param("timeout", &format!("{}", timeout.as_secs()))
        .send()
        .map(|result| match result {
            Ok(nodes) => Ok(Some(nodes)),
            Err(e) => {
                if e.is_timeout() {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        })
    }

    pub fn watch_app_nodes(
        &self,
        app: &str,
        label: Option<&str>,
        timeout: Duration,
    ) -> WatchStream<AppNodes> {
        let client = self.clone();
        let app = app.to_string();
        let label = label.map(|s| s.to_string());
        WatchTask::spawn(None, move |revision| {
            let label: Option<&str> = label.as_deref();
            match revision {
                Some(revision) => client
                    .watch_app_nodes_once(&app, label, revision + 1, timeout)
                    .boxed(),
                None => client
                    .get_app_nodes(&app, label)
                    .map(|r| r.map(Some))
                    .boxed(),
            }
        })
    }

    pub fn is_app_node_online(
        &self,
        app: &str,
        label: Option<&str>,
        key: &str,
    ) -> impl Future<Output = Result<bool, Error>> {
        self.request(Method::GET, &format!("/api/apps/{}/online", app))
            .param_opt("label", label)
            .param("key", key)
            .send()
    }

    pub fn watch_service_once(
        &self,
        service: &str,
        revision: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<Option<ServiceResult>, Error>> {
        self.request_timeout(
            Method::GET,
            &format!("/api/v1/services/{}", service),
            timeout + self.config.request_timeout,
        )
        .param("watch", "true")
        .param("revision", &format!("{}", revision))
        .param("timeout", &format!("{}", timeout.as_secs()))
        .send()
        .map(|result| match result {
            Ok(r) => Ok(Some(r)),
            Err(e) => {
                if e.is_timeout() {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        })
    }

    pub fn delete_service(
        &self,
        service: &str,
        zone: Option<&str>,
    ) -> impl Future<Output = Result<(), Error>> {
        self.request(Method::DELETE, &format!("/api/v1/services/{}", service))
            .param("zone", zone.unwrap_or(""))
            .get_ok()
    }

    pub fn watch_service(
        &self,
        service: &str,
        revision: Option<u64>,
        interval: Duration,
    ) -> WatchStream<ServiceResult> {
        let client = self.clone();
        let service = service.to_string();
        WatchTask::spawn(revision, move |revision| match revision {
            Some(revision) => client
                .watch_service_once(&service, revision + 1, interval)
                .boxed(),
            None => client
                .get_service(&service)
                .map(|result| result.map(Some))
                .boxed(),
        })
    }

    pub fn service_keeper(
        &self,
        ttl: Option<i64>,
        app_node: Option<AppNode>,
        endpoint: ServiceEndpoint,
    ) -> ServiceKeeper {
        ServiceKeeper::new(&self, ttl, app_node, endpoint)
    }

    pub fn watch_service_descs_once(
        &self,
        zone: Option<&str>,
        revision: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<Option<ServiceDescWatchResult>, Error>> {
        self.request_timeout(
            Method::GET,
            "/api/v1/service-descs",
            timeout + self.config.request_timeout,
        )
        .param_opt("zone", zone)
        .param("revision", &format!("{}", revision))
        .param("timeout", &format!("{}", timeout.as_secs()))
        .get_option()
        .map(|result| match result {
            Ok(r) => Ok(r),
            Err(e) => {
                if e.is_timeout() {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        })
    }

    pub fn watch_service_descs(
        &self,
        zone: Option<&str>,
        revision: Option<u64>,
        interval: Duration,
    ) -> WatchStream<ServiceDescWatchResult> {
        let client = self.clone();
        let zone = zone.map(|s| s.to_string());
        WatchTask::spawn(revision, move |revision| {
            client
                .watch_service_descs_once(zone.as_deref(), revision.unwrap_or(0) + 1, interval)
                .boxed()
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ItemsResult {
    configs: Vec<Item>,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ItemResult {
    config: Item,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Item {
    pub name: String,
    pub value: String,
    pub version: u64,
}

impl Item {
    pub fn json<T>(&self) -> Result<T, serde_json::Error>
    where
        for<'de> T: Deserialize<'de>,
    {
        serde_json::from_str(&self.value)
    }

    pub fn yaml<T>(&self) -> Result<T, serde_yaml::Error>
    where
        for<'de> T: Deserialize<'de>,
    {
        serde_yaml::from_str(&self.value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceResult {
    pub service: Service,
    pub revision: u64,
}

impl RevisionResult for ServiceResult {
    fn get_revision(&self) -> u64 {
        self.revision
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LeaseGrantResult {
    pub lease_id: i64,
    pub ttl: i64,
    pub new_app_node: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PlugResult {
    pub lease_id: i64,
    pub ttl: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServiceDescWatchResult {
    pub events: Option<Vec<ServiceDescEvent>>,
    pub revision: u64,
}

impl RevisionResult for ServiceDescWatchResult {
    fn get_revision(&self) -> u64 {
        self.revision
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceWithRawZone {
    pub service: String,
    pub zones: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceWithRawZoneResult {
    pub service: ServiceWithRawZone,
    pub revision: u64,
}
