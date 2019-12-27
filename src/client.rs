use super::addr_serde;

use super::error::Error;

use super::watcher::{WatchStream, WatchTask};

use futures::prelude::*;
use https::{ClientConfigPemExt, HttpsConnector};
use hyper::client::{Client as HttpClient, HttpConnector};
use hyper::Method;
use request::{Form, RequestBuilder};
use serde::Deserialize;
use serde_json;
use serde_yaml;
use service_keeper::ServiceKeeper;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

const DEFAULT_THREADS: usize = 4;
const DEFAULT_REQUEST_TIMEOUT: u64 = 5;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct Config {
    pub endpoint: String,
    pub insecure: bool,
    pub dev_app: Option<String>,
    pub ca_file: Option<String>,
    pub cert_key_file: Option<(String, String)>,
    pub max_idle_connections: Option<usize>,
    pub request_timeout: Duration,
}

impl Config {
    pub fn new(endpoint: &str) -> Config {
        Config {
            endpoint: endpoint.to_owned(),
            insecure: false,
            dev_app: None,
            ca_file: None,
            cert_key_file: None,
            max_idle_connections: None,
            request_timeout: Duration::from_secs(DEFAULT_REQUEST_TIMEOUT),
        }
    }

    pub fn ca_file(mut self, file: &str) -> Config {
        self.ca_file = Some(file.to_owned());
        self
    }

    pub fn cert_key_file(mut self, cert: &str, key: &str) -> Config {
        self.cert_key_file = Some((cert.to_owned(), key.to_owned()));
        self
    }
}

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
            tls_config.set_insecure();
        }
        if let Some(ref path) = config.ca_file {
            tls_config.add_root_cert(path)?;
        }

        let app_name = if let Some((ref cert, ref key)) = config.cert_key_file {
            Some(tls_config.add_cert_key(cert, key)?)
        } else {
            None
        };

        let mut http_connector = HttpConnector::new(DEFAULT_THREADS);
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
            .max_idle_per_host(max_idle_per_host)
            .build(https_connector);
        Ok(Client {
            app_name,
            config,
            client,
        })
    }

    pub fn get_app_name(&self) -> Option<&str> {
        self.app_name.as_ref().map(|s| s.as_str())
    }

    fn request<'a>(
        &'a self,
        method: Method,
        path: &'a str,
    ) -> RequestBuilder<'a, HttpsConnector<HttpConnector>> {
        self.request_timeout(method, path, self.config.request_timeout)
    }

    fn request_timeout<'a>(
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

    pub fn get(&self, key: &str) -> Box<Future<Item = Item, Error = Error> + Send> {
        Box::new(
            self.request(Method::GET, &format!("/api/configs/{}", key))
                .send()
                .map(|r: ItemResult| r.config),
        )
    }

    pub fn get_all(&self, keys: &[String]) -> Box<Future<Item = Vec<Item>, Error = Error> + Send> {
        let val = match serde_json::to_string(keys) {
            Ok(v) => v,
            Err(e) => {
                return Box::new(Err(Error::from(e)).into_future());
            }
        };
        Box::new(
            self.request(Method::GET, "/api/configs")
                .param("keys", &val)
                .send()
                .map(|r: ItemsResult| r.configs),
        )
    }

    pub fn get_service(
        &self,
        service: &str,
    ) -> Box<Future<Item = ServiceResult, Error = Error> + Send> {
        self.request(Method::GET, &format!("/api/v1/services/{}", service))
            .send()
    }

    pub fn plug_service(
        &self,
        service: &ZoneService,
        endpoint: &ServiceEndpoint,
        ttl: Option<i64>,
        lease_id: Option<i64>,
    ) -> Box<Future<Item = PlugResult, Error = Error> + Send> {
        let form = match form!("ttl" => ttl, "lease_id" => lease_id,
                               "desc" => service, "endpoint" => endpoint)
        {
            Ok(f) => f,
            Err(e) => {
                return Box::new(Err(e).into_future());
            }
        };
        self.request(
            Method::POST,
            &format!("/api/v1/services/{}", &service.service),
        )
        .form(form)
        .send()
    }

    pub fn plug_all_services(
        &self,
        services: &[ZoneService],
        endpoint: &ServiceEndpoint,
        lease_id: Option<i64>,
        ttl: Option<i64>,
    ) -> Box<Future<Item = PlugResult, Error = Error> + Send> {
        let form = match form!("ttl" => ttl, "lease_id" => lease_id,
                               "desces" => services, "endpoint" => endpoint)
        {
            Ok(f) => f,
            Err(e) => {
                return Box::new(Err(e).into_future());
            }
        };
        self.request(Method::POST, "/api/v1/services")
            .form(form)
            .send()
    }

    pub fn unplug_service(
        &self,
        service: &str,
        zone: &str,
        addr: &str,
    ) -> Box<Future<Item = (), Error = Error> + Send> {
        self.request(
            Method::DELETE,
            &format!("/api/v1/service/{}/{}/{}", service, zone, addr),
        )
        .get_ok()
    }

    pub fn grant_lease(
        &self,
        ttl: Option<i64>,
        app_node: Option<&AppNode>,
    ) -> Box<Future<Item = LeaseGrantResult, Error = Error> + Send> {
        let ttl = ttl.map(|n| format!("{}", n));
        let ttl = ttl.as_ref();
        let mut req = self.request(Method::POST, "/api/leases");
        if let Some(ttl) = ttl {
            req = req.param("ttl", ttl);
        }
        if let Some(app_node) = app_node {
            match form!("app_node" => app_node) {
                Ok(form) => {
                    req = req.form(form);
                }
                Err(e) => {
                    return Box::new(Err(e).into_future());
                }
            }
        }
        req.send()
    }

    pub fn keepalive_lease(&self, lease_id: i64) -> Box<Future<Item = (), Error = Error> + Send> {
        self.request(Method::POST, &format!("/api/leases/{}", lease_id))
            .get_ok()
    }

    pub fn revoke_lease(&self, lease_id: i64) -> Box<Future<Item = (), Error = Error> + Send> {
        self.request(Method::DELETE, &format!("/api/leases/{}", lease_id))
            .get_ok()
    }

    pub fn revoke_lease_with_node(
        &self,
        lease_id: i64,
        key: &str,
        label: Option<&str>,
    ) -> Box<Future<Item = (), Error = Error> + Send> {
        self.request(Method::DELETE, &format!("/api/leases/{}", lease_id))
            .param("rm_node_key", key)
            .param_opt("app_node_label", label)
            .get_ok()
    }

    pub fn get_app_nodes(
        &self,
        name: &str,
        label: Option<&str>,
    ) -> Box<Future<Item = AppNodes, Error = Error> + Send> {
        Box::new(
            self.request(Method::GET, &format!("/api/apps/{}/nodes", name))
                .param_opt("label", label)
                .send(),
        )
    }

    pub fn watch_app_nodes_once(
        &self,
        app: &str,
        label: Option<&str>,
        revision: u64,
        timeout: Duration,
    ) -> Box<Future<Item = Option<AppNodes>, Error = Error> + Send> {
        Box::new(
            self.request_timeout(
                Method::GET,
                &format!("/api/apps/{}/nodes", app),
                timeout + self.config.request_timeout,
            )
            .param_opt("label", label)
            .param("revision", &format!("{}", revision))
            .param("timeout", &format!("{}", timeout.as_secs()))
            .send()
            .and_then(|r| Ok(Some(r)))
            .or_else(|e| if e.is_timeout() { Ok(None) } else { Err(e) }),
        )
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
            let label = label.as_ref().map(|s| s.as_str());
            match revision {
                Some(revision) => client.watch_app_nodes_once(&app, label, revision, timeout),
                None => Box::new(client.get_app_nodes(&app, label).map(Some)),
            }
        })
    }

    pub fn is_app_node_online(
        &self,
        app: &str,
        label: Option<&str>,
        key: &str,
    ) -> Box<Future<Item = bool, Error = Error> + Send> {
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
    ) -> Box<Future<Item = Option<ServiceResult>, Error = Error> + Send> {
        Box::new(
            self.request_timeout(
                Method::GET,
                &format!("/api/v1/services/{}", service),
                timeout + self.config.request_timeout,
            )
            .param("watch", "true")
            .param("revision", &format!("{}", revision))
            .param("timeout", &format!("{}", timeout.as_secs()))
            .send()
            .and_then(|r| Ok(Some(r)))
            .or_else(|e| if e.is_timeout() { Ok(None) } else { Err(e) }),
        )
    }

    pub fn delete_service(
        &self,
        service: &str,
        zone: Option<&str>,
    ) -> Box<Future<Item = (), Error = Error> + Send> {
        Box::new(
            self.request(Method::DELETE, &format!("/api/v1/services/{}", service))
                .param("zone", zone.unwrap_or(""))
                .get_ok(),
        )
    }

    pub fn watch_service(
        &self,
        service: &str,
        revision: Option<u64>,
        timeout: Duration,
    ) -> WatchStream<ServiceResult> {
        let client = self.clone();
        let service = service.to_string();
        WatchTask::spawn(revision, move |revision| match revision {
            Some(revision) => client.watch_service_once(&service, revision + 1, timeout),
            None => Box::new(client.get_service(&service).map(Some)),
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
}

#[derive(Deserialize, Clone)]
pub struct ItemsResult {
    configs: Vec<Item>,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Deserialize, Clone)]
pub struct ItemResult {
    config: Item,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Item {
    pub name: String,
    pub value: String,
    pub version: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceEndpoint {
    #[serde(
        serialize_with = "addr_serde::serialize_address",
        deserialize_with = "addr_serde::deserialize_address"
    )]
    pub address: SocketAddr,
    pub config: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ZoneService {
    pub service: String,
    pub zone: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub proto: Option<String>,
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Vec::is_empty", default = "Vec::new")]
    pub endpoints: Vec<ServiceEndpoint>,
}

impl ZoneService {
    pub fn addresses<'a>(&'a self) -> impl Iterator<Item = SocketAddr> + 'a {
        self.endpoints.iter().map(|e| e.address)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Service {
    pub service: String,
    pub zones: HashMap<String, ZoneService>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ServiceResult {
    pub service: Service,
    pub revision: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LeaseGrantResult {
    pub lease_id: i64,
    pub ttl: i64,
    pub new_app_node: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PlugResult {
    pub lease_id: i64,
    pub ttl: i64,
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
pub struct AppNode {
    pub label: Option<String>,
    pub key: String,
    pub config: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct AppNodes {
    pub nodes: HashMap<String, String>,
    pub revision: u64,
}

pub(crate) trait RevisionResult {
    fn get_revision(&self) -> u64;
}

impl RevisionResult for ServiceResult {
    fn get_revision(&self) -> u64 {
        self.revision
    }
}

impl RevisionResult for AppNodes {
    fn get_revision(&self) -> u64 {
        self.revision
    }
}
