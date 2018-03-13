use tokio_core::reactor::Handle;
use hyper::client::{Client as HttpClient, HttpConnector};
use hyper::Method;
use hyper_tls::HttpsConnector;
use native_tls::TlsConnector;
use native_tls::backend::openssl::TlsConnectorBuilderExt;
use openssl::x509::X509_FILETYPE_PEM;
use serde_json;
use serde_yaml;
use serde::Deserialize;
use request::{Form, RequestBuilder};
use error::Error;
use futures::prelude::*;
use futures::future::{loop_fn, Loop};
use futures::sync::mpsc;
use tokio_core::reactor::Timeout;
use std::time::Duration;
use service_keeper::ServiceKeeper;

const DEFAULT_THREADS: usize = 4;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Config {
    pub endpoint: String,
    pub ca_file: Option<String>,
    pub cert_key_file: Option<(String, String)>,
    pub max_idle_connections: Option<usize>,
}

impl Config {
    pub fn new(endpoint: &str) -> Config {
        Config {
            endpoint: endpoint.to_owned(),
            ca_file: None,
            cert_key_file: None,
            max_idle_connections: None,
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
    config: Config,
    handle: Handle,
    client: HttpClient<HttpsConnector<HttpConnector>>,
}

impl Client {
    fn tls_connector(config: &Config) -> Result<TlsConnector, Error> {
        let mut tls_builder = TlsConnector::builder()?;
        {
            let builder = tls_builder.builder_mut().builder_mut();
            if let Some(ref path) = config.ca_file {
                builder
                    .set_ca_file(path)
                    .map_err(|e| Error::Ssl(format!("set cacert({}) fail: {}", path, e)))?;
            }
            if let Some((ref cert, ref key)) = config.cert_key_file {
                builder
                    .set_certificate_file(cert, X509_FILETYPE_PEM)
                    .map_err(|e| Error::Ssl(format!("set cert({}) fail: {}", cert, e)))?;
                builder
                    .set_private_key_file(key, X509_FILETYPE_PEM)
                    .map_err(|e| Error::Ssl(format!("set private key({}) fail: {}", key, e)))?;
            }
        }
        Ok(tls_builder.build()?)
    }

    pub fn new(config: Config, handle: &Handle) -> Result<Client, Error> {
        let mut http_connector = HttpConnector::new(DEFAULT_THREADS, handle);
        http_connector.enforce_http(false);
        let https_connector = HttpsConnector::from((http_connector, Self::tls_connector(&config)?));
        let http_client = HttpClient::configure()
            .connector(https_connector)
            .build(handle);
        Ok(Client {
            config: config,
            handle: handle.clone(),
            client: http_client,
        })
    }

    fn request<'a>(
        &'a self,
        method: Method,
        path: &'a str,
    ) -> RequestBuilder<'a, HttpsConnector<HttpConnector>> {
        RequestBuilder::new(&self.client, &self.config.endpoint, method, path)
    }

    pub fn get(&self, key: &str) -> Box<Future<Item = Item, Error = Error>> {
        Box::new(
            self.request(Method::Get, &format!("/api/configs/{}", key))
                .send()
                .map(|r: ItemResult| r.config),
        )
    }

    pub fn get_all(&self, keys: &Vec<String>) -> Box<Future<Item = Vec<Item>, Error = Error>> {
        let val = match serde_json::to_string(keys) {
            Ok(v) => v,
            Err(e) => {
                return Box::new(Err(Error::from(e)).into_future());
            }
        };
        Box::new(
            self.request(Method::Get, "/api/configs")
                .param("keys", &val)
                .send()
                .map(|r: ItemsResult| r.configs),
        )
    }

    pub fn get_service(
        &self,
        name: &str,
        version: &str,
    ) -> Box<Future<Item = ServiceResult, Error = Error>> {
        self.request(Method::Get, &format!("/api/services/{}/{}", name, version))
            .send()
    }

    pub fn plug_service(
        &self,
        service: &Service,
        endpoint: &ServiceEndpoint,
        ttl: Option<i64>,
        lease_id: Option<i64>,
    ) -> Box<Future<Item = PlugResult, Error = Error>> {
        let mut form = Form::new();
        form.set("ttl", ttl).unwrap();
        form.set("lease_id", lease_id).unwrap();
        form.set("desc", service).unwrap();
        form.set("endpoint", endpoint).unwrap();
        self.request(
            Method::Post,
            &format!("/api/services/{}/{}", &service.name, &service.version),
        ).form(form)
            .send()
    }

    pub fn plug_all_services(
        &self,
        services: &[Service],
        endpoint: &ServiceEndpoint,
        lease_id: Option<i64>,
        ttl: Option<i64>,
    ) -> Box<Future<Item = PlugResult, Error = Error>> {
        let mut form = Form::new();
        form.set("ttl", ttl).unwrap();
        form.set("lease_id", lease_id).unwrap();
        form.set("desces", services).unwrap();
        form.set("endpoint", endpoint).unwrap();
        self.request(Method::Post, "/api/services")
            .form(form)
            .send()
    }

    pub fn unplug_service(
        &self,
        name: &str,
        version: &str,
    ) -> Box<Future<Item = (), Error = Error>> {
        self.request(
            Method::Delete,
            &format!("/api/service/{}/{}", name, version),
        ).get_ok()
    }

    pub fn grant_lease(
        &self,
        ttl: Option<i64>,
    ) -> Box<Future<Item = LeaseGrantResult, Error = Error>> {
        let ttl = ttl.map(|n| format!("{}", n));
        let ttl = ttl.as_ref();
        let mut req = self.request(Method::Post, "/api/leases");
        if let Some(ttl) = ttl {
            req = req.param("ttl", ttl);
        }
        req.send()
    }

    pub fn keepalive_lease(&self, lease_id: i64) -> Box<Future<Item = (), Error = Error>> {
        self.request(Method::Post, &format!("/api/leases/{}", lease_id))
            .get_ok()
    }

    pub fn revoke_lease(&self, lease_id: i64) -> Box<Future<Item = (), Error = Error>> {
        self.request(Method::Delete, &format!("/api/leases/{}", lease_id))
            .get_ok()
    }

    pub fn watch_service_once(
        &self,
        name: &str,
        version: &str,
        revision: u64,
        timeout: u64,
    ) -> Box<Future<Item = Option<ServiceResult>, Error = Error>> {
        Box::new(
            self.request(Method::Get, &format!("/api/services/{}/{}", name, version))
                .param("watch", "true")
                .param("revision", &format!("{}", revision))
                .param("timeout", &format!("{}", timeout))
                .send()
                .and_then(|r| Ok(Some(r)))
                .or_else(|e| if e.is_timeout() { Ok(None) } else { Err(e) }),
        )
    }

    pub fn watch_service(
        &self,
        name: &str,
        version: &str,
        revision: Option<u64>,
        timeout: u64,
    ) -> mpsc::UnboundedReceiver<ServiceResult> {
        let (name, version) = (name.to_string(), version.to_string());
        let (tx, rx) = mpsc::unbounded();
        self.handle.spawn(loop_fn(
            (self.clone(), revision),
            move |(client, revision)| {
                let (name, version) = (name.clone(), version.clone());
                let tx = tx.clone();
                let h = client.handle.clone();
                match revision {
                    Some(rev) => client.watch_service_once(&name, &version, rev, timeout),
                    None => Box::new(client.get_service(&name, &version).map(Some)),
                }.or_else(move |e| {
                    error!("watch service({}:{}) error: {}", &name, &version, e);
                    Timeout::new(Duration::from_secs(5), &h)
                        .expect("create timeout fail")
                        .map(|_| None)
                        .or_else(|e| {
                            error!("poll watch err timeout fail: {}", e);
                            Ok(None)
                        })
                })
                    .and_then(move |result| match result {
                        Some(service_result) => {
                            let revision = service_result.revision + 1;
                            if tx.unbounded_send(service_result).is_err() {
                                return Ok(Loop::Break(()));
                            }
                            Ok(Loop::Continue((client, Some(revision))))
                        }
                        None => Ok(Loop::Continue((client, revision))),
                    })
            },
        ));
        rx
    }

    pub fn service_keeper(&self, ttl: Option<i64>, endpoint: ServiceEndpoint) -> ServiceKeeper {
        ServiceKeeper::new(&self, &self.handle, ttl, endpoint)
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
    pub address: String,
    pub config: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Service {
    pub name: String,
    pub version: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub proto: Option<String>,
    pub description: Option<String>,

    pub endpoints: Vec<ServiceEndpoint>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ServiceResult {
    pub service: Service,
    #[allow(dead_code)]
    pub revision: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct LeaseGrantResult {
    pub lease_id: i64,
    pub ttl: i64,
}

#[derive(Serialize, Debug, Clone)]
pub struct PlugRequest {
    pub ttl: Option<i64>,
    pub lease_id: Option<i64>,
    pub desc: Service,
    pub endpoint: ServiceEndpoint,
}

#[derive(Serialize, Debug, Clone)]
pub struct PlugAllRequest<'a> {
    pub ttl: Option<i64>,
    pub lease_id: Option<i64>,
    pub desces: &'a [Service],
    pub endpoint: ServiceEndpoint,
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

#[cfg(test)]
mod test {
    use super::Client;
    use super::Config;

    fn client() -> Client {
        let config = Config {
            endpoint: "https://localhost:4433".to_owned(),
            ca_file: Some("cacert.pem".to_owned()),
            cert_key_file: None,
        };
        Client::new(config).unwrap()
    }

    #[test]
    fn test_get() {
        let cli = client();
        let item = cli.get("fms.testkey").unwrap();
        println!("item: {:?}", item);
    }
}
