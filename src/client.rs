use error::Error;
use futures::future::{loop_fn, Loop};
use futures::prelude::*;
use futures::sync::mpsc;
use https::{ClientConfigPemExt, HttpsConnector};
use hyper::client::{Client as HttpClient, HttpConnector};
use hyper::Method;
use request::{Form, RequestBuilder};
use serde::Deserialize;
use serde_json;
use serde_yaml;
use service_keeper::ServiceKeeper;
use std::time::{Duration, Instant};
use tokio::spawn;
use tokio::timer::Delay;

const DEFAULT_THREADS: usize = 4;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Config {
    pub endpoint: String,
    pub insecure: bool,
    pub dev_app: Option<String>,
    pub ca_file: Option<String>,
    pub cert_key_file: Option<(String, String)>,
    pub max_idle_connections: Option<usize>,
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
        let client = HttpClient::builder().build(https_connector);
        Ok(Client {
            app_name,
            config,
            client,
        })
    }

    pub fn get_app_name(&self) -> Option<&String> {
        self.app_name.as_ref()
    }

    fn request<'a>(
        &'a self,
        method: Method,
        path: &'a str,
    ) -> RequestBuilder<'a, HttpsConnector<HttpConnector>> {
        let mut builder = RequestBuilder::new(&self.client, &self.config.endpoint, method, path);
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
        name: &str,
        version: &str,
    ) -> Box<Future<Item = ServiceResult, Error = Error> + Send> {
        self.request(Method::GET, &format!("/api/services/{}/{}", name, version))
            .send()
    }

    pub fn plug_service(
        &self,
        service: &Service,
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
    ) -> Box<Future<Item = PlugResult, Error = Error> + Send> {
        let form = match form!("ttl" => ttl, "lease_id" => lease_id,
                               "desces" => services, "endpoint" => endpoint)
        {
            Ok(f) => f,
            Err(e) => {
                return Box::new(Err(e).into_future());
            }
        };
        self.request(Method::POST, "/api/services")
            .form(form)
            .send()
    }

    pub fn unplug_service(
        &self,
        name: &str,
        version: &str,
    ) -> Box<Future<Item = (), Error = Error> + Send> {
        self.request(
            Method::DELETE,
            &format!("/api/service/{}/{}", name, version),
        ).get_ok()
    }

    pub fn grant_lease(
        &self,
        ttl: Option<i64>,
    ) -> Box<Future<Item = LeaseGrantResult, Error = Error> + Send> {
        let ttl = ttl.map(|n| format!("{}", n));
        let ttl = ttl.as_ref();
        let mut req = self.request(Method::POST, "/api/leases");
        if let Some(ttl) = ttl {
            req = req.param("ttl", ttl);
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

    pub fn watch_service_once(
        &self,
        name: &str,
        version: &str,
        revision: u64,
        timeout: u64,
    ) -> Box<Future<Item = Option<ServiceResult>, Error = Error> + Send> {
        Box::new(
            self.request(Method::GET, &format!("/api/services/{}/{}", name, version))
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
        spawn(loop_fn(
            (self.clone(), revision),
            move |(client, revision)| {
                let (name, version) = (name.clone(), version.clone());
                let tx = tx.clone();
                match revision {
                    Some(rev) => client.watch_service_once(&name, &version, rev, timeout),
                    None => Box::new(client.get_service(&name, &version).map(Some)),
                }.or_else(move |e| {
                    error!("watch service({}:{}) error: {}", &name, &version, e);
                    Delay::new(Instant::now() + Duration::from_secs(5))
                        .map(|_| None)
                        .or_else(|e| {
                            error!("poll watch err timeout fail: {}", e);
                            Ok(None)
                        })
                }).and_then(move |result| match result {
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
        ServiceKeeper::new(&self, ttl, endpoint)
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
