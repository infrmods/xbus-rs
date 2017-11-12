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
use request::RequestBuilder;
use error::Error;
use futures::{Future, IntoFuture};

const DEFAULT_IDLE: usize = 4;


#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    endpoint: String,
    ca_file: Option<String>,
    cert_key_file: Option<(String, String)>,
    max_idle_connections: Option<usize>,
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

pub struct Client {
    config: Config,
    client: HttpClient<HttpsConnector<HttpConnector>>,
}

impl Client {
    fn tls_connector(config: &Config) -> Result<TlsConnector, Error> {
        let mut tls_builder = TlsConnector::builder()?;
        {
            let builder = tls_builder.builder_mut().builder_mut();
            if let Some(ref path) = config.ca_file {
                builder.set_ca_file(path)
                    .map_err(|e| Error::Ssl(format!("set cacert({}) fail: {}", path, e)))?;
            }
            if let Some((ref cert, ref key)) = config.cert_key_file {
                builder.set_certificate_file(cert, X509_FILETYPE_PEM)
                    .map_err(|e| Error::Ssl(format!("set cert({}) fail: {}", cert, e)))?;
                builder.set_private_key_file(key, X509_FILETYPE_PEM)
                    .map_err(|e| Error::Ssl(format!("set private key({}) fail: {}", key, e)))?;
            }
        }
        Ok(tls_builder.build()?)
    }

    pub fn new(config: Config, handle: &Handle) -> Result<Client, Error> {
        let mut http_connector = HttpConnector::new(4, handle);
        http_connector.enforce_http(false);
        let https_connector = HttpsConnector::from((http_connector, Self::tls_connector(&config)?));
        let http_client = HttpClient::configure().connector(https_connector).build(handle);
        Ok(Client {
            config: config,
            client: http_client,
        })
    }

    fn request<'a>(&'a self,
                   method: Method,
                   path: &'a str)
                   -> RequestBuilder<'a, HttpsConnector<HttpConnector>> {
        RequestBuilder::new(&self.client, &self.config.endpoint, method, path)
    }

    pub fn get(&self, key: &str) -> Box<Future<Item = Item, Error = Error>> {
        Box::new(self.request(Method::Get, &format!("/api/configs/{}", key))
            .send()
            .map(|r: ItemResult| r.config))
    }

    pub fn get_all(&self, keys: &Vec<String>) -> Box<Future<Item = Vec<Item>, Error = Error>> {
        let val = match serde_json::to_string(keys) {
            Ok(v) => v,
            Err(e) => {
                return Box::new(Err(Error::from(e)).into_future());
            }
        };
        Box::new(self.request(Method::Get, "/api/configs")
            .param("keys", &val)
            .send()
            .map(|r: ItemsResult| r.configs))
    }

    pub fn get_service(&self,
                       name: &str,
                       version: &str)
                       -> Box<Future<Item = ServiceResult, Error = Error>> {
        self.request(Method::Get, &format!("/api/services/{}/{}", name, version))
            .send()
    }

    pub fn watch_service(&self,
                         name: &str,
                         version: &str,
                         revision: u64,
                         timeout: u64)
                         -> Box<Future<Item = Option<ServiceResult>, Error = Error>> {
        Box::new(self.request(Method::Get, &format!("/api/services/{}/{}", name, version))
            .param("watch", "true")
            .param("revision", &format!("{}", revision))
            .param("timeout", &format!("{}", timeout))
            .send()
            .and_then(|r| Ok(Some(r)))
            .or_else(|e| {
                if e.is_timeout() {
                    Ok(None)
                } else {
                    Err(e)
                }
            }))
    }
}

#[derive(Deserialize)]
pub struct ItemsResult {
    configs: Vec<Item>,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Deserialize)]
pub struct ItemResult {
    config: Item,
    #[allow(dead_code)]
    revision: u64,
}

#[derive(Deserialize, Debug)]
pub struct Item {
    pub name: String,
    pub value: String,
    pub version: u64,
}


#[derive(Deserialize, Debug)]
pub struct ServiceEndpoint {
    pub address: String,
    pub config: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Service {
    pub name: String,
    pub version: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub proto: Option<String>,
    pub description: Option<String>,

    pub endpoints: Vec<ServiceEndpoint>,
}

#[derive(Deserialize, Debug)]
pub struct ServiceResult {
    pub service: Service,
    #[allow(dead_code)]
    pub revision: u64,
}

impl Item {
    pub fn json<T>(&self) -> Result<T, serde_json::Error>
        where for<'de> T: Deserialize<'de>
    {
        serde_json::from_str(&self.value)
    }

    pub fn yaml<T>(&self) -> Result<T, serde_yaml::Error>
        where for<'de> T: Deserialize<'de>
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
