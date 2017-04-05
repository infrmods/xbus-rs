use hyper::client::Client as HttpClient;
use hyper::method::Method;
use hyper::net::HttpsConnector;
use hyper_native_tls::NativeTlsClient;
use native_tls::TlsConnector;
use native_tls::backend::openssl::TlsConnectorBuilderExt;
use openssl::x509::X509_FILETYPE_PEM;
use serde_json;
use serde_yaml;
use serde::Deserialize;
use request::RequestBuilder;
use error::Error;


#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    endpoint: String,
    ca_file: Option<String>,
    cert_key_file: Option<(String, String)>,
}

impl Config {
    pub fn new(endpoint: &str) -> Config {
        Config {
            endpoint: endpoint.to_owned(),
            ca_file: None,
            cert_key_file: None,
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
    client: HttpClient,
}

impl Client {
    fn tls_client(config: &Config) -> Result<NativeTlsClient, Error> {
        let mut tls_builder = TlsConnector::builder()?;
        {
            let mut builder = tls_builder.builder_mut().builder_mut();
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
        Ok(tls_builder.build()?.into())
    }

    pub fn new(config: Config) -> Result<Client, Error> {
        let connector = HttpsConnector::new(Self::tls_client(&config)?);
        let http_client = HttpClient::with_connector(connector);
        Ok(Client {
            config: config,
            client: http_client,
        })
    }

    fn request<'a>(&'a self, method: Method, path: &'a str) -> RequestBuilder<'a> {
        RequestBuilder::new(&self.client, &self.config.endpoint, method, path)
    }

    pub fn get(&self, key: &str) -> Result<Item, Error> {
        self.request(Method::Get, &format!("/api/configs/{}", key))
            .send()
            .map(|r: ItemResult| r.config)
    }

    pub fn get_all(&self, keys: &Vec<String>) -> Result<Vec<Item>, Error> {
        let val = serde_json::to_string(keys)?;
        self.request(Method::Get, "/api/configs")
            .param("keys", &val)
            .send()
            .map(|r: ItemsResult| r.configs)
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

impl Item {
    pub fn json<T: Deserialize>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(&self.value)
    }

    pub fn yaml<T: Deserialize>(&self) -> Result<T, serde_yaml::Error> {
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
