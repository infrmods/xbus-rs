use std::collections::HashMap;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;
use http::request::Builder;
use http::{Method, Uri};
use error::Error;
use hyper::client::Client;
use hyper::client::connect::Connect;
use hyper::{Body, Chunk};
use serde_json::{from_slice, to_string};
use percent_encoding::{percent_encode, DEFAULT_ENCODE_SET};

pub struct RequestBuilder<'a, C: 'static + Connect> {
    client: &'a Client<C>,
    endpoint: &'a str,
    path: &'a str,
    params: HashMap<&'a str, &'a str>,
    body: Option<Body>,
    builder: Builder,
}

impl<'a, C: 'static + Connect> RequestBuilder<'a, C> {
    pub fn new(
        client: &'a Client<C>,
        endpoint: &'a str,
        method: Method,
        path: &'a str,
    ) -> RequestBuilder<'a, C> {
        let mut builder = Builder::new();
        builder.method(method);
        RequestBuilder {
            client,
            endpoint,
            path,
            params: HashMap::new(),
            body: None,
            builder,
        }
    }

    pub fn param(mut self, name: &'a str, value: &'a str) -> RequestBuilder<'a, C> {
        self.params.insert(name, value);
        self
    }

    pub fn header(mut self, name: &'a str, value: &'a str) -> RequestBuilder<'a, C> {
        self.builder.header(name, value);
        self
    }

    pub fn body<B: Into<Body>>(mut self, body: B) -> RequestBuilder<'a, C> {
        self.body = Some(body.into());
        self
    }

    pub fn form(mut self, form: Form) -> RequestBuilder<'a, C> {
        self.builder
            .header("Content-Type", "application/x-www-form-urlencoded");
        self.body(form)
    }

    fn get_response<T>(mut self) -> Box<Future<Item = Response<T>, Error = Error> + Send>
    where
        for<'de> T: Deserialize<'de> + Send + 'static,
    {
        let mut url_str = self.endpoint.to_owned();
        url_str.push_str(self.path);
        if !self.params.is_empty() {
            url_str.push('?');
            url_str.push_str(&self.params
                .iter()
                .map(|(k, v)| format!("{}={}", k, percent_encode(v.as_bytes(), DEFAULT_ENCODE_SET)))
                .collect::<Vec<String>>()
                .join("&"));
        }
        let uri = match url_str.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => {
                return Box::new(
                    Err(Error::Other(format!("invalid url: {}", url_str))).into_future(),
                );
            }
        };
        self.builder.uri(uri);
        let request = match self.builder.body(self.body.unwrap_or_else(Body::empty)) {
            Ok(r) => r,
            Err(e) => {
                return Box::new(Err(Error::from(e)).into_future());
            }
        };
        let resp = self.client
            .request(request)
            .map_err(Error::from)
            .and_then(|resp| {
                let status = resp.status();
                join_chunks(resp.into_body().map_err(Error::from)).and_then(move |body| {
                    if !status.is_success() {
                        let msg = format!("[{}]: {}", status, String::from_utf8_lossy(&body));
                        return Err(Error::from(msg));
                    }
                    let json_rep: Response<T> = from_slice(&body)?;
                    Ok(json_rep)
                })
            });
        Box::new(resp)
    }

    pub fn send<T>(self) -> Box<Future<Item = T, Error = Error> + Send>
    where
        for<'de> T: Deserialize<'de> + Send + 'static,
    {
        Box::new(self.get_response().and_then(|resp| resp.get()))
    }

    pub fn get_ok(self) -> Box<Future<Item = (), Error = Error> + Send> {
        Box::new(self.get_response::<()>().and_then(|resp| resp.get_ok()))
    }
}

fn join_chunks<S>(s: S) -> Box<Future<Item = Vec<u8>, Error = Error> + Send>
where
    S: Stream<Item = Chunk, Error = Error> + Send + 'static,
{
    Box::new(s.collect().map(|cs| {
        let mut r = Vec::new();
        for c in cs {
            r.extend(c);
        }
        r
    }))
}

#[derive(Deserialize, Debug)]
struct RespError {
    pub code: String,
    pub message: Option<String>,
    pub keys: Option<Vec<String>>,
}

#[derive(Deserialize, Debug)]
struct Response<T> {
    pub ok: bool,
    pub result: Option<T>,
    pub error: Option<RespError>,
}

impl<T> Response<T> {
    fn get_ok(self) -> Result<(), Error> {
        if self.ok {
            Ok(())
        } else {
            Err(Self::convert_err(self.error))
        }
    }

    fn convert_err(error: Option<RespError>) -> Error {
        match error {
            Some(err) => {
                if err.code == "NOT_PERMITTED" {
                    Error::NotPermitted(
                        err.message.unwrap_or_else(String::new),
                        err.keys.unwrap_or_else(Vec::new),
                    )
                } else {
                    Error::Request(err.code, err.message.unwrap_or_else(String::new))
                }
            }
            None => Error::from("missing error"),
        }
    }

    fn get(self) -> Result<T, Error> {
        if self.ok {
            match self.result {
                Some(t) => Ok(t),
                None => Err(Error::from("missing result")),
            }
        } else {
            Err(Self::convert_err(self.error))
        }
    }
}

pub struct Form {
    serializer: form_urlencoded::Serializer<String>,
}

impl Form {
    pub fn new() -> Form {
        Form {
            serializer: form_urlencoded::Serializer::new(String::new()),
        }
    }

    pub fn set<T: Serialize>(&mut self, name: &str, v: T) -> Result<(), Error> {
        let mut data = match to_string(&v) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::Serialize(format!(
                    "serialize json({}) fail: {}",
                    name, e
                )));
            }
        };
        if data == "null" {
            data = String::new();
        }
        self.serializer.append_pair(name, &data);
        Ok(())
    }
}

impl Into<Body> for Form {
    fn into(mut self) -> Body {
        Body::from(self.serializer.finish())
    }
}

macro_rules! form {
    ($($k: expr => $v: expr), *) => ({
        let mut form = Form::new();
        let mut result = Ok(());
        $(result = result.and_then(|_| form.set($k, $v));)*
        result.and(Ok(form))
    })
}
