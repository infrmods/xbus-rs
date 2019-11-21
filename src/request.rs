use crate::error::Error;
use futures::prelude::*;
use std::pin::Pin;

use http::request::Builder;
use http::{Method, Uri};
use hyper::client::connect::Connect;
use hyper::client::Client;
use hyper::Body;
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_string};
use std::collections::HashMap;
use std::time::Duration;
use tokio::timer::Timeout;
use url::form_urlencoded;

pub struct RequestBuilder<'a, C: 'static + Connect> {
    client: &'a Client<C>,
    endpoint: &'a str,
    path: &'a str,
    params: HashMap<&'a str, &'a str>,
    body: Option<Body>,
    builder: Builder,
    timeout: Option<Duration>,
    pending_err: Option<Error>,
}

impl<'a, C: 'static + Connect> RequestBuilder<'a, C> {
    pub fn new(
        client: &'a Client<C>,
        endpoint: &'a str,
        method: Method,
        path: &'a str,
        timeout: Option<Duration>,
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
            timeout,
            pending_err: None,
        }
    }

    pub fn param(mut self, name: &'a str, value: &'a str) -> RequestBuilder<'a, C> {
        self.params.insert(name, value);
        self
    }

    pub fn param_opt(mut self, name: &'a str, value: Option<&'a str>) -> RequestBuilder<'a, C> {
        if let Some(val) = value {
            self.params.insert(name, val);
        }
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

    pub fn form_result(mut self, form: Result<Form, Error>) -> RequestBuilder<'a, C> {
        match form {
            Ok(form) => self.form(form),
            Err(e) => {
                if self.pending_err.is_none() {
                    self.pending_err = Some(e);
                }
                self
            }
        }
    }

    fn get_response<T>(mut self) -> Pin<Box<dyn Future<Output = Result<Response<T>, Error>> + Send>>
    where
        for<'de> T: Deserialize<'de> + Send + 'static,
    {
        if let Some(err) = self.pending_err {
            return future::err(err).boxed();
        }

        let mut url_str = self.endpoint.to_owned();
        url_str.push_str(self.path);
        if !self.params.is_empty() {
            url_str.push('?');
            url_str.push_str(
                &self
                    .params
                    .iter()
                    .map(|(k, v)| {
                        format!("{}={}", k, percent_encode(v.as_bytes(), NON_ALPHANUMERIC))
                    })
                    .collect::<Vec<String>>()
                    .join("&"),
            );
        }
        let uri = match url_str.parse::<Uri>() {
            Ok(u) => u,
            Err(_) => {
                return future::err(Error::Other(format!("invalid url: {}", url_str))).boxed();
            }
        };
        self.builder.uri(uri);
        let request = match self.builder.body(self.body.unwrap_or_else(Body::empty)) {
            Ok(r) => r,
            Err(e) => {
                return future::err(Error::from(e)).boxed();
            }
        };
        trace!("request xbus: {} {}", request.method(), request.uri());
        let resp_fut = self
            .client
            .request(request)
            .map_err(Error::from)
            .and_then(|resp| {
                let status = resp.status();
                resp.into_body()
                    .try_concat()
                    .map(move |result| match result {
                        Ok(body) => {
                            if !status.is_success() {
                                let msg =
                                    format!("[{}]: {}", status, String::from_utf8_lossy(&body));
                                return Err(Error::from(msg));
                            }
                            let json_rep: Response<T> = from_slice(&body)?;
                            Ok(json_rep)
                        }
                        Err(e) => Err(Error::from(e)),
                    })
            });
        if let Some(timeout) = self.timeout {
            return Timeout::new(resp_fut, timeout)
                .map(|result| match result {
                    Ok(x) => x,
                    Err(_) => Err(Error::io_timeout()),
                })
                .boxed();
        }
        resp_fut.boxed()
    }

    pub fn send<T>(self) -> impl Future<Output = Result<T, Error>>
    where
        for<'de> T: Deserialize<'de> + Send + 'static,
    {
        self.get_response()
            .map(|result| result.and_then(|resp| resp.get()))
    }

    pub fn get_ok(self) -> impl Future<Output = Result<(), Error>> {
        self.get_response::<()>()
            .map(|result| result.and_then(|resp| resp.get_ok()))
    }
}

/*
fn join_chunks<S>(s: S) -> Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send>
where
    S: Stream<Item = Result<Chunk, Error>> + Send + 'static,
{
    Box::new(s.collect().map(|cs| {
        let mut r = Vec::new();
        for c in cs {
            r.extend(c);
        }
        r
    }))
}
*/

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
