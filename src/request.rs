use std::collections::HashMap;
use hyper::{Body, Chunk, Client as HttpClient, Method, Request, StatusCode};
use hyper::client::Connect;
use hyper::header::{ContentType, Header, Headers};
use mime::{APPLICATION_JSON, APPLICATION_WWW_FORM_URLENCODED};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_string as to_json};
use error::Error;
use futures::{Future, IntoFuture, Stream};
use url::form_urlencoded;

pub struct RequestBuilder<'a, C: 'a + Connect> {
    client: &'a HttpClient<C>,
    endpoint: &'a str,
    method: Method,
    path: &'a str,
    params: Option<HashMap<&'a str, &'a str>>,
    body: Option<Body>,
    headers: Option<Headers>,
}

impl<'a, C: Connect> RequestBuilder<'a, C> {
    pub fn new(
        client: &'a HttpClient<C>,
        endpoint: &'a str,
        method: Method,
        path: &'a str,
    ) -> RequestBuilder<'a, C> {
        RequestBuilder {
            client: client,
            endpoint: endpoint,
            method: method,
            path: path,
            params: None,
            body: None,
            headers: None,
        }
    }

    pub fn param(mut self, name: &'a str, value: &'a str) -> RequestBuilder<'a, C> {
        if let Some(ref mut params) = self.params {
            params.insert(name, value);
        } else {
            let mut params = HashMap::new();
            params.insert(name, value);
            self.params = Some(params);
        }
        self
    }

    pub fn body<B: Into<Body>>(mut self, body: B) -> RequestBuilder<'a, C> {
        self.body = Some(body.into());
        self
    }

    pub fn form(mut self, form: Form) -> RequestBuilder<'a, C> {
        self.body = Some(form.into());
        self.set_header(ContentType(APPLICATION_WWW_FORM_URLENCODED));
        self
    }

    fn set_header<H: Header>(&mut self, header: H) {
        if let Some(ref mut headers) = self.headers {
            headers.set(header);
        } else {
            let mut headers = Headers::new();
            headers.set(header);
            self.headers = Some(headers);
        }
    }

    pub fn send_with_body<T, B: Serialize>(
        mut self,
        body: B,
    ) -> Box<Future<Item = T, Error = Error>>
    where
        for<'de> T: Deserialize<'de> + 'static,
    {
        let data = match to_json(&body) {
            Ok(x) => x,
            Err(e) => {
                return Box::new(
                    Err(Error::Serialize(format!("serialize json fail: {}", e))).into_future(),
                );
            }
        };
        self.set_header(ContentType(APPLICATION_JSON));
        self.body = Some(data.into());
        self.send()
    }

    fn get_response<T>(self) -> Box<Future<Item = Response<T>, Error = Error>>
    where
        for<'de> T: Deserialize<'de> + 'static,
    {
        let mut url_str = self.endpoint.to_owned();
        url_str.push_str(self.path);
        if let Some(ref params) = self.params {
            if params.len() > 0 {
                let ps = params
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<String>>();
                url_str.push('?');
                url_str.push_str(&ps.join("&"))
            }
        }
        let uri = match url_str.parse() {
            Ok(u) => u,
            Err(_) => {
                return Box::new(Err(Error::from("invalid url")).into_future());
            }
        };
        let mut request = Request::new(self.method, uri);
        if let Some(body) = self.body {
            request.set_body(body);
        }
        if let Some(ref headers) = self.headers {
            request.headers_mut().extend(headers.iter());
        }
        Box::new(
            self.client
                .request(request)
                .map_err(Error::from)
                .and_then(|resp| {
                    let status = resp.status();
                    join_chunks(resp.body().map_err(Error::from)).and_then(move |body| {
                        if status != StatusCode::Ok {
                            let msg = format!("[{}]: {}", status, String::from_utf8_lossy(&body));
                            return Err(Error::from(msg));
                        }
                        let json_rep: Response<T> = from_slice(&body)?;
                        Ok(json_rep)
                    })
                }),
        )
    }

    pub fn send<T>(self) -> Box<Future<Item = T, Error = Error>>
    where
        for<'de> T: Deserialize<'de> + 'static,
    {
        Box::new(self.get_response().and_then(|resp| resp.get()))
    }

    pub fn get_ok(self) -> Box<Future<Item = (), Error = Error>> {
        Box::new(self.get_response::<()>().and_then(|resp| resp.get_ok()))
    }
}

fn join_chunks<S: 'static>(s: S) -> Box<Future<Item = Vec<u8>, Error = Error>>
where
    S: Stream<Item = Chunk, Error = Error>,
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
                        err.message.unwrap_or("".to_owned()),
                        err.keys.unwrap_or(Vec::new()),
                    )
                } else {
                    Error::Request(err.code, err.message.unwrap_or("".to_owned()))
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
        let mut data = match to_json(&v) {
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

    pub fn from_iter<'a, I: IntoIterator<Item = (&'a str, T)>, T: Serialize>(
        it: I,
    ) -> Result<Form, Error> {
        let mut form = Form::new();
        for (k, v) in it.into_iter() {
            form.set(k, v)?;
        }
        Ok(form)
    }
}

impl Into<Body> for Form {
    fn into(mut self) -> Body {
        Body::from(self.serializer.finish())
    }
}
