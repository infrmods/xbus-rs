use std::collections::HashMap;
use hyper::{Client as HttpClient, Method, StatusCode, Request, Chunk};
use hyper::client::Connect;
use serde::Deserialize;
use serde_json::from_slice;
use error::Error;
use futures::{Future, Stream, IntoFuture};


pub struct RequestBuilder<'a, C: 'a + Connect> {
    client: &'a HttpClient<C>,
    endpoint: &'a str,
    method: Method,
    path: &'a str,
    params: Option<HashMap<&'a str, &'a str>>,
}


impl<'a, C: Connect> RequestBuilder<'a, C> {
    pub fn new(client: &'a HttpClient<C>,
               endpoint: &'a str,
               method: Method,
               path: &'a str)
               -> RequestBuilder<'a, C> {
        RequestBuilder {
            client: client,
            endpoint: endpoint,
            method: method,
            path: path,
            params: None,
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

    pub fn send<T>(self) -> Box<Future<Item = T, Error = Error>>
        where for<'de> T: Deserialize<'de> + 'static
    {
        let mut url_str = self.endpoint.to_owned();
        url_str.push_str(self.path);
        if let Some(ref params) = self.params {
            if params.len() > 0 {
                let ps =
                    params.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<String>>();
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
        Box::new(self.client
            .request(Request::new(self.method, uri))
            .map_err(Error::from)
            .and_then(|resp| {
                let status = resp.status();
                join_chunks(resp.body().map_err(Error::from)).and_then(move |body| {
                    if status != StatusCode::Ok {
                        let msg = format!("[{}]: {}", status, String::from_utf8_lossy(&body));
                        return Err(Error::from(msg));
                    }
                    let json_rep: Response<T> = from_slice(&body)?;
                    json_rep.get()
                })
            }))
    }
}

fn join_chunks<S: 'static>(s: S) -> Box<Future<Item = Vec<u8>, Error = Error>>
    where S: Stream<Item = Chunk, Error = Error>
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
}


#[derive(Deserialize, Debug)]
struct Response<T> {
    pub ok: bool,
    pub result: Option<T>,
    pub error: Option<RespError>,
}

impl<T> Response<T> {
    fn get(self) -> Result<T, Error> {
        if self.ok {
            match self.result {
                Some(t) => Ok(t),
                None => Err(Error::from("missing result")),
            }
        } else {
            match self.error {
                Some(err) => Err(Error::Request(err.code, err.message.unwrap_or("".to_owned()))),
                None => Err(Error::from("missing error")),
            }
        }
    }
}
