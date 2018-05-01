extern crate futures;
extern crate http;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate native_tls;
extern crate openssl;
extern crate percent_encoding;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;
extern crate tokio;
extern crate tokio_io;
extern crate url;

pub mod error;
mod https;
#[macro_use]
mod request;
pub mod client;
mod service_keeper;

pub use error::Error;
pub use client::Config;
pub use client::Client;
pub use service_keeper::ServiceKeeper;
