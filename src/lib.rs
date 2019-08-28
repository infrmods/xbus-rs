#[macro_use]
extern crate futures;
extern crate http;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate percent_encoding;
extern crate ring;
extern crate rustls;
extern crate serde;
extern crate tokio_rustls;
extern crate untrusted;
extern crate webpki;
extern crate webpki_roots;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;
extern crate tokio;
extern crate tokio_io;
extern crate url;

pub mod cert;
pub mod error;
mod https;
#[macro_use]
mod request;
mod addr_serde;
pub mod client;
mod service_keeper;
mod watcher;

pub use self::client::Client;
pub use self::client::Config;
pub use self::error::Error;
pub use self::service_keeper::ServiceKeeper;
pub use self::watcher::WatchHandle;

pub const DEFAULT_ZONE: &str = "default";
