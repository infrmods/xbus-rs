extern crate futures;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

pub mod cert;
pub mod error;
mod https;
#[macro_use]
mod request;
mod addr_serde;
pub mod client;
mod config;
mod service_keeper;
mod watcher;

pub use self::client::Client;
pub use self::config::Config;
pub use self::error::Error;
pub use self::https::TlsClientConfigExt;
pub use self::service_keeper::ServiceKeeper;
pub use self::watcher::WatchHandle;

pub const DEFAULT_ZONE: &str = "default";
