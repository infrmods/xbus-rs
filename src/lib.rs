#![feature(proc_macro)]

extern crate openssl;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;
extern crate hyper;
extern crate url;

pub mod error;
pub mod client;
mod request;

pub use error::Error;
pub use client::Config;
pub use client::Client;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
