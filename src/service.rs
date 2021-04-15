use std::{collections::HashMap, net::SocketAddr};

use crate::addr_serde;
use crate::RevisionResult;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServiceDesc {
    pub service: String,
    pub zone: String,
    #[serde(rename = "type")]
    pub typ: Option<String>,
    pub proto: Option<String>,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServiceDescEvent {
    pub event_type: String,
    pub service: ServiceDesc,
}

impl ServiceDescEvent {
    pub const PUT: &'static str = "put";
    pub const DELETE: &'static str = "delete";
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Service {
    pub service: String,
    pub zones: HashMap<String, ZoneService>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZoneService {
    #[serde(flatten)]
    pub desc: ServiceDesc,
    pub endpoints: Vec<ServiceEndpoint>,
}

impl ZoneService {
    pub fn addresses<'a>(&'a self) -> impl Iterator<Item = SocketAddr> + 'a {
        self.endpoints.iter().map(|e| e.address)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceEndpoint {
    #[serde(
        serialize_with = "addr_serde::serialize_address",
        deserialize_with = "addr_serde::deserialize_address"
    )]
    pub address: SocketAddr,
    pub config: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppNode {
    pub label: Option<String>,
    pub key: String,
    pub config: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AppNodes {
    pub nodes: HashMap<String, String>,
    pub revision: u64,
}

impl RevisionResult for AppNodes {
    fn get_revision(&self) -> u64 {
        self.revision
    }
}
