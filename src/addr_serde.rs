use serde::de::{Deserializer, Error, Unexpected, Visitor};
use serde::Serializer;
use std::fmt::{Formatter, Result as FmtResult};
use std::net::SocketAddr;

pub fn serialize_address<S>(addr: &SocketAddr, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&addr.to_string())
}

pub fn deserialize_address<'de, D>(de: D) -> Result<SocketAddr, D::Error>
where
    D: Deserializer<'de>,
{
    de.deserialize_any(SocketAddrVisitor)
}

struct SocketAddrVisitor;

impl<'de> Visitor<'de> for SocketAddrVisitor {
    type Value = SocketAddr;

    fn expecting(&self, formatter: &mut Formatter) -> FmtResult {
        write!(formatter, "<ip:port>")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        v.parse()
            .map_err(|_| Error::invalid_value(Unexpected::Str(v), &"<ip:port>"))
    }
}
