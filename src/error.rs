use http;
use hyper::Error as HttpError;
use serde_json::Error as JsonError;
use serde_yaml::Error as YamlError;
use std::error::Error as StdError;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::io::{Error as IOError, ErrorKind};
use url::ParseError;

#[derive(Debug)]
pub enum Error {
    Io(IOError),
    Http(String),
    Ssl(String),
    Serialize(String),
    Request(String, String),
    NotPermitted(String, Vec<String>),
    Other(String),
}

impl Error {
    pub fn io_timeout() -> Error {
        Error::Io(IOError::new(ErrorKind::TimedOut, "timeout"))
    }

    pub fn is_timeout(&self) -> bool {
        match self {
            Error::Io(e) => e.kind() == ErrorKind::TimedOut,
            Error::Request(code, _) => code == "DEADLINE_EXCEEDED",
            _ => false,
        }
    }

    pub fn is_not_found(&self) -> bool {
        if let Error::Request(code, _) = self {
            if code == "NOT_FOUND" {
                return true;
            }
        }
        false
    }

    pub fn can_retry(&self) -> bool {
        match self {
            Error::Ssl(_) => false,
            Error::Serialize(_) => false,
            Error::Request(code, _) => match code.as_str() {
                "SYSTEM_ERROR" | "TOO_MANY_ATTEMPTS" | "DEADLINE_EXCEEDED" | "CANCELLED" => true,
                _ => false,
            },
            Error::NotPermitted(_, _) => false,
            Error::Other(_) => false,
            _ => true,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        match self {
            Error::Io(e) => e.fmt(f),
            Error::Http(e) => write!(f, "{}", e),
            Error::Ssl(e) => write!(f, "{}", e),
            Error::Serialize(e) => write!(f, "{}", e),
            Error::Request(code, message) => write!(f, "request fail[{}]: {}", code, message),
            Error::NotPermitted(message, _) => write!(f, "not permitted: {}", message),
            Error::Other(e) => write!(f, "{}", e),
        }
    }
}

impl StdError for Error {}

impl From<IOError> for Error {
    fn from(err: IOError) -> Error {
        Error::Io(err)
    }
}

impl From<HttpError> for Error {
    fn from(err: HttpError) -> Error {
        Error::Http(format!("{}", err))
    }
}

impl From<JsonError> for Error {
    fn from(err: JsonError) -> Error {
        Error::Serialize(format!("{}", err))
    }
}

impl From<YamlError> for Error {
    fn from(err: YamlError) -> Error {
        Error::Serialize(format!("{}", err))
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Error {
        Error::Other(format!("parse url fail: {}", err))
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Error {
        Error::Other(msg)
    }
}

impl<'a> From<&'a str> for Error {
    fn from(msg: &'a str) -> Error {
        Error::Other(msg.to_owned())
    }
}

impl From<http::Error> for Error {
    fn from(e: http::Error) -> Error {
        Error::Http(format!("{}", e))
    }
}
