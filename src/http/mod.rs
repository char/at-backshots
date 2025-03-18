use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::body::Bytes;

pub type Body = BoxBody<Bytes, hyper::Error>;
pub fn body_empty() -> Body {
    Empty::<Bytes>::new().map_err(|e| match e {}).boxed()
}
pub fn body_full<T: Into<Bytes>>(chunk: T) -> Body {
    Full::new(chunk.into()).map_err(|e| match e {}).boxed()
}

pub mod client;
pub mod tls;
