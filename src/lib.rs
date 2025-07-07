mod client;
mod n0des;

pub mod caps;
pub mod protocol;
pub mod simulation;

pub use iroh_n0des_macro::sim;

// This lets us use the derive metrics in the lib tests within this crate.
extern crate self as iroh_n0des;

pub use self::{
    client::{Client, ClientBuilder},
    n0des::N0de,
    protocol::ALPN,
};

pub use iroh_metrics::Registry;
