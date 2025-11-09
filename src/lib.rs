mod client;
mod n0des;

pub mod api_secret;
pub mod caps;
pub mod protocol;
pub mod simulation;
pub mod ticket;

pub use iroh_n0des_macro::sim;

// This lets us use the derive metrics in the lib tests within this crate.
extern crate self as iroh_n0des;

pub use anyhow;
pub use iroh_metrics::Registry;

pub use self::{
    api_secret::ApiSecret,
    client::{Client, ClientBuilder},
    n0des::N0de,
    protocol::ALPN,
};

#[cfg(test)]
mod tests {
    // keep to make sure that there is a test even with all features disabled.
    #[test]
    fn dummy() {
        assert_eq!(2 + 2, 4);
    }
}
