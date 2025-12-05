//! iroh-n0des is the client side of interacting with [n0des]. n0des gives
//! visibility into a running iroh network by pushing metrics aggregations
//! from [iroh] endpoints into a central hub for monitoring.
//!
//! Typical setup looks something like this:
//! ```no_run
//! use iroh::Endpoint;
//! use iroh_n0des::Client;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let endpoint = Endpoint::bind().await?;
//!
//!     // needs N0DES_API_SECRET set to an environment variable
//!     // client will now push endpoint metrics to n0des.
//!     let client = Client::builder(&endpoint)
//!         .api_secret_from_env()?
//!         .build()
//!         .await?;
//!
//!     // we can also ping the service just to confirm everything is working
//!     client.ping().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! [n0des]: https://n0des.iroh.computer
//! [iroh]: https://iroh.computer
mod client;
mod n0des;

pub mod api_secret;
pub mod caps;
pub mod protocol;
#[cfg(feature = "simulation")]
pub mod simulation;

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

#[cfg(feature = "tickets")]
pub use self::client::PublishedTicket;

#[cfg(test)]
mod tests {
    // keep to make sure that there is a test even with all features disabled.
    #[test]
    fn dummy() {
        assert_eq!(2 + 2, 4);
    }
}
