#[cfg(feature = "iroh_main")]
mod client;
#[cfg(any(feature = "iroh_v035", feature = "iroh_main"))]
mod n0des;

#[cfg(feature = "iroh_main")]
pub mod caps;
#[cfg(feature = "iroh_main")]
pub mod protocol;
#[cfg(any(feature = "iroh_v035", feature = "iroh_main"))]
pub mod simulation;

pub use iroh_n0des_macro::sim;

// This lets us use the derive metrics in the lib tests within this crate.
extern crate self as iroh_n0des;

pub use iroh_metrics::Registry;

#[cfg(feature = "iroh_v035")]
pub use iroh_035 as iroh;

#[cfg(all(feature = "iroh_main", not(feature = "iroh_v035")))]
pub use iroh;

pub use anyhow;

#[cfg(feature = "iroh_main")]
pub use self::{
    client::{Client, ClientBuilder},
    protocol::ALPN,
};

#[cfg(any(feature = "iroh_v035", feature = "iroh_main"))]
pub use self::n0des::N0de;
