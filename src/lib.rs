#[cfg(feature = "iroh_main")]
mod client;
mod n0des;

#[cfg(feature = "iroh_main")]
pub mod caps;
#[cfg(feature = "iroh_main")]
pub mod protocol;
pub mod simulation;

pub use iroh_n0des_macro::sim;

// This lets us use the derive metrics in the lib tests within this crate.
extern crate self as iroh_n0des;

pub use iroh_metrics::Registry;

#[cfg(feature = "iroh_v035")]
pub use iroh_035 as iroh;

#[cfg(feature = "iroh_main")]
pub use iroh;

pub use anyhow;

#[cfg(all(feature = "iroh_v035", feature = "iroh_main"))]
compile_error!(
    "Features 'iroh_v035' and 'iroh_main' cannot be enabled at the same time. Choose only one."
);

#[cfg(not(any(feature = "iroh_v035", feature = "iroh_main")))]
compile_error!("You must enable exactly one of the features: 'iroh_v035' or 'iroh_main'.");

#[cfg(feature = "iroh_main")]
pub use self::{
    client::{Client, ClientBuilder},
    protocol::ALPN,
};

pub use self::n0des::N0de;
