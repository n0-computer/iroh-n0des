use std::future::Future;

use anyhow::Result;
use iroh::Endpoint;
use iroh_metrics::Registry;

/// A trait for nodes that can be spawned and shut down
pub trait N0de: 'static + Send + Sync {
    fn spawn(
        endpoint: Endpoint,
        metrics: &mut Registry,
    ) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;

    /// Asynchronously shut down the node
    fn shutdown(&mut self) -> impl Future<Output = Result<()>> + Send {
        async move { Ok(()) }
    }
}
