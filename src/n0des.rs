use std::future::Future;

use anyhow::Result;
use iroh::Endpoint;

/// A trait for nodes that can be spawned and shut down
pub trait N0de: 'static + Send {
    fn spawn(endpoint: Endpoint) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;

    /// Asynchronously shut down the node
    fn shutdown(&mut self) -> impl Future<Output = Result<()>> + Send;
}
