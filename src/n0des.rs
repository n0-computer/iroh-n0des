use anyhow::Result;
use iroh::Endpoint;
use n0_future::Future;

pub trait N0de: 'static + Send {
    fn spawn(endpoint: Endpoint) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;
    fn shutdown(&mut self) -> impl Future<Output = Result<()>> + Send;
}
