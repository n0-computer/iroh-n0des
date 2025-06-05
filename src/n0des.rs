use anyhow::Result;
use iroh::Endpoint;
use n0_future::Future;

pub trait N0de: 'static + Send {
    fn spawn(endpoint: Endpoint) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;
    fn shutdown(&mut self) -> impl Future<Output = Result<()>> + Send;
}

#[macro_export]
macro_rules! export_node {
    ($type:ty) => {
        #[no_mangle]
        pub extern "C" fn load_plugin() -> Box<dyn $crate::N0de> {
            Box::new(<$type>::default())
        }
    };
}
