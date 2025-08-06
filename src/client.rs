use std::{
    path::Path,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Result, anyhow, ensure};
use iroh::{Endpoint, NodeAddr, NodeId};
use iroh_metrics::{Registry, encoding::Encoder};
use irpc_iroh::IrohRemoteConnection;
use n0_future::task::AbortOnDropHandle;
use rand::Rng;
use rcan::Rcan;
use tracing::warn;
use uuid::Uuid;

use crate::{
    caps::Caps,
    protocol::{ALPN, Auth, N0desClient, Ping, PutMetrics, RemoteError},
};

#[derive(Debug)]
pub struct Client {
    client: N0desClient,
    _metrics_task: Option<AbortOnDropHandle<()>>,
}

/// Constructs an IPS client
pub struct ClientBuilder {
    cap_expiry: Duration,
    cap: Option<Rcan<Caps>>,
    endpoint: Endpoint,
    enable_metrics: Option<Duration>,
}

const DEFAULT_CAP_EXPIRY: Duration = Duration::from_secs(60 * 60 * 24 * 30); // 1 month

impl ClientBuilder {
    pub fn new(endpoint: &Endpoint) -> Self {
        Self {
            cap: None,
            cap_expiry: DEFAULT_CAP_EXPIRY,
            endpoint: endpoint.clone(),
            enable_metrics: Some(Duration::from_secs(60)),
        }
    }

    /// Set the metrics collection interval
    ///
    /// Defaults to enabled, every 60 seconds.
    pub fn metrics_interval(mut self, interval: Duration) -> Self {
        self.enable_metrics = Some(interval);
        self
    }

    /// Disable metrics collection.
    pub fn disable_metrics(mut self) -> Self {
        self.enable_metrics = None;
        self
    }

    /// Loads the private ssh key from the given path, and creates the needed capability.
    pub async fn ssh_key_from_file<P: AsRef<Path>>(self, path: P) -> Result<Self> {
        let file_content = tokio::fs::read_to_string(path).await?;
        let private_key = ssh_key::PrivateKey::from_openssh(&file_content)?;

        self.ssh_key(&private_key)
    }

    /// Creates the capability from the provided private ssh key.
    pub fn ssh_key(mut self, key: &ssh_key::PrivateKey) -> Result<Self> {
        let local_node = self.endpoint.node_id();
        let rcan = crate::caps::create_api_token(key, local_node, self.cap_expiry, Caps::all())?;
        self.cap.replace(rcan);

        Ok(self)
    }

    /// Sets the rcan directly.
    pub fn rcan(mut self, cap: Rcan<Caps>) -> Result<Self> {
        ensure!(
            NodeId::from(*cap.audience()) == self.endpoint.node_id(),
            "invalid audience"
        );
        self.cap.replace(cap);
        Ok(self)
    }

    /// Create a new client, connected to the provide service node
    pub async fn build(self, remote: impl Into<NodeAddr>) -> Result<Client, BuildError> {
        let cap = self.cap.ok_or(BuildError::MissingCapability)?;
        let conn = IrohRemoteConnection::new(self.endpoint.clone(), remote.into(), ALPN.to_vec());
        let client = N0desClient::boxed(conn);

        // If auth fails, the connection is aborted.
        let () = client.rpc(Auth { caps: cap }).await?;

        let metrics_task = self.enable_metrics.map(|interval| {
            AbortOnDropHandle::new(n0_future::task::spawn(
                MetricsTask {
                    client: client.clone(),
                    session_id: Uuid::new_v4(),
                    endpoint: self.endpoint.clone(),
                }
                .run(interval),
            ))
        });

        Ok(Client {
            client,
            _metrics_task: metrics_task,
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BuildError {
    #[error("Missing capability")]
    MissingCapability,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Remote error: {0}")]
    Remote(#[from] RemoteError),
    #[error("Connection error: {0}")]
    Rpc(irpc::Error),
}

impl From<irpc::Error> for BuildError {
    fn from(value: irpc::Error) -> Self {
        match value {
            irpc::Error::Request(irpc::RequestError::Connection(
                crate::iroh::endpoint::ConnectionError::ApplicationClosed(frame),
            )) if frame.error_code == 401u32.into() => Self::Unauthorized,
            value => Self::Rpc(value),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Remote error: {0}")]
    Remote(#[from] RemoteError),
    #[error("Connection error: {0}")]
    Rpc(#[from] irpc::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Client {
    pub fn builder(endpoint: &Endpoint) -> ClientBuilder {
        ClientBuilder::new(endpoint)
    }

    /// Pings the remote node.
    pub async fn ping(&mut self) -> Result<(), Error> {
        let req = rand::thread_rng().r#gen();
        let pong = self.client.rpc(Ping { req }).await?;
        if pong.req == req {
            Ok(())
        } else {
            Err(Error::Other(anyhow!("unexpected pong response")))
        }
    }
}

struct MetricsTask {
    client: N0desClient,
    session_id: Uuid,
    endpoint: Endpoint,
}

impl MetricsTask {
    async fn run(self, interval: Duration) {
        let mut registry = Registry::default();
        registry.register_all(self.endpoint.metrics());
        let registry = Arc::new(RwLock::new(registry));
        let mut encoder = Encoder::new(registry);

        let mut metrics_timer = tokio::time::interval(interval);

        loop {
            metrics_timer.tick().await;
            if let Err(err) = self.send_metrics(&mut encoder).await {
                warn!("failed to push metrics: {:#?}", err);
            }
        }
    }

    async fn send_metrics(&self, encoder: &mut Encoder) -> Result<()> {
        let update = encoder.export();
        let req = PutMetrics {
            session_id: self.session_id,
            update,
        };
        self.client.rpc(req).await??;
        Ok(())
    }
}
