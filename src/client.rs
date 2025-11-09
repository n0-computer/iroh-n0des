use std::{
    env::VarError,
    path::Path,
    str::FromStr,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Result, anyhow, ensure};
use iroh::{Endpoint, EndpointAddr, EndpointId, endpoint::ConnectError};
use iroh_metrics::{Registry, encoding::Encoder};
use irpc_iroh::IrohLazyRemoteConnection;
use n0_error::StackResultExt;
use n0_future::task::AbortOnDropHandle;
use rcan::Rcan;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::{
    caps::Caps,
    protocol::{ALPN, Auth, N0desClient, Ping, PutMetrics, RemoteError},
    ticket::N0desTicket,
};

#[derive(Debug)]
pub struct Client {
    client: N0desClient,
    _metrics_task: Option<AbortOnDropHandle<()>>,
}

impl Drop for Client {
    fn drop(&mut self) {
        debug!("n0des client is being dropped");
    }
}

/// Constructs a n0des client
pub struct ClientBuilder {
    cap_expiry: Duration,
    cap: Option<Rcan<Caps>>,
    endpoint: Endpoint,
    enable_metrics: Option<Duration>,
    remote: Option<EndpointAddr>,
}

const DEFAULT_CAP_EXPIRY: Duration = Duration::from_secs(60 * 60 * 24 * 30); // 1 month

impl ClientBuilder {
    pub fn new(endpoint: &Endpoint) -> Self {
        Self {
            cap: None,
            cap_expiry: DEFAULT_CAP_EXPIRY,
            endpoint: endpoint.clone(),
            enable_metrics: Some(Duration::from_secs(10)),
            remote: None,
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

    /// Check N0DES_SECRET environment variable to supply a N0desTicket
    pub fn secret_from_env(self) -> Result<Self> {
        match std::env::var("N0DES_SECRET") {
            Ok(ticket_string) => {
                let ticket =
                    N0desTicket::from_str(&ticket_string).context("invalid N0DES_SECRET")?;
                self.ticket(ticket)
            }
            Err(VarError::NotPresent) => {
                Err(anyhow!("N0DES_SECRET environment variable is not set"))
            }
            Err(VarError::NotUnicode(e)) => Err(anyhow!(
                "N0DES_SECRET environment variable is not valid unicode: {:?}",
                e
            )),
        }
    }

    /// Use a shared secret & remote n0des endpoint ID contained within a ticket
    /// to construct a n0des client. The resulting client will have "Client"
    /// capabilities.
    pub fn ticket(mut self, ticket: N0desTicket) -> Result<Self> {
        let local_id = self.endpoint.id();
        let rcan = crate::caps::create_api_token_from_secret_key(
            ticket.secret,
            local_id,
            self.cap_expiry,
            Caps::for_shared_secret(),
        )?;

        self.remote = Some(ticket.remote);
        self.rcan(rcan)
    }

    /// Loads the private ssh key from the given path, and creates the needed capability.
    pub async fn ssh_key_from_file<P: AsRef<Path>>(self, path: P) -> Result<Self> {
        let file_content = tokio::fs::read_to_string(path).await?;
        let private_key = ssh_key::PrivateKey::from_openssh(&file_content)?;

        self.ssh_key(&private_key)
    }

    /// Creates the capability from the provided private ssh key.
    pub fn ssh_key(mut self, key: &ssh_key::PrivateKey) -> Result<Self> {
        let local_id = self.endpoint.id();
        let rcan = crate::caps::create_api_token(key, local_id, self.cap_expiry, Caps::all())?;
        self.cap.replace(rcan);

        Ok(self)
    }

    /// Sets the rcan directly.
    pub fn rcan(mut self, cap: Rcan<Caps>) -> Result<Self> {
        ensure!(
            EndpointId::from_verifying_key(*cap.audience()) == self.endpoint.id(),
            "invalid audience"
        );
        self.cap.replace(cap);
        Ok(self)
    }

    /// Sets the remote to dial, must be provided either directly by calling
    /// this method, or via the
    pub fn remote(mut self, remote: impl Into<EndpointAddr>) -> Self {
        self.remote = Some(remote.into());
        self
    }

    /// Create a new client, connected to the provide service node
    pub async fn build(self) -> Result<Client, BuildError> {
        debug!("starting iroh-n0des client");
        let remote = self.remote.ok_or(BuildError::MissingRemote)?;
        let cap = self.cap.ok_or(BuildError::MissingCapability)?;

        let conn = IrohLazyRemoteConnection::new(self.endpoint.clone(), remote, ALPN.to_vec());
        let client = N0desClient::boxed(conn);

        // If auth fails, the connection is aborted.
        let () = client.rpc(Auth { caps: cap }).await?;

        let metrics_task = self.enable_metrics.map(|interval| {
            debug!(interval = ?interval, "starting metrics task");
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
    #[error("Missing remote endpoint to dial")]
    MissingRemote,
    #[error("Missing capability")]
    MissingCapability,
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Remote error: {0}")]
    Remote(#[from] RemoteError),
    #[error("Rpc connection error: {0}")]
    Rpc(irpc::Error),
    #[error("Connection error: {0}")]
    Connect(ConnectError),
}

impl From<irpc::Error> for BuildError {
    fn from(value: irpc::Error) -> Self {
        match value {
            irpc::Error::Request {
                source:
                    irpc::RequestError::Connection {
                        source: iroh::endpoint::ConnectionError::ApplicationClosed(frame),
                        ..
                    },
                ..
            } if frame.error_code == 401u32.into() => Self::Unauthorized,
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
        let req = rand::random();
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
            trace!("metrics send tick");
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

        self.client
            .rpc(req)
            .await
            .inspect_err(|e| warn!("rpc send metrics error: {e}"))?
            .inspect_err(|e| warn!("metrics server response error: {e}"))?;
        trace!("sent metrics");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iroh::{Endpoint, EndpointAddr, SecretKey};

    use crate::{Client, caps::Caps, ticket::N0desTicket};

    #[tokio::test]
    async fn test_builder_from_env() {
        // construct
        let mut rng = rand::rng();
        let shared_secret = SecretKey::generate(&mut rng);
        let fake_endpoint_id = SecretKey::generate(&mut rng).public();
        let n0des_ticket = N0desTicket::new(shared_secret.clone(), fake_endpoint_id);
        unsafe {
            std::env::set_var("N0DES_SECRET", n0des_ticket.to_string());
        };

        let endpoint = Endpoint::empty_builder(iroh::RelayMode::Disabled)
            .bind()
            .await
            .unwrap();

        let builder = Client::builder(&endpoint).secret_from_env().unwrap();

        let fake_endpoint_addr: EndpointAddr = fake_endpoint_id.into();
        assert_eq!(builder.remote, Some(fake_endpoint_addr));

        let rcan = crate::caps::create_api_token_from_secret_key(
            shared_secret,
            endpoint.id(),
            builder.cap_expiry,
            Caps::for_shared_secret(),
        )
        .unwrap();
        assert_eq!(builder.cap, Some(rcan));

        unsafe {
            std::env::remove_var("N0DES_SECRET");
        };
    }
}
