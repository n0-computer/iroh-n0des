use std::{
    env::VarError,
    str::FromStr,
    sync::{Arc, RwLock},
};

use anyhow::{Result, anyhow, ensure};
use iroh::{Endpoint, EndpointAddr, EndpointId, endpoint::ConnectError};
use iroh_metrics::{
    MetricValue, MetricsGroup, Registry,
    encoding::{Encoder, Update},
};
use irpc_iroh::IrohLazyRemoteConnection;
use n0_error::StackResultExt;
use n0_future::{task::AbortOnDropHandle, time::Duration};
use rcan::Rcan;
use tokio::sync::oneshot;
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::{
    api_secret::ApiSecret,
    caps::Caps,
    protocol::{ALPN, Auth, N0desClient, Ping, Pong, PutMetrics, RemoteError},
};

/// Client is the main handle for interacting with n0des. It communicates with
/// n0des entirely through an iroh endpoint, and is configured through a builder.
/// Client requires either an Ssh Key or [`ApiSecret`]
///
/// ```no_run
/// use iroh_n0des::Client;
///
/// async fn build_client() -> anyhow::Result<()> {
///     let endpoint = iroh::Endpoint::bind().await?;
///
///     // needs N0DES_API_SECRET set to an environment variable
///     // client will now push endpoint metrics to n0des.
///     let client = Client::builder(&endpoint)
///         .api_secret_from_str("MY_API_SECRET")?
///         .build()
///         .await;
///
///     Ok(())
/// }
/// ```
///
/// [`ApiSecret`]: crate::api_secret::ApiSecret
#[derive(Debug)]
pub struct Client {
    message_channel: tokio::sync::mpsc::Sender<ClientActorMessage>,
    _actor_task: AbortOnDropHandle<()>,
}

/// ClientBuilder provides configures and builds a n0des client, typically
/// created with [`Client::builder`]
pub struct ClientBuilder {
    #[allow(dead_code)]
    cap_expiry: Duration,
    cap: Option<Rcan<Caps>>,
    endpoint: Endpoint,
    metrics_interval: Option<Duration>,
    remote: Option<EndpointAddr>,
    registry: Registry,
}

const DEFAULT_CAP_EXPIRY: Duration = Duration::from_secs(60 * 60 * 24 * 30); // 1 month
const API_SECRET_ENV_VAR_NAME: &str = "N0DES_API_SECRET";

impl ClientBuilder {
    pub fn new(endpoint: &Endpoint) -> Self {
        let mut registry = Registry::default();
        registry.register_all(endpoint.metrics());

        Self {
            cap: None,
            cap_expiry: DEFAULT_CAP_EXPIRY,
            endpoint: endpoint.clone(),
            metrics_interval: Some(Duration::from_secs(20)),
            remote: None,
            registry,
        }
    }

    /// Register a metrics group to forward to n0des
    ///
    /// The default registered metrics uses only the endpoint
    pub fn register_metrics_group(mut self, metrics_group: Arc<dyn MetricsGroup>) -> Self {
        self.registry.register(metrics_group);
        self
    }

    /// Set the metrics collection interval
    ///
    /// Defaults to enabled, every 60 seconds.
    pub fn metrics_interval(mut self, interval: Duration) -> Self {
        self.metrics_interval = Some(interval);
        self
    }

    /// Disable metrics collection.
    pub fn disable_metrics_interval(mut self) -> Self {
        self.metrics_interval = None;
        self
    }

    /// Check N0DES_API_SECRET environment variable for a valid API secret
    pub fn api_secret_from_env(self) -> Result<Self> {
        match std::env::var(API_SECRET_ENV_VAR_NAME) {
            Ok(ticket_string) => {
                let ticket = ApiSecret::from_str(&ticket_string)
                    .context("invalid {API_SECRET_ENV_VAR_NAME}")?;
                self.api_secret(ticket)
            }
            Err(VarError::NotPresent) => Err(anyhow!(
                "{API_SECRET_ENV_VAR_NAME} environment variable is not set"
            )),
            Err(VarError::NotUnicode(e)) => Err(anyhow!(
                "{API_SECRET_ENV_VAR_NAME} environment variable is not valid unicode: {:?}",
                e
            )),
        }
    }

    /// set client API secret from an encoded string
    pub fn api_secret_from_str(self, secret_key: &str) -> Result<Self> {
        let key = ApiSecret::from_str(secret_key).context("invalid n0des api secret")?;
        self.api_secret(key)
    }

    /// Use a shared secret & remote n0des endpoint ID contained within a ticket
    /// to construct a n0des client. The resulting client will have "Client"
    /// capabilities.
    ///
    /// API secrets include remote details within them, and will set both the
    /// remote and rcan values on the builder
    pub fn api_secret(mut self, ticket: ApiSecret) -> Result<Self> {
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
    #[cfg(feature = "ssh-key")]
    pub async fn ssh_key_from_file<P: AsRef<std::path::Path>>(self, path: P) -> Result<Self> {
        let file_content = tokio::fs::read_to_string(path).await?;
        let private_key = ssh_key::PrivateKey::from_openssh(&file_content)?;

        self.ssh_key(&private_key)
    }

    /// Creates the capability from the provided private ssh key.
    #[cfg(feature = "ssh-key")]
    pub fn ssh_key(mut self, key: &ssh_key::PrivateKey) -> Result<Self> {
        let local_id = self.endpoint.id();
        let rcan = crate::caps::create_api_token_from_ssh_key(
            key,
            local_id,
            self.cap_expiry,
            Caps::all(),
        )?;
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
    /// this method, or through calling the api_secret builder methods.
    pub fn remote(mut self, remote: impl Into<EndpointAddr>) -> Self {
        self.remote = Some(remote.into());
        self
    }

    /// Create a new client, connected to the provide service node
    #[must_use = "dropping the client will silently cancel all client tasks"]
    pub async fn build(self) -> Result<Client, BuildError> {
        debug!("starting iroh-n0des client");
        let remote = self.remote.ok_or(BuildError::MissingRemote)?;
        let capabilities = self.cap.ok_or(BuildError::MissingCapability)?;

        let conn = IrohLazyRemoteConnection::new(self.endpoint, remote, ALPN.to_vec());
        let client = N0desClient::boxed(conn);

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let metrics_task = AbortOnDropHandle::new(n0_future::task::spawn(
            ClientActor {
                capabilities,
                client,
                session_id: Uuid::new_v4(),
                authorized: false,
                latest_ackd_update: None,
            }
            .run(self.registry, self.metrics_interval, rx),
        ));

        Ok(Client {
            message_channel: tx,
            _actor_task: metrics_task,
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
    pub async fn ping(&self) -> Result<Pong, Error> {
        let (tx, rx) = oneshot::channel();
        self.message_channel
            .send(ClientActorMessage::Ping { done: tx })
            .await
            .map_err(|_| Error::Other(anyhow!("sending ping request")))?;

        rx.await
            .map_err(|e| Error::Other(anyhow!("response on internal channel: {:?}", e)))?
            .map_err(Error::Remote)
    }

    /// immediately send a single dump of metrics to n0des. It's not necessary
    /// to call this function if you're using a non-zero metrics interval,
    /// which will automatically propagate metrics on the set interval for you
    pub async fn push_metrics(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.message_channel
            .send(ClientActorMessage::SendMetrics { done: tx })
            .await
            .map_err(|_| Error::Other(anyhow!("sending metrics")))?;

        rx.await
            .map_err(|e| Error::Other(anyhow!("response on internal channel: {:?}", e)))?
            .map_err(Error::Remote)
    }
}

enum ClientActorMessage {
    SendMetrics {
        done: oneshot::Sender<Result<(), RemoteError>>,
    },
    Ping {
        done: oneshot::Sender<Result<Pong, RemoteError>>,
    },
}

struct ClientActor {
    capabilities: Rcan<Caps>,
    client: N0desClient,
    session_id: Uuid,
    authorized: bool,
    latest_ackd_update: Option<Update>,
}

impl ClientActor {
    async fn run(
        mut self,
        registry: Registry,
        interval: Option<Duration>,
        mut inbox: tokio::sync::mpsc::Receiver<ClientActorMessage>,
    ) {
        let registry = Arc::new(RwLock::new(registry));
        let mut encoder = Encoder::new(registry);
        let mut metrics_timer = interval.map(|interval| n0_future::time::interval(interval));
        trace!("starting client actor");
        loop {
            trace!("client actor tick");
            tokio::select! {
                biased;
                Some(msg) = inbox.recv() => {
                    match msg {
                        ClientActorMessage::Ping{ done } => {
                            let res = self.send_ping().await;
                            if let Err(err) = done.send(res) {
                                debug!("failed to send ping: {:#?}", err);
                                self.authorized = false;
                            }
                        },
                        ClientActorMessage::SendMetrics{ done } => {
                            trace!("sending metrics manually triggered");
                            let res = self.send_metrics(&mut encoder).await;
                            if let Err(err) = done.send(res) {
                                debug!("failed to push metrics: {:#?}", err);
                                self.authorized = false;
                            }
                        }
                    }
                }
                _ = async {
                    if let Some(ref mut timer) = metrics_timer {
                        timer.tick().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    trace!("metrics send tick");
                    if let Err(err) = self.send_metrics(&mut encoder).await {
                        debug!("failed to push metrics: {:#?}", err);
                        self.authorized = false;
                    }
                },
            }
        }
    }

    // sends an authorization request to the server
    async fn auth(&mut self) -> Result<(), RemoteError> {
        if self.authorized {
            return Ok(());
        }
        trace!("client authorizing");
        self.client
            .rpc(Auth {
                caps: self.capabilities.clone(),
            })
            .await
            .inspect_err(|e| debug!("authorization failed: {:?}", e))
            .map_err(|e| RemoteError::AuthError(e.to_string()))?;
        self.authorized = true;
        Ok(())
    }

    async fn send_ping(&mut self) -> Result<Pong, RemoteError> {
        trace!("client actor send ping");
        self.auth().await?;

        let req = rand::random();
        self.client
            .rpc(Ping { req })
            .await
            .inspect_err(|e| warn!("rpc ping error: {e}"))
            .map_err(|_| RemoteError::InternalServerError)
    }

    async fn send_metrics(&mut self, encoder: &mut Encoder) -> Result<(), RemoteError> {
        trace!("client actor send metrics");
        self.auth().await?;

        let update = encoder.export();
        let delta = update_delta(&self.latest_ackd_update, &update);
        let req = PutMetrics {
            session_id: self.session_id,
            update: delta,
        };

        self.client
            .rpc(req)
            .await
            .map_err(|_| RemoteError::InternalServerError)??;

        self.latest_ackd_update = Some(update);

        Ok(())
    }
}

fn update_delta(t1: &Option<Update>, t2: &Update) -> Update {
    // full reset on schema changes
    if t2.schema.is_some() {
        return t2.clone();
    }

    match t1 {
        Some(t1) => {
            if t2.values.items.len() != t2.values.items.len() {
                return t2.clone();
            }

            let mut delta = t2.clone();
            for (i, rhs) in delta.values.items.iter_mut().enumerate() {
                let lhs = &t1.values.items[i];
                match (rhs, lhs) {
                    (MetricValue::Counter(rhs), MetricValue::Counter(lhs)) => {
                        let res = *rhs - *lhs;
                        *rhs = res;
                    }
                    (MetricValue::Gauge(_), MetricValue::Gauge(_)) => {
                        // intentionally ignore gagues, gauges are moment-in-time and shouldn't
                        // be subtracted
                    }
                    (
                        MetricValue::Histogram {
                            buckets: rhs_buckets,
                            sum: rhs_sum,
                            count: rhs_count,
                        },
                        MetricValue::Histogram {
                            buckets: lhs_buckets,
                            sum: lhs_sum,
                            count: lhs_count,
                        },
                    ) => {
                        for rhs in rhs_buckets {
                            for lhs in lhs_buckets {
                                let res = rhs.1 - lhs.1;
                                *rhs = (rhs.0, res);
                            }
                        }
                        *rhs_sum -= *lhs_sum;
                        *rhs_count -= *lhs_count;
                    }
                    (_, _) => {
                        trace!("unexpected metrics comparison");
                    }
                }
            }
            delta
        }
        None => t2.clone(),
    }
}

#[cfg(test)]
mod tests {
    use iroh::{Endpoint, EndpointAddr, SecretKey};
    use temp_env_vars::temp_env_vars;

    use crate::{
        Client,
        api_secret::ApiSecret,
        caps::{Cap, Caps},
        client::API_SECRET_ENV_VAR_NAME,
    };

    #[tokio::test]
    #[temp_env_vars]
    async fn test_api_key_from_env() {
        // construct
        let mut rng = rand::rng();
        let shared_secret = SecretKey::generate(&mut rng);
        let fake_endpoint_id = SecretKey::generate(&mut rng).public();
        let api_secret = ApiSecret::new(shared_secret.clone(), fake_endpoint_id);
        unsafe {
            std::env::set_var(API_SECRET_ENV_VAR_NAME, api_secret.to_string());
        };

        let endpoint = Endpoint::empty_builder(iroh::RelayMode::Disabled)
            .bind()
            .await
            .unwrap();

        let builder = Client::builder(&endpoint).api_secret_from_env().unwrap();

        let fake_endpoint_addr: EndpointAddr = fake_endpoint_id.into();
        assert_eq!(builder.remote, Some(fake_endpoint_addr));

        let rcan = crate::caps::create_api_token_from_secret_key(
            shared_secret,
            endpoint.id(),
            builder.cap_expiry,
            Caps::for_shared_secret(),
        )
        .unwrap();
        assert_eq!(builder.cap, Some(rcan.clone()));
        assert_eq!(rcan.capability(), &Caps::new([Cap::Client]));
    }

    /// Assert that disabling metrics interval can manually send metrics without
    /// panicking. Metrics sending itself is expected to fail.
    #[tokio::test]
    async fn test_no_metrics_interval() {
        let mut rng = rand::rng();
        let shared_secret = SecretKey::generate(&mut rng);
        let fake_endpoint_id = SecretKey::generate(&mut rng).public();
        let api_secret = ApiSecret::new(shared_secret.clone(), fake_endpoint_id);

        let endpoint = Endpoint::empty_builder(iroh::RelayMode::Disabled)
            .bind()
            .await
            .unwrap();

        let client = Client::builder(&endpoint)
            .disable_metrics_interval()
            .api_secret(api_secret)
            .unwrap()
            .build()
            .await
            .unwrap();

        let err = client.push_metrics().await;
        assert!(err.is_err());
    }
}
