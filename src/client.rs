use std::{path::Path, time::Duration};

use anyhow::{anyhow, ensure, Context, Result};
use iroh::{Endpoint, NodeAddr, NodeId};
use iroh_blobs::{ticket::BlobTicket, BlobFormat, Hash};
use n0_future::task::AbortOnDropHandle;
use rand::Rng;
use rcan::Rcan;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::caps::Caps;
use crate::protocol::N0desApi;

#[derive(Debug)]
pub struct Client {
    sender: mpsc::Sender<ActorMessage>,
    _actor_task: AbortOnDropHandle<()>,
    cap: Rcan<Caps>,
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

    /// Disbale metrics collection.
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
    pub async fn build(self, remote: impl Into<NodeAddr>) -> Result<Client> {
        let cap = self.cap.context("missing capability")?;

        let remote_addr = remote.into();
        let api = N0desApi::connect(self.endpoint.clone(), remote_addr.clone())?;

        let (internal_sender, internal_receiver) = mpsc::channel(64);

        let actor = Actor {
            _endpoint: self.endpoint,
            api,
            internal_receiver,
            internal_sender: internal_sender.clone(),
            session_id: Uuid::new_v4(),
        };
        let enable_metrics = self.enable_metrics;
        let run_handle = tokio::task::spawn(async move {
            actor.run(enable_metrics).await;
        });
        let actor_task = AbortOnDropHandle::new(run_handle);

        let mut this = Client {
            cap,
            sender: internal_sender,
            _actor_task: actor_task,
        };

        this.authenticate().await?;

        Ok(this)
    }
}

impl Client {
    pub fn builder(endpoint: &Endpoint) -> ClientBuilder {
        ClientBuilder::new(endpoint)
    }

    /// Trigger the auth handshake with the server
    async fn authenticate(&mut self) -> Result<()> {
        let (s, r) = oneshot::channel();
        self.sender
            .send(ActorMessage::Auth {
                rcan: self.cap.clone(),
                s,
            })
            .await?;
        r.await??;
        Ok(())
    }

    /// Transfer the blob from the local iroh node to the service node.
    pub async fn put_blob(
        &mut self,
        node: impl Into<NodeAddr>,
        hash: Hash,
        format: BlobFormat,
        name: String,
    ) -> Result<()> {
        let ticket = BlobTicket::new(node.into(), hash, format)?;

        let (s, r) = oneshot::channel();
        self.sender
            .send(ActorMessage::PutBlob { ticket, name, s })
            .await?;
        r.await??;
        Ok(())
    }

    /// Pings the remote node.
    pub async fn ping(&mut self) -> Result<()> {
        let (s, r) = oneshot::channel();
        let req = rand::thread_rng().gen();
        self.sender.send(ActorMessage::Ping { req, s }).await?;
        r.await??;
        Ok(())
    }

    /// Get the `Hash` behind the tag, if available.
    pub async fn get_tag(&mut self, name: String) -> Result<Option<Hash>> {
        let (s, r) = oneshot::channel();
        self.sender.send(ActorMessage::GetTag { name, s }).await?;
        let res = r.await??;
        Ok(res)
    }
}

struct Actor {
    _endpoint: Endpoint,
    api: N0desApi,
    internal_receiver: mpsc::Receiver<ActorMessage>,
    internal_sender: mpsc::Sender<ActorMessage>,
    session_id: Uuid,
}

#[allow(clippy::large_enum_variant)]
enum ActorMessage {
    Auth {
        rcan: Rcan<Caps>,
        s: oneshot::Sender<anyhow::Result<()>>,
    },
    PutBlob {
        ticket: BlobTicket,
        name: String,
        s: oneshot::Sender<anyhow::Result<()>>,
    },
    Ping {
        req: [u8; 32],
        s: oneshot::Sender<anyhow::Result<()>>,
    },
    PutMetrics {
        encoded: String,
        session_id: Uuid,
        s: oneshot::Sender<anyhow::Result<()>>,
    },
    GetTag {
        name: String,
        s: oneshot::Sender<anyhow::Result<Option<Hash>>>,
    },
}

impl Actor {
    async fn run(mut self, enable_metrics: Option<Duration>) {
        if enable_metrics.is_some() {
            if let Err(err) = iroh_metrics::core::Core::try_init(|reg, metrics| {
                use iroh::metrics::*;
                use iroh_metrics::core::Metric;

                metrics.insert(NetReportMetrics::new(reg));
                metrics.insert(PortmapMetrics::new(reg));
                metrics.insert(MagicsockMetrics::new(reg));
            }) {
                // This is usually okay, as it just means metrics already got initialized somewhere else
                debug!("failed to initialize metrics: {:?}", err);
            }
        }
        let metrics_time = enable_metrics.unwrap_or_else(|| Duration::from_secs(60 * 60 * 24));
        let mut metrics_timer = tokio::time::interval(metrics_time);

        loop {
            tokio::select! {
                biased;
                msg = self.internal_receiver.recv() => {
                    match msg {
                        Some(server_msg) => {
                            self.handle_message(server_msg).await;
                        }
                        None => {
                            break;
                        }
                    }
                }
                _ = metrics_timer.tick(), if enable_metrics.is_some() => {
                    debug!("metrics_timer::tick()");
                    self.send_metrics().await;
                }
            }
        }

        debug!("shutting down");
    }

    async fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::Auth { rcan, s } => match self.api.auth(rcan).await {
                Ok(()) => {
                    s.send(Ok(())).ok();
                }
                Err(err) => {
                    s.send(Err(err.into())).ok();
                }
            },
            ActorMessage::PutBlob { ticket, name, s } => {
                match self.api.put_blob(name, ticket).await {
                    Ok(()) => {
                        s.send(Ok(())).ok();
                    }
                    Err(err) => {
                        s.send(Err(err.into())).ok();
                    }
                }
            }
            ActorMessage::GetTag { name, s } => match self.api.get_tag(name).await {
                Ok(maybe_hash) => {
                    s.send(Ok(maybe_hash)).ok();
                }
                Err(err) => {
                    s.send(Err(err.into())).ok();
                }
            },
            ActorMessage::PutMetrics {
                session_id,
                encoded,
                s,
            } => match self.api.put_metrics(session_id, encoded).await {
                Ok(()) => {
                    s.send(Ok(())).ok();
                }
                Err(err) => {
                    s.send(Err(err.into())).ok();
                }
            },
            ActorMessage::Ping { req, s } => match self.api.ping(req).await {
                Ok(res) => {
                    if res == req {
                        s.send(Ok(())).ok();
                    } else {
                        s.send(Err(anyhow!("unexpected pong resonse").into())).ok();
                    }
                }
                Err(err) => {
                    s.send(Err(err.into())).ok();
                }
            },
        }
    }

    async fn send_metrics(&mut self) {
        if let Some(core) = iroh_metrics::core::Core::get() {
            let dump = core.encode();

            let (s, r) = oneshot::channel();
            if let Err(err) = self
                .internal_sender
                .send(ActorMessage::PutMetrics {
                    encoded: dump,
                    session_id: self.session_id,
                    s,
                })
                .await
            {
                warn!("failed to send internal message: {:?}", err);
            }
            // spawn a task, to not block the run loop
            tokio::task::spawn(async move {
                let res = r.await;
                debug!("metrics sent: {:?}", res);
            });
        }
    }
}
