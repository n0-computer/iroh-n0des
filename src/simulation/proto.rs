use std::net::SocketAddr;

use anyhow::Result;
use iroh::{Endpoint, NodeAddr, NodeId};
use irpc::{channel::oneshot, rpc_requests, util::make_insecure_client_endpoint, Service};
use irpc_iroh::IrohRemoteConnection;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime as DateTime;
use uuid::Uuid;

use crate::N0de;

use super::{Context, SimNode};

pub const ALPN: &[u8] = b"/iroh/n0des-sim/1";

#[derive(Debug, Clone, Copy)]
pub struct SimService;

impl Service for SimService {}

pub type RemoteResult<T> = Result<T, RemoteError>;

#[derive(Serialize, Deserialize, thiserror::Error, Debug)]
pub enum RemoteError {
    #[error("{0}")]
    Other(String),
}

#[rpc_requests(SimService, message = SimMessage)]
#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum SimProtocol {
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutMetrics(PutMetrics),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutMetrics {
    pub session_id: Uuid,
    pub simulation_name: String,
    pub node_id: NodeId,
    pub round: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: DateTime,
    pub metrics: iroh_metrics::encoding::Update,
}

#[derive(Debug, Clone)]
pub struct SimClient {
    client: irpc::Client<SimMessage, SimProtocol, SimService>,
    session_id: Uuid,
}

impl SimClient {
    pub fn new_quinn_insecure(remote: SocketAddr) -> Result<Self> {
        let addr_localhost = "127.0.0.1:0".parse().unwrap();
        let endpoint = make_insecure_client_endpoint(addr_localhost)?;
        Ok(Self::connect_quinn(endpoint, remote))
    }

    pub fn connect_quinn(endpoint: quinn::Endpoint, remote: SocketAddr) -> Self {
        let client = irpc::Client::quinn(endpoint, remote);
        let session_id = Uuid::new_v4();
        Self { client, session_id }
    }

    pub async fn new_iroh(remote: NodeId) -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_local_network().bind().await?;
        Ok(Self::connect_iroh(endpoint, remote))
    }

    pub fn connect_iroh(endpoint: Endpoint, remote: impl Into<NodeAddr>) -> Self {
        let conn = IrohRemoteConnection::new(endpoint, remote.into(), ALPN.to_vec());
        let client = irpc::Client::boxed(conn);
        let session_id = Uuid::new_v4();
        Self { client, session_id }
    }

    pub(crate) fn session(&self, simulation_name: String) -> SimSession {
        SimSession {
            client: self.clone(),
            simulation_name,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SimSession {
    client: SimClient,
    simulation_name: String,
}

impl SimSession {
    pub(crate) async fn put_metrics<N: N0de>(
        &self,
        context: &Context,
        node: &mut SimNode<N>,
    ) -> Result<()> {
        let timestamp = DateTime::now_utc();
        let update = node.encoder.export();
        self.client
            .client
            .rpc(PutMetrics {
                session_id: self.client.session_id.clone(),
                simulation_name: self.simulation_name.clone(),
                node_id: node.endpoint.node_id(),
                round: context.round,
                timestamp,
                metrics: update,
            })
            .await??;
        Ok(())
    }
}
