use std::net::SocketAddr;

use anyhow::Result;
use iroh::{Endpoint, NodeAddr, NodeId};
use iroh_metrics::encoding::Encoder;
use irpc::{channel::oneshot, rpc_requests, util::make_insecure_client_endpoint, Service};
use irpc_iroh::IrohRemoteConnection;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime as DateTime;
use uuid::Uuid;

use super::RoundOutcome;

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
    pub node_index: usize,
    pub round: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub end_time: DateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: DateTime,
    pub duration_micros: u64,
    pub metrics: iroh_metrics::encoding::Update,
}

#[derive(Debug, Clone)]
pub struct SimClient {
    client: irpc::Client<SimMessage, SimProtocol, SimService>,
    session_id: Uuid,
}

impl SimClient {
    pub fn from_addr_str(addr: &str) -> Result<Self> {
        let addr: SocketAddr = addr.parse()?;
        let client = SimClient::new_quinn_insecure(addr)?;
        Ok(client)
    }

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

    pub(crate) fn session(&self, simulation_name: &str) -> SimSession {
        SimSession {
            client: self.clone(),
            simulation_name: simulation_name.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SimSession {
    client: SimClient,
    simulation_name: String,
}

impl SimSession {
    pub(crate) async fn put_round(
        &self,
        outcome: RoundOutcome,
        metrics_encoder: &mut Encoder,
    ) -> Result<()> {
        let update = metrics_encoder.export();
        self.client
            .client
            .rpc(PutMetrics {
                session_id: self.client.session_id.clone(),
                simulation_name: self.simulation_name.clone(),
                node_id: outcome.node_id,
                node_index: outcome.node_index,
                round: outcome.round,
                start_time: outcome.start_time,
                end_time: outcome.end_time,
                duration_micros: outcome.duration.as_micros() as u64,
                metrics: update,
            })
            .await??;
        Ok(())
    }
}
