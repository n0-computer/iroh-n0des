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
    StartSim(StartSim),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutMetrics(PutMetrics),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    EndSim(EndSim),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StartSim {
    pub session_id: Uuid,
    pub simulation_name: String,
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: DateTime,
    pub node_count: u64,
    pub round_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EndSim {
    pub session_id: Uuid,
    pub simulation_name: String,
    #[serde(with = "time::serde::rfc3339")]
    pub end_time: DateTime,
    pub result: Result<(), String>,
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
    pub logs: Option<String>,
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

    pub(crate) async fn session(
        &self,
        simulation_name: &str,
        node_count: u64,
        round_count: u64,
    ) -> Result<SimSession> {
        let start_time = DateTime::now_utc();
        self.client
            .rpc(StartSim {
                session_id: self.session_id,
                simulation_name: simulation_name.to_string(),
                start_time,
                node_count,
                round_count,
            })
            .await??;
        Ok(SimSession {
            client: self.clone(),
            simulation_name: simulation_name.to_string(),
        })
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
        logs: Option<String>,
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
                logs,
            })
            .await??;
        Ok(())
    }

    pub(crate) async fn finalize(&self, result: Result<(), String>) -> Result<()> {
        let end_time = DateTime::now_utc();
        self.client
            .client
            .rpc(EndSim {
                session_id: self.client.session_id.clone(),
                simulation_name: self.simulation_name.clone(),
                end_time,
                result,
            })
            .await??;
        Ok(())
    }
}
