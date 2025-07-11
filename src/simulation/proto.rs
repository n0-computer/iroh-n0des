use std::net::SocketAddr;

use anyhow::Result;
use iroh::{Endpoint, NodeAddr, NodeId};
use iroh_metrics::encoding::Update;
use irpc::{channel::oneshot, rpc_requests, util::make_insecure_client_endpoint, Service};
use irpc_iroh::IrohRemoteConnection;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime as DateTime;
use uuid::Uuid;

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

#[derive(Debug, Serialize, Deserialize)]
#[rpc_requests(SimService, message = SimMessage)]
#[allow(clippy::large_enum_variant)]
pub enum SimProtocol {
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    StartTrace(StartTrace),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    EndTrace(EndTrace),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutCheckpoint(PutCheckpoint),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    Logs(PutLogs),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    Metrics(PutMetrics),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StartTrace {
    pub trace_id: Uuid,
    pub session_id: Uuid,
    pub name: String,
    pub kind: TraceKind,
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TraceKind {
    Simulation(SimulationInfo),
    Test {},
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SimulationInfo {
    pub nodes: Vec<NodeInfo>,
    pub expected_rounds: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub node_index: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EndTrace {
    pub trace_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    pub end_time: DateTime,
    pub result: Result<(), String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PutLogs {
    pub trace_id: Uuid,
    // pub round_idx: Option<RoundIdx>,
    pub json_lines: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutMetrics {
    pub trace_id: Uuid,
    pub node_id: NodeId,
    pub checkpoint_id: Option<CheckpointId>,
    #[serde(with = "time::serde::rfc3339")]
    pub time: DateTime,
    pub metrics: iroh_metrics::encoding::Update,
}

pub type CheckpointId = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PutCheckpoint {
    pub trace_id: Uuid,
    pub checkpoint_id: CheckpointId,
    pub node_stats: Vec<NodeStats>,
    pub label: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub time: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeStats {
    pub node_id: NodeId,
    pub round_duration_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SimClient {
    client: irpc::Client<SimMessage, SimProtocol, SimService>,
    session_id: Uuid,
}

impl SimClient {
    pub fn from_addr_str(addr: &str) -> Result<Self> {
        let (addr, session_id) = match addr.split_once("/") {
            None => (addr.parse()?, Uuid::now_v7()),
            Some((addr, session_id)) => (addr.parse()?, session_id.parse()?),
        };
        let client = Self::new_quinn_insecure(addr, session_id)?;
        Ok(client)
    }

    pub fn new_quinn_insecure(remote: SocketAddr, session_id: Uuid) -> Result<Self> {
        let addr_localhost = "127.0.0.1:0".parse().unwrap();
        let endpoint = make_insecure_client_endpoint(addr_localhost)?;
        Ok(Self::connect_quinn(endpoint, remote, session_id))
    }

    pub fn connect_quinn(endpoint: quinn::Endpoint, remote: SocketAddr, session_id: Uuid) -> Self {
        let client = irpc::Client::quinn(endpoint, remote);
        Self { client, session_id }
    }

    pub async fn new_iroh(remote: NodeId, session_id: Uuid) -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_local_network().bind().await?;
        Ok(Self::connect_iroh(endpoint, remote, session_id))
    }

    pub fn connect_iroh(endpoint: Endpoint, remote: impl Into<NodeAddr>, session_id: Uuid) -> Self {
        let conn = IrohRemoteConnection::new(endpoint, remote.into(), ALPN.to_vec());
        let client = irpc::Client::boxed(conn);
        Self { client, session_id }
    }

    pub(crate) async fn start_trace(&self, name: &str, kind: TraceKind) -> Result<TraceClient> {
        let start_time = DateTime::now_utc();
        let trace_id = Uuid::now_v7();
        self.client
            .rpc(StartTrace {
                trace_id,
                session_id: self.session_id,
                name: name.to_string(),
                kind,
                start_time,
            })
            .await??;
        Ok(TraceClient {
            client: self.client.clone(),
            trace_id,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TraceClient {
    client: irpc::Client<SimMessage, SimProtocol, SimService>,
    trace_id: Uuid,
}

impl TraceClient {
    pub(crate) async fn put_checkpoint(
        &self,
        id: CheckpointId,
        label: Option<String>,
        node_stats: Vec<NodeStats>,
    ) -> Result<()> {
        let time = DateTime::now_utc();
        self.client
            .rpc(PutCheckpoint {
                trace_id: self.trace_id,
                checkpoint_id: id,
                node_stats,
                time,
                label,
            })
            .await??;
        Ok(())
    }

    pub(crate) async fn put_metrics(
        &self,
        node_id: NodeId,
        checkpoint_id: Option<CheckpointId>,
        metrics: Update,
    ) -> Result<()> {
        let time = DateTime::now_utc();
        self.client
            .rpc(PutMetrics {
                trace_id: self.trace_id,
                node_id,
                checkpoint_id,
                time,
                metrics,
            })
            .await??;
        Ok(())
    }

    pub(crate) async fn put_logs(&self, json_lines: Vec<String>) -> Result<()> {
        self.client
            .rpc(PutLogs {
                trace_id: self.trace_id,
                json_lines,
            })
            .await??;
        Ok(())
    }

    pub(crate) async fn close(&self, result: Result<(), String>) -> Result<()> {
        let end_time = DateTime::now_utc();
        self.client
            .rpc(EndTrace {
                trace_id: self.trace_id,
                end_time,
                result,
            })
            .await??;
        Ok(())
    }

    // pub(crate) async fn put_round(
    //     &self,
    //     outcome: RoundOutcome,
    //     metrics_encoder: &mut Encoder,
    //     logs: Option<String>,
    // ) -> Result<()> {
    //     let update = metrics_encoder.export();
    //     self.client
    //         .client
    //         .rpc(PutMetrics {
    //             session_id: self.client.session_id.clone(),
    //             simulation_name: self.simulation_name.clone(),
    //             node_id: outcome.node_id,
    //             node_index: outcome.node_index,
    //             round: outcome.round,
    //             start_time: outcome.start_time,
    //             end_time: outcome.end_time,
    //             duration_micros: outcome.duration.as_micros() as u64,
    //             metrics: update,
    //             logs,
    //         })
    //         .await??;
    //     Ok(())
    // }

    // pub(crate) async fn finalize(&self, result: Result<(), String>) -> Result<()> {
    //     let end_time = DateTime::now_utc();
    //     self.client
    //         .client
    //         .rpc(EndSim {
    //             session_id: self.client.session_id.clone(),
    //             simulation_name: self.simulation_name.clone(),
    //             end_time,
    //             result,
    //         })
    //         .await??;
    //     Ok(())
    // }
}
