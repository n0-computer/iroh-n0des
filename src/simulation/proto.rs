use std::net::SocketAddr;

use anyhow::Result;
use iroh::{Endpoint, NodeAddr, NodeId};
use iroh_metrics::encoding::Update;
use irpc::{channel::oneshot, rpc_requests, util::make_insecure_client_endpoint, Service};
use irpc_iroh::IrohRemoteConnection;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime as DateTime;
use tracing::debug;
use uuid::Uuid;

use super::{ENV_TRACE_SERVER, ENV_TRACE_SESSION_ID};

pub const ALPN: &[u8] = b"/iroh/n0des-sim/1";

#[derive(Debug, Clone, Copy)]
pub struct SimService;

impl Service for SimService {}

pub type RemoteResult<T> = Result<T, RemoteError>;

#[derive(Serialize, Deserialize, thiserror::Error, Debug)]
pub enum RemoteError {
    #[error("{0}")]
    Other(String),
    #[error("Trace not found ({0})")]
    TraceNotFound(Uuid),
}

impl RemoteError {
    pub fn other(s: impl ToString) -> Self {
        Self::Other(s.to_string())
    }
}

impl From<anyhow::Error> for RemoteError {
    fn from(value: anyhow::Error) -> Self {
        Self::other(value)
    }
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
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    WaitCheckpoint(WaitCheckpoint),
    #[rpc(tx=oneshot::Sender<RemoteResult<WaitStartResponse>>)]
    WaitStart(WaitStart),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WaitCheckpoint {
    pub trace_id: Uuid,
    pub checkpoint_id: CheckpointId,
    pub required_count: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WaitStart {
    pub trace_id: Uuid,
    pub info: NodeInfoWithAddr,
    pub required_count: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WaitStartResponse {
    pub infos: Vec<NodeInfoWithAddr>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StartTrace {
    pub trace_id: Uuid,
    pub session_id: Uuid,
    pub name: String,
    pub scope: ScopeInfo,
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ScopeInfo {
    Integrated(Vec<NodeInfo>),
    Isolated { node: NodeInfo, total_count: u32 },
}

impl From<&ScopeInfo> for Scope {
    fn from(value: &ScopeInfo) -> Self {
        match value {
            ScopeInfo::Integrated(_) => Scope::Integrated,
            ScopeInfo::Isolated { node, .. } => Scope::Isolated(node.idx),
        }
    }
}

pub type NodeIdx = u32;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum Scope {
    Integrated,
    Isolated(NodeIdx),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    pub idx: u32,
    pub label: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeInfoWithAddr {
    pub info: NodeInfo,
    pub addr: NodeAddr,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EndTrace {
    pub trace_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    pub end_time: DateTime,
    pub scope: Scope,
    pub result: Result<(), String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PutLogs {
    pub trace_id: Uuid,
    pub scope: Scope,
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
    pub scope: Scope,
    pub label: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub time: DateTime,
}

#[derive(Debug, Clone)]
pub struct SimClient {
    client: irpc::Client<SimMessage, SimProtocol, SimService>,
    session_id: Uuid,
}

impl SimClient {
    pub fn from_env() -> Result<Option<Self>> {
        if let Ok(addr) = std::env::var(ENV_TRACE_SERVER) {
            let addr: SocketAddr = addr.parse()?;
            let session_id: Uuid = match std::env::var(ENV_TRACE_SESSION_ID) {
                Ok(id) => id.parse()?,
                Err(_) => Uuid::now_v7(),
            };
            Ok(Some(Self::new_quinn_insecure(addr, session_id)?))
        } else {
            Ok(None)
        }
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

    #[cfg(test)]
    pub(crate) async fn start_standalone(&self, name: &str) -> Result<TraceClient> {
        self.start_trace(name, ScopeInfo::Integrated(Default::default()), None)
            .await
    }

    pub(crate) async fn start_trace(
        &self,
        name: &str,
        scope_info: ScopeInfo,
        trace_id: Option<Uuid>,
    ) -> Result<TraceClient> {
        let start_time = DateTime::now_utc();
        let trace_id = trace_id.unwrap_or_else(Uuid::now_v7);
        let scope = Scope::from(&scope_info);
        debug!("start trace {trace_id}");
        self.client
            .rpc(StartTrace {
                trace_id,
                session_id: self.session_id,
                name: name.to_string(),
                scope: scope_info,
                start_time,
            })
            .await??;
        debug!("start trace {trace_id}: OK");
        Ok(TraceClient {
            client: self.client.clone(),
            trace_id,
            scope,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TraceClient {
    client: irpc::Client<SimMessage, SimProtocol, SimService>,
    trace_id: Uuid,
    scope: Scope,
}

impl TraceClient {
    pub(crate) async fn put_checkpoint(
        &self,
        id: CheckpointId,
        label: Option<String>,
    ) -> Result<()> {
        let time = DateTime::now_utc();
        self.client
            .rpc(PutCheckpoint {
                trace_id: self.trace_id,
                checkpoint_id: id,
                scope: self.scope,
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
                scope: self.scope,
                trace_id: self.trace_id,
                json_lines,
            })
            .await??;
        Ok(())
    }

    pub(crate) async fn wait_start(
        &self,
        info: NodeInfoWithAddr,
        required_count: u32,
    ) -> Result<Vec<NodeInfoWithAddr>> {
        debug!("waiting for {required_count} nodes to confirm start...");
        let res = self
            .client
            .rpc(WaitStart {
                info,
                required_count,
                trace_id: self.trace_id,
            })
            .await??;
        debug!("start confirmed");
        Ok(res.infos)
    }

    pub(crate) async fn wait_checkpoint(
        &self,
        checkpoint_id: CheckpointId,
        required_count: u32,
    ) -> Result<()> {
        debug!("waiting for {required_count} nodes to confirm checkpoint {checkpoint_id}...");
        let res = self
            .client
            .rpc(WaitCheckpoint {
                checkpoint_id,
                required_count,
                trace_id: self.trace_id,
            })
            .await;
        res??;
        debug!("checkpoint {checkpoint_id} confirmed");
        Ok(())
    }

    pub(crate) async fn end_trace(&self, result: Result<(), String>) -> Result<()> {
        let end_time = DateTime::now_utc();
        self.client
            .rpc(EndTrace {
                trace_id: self.trace_id,
                scope: self.scope,
                end_time,
                result,
            })
            .await??;
        Ok(())
    }
}
