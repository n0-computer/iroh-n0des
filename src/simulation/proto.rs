use std::net::SocketAddr;

use anyhow::Result;
use iroh::{Endpoint, NodeAddr, NodeId};
use iroh_metrics::encoding::Update;
use irpc::{Service, channel::oneshot, rpc_requests, util::make_insecure_client_endpoint};
use irpc_iroh::IrohRemoteConnection;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime as DateTime;
use tracing::debug;
use uuid::Uuid;

use super::{ENV_TRACE_SERVER, ENV_TRACE_SESSION_ID};

pub const ALPN: &[u8] = b"/iroh/n0des-sim/1";

#[derive(Debug, Clone, Copy)]
pub struct TraceService;

impl Service for TraceService {}

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
#[rpc_requests(TraceService, message = TraceMessage)]
#[allow(clippy::large_enum_variant)]
pub enum TraceProtocol {
    #[rpc(tx=oneshot::Sender<RemoteResult<Option<GetSessionResponse>>>)]
    GetSession(GetSession),
    #[rpc(tx=oneshot::Sender<RemoteResult<Uuid>>)]
    InitTrace(InitTrace),
    #[rpc(tx=oneshot::Sender<RemoteResult<Uuid>>)]
    StartTrace(StartTrace),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    EndTrace(EndTrace),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutCheckpoint(PutCheckpoint),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutLogs(PutLogs),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutMetrics(PutMetrics),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    WaitCheckpoint(WaitCheckpoint),
    #[rpc(tx=oneshot::Sender<RemoteResult<WaitStartResponse>>)]
    WaitStart(WaitStart),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetSession {
    pub session_id: Uuid,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetSessionResponse {
    pub traces: Vec<TraceDetails>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TraceDetails {
    pub trace_id: Uuid,
    pub info: TraceInfo,
    pub finished: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InitTrace {
    pub session_id: Uuid,
    pub info: TraceInfo,
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TraceInfo {
    pub name: String,
    pub expected_nodes: Option<u32>,
    pub expected_checkpoints: Option<u64>,
}

impl TraceInfo {
    pub fn new(name: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            expected_nodes: None,
            expected_checkpoints: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StartTrace {
    pub session_id: Uuid,
    pub name: String,
    pub scope_info: ScopeInfo,
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ScopeInfo {
    Integrated(Vec<NodeInfo>),
    Isolated(NodeInfo),
}

impl From<&ScopeInfo> for Scope {
    fn from(value: &ScopeInfo) -> Self {
        match value {
            ScopeInfo::Integrated(_) => Scope::Integrated,
            ScopeInfo::Isolated(info) => Scope::Isolated(info.idx),
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
pub struct EndTrace {
    pub scope: Scope,
    pub trace_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    pub end_time: DateTime,
    pub result: Result<(), String>,
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
    pub node_idx: NodeIdx,
    pub label: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub time: DateTime,
    pub result: Result<(), String>,
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

#[derive(Debug, Clone)]
pub struct TraceClient {
    client: irpc::Client<TraceMessage, TraceProtocol, TraceService>,
    session_id: Uuid,
}

impl TraceClient {
    pub fn from_env() -> Result<Option<Self>> {
        if let Ok(addr) = std::env::var(ENV_TRACE_SERVER) {
            let addr: SocketAddr = addr.parse()?;
            let session_id: Uuid = match std::env::var(ENV_TRACE_SESSION_ID) {
                Ok(id) => id.parse()?,
                Err(_) => Uuid::now_v7(),
            };
            Ok(Some(Self::connect_quinn_insecure(addr, session_id)?))
        } else {
            Ok(None)
        }
    }

    pub fn connect_quinn_insecure(remote: SocketAddr, session_id: Uuid) -> Result<Self> {
        let addr_localhost = "127.0.0.1:0".parse().unwrap();
        let endpoint = make_insecure_client_endpoint(addr_localhost)?;
        Ok(Self::connect_quinn_endpoint(endpoint, remote, session_id))
    }

    pub fn connect_quinn_endpoint(
        endpoint: quinn::Endpoint,
        remote: SocketAddr,
        session_id: Uuid,
    ) -> Self {
        let client = irpc::Client::quinn(endpoint, remote);
        Self { client, session_id }
    }

    pub async fn connect_iroh(remote: NodeId, session_id: Uuid) -> Result<Self> {
        let endpoint = Endpoint::builder().discovery_local_network().bind().await?;
        Ok(Self::connect_iroh_endpoint(endpoint, remote, session_id))
    }

    pub fn connect_iroh_endpoint(
        endpoint: Endpoint,
        remote: impl Into<NodeAddr>,
        session_id: Uuid,
    ) -> Self {
        let conn = IrohRemoteConnection::new(endpoint, remote.into(), ALPN.to_vec());
        let client = irpc::Client::boxed(conn);
        Self { client, session_id }
    }

    #[cfg(test)]
    pub(crate) async fn init_and_start_trace(&self, name: &str) -> Result<ActiveTrace> {
        let trace_info = TraceInfo::new(name);
        self.init_trace(trace_info).await?;
        self.start_trace(name.to_string(), ScopeInfo::Integrated(Default::default()))
            .await
    }

    pub async fn init_trace(&self, info: TraceInfo) -> Result<()> {
        debug!("init trace {info:?}");
        let trace_id = self
            .client
            .rpc(InitTrace {
                info,
                session_id: self.session_id,
                start_time: DateTime::now_utc(),
            })
            .await??;
        debug!("init trace {trace_id}: OK");
        Ok(())
    }

    pub async fn status(&self, session_id: Uuid) -> Result<Option<GetSessionResponse>> {
        let res = self.client.rpc(GetSession { session_id }).await??;
        Ok(res)
    }

    pub async fn start_trace(&self, name: String, scope_info: ScopeInfo) -> Result<ActiveTrace> {
        let start_time = DateTime::now_utc();
        debug!("start trace {name}");
        let scope = Scope::from(&scope_info);
        let trace_id = self
            .client
            .rpc(StartTrace {
                session_id: self.session_id,
                name,
                scope_info,
                start_time,
            })
            .await??;
        debug!("start trace {trace_id}: OK");
        Ok(ActiveTrace {
            client: self.client.clone(),
            trace_id,
            scope,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ActiveTrace {
    client: irpc::Client<TraceMessage, TraceProtocol, TraceService>,
    trace_id: Uuid,
    scope: Scope,
}

impl ActiveTrace {
    pub(crate) async fn put_checkpoint(
        &self,
        id: CheckpointId,
        node_idx: NodeIdx,
        label: Option<String>,
        result: Result<(), String>,
    ) -> Result<()> {
        let time = DateTime::now_utc();
        self.client
            .rpc(PutCheckpoint {
                trace_id: self.trace_id,
                checkpoint_id: id,
                node_idx,
                time,
                label,
                result,
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
