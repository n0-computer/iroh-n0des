use std::{collections::BTreeMap, net::SocketAddr};

use anyhow::Result;
use bytes::Bytes;
use iroh_metrics::encoding::Update;
use irpc::{WithChannels, channel::oneshot, rpc_requests, util::make_insecure_client_endpoint};
#[cfg(feature = "iroh_main")]
use irpc_iroh::IrohRemoteConnection;
use n0_future::{IterExt, StreamExt};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime as DateTime;
use tracing::debug;
use uuid::Uuid;

use super::{ENV_TRACE_SERVER, ENV_TRACE_SESSION_ID};
use crate::iroh::{NodeAddr, NodeId};

pub const ALPN: &[u8] = b"/iroh/n0des-sim/1";

pub type RemoteResult<T> = Result<T, RemoteError>;

#[derive(Serialize, Deserialize, thiserror::Error, Debug)]
pub enum RemoteError {
    #[error("{0}")]
    Other(String),
    #[error("Trace not found")]
    TraceNotFound,
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
#[rpc_requests(message = TraceMessage)]
#[allow(clippy::large_enum_variant)]
pub enum TraceProtocol {
    #[rpc(tx=oneshot::Sender<RemoteResult<Option<GetSessionResponse>>>)]
    GetSession(GetSession),
    #[rpc(tx=oneshot::Sender<RemoteResult<Uuid>>)]
    InitTrace(InitTrace),
    #[rpc(tx=oneshot::Sender<RemoteResult<GetTraceResponse>>)]
    GetTrace(GetTrace),
    #[rpc(tx=oneshot::Sender<RemoteResult<StartNodeResponse>>)]
    StartNode(StartNode),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    EndNode(EndNode),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    CloseTrace(CloseTrace),
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
    pub setup_data: Option<Bytes>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CloseTrace {
    pub trace_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    pub end_time: DateTime,
    pub result: Result<(), String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TraceInfo {
    pub name: String,
    pub node_count: u32,
    pub expected_checkpoints: Option<u64>,
}

impl TraceInfo {
    pub fn new(name: impl ToString, node_count: u32) -> Self {
        Self {
            name: name.to_string(),
            node_count,
            expected_checkpoints: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetTrace {
    pub session_id: Uuid,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetTraceResponse {
    pub trace_id: Uuid,
    pub info: TraceInfo,
    pub setup_data: Option<Bytes>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StartNode {
    pub trace_id: Uuid,
    pub node_info: NodeInfo,
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StartNodeResponse {
    // pub trace_id: Uuid,
    // pub setup_data: Option<Bytes>,
}

pub type NodeIdx = u32;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum Scope {
    Integrated,
    Isolated(NodeIdx),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EndNode {
    pub trace_id: Uuid,
    pub node_idx: NodeIdx,
    #[serde(with = "time::serde::rfc3339")]
    pub end_time: DateTime,
    pub result: Result<(), String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeInfo {
    pub idx: NodeIdx,
    pub node_id: Option<NodeId>,
    pub label: Option<String>,
}

impl NodeInfo {
    pub fn new_empty(idx: NodeIdx) -> Self {
        Self {
            idx,
            node_id: None,
            label: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NodeInfoWithAddr {
    pub info: NodeInfo,
    pub addr: Option<NodeAddr>,
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WaitStart {
    pub trace_id: Uuid,
    pub info: NodeInfoWithAddr,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WaitStartResponse {
    pub infos: Vec<NodeInfoWithAddr>,
}

#[derive(Debug, Clone)]
pub struct TraceClient {
    client: irpc::Client<TraceProtocol>,
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

    pub fn from_env_or_local() -> Result<Self> {
        Ok(Self::from_env()?.unwrap_or_else(Self::local))
    }

    pub fn local() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        LocalActor::spawn(rx);
        let session_id = Uuid::now_v7();
        Self {
            client: irpc::Client::from(tx),
            session_id,
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

    #[cfg(feature = "iroh_main")]
    pub async fn connect_iroh(remote: iroh::NodeId, session_id: Uuid) -> Result<Self> {
        let endpoint = iroh::Endpoint::builder().bind().await?;
        Ok(Self::connect_iroh_endpoint(endpoint, remote, session_id))
    }

    #[cfg(feature = "iroh_main")]
    pub fn connect_iroh_endpoint(
        endpoint: iroh::Endpoint,
        remote: impl Into<iroh::NodeAddr>,
        session_id: Uuid,
    ) -> Self {
        let conn = IrohRemoteConnection::new(endpoint, remote.into(), ALPN.to_vec());
        let client = irpc::Client::boxed(conn);
        Self { client, session_id }
    }

    pub async fn init_and_start_trace(&self, name: &str) -> Result<ActiveTrace> {
        let trace_info = TraceInfo::new(name, 1);
        let trace_id = self.init_trace(trace_info, None).await?;
        let node_info = NodeInfo::new_empty(0);
        let client = self.start_node(trace_id, node_info).await?;
        Ok(client)
    }

    pub async fn init_trace(&self, info: TraceInfo, setup_data: Option<Bytes>) -> Result<Uuid> {
        debug!("init trace {info:?}");
        let trace_id = self
            .client
            .rpc(InitTrace {
                info,
                session_id: self.session_id,
                start_time: DateTime::now_utc(),
                setup_data,
            })
            .await??;
        debug!("init trace {trace_id}: OK");
        Ok(trace_id)
    }

    pub async fn get_trace(&self, name: String) -> Result<GetTraceResponse> {
        debug!("get trace {name}");
        let data = self
            .client
            .rpc(GetTrace {
                session_id: self.session_id,
                name,
            })
            .await??;
        debug!(?data, "get trace: OK");
        Ok(data)
    }

    pub async fn close_trace(&self, trace_id: Uuid, result: Result<(), String>) -> Result<()> {
        let end_time = DateTime::now_utc();
        debug!(%trace_id, ?result, "close trace");
        self.client
            .rpc(CloseTrace {
                trace_id,
                end_time,
                result,
            })
            .await??;
        debug!(%trace_id, "close trace: OK");
        Ok(())
    }

    pub async fn get_session(&self, session_id: Uuid) -> Result<Option<GetSessionResponse>> {
        let res = self.client.rpc(GetSession { session_id }).await??;
        Ok(res)
    }

    pub async fn put_logs(
        &self,
        trace_id: Uuid,
        scope: Scope,
        json_lines: Vec<String>,
    ) -> Result<()> {
        self.client
            .rpc(PutLogs {
                scope,
                trace_id,
                json_lines,
            })
            .await??;
        Ok(())
    }

    pub async fn start_node(&self, trace_id: Uuid, node_info: NodeInfo) -> Result<ActiveTrace> {
        let start_time = DateTime::now_utc();
        let node_idx = node_info.idx;
        debug!(%trace_id, node_idx, "start node");
        let res = self
            .client
            .rpc(StartNode {
                trace_id,
                node_info,
                start_time,
            })
            .await??;
        let StartNodeResponse {} = res;
        debug!(%trace_id, node_idx, "start node: OK");
        Ok(ActiveTrace {
            client: self.client.clone(),
            trace_id,
            node_idx,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ActiveTrace {
    client: irpc::Client<TraceProtocol>,
    trace_id: Uuid,
    node_idx: u32,
}

impl ActiveTrace {
    pub async fn put_checkpoint(
        &self,
        id: CheckpointId,
        label: Option<String>,
        result: Result<(), String>,
    ) -> Result<()> {
        debug!(id, "put checkpoint");
        let time = DateTime::now_utc();
        self.client
            .rpc(PutCheckpoint {
                trace_id: self.trace_id,
                checkpoint_id: id,
                node_idx: self.node_idx,
                time,
                label,
                result,
            })
            .await??;
        Ok(())
    }

    pub async fn put_metrics(
        &self,
        node_id: NodeId,
        checkpoint_id: Option<CheckpointId>,
        metrics: Update,
    ) -> Result<()> {
        let time = DateTime::now_utc();
        debug!(count = metrics.values.items.len(), "put metrics");
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

    pub async fn put_logs(&self, json_lines: Vec<String>) -> Result<()> {
        self.client
            .rpc(PutLogs {
                scope: Scope::Isolated(self.node_idx),
                trace_id: self.trace_id,
                json_lines,
            })
            .await??;
        Ok(())
    }

    pub async fn wait_start(&self, info: NodeInfoWithAddr) -> Result<Vec<NodeInfoWithAddr>> {
        debug!("waiting for start...");
        let res = self
            .client
            .rpc(WaitStart {
                info,
                trace_id: self.trace_id,
            })
            .await??;
        debug!("start confirmed");
        Ok(res.infos)
    }

    pub async fn wait_checkpoint(&self, checkpoint_id: CheckpointId) -> Result<()> {
        debug!(?checkpoint_id, "waiting for checkpoint...");
        let res = self
            .client
            .rpc(WaitCheckpoint {
                checkpoint_id,
                trace_id: self.trace_id,
            })
            .await;
        res??;
        debug!(?checkpoint_id, "checkpoint confirmed");
        Ok(())
    }

    pub async fn end(&self, result: Result<(), String>) -> Result<()> {
        debug!("end node");
        let end_time = DateTime::now_utc();
        self.client
            .rpc(EndNode {
                trace_id: self.trace_id,
                node_idx: self.node_idx,
                end_time,
                result,
            })
            .await??;
        Ok(())
    }
}

#[derive(Default)]
struct LocalActor {
    traces_by_name: BTreeMap<String, Uuid>,
    traces: BTreeMap<Uuid, TraceState>,
}

struct TraceState {
    init: InitTrace,
    nodes: Vec<NodeInfoWithAddr>,
    barrier_start: Vec<oneshot::Sender<RemoteResult<WaitStartResponse>>>,
    barrier_checkpoint: BTreeMap<CheckpointId, Vec<oneshot::Sender<RemoteResult<()>>>>,
}

impl TraceState {
    fn node_count(&self) -> usize {
        self.init.info.node_count as usize
    }
}
impl LocalActor {
    pub fn spawn(rx: tokio::sync::mpsc::Receiver<TraceMessage>) {
        let actor = Self::default();
        tokio::task::spawn(actor.run(rx));
    }

    pub async fn run(mut self, mut rx: tokio::sync::mpsc::Receiver<TraceMessage>) {
        while let Some(message) = rx.recv().await {
            self.handle_message(message).await;
        }
    }
    async fn handle_message(&mut self, message: TraceMessage) {
        match message {
            TraceMessage::GetSession(_msg) => {}
            TraceMessage::InitTrace(msg) => {
                let WithChannels { inner, tx, .. } = msg;
                if self.traces_by_name.contains_key(&inner.info.name) {
                    return send_err(tx, RemoteError::other("Trace already initialized")).await;
                }
                let uuid = Uuid::now_v7();
                self.traces_by_name.insert(inner.info.name.clone(), uuid);
                self.traces.insert(
                    uuid,
                    TraceState {
                        init: inner,
                        nodes: Default::default(),
                        barrier_start: Default::default(),
                        barrier_checkpoint: Default::default(),
                    },
                );
                tx.send(Ok(uuid)).await.ok();
            }
            TraceMessage::GetTrace(msg) => {
                let WithChannels { inner, tx, .. } = msg;
                let GetTrace {
                    session_id: _,
                    name,
                } = inner;
                let Some((trace_id, info)) = self
                    .traces_by_name
                    .get(&name)
                    .and_then(|trace_id| self.traces.get_key_value(trace_id))
                else {
                    return send_err(tx, RemoteError::other("Trace not initialized")).await;
                };
                tx.send(Ok(GetTraceResponse {
                    trace_id: *trace_id,
                    info: info.init.info.clone(),
                    setup_data: info.init.setup_data.clone(),
                }))
                .await
                .ok();
            }
            TraceMessage::StartNode(msg) => {
                let WithChannels { inner, tx, .. } = msg;
                if self.traces.contains_key(&inner.trace_id) {
                    tx.send(Ok(StartNodeResponse {})).await.ok();
                } else {
                    send_err(tx, RemoteError::other("Trace not initialized")).await;
                }
            }
            TraceMessage::EndNode(msg) => {
                // noop
                msg.tx.send(Ok(())).await.ok();
            }
            TraceMessage::CloseTrace(msg) => {
                // noop
                msg.tx.send(Ok(())).await.ok();
            }
            TraceMessage::PutCheckpoint(msg) => {
                // noop
                msg.tx.send(Ok(())).await.ok();
            }
            TraceMessage::PutLogs(msg) => {
                // noop
                msg.tx.send(Ok(())).await.ok();
            }
            TraceMessage::PutMetrics(msg) => {
                // noop
                msg.tx.send(Ok(())).await.ok();
            }
            TraceMessage::WaitCheckpoint(msg) => {
                let WithChannels { inner, tx, .. } = msg;
                let Some(trace) = self.traces.get_mut(&inner.trace_id) else {
                    return send_err(tx, RemoteError::TraceNotFound).await;
                };
                let node_count = trace.node_count();
                let barrier = trace
                    .barrier_checkpoint
                    .entry(inner.checkpoint_id)
                    .or_default();
                barrier.push(tx);
                debug!(trace_id=%inner.trace_id, checkpoint=inner.checkpoint_id, count=barrier.len(), total=node_count, "wait checkpoint");
                if barrier.len() == node_count {
                    debug!(trace_id=%inner.trace_id, checkpoint=inner.checkpoint_id, "release");
                    barrier
                        .drain(..)
                        .map(|tx| tx.send(Ok(())))
                        .into_unordered_stream()
                        .count()
                        .await;
                }
            }
            TraceMessage::WaitStart(msg) => {
                let WithChannels { inner, tx, .. } = msg;
                let Some(trace) = self.traces.get_mut(&inner.trace_id) else {
                    return send_err(tx, RemoteError::TraceNotFound).await;
                };
                trace.nodes.push(inner.info);
                trace.barrier_start.push(tx);
                if trace.barrier_start.len() == trace.init.info.node_count as usize {
                    let data = WaitStartResponse {
                        infos: trace.nodes.clone(),
                    };
                    for tx in trace.barrier_start.drain(..) {
                        tx.send(Ok(data.clone())).await.ok();
                    }
                }
            }
        }
    }
}

async fn send_err<T>(tx: oneshot::Sender<RemoteResult<T>>, err: RemoteError) {
    tx.send(Err(err)).await.ok();
}
