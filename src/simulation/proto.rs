use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use iroh_metrics::encoding::Update;
use irpc::{WithChannels, channel::oneshot, rpc_requests, util::make_insecure_client_endpoint};
#[cfg(feature = "iroh_main")]
use irpc_iroh::IrohRemoteConnection;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime as DateTime;
use tokio::sync::Barrier;
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
#[rpc_requests(message = TraceMessage)]
#[allow(clippy::large_enum_variant)]
pub enum TraceProtocol {
    #[rpc(tx=oneshot::Sender<RemoteResult<Option<GetSessionResponse>>>)]
    GetSession(GetSession),
    #[rpc(tx=oneshot::Sender<RemoteResult<Uuid>>)]
    InitTrace(InitTrace),
    #[rpc(tx=oneshot::Sender<RemoteResult<StartTraceResponse>>)]
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
    pub user_data: Option<Bytes>,
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
pub struct StartTrace {
    pub session_id: Uuid,
    pub name: String,
    pub scope_info: ScopeInfo,
    #[serde(with = "time::serde::rfc3339")]
    pub start_time: DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StartTraceResponse {
    pub trace_id: Uuid,
    pub user_data: Option<Bytes>,
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
    pub node_id: NodeId,
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
        self.init_trace(trace_info, None).await?;
        let (client, _data) = self
            .start_trace(name.to_string(), ScopeInfo::Integrated(Default::default()))
            .await?;
        Ok(client)
    }

    pub async fn init_trace(&self, info: TraceInfo, user_data: Option<Bytes>) -> Result<()> {
        debug!("init trace {info:?}");
        let trace_id = self
            .client
            .rpc(InitTrace {
                info,
                session_id: self.session_id,
                start_time: DateTime::now_utc(),
                user_data,
            })
            .await??;
        debug!("init trace {trace_id}: OK");
        Ok(())
    }

    pub async fn status(&self, session_id: Uuid) -> Result<Option<GetSessionResponse>> {
        let res = self.client.rpc(GetSession { session_id }).await??;
        Ok(res)
    }

    pub async fn start_trace(
        &self,
        name: String,
        scope_info: ScopeInfo,
    ) -> Result<(ActiveTrace, Option<Bytes>)> {
        let start_time = DateTime::now_utc();
        debug!("start trace {name}");
        let scope = Scope::from(&scope_info);
        let res = self
            .client
            .rpc(StartTrace {
                session_id: self.session_id,
                name,
                scope_info,
                start_time,
            })
            .await??;
        let StartTraceResponse {
            trace_id,
            user_data,
        } = res;
        debug!("start trace {trace_id}: OK");
        Ok((
            ActiveTrace {
                client: self.client.clone(),
                trace_id,
                scope,
            },
            user_data,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct ActiveTrace {
    client: irpc::Client<TraceProtocol>,
    trace_id: Uuid,
    scope: Scope,
}

impl ActiveTrace {
    pub async fn put_checkpoint(
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

    pub async fn put_metrics(
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

    pub async fn put_logs(&self, json_lines: Vec<String>) -> Result<()> {
        self.client
            .rpc(PutLogs {
                scope: self.scope,
                trace_id: self.trace_id,
                json_lines,
            })
            .await??;
        Ok(())
    }

    pub async fn wait_start(&self, info: NodeInfoWithAddr) -> Result<Vec<NodeInfoWithAddr>> {
        debug!("waiting for all nodes to confirm start...");
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
        debug!("waiting for all nodes to confirm checkpoint {checkpoint_id}...");
        let res = self
            .client
            .rpc(WaitCheckpoint {
                checkpoint_id,
                trace_id: self.trace_id,
            })
            .await;
        res??;
        debug!("checkpoint {checkpoint_id} confirmed");
        Ok(())
    }

    pub async fn end_trace(&self, result: Result<(), String>) -> Result<()> {
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

#[derive(Default)]
struct LocalActor {
    traces_by_name: BTreeMap<String, Uuid>,
    traces: BTreeMap<Uuid, TraceState>,
}

struct TraceState {
    init: InitTrace,
    nodes: Vec<NodeInfoWithAddr>,
    barrier_start: Vec<oneshot::Sender<RemoteResult<WaitStartResponse>>>,
    barrier_checkpoint: BTreeMap<CheckpointId, Arc<Barrier>>,
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
            TraceMessage::StartTrace(msg) => {
                let WithChannels { inner, tx, .. } = msg;
                let Some((trace_id, info)) = self
                    .traces_by_name
                    .get(&inner.name)
                    .and_then(|trace_id| self.traces.get_key_value(trace_id))
                else {
                    return send_err(tx, RemoteError::other("Trace not initialized")).await;
                };
                tx.send(Ok(StartTraceResponse {
                    trace_id: *trace_id,
                    user_data: info.init.user_data.clone(),
                }))
                .await
                .ok();
            }
            TraceMessage::EndTrace(msg) => {
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
                    return send_err(tx, RemoteError::TraceNotFound(inner.trace_id)).await;
                };
                let barrier = trace
                    .barrier_checkpoint
                    .entry(inner.checkpoint_id)
                    .or_insert_with(|| Arc::new(Barrier::new(trace.init.info.node_count as usize)))
                    .clone();
                tokio::task::spawn(async move {
                    barrier.wait().await;
                    tx.send(Ok(())).await.ok();
                });
            }
            TraceMessage::WaitStart(msg) => {
                let WithChannels { inner, tx, .. } = msg;
                let Some(trace) = self.traces.get_mut(&inner.trace_id) else {
                    return send_err(tx, RemoteError::TraceNotFound(inner.trace_id)).await;
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
