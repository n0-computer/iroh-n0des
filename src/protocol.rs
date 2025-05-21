use anyhow::Result;
use iroh::NodeId;
use iroh_blobs::{ticket::BlobTicket, Hash};
use irpc::{channel::oneshot, Service};
use irpc_derive::rpc_requests;
use rcan::Rcan;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::caps::Caps;

pub const ALPN: &[u8] = b"/iroh/n0des/1";

pub type N0desClient = irpc::Client<N0desMessage, N0desProtocol, N0desService>;

#[derive(Debug, Clone, Copy)]
pub struct N0desService;

impl Service for N0desService {}

#[rpc_requests(N0desService, message = N0desMessage)]
#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum N0desProtocol {
    #[rpc(tx=oneshot::Sender<()>)]
    Auth(Auth),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutBlob(PutBlob),
    #[rpc(tx=oneshot::Sender<RemoteResult<Option<Hash>>>)]
    GetTag(GetTag),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutTopic(PutTopic),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    DeleteTopic(DeleteTopic),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutMetrics(PutMetrics),
    #[rpc(tx=oneshot::Sender<Pong>)]
    Ping(Ping),
}

pub type RemoteResult<T> = Result<T, RemoteError>;

// #[derive(Debug, Serialize, Deserialize)]
// pub struct RemoteError(pub String);

#[derive(Serialize, Deserialize, thiserror::Error, Debug)]
pub enum RemoteError {
    #[error("Missing capability: {}", _0.to_strings().join(", "))]
    MissingCapability(Caps),
    #[error("Internal server error")]
    InternalServerError,
}

/// Authentication on first request
#[derive(Debug, Serialize, Deserialize)]
pub struct Auth {
    pub caps: Rcan<Caps>,
}

/// Request that the node fetches the given blob.
#[derive(Debug, Serialize, Deserialize)]
pub struct PutBlob {
    pub ticket: BlobTicket,
    pub name: String,
}

/// Request the name of a blob held by the node
#[derive(Debug, Serialize, Deserialize)]
pub struct GetTag {
    pub name: String,
}

pub type ProtoTopicId = [u8; 32];

/// Request that the node joins the given tossip topic
#[derive(Debug, Serialize, Deserialize)]
pub struct PutTopic {
    pub topic: ProtoTopicId,
    pub label: String,
    pub bootstrap: Vec<NodeId>,
}

/// Request that the node joins the given tossip topic
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteTopic {
    pub topic: ProtoTopicId,
}

/// Request to store the given metrics data
#[derive(Debug, Serialize, Deserialize)]
pub struct PutMetrics {
    pub encoded: String,
    pub session_id: Uuid,
}

/// Simple ping requests
#[derive(Debug, Serialize, Deserialize)]
pub struct Ping {
    pub req: [u8; 32],
}

/// Simple ping response
#[derive(Debug, Serialize, Deserialize)]
pub struct Pong {
    pub req: [u8; 32],
}
