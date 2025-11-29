use anyhow::Result;
use irpc::{channel::oneshot, rpc_requests};
use rcan::Rcan;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::caps::Caps;

pub const ALPN: &[u8] = b"/iroh/n0des/1";

pub type N0desClient = irpc::Client<N0desProtocol>;

#[rpc_requests(message = N0desMessage)]
#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum N0desProtocol {
    #[rpc(tx=oneshot::Sender<()>)]
    Auth(Auth),
    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    PutMetrics(PutMetrics),
    #[rpc(tx=oneshot::Sender<Pong>)]
    Ping(Ping),
    #[rpc(tx=oneshot::Sender<()>)]
    CreateSignal(CreateSignal),
    #[rpc(tx=oneshot::Sender<Vec<Signal>>)]
    GetSignals(GetSignals),
}

pub type RemoteResult<T> = Result<T, RemoteError>;

#[derive(Clone, Serialize, Deserialize, thiserror::Error, Debug)]
pub enum RemoteError {
    #[error("Missing capability: {}", _0.to_strings().join(", "))]
    MissingCapability(Caps),
    #[error("Unauthorized: {}", _0)]
    AuthError(String),
    #[error("Internal server error")]
    InternalServerError,
}

/// Authentication on first request
#[derive(Debug, Serialize, Deserialize)]
pub struct Auth {
    pub caps: Rcan<Caps>,
}

/// Request to store the given metrics data
#[derive(Debug, Serialize, Deserialize)]
pub struct PutMetrics {
    pub session_id: Uuid,
    pub update: iroh_metrics::encoding::Update,
}

/// Simple ping requests
#[derive(Debug, Serialize, Deserialize)]
pub struct Ping {
    pub req: [u8; 16],
}

/// Simple ping response
#[derive(Debug, Serialize, Deserialize)]
pub struct Pong {
    pub req: [u8; 16],
}

/// Signals are opaque data that n0des can ferry between endpoints
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateSignal {
    pub ttl: u64,
    pub name: String,
    pub value: Vec<u8>,
}

/// Simple ping response
#[derive(Debug, Serialize, Deserialize)]
pub struct GetSignals {
    pub req: [u8; 32],
}

/// Signals are opaque data that n0des can ferry between endpoints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub ttl: u64,
    pub name: String,
    pub value: Vec<u8>,
}

impl From<CreateSignal> for Signal {
    fn from(msg: CreateSignal) -> Self {
        Signal {
            ttl: msg.ttl,
            name: msg.name,
            value: msg.value,
        }
    }
}
