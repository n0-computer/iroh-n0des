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

    #[rpc(tx=oneshot::Sender<RemoteResult<()>>)]
    TicketPublish(PublishTicket),
    #[rpc(tx=oneshot::Sender<RemoteResult<bool>>)]
    TicketUnpublish(UnpublishTicket),
    #[rpc(tx=oneshot::Sender<RemoteResult<Option<TicketData>>>)]
    TicketGet(GetTicket),
    #[rpc(tx=oneshot::Sender<RemoteResult<Vec<TicketData>>>)]
    TicketList(ListTickets),
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
    pub req_id: [u8; 16],
}

/// Simple ping response
#[derive(Debug, Serialize, Deserialize)]
pub struct Pong {
    pub req_id: [u8; 16],
}

// dummy type to make irpc happy
#[cfg(not(feature = "tickets"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct PublishTicket;

// dummy type to make irpc happy
#[cfg(not(feature = "tickets"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct GetTicket;

// dummy type to make irpc happy
#[cfg(not(feature = "tickets"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct UnpublishTicket;

// dummy type to make irpc happy
#[cfg(not(feature = "tickets"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTickets;

// dummy type to make irpc happy
#[cfg(not(feature = "tickets"))]
#[derive(Debug, Serialize, Deserialize)]
pub struct TicketData;

/// Publishing a ticket allows n0des to act as a central hub to ferry tickets
/// between endpoints.
#[cfg(feature = "tickets")]
#[derive(Debug, Serialize, Deserialize)]
pub struct PublishTicket {
    pub req_id: [u8; 16],
    pub name: String,
    pub ticket_kind: String,
    pub ticket: Vec<u8>,
}

/// wire-level request to remove a ticket. Useful for undos, and any situation
/// where the uptime of the endpoint outlasts the utility of the ticket.
#[cfg(feature = "tickets")]
#[derive(Debug, Serialize, Deserialize)]
pub struct UnpublishTicket {
    pub req_id: [u8; 16],
    pub name: String,
    pub ticket_kind: String,
}

/// wire-level request get a ticket by name.
#[cfg(feature = "tickets")]
#[derive(Debug, Serialize, Deserialize)]
pub struct GetTicket {
    pub req_id: [u8; 16],
    pub name: String,
    pub ticket_kind: String,
}

/// Wire format for requesting a list of tickets
#[cfg(feature = "tickets")]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTickets {
    pub req_id: [u8; 16],
    pub ticket_kind: String,
    pub offset: u32,
    pub limit: u32,
}

/// Signals are opaque data that n0des can ferry between endpoints
#[cfg(feature = "tickets")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TicketData {
    pub name: String,
    pub ticket_kind: String,
    pub ticket_bytes: Vec<u8>,
}

#[cfg(feature = "tickets")]
impl From<PublishTicket> for TicketData {
    fn from(msg: PublishTicket) -> Self {
        TicketData {
            name: msg.name,
            ticket_kind: msg.ticket_kind,
            ticket_bytes: msg.ticket,
        }
    }
}
