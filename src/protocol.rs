use anyhow::Result;
use iroh::Endpoint;
use iroh_blobs::{ticket::BlobTicket, Hash};
use irpc::{channel::oneshot, Client, Service};
use irpc_derive::rpc_requests;
use irpc_iroh::IrohRemoteConnection;
use rcan::Rcan;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::caps::Caps;

#[derive(Debug, Clone, Copy)]
struct N0desService;

impl Service for N0desService {}

#[derive(Debug, Serialize, Deserialize)]
struct Get {
    key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct List;

#[derive(Debug, Serialize, Deserialize)]
struct Set {
    key: String,
    value: String,
}

#[rpc_requests(N0desService, message = N0desMessage)]
#[derive(Serialize, Deserialize)]
pub enum N0desProtocol {
    #[rpc(tx=oneshot::Sender<()>)]
    Auth(Auth),
    #[rpc(tx=oneshot::Sender<()>)]
    PutBlob(PutBlob),
    #[rpc(tx=oneshot::Sender<Option<Hash>>)]
    GetTag(GetTag),
    #[rpc(tx=oneshot::Sender<()>)]
    PutMetrics(PutMetrics),
    #[rpc(tx=oneshot::Sender<[u8; 32]>)]
    Ping(Ping),
}

/// Authentication on first request
#[derive(Debug, Serialize, Deserialize)]
pub struct Auth {
    caps: Rcan<Caps>,
}

/// Request that the node fetches the given blob.
#[derive(Debug, Serialize, Deserialize)]
pub struct PutBlob {
    ticket: BlobTicket,
    name: String,
}

/// Request the name of a blob held by the node
#[derive(Debug, Serialize, Deserialize)]
pub struct GetTag {
    name: String,
}

/// Request to store the given metrics data
#[derive(Debug, Serialize, Deserialize)]
pub struct PutMetrics {
    encoded: String,
    session_id: Uuid,
}

/// Simple ping requests
#[derive(Debug, Serialize, Deserialize)]
pub struct Ping {
    req: [u8; 32],
}

pub struct N0desApi {
    inner: Client<N0desMessage, N0desProtocol, N0desService>,
}

impl N0desApi {
    pub const ALPN: &[u8] = b"/iroh/n0des/1";

    pub fn connect(endpoint: Endpoint, addr: impl Into<iroh::NodeAddr>) -> Result<N0desApi> {
        let conn = IrohRemoteConnection::new(endpoint, addr.into(), Self::ALPN.to_vec());
        Ok(N0desApi {
            inner: Client::boxed(conn),
        })
    }

    pub async fn auth(&self, caps: Rcan<Caps>) -> Result<(), irpc::Error> {
        self.inner.rpc(Auth { caps }).await
    }

    pub async fn put_blob(
        &self,
        name: impl AsRef<str>,
        ticket: BlobTicket,
    ) -> Result<(), irpc::Error> {
        self.inner
            .rpc(PutBlob {
                name: name.as_ref().to_string(),
                ticket,
            })
            .await
    }

    pub async fn get_tag(&self, name: impl AsRef<str>) -> Result<Option<Hash>, irpc::Error> {
        self.inner
            .rpc(GetTag {
                name: name.as_ref().to_string(),
            })
            .await
    }

    pub async fn put_metrics(&self, session_id: Uuid, encoded: String) -> Result<(), irpc::Error> {
        self.inner
            .rpc(PutMetrics {
                session_id,
                encoded,
            })
            .await
    }

    pub async fn ping(&self, req: [u8; 32]) -> Result<[u8; 32], irpc::Error> {
        self.inner.rpc(Ping { req }).await
    }
}
