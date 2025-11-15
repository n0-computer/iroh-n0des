use std::{
    collections::BTreeSet,
    fmt::{self, Display},
    str::FromStr,
};

use iroh::{EndpointAddr, EndpointId, SecretKey, TransportAddr};
use iroh_tickets::{ParseError, Ticket};
use serde::{Deserialize, Serialize};

/// The secret material used to connect your n0des.iroh.computer project. The
/// value of these should be treated like any other API key: guard them carefully.
#[derive(Debug, Clone)]
pub struct ApiSecret {
    /// ED25519 secret used to construct rcans from
    pub secret: SecretKey,
    /// the n0des endpoint to direct requests to
    pub remote: EndpointAddr,
}

impl Display for ApiSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Ticket::serialize(self))
    }
}

#[derive(Serialize, Deserialize)]
struct Variant0ApiSecret {
    endpoint_id: EndpointId,
    info: Variant0AddrInfo,
}

#[derive(Serialize, Deserialize)]
struct Variant0AddrInfo {
    addrs: BTreeSet<TransportAddr>,
}

/// Wire format for [`NodeTicket`].
#[derive(Serialize, Deserialize)]
enum TicketWireFormat {
    Variant0(Variant0N0desTicket),
}

#[derive(Serialize, Deserialize)]
struct Variant0N0desTicket {
    secret: SecretKey,
    addr: Variant0ApiSecret,
}

impl Ticket for ApiSecret {
    // KIND is the constant that's added to the front of a serialized ticket
    // string. It should be a short, human readable string
    const KIND: &'static str = "n0des";

    fn to_bytes(&self) -> Vec<u8> {
        let data = TicketWireFormat::Variant0(Variant0N0desTicket {
            secret: self.secret.clone(),
            addr: Variant0ApiSecret {
                endpoint_id: self.remote.id,
                info: Variant0AddrInfo {
                    addrs: self.remote.addrs.clone(),
                },
            },
        });
        postcard::to_stdvec(&data).expect("postcard serialization failed")
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        let res: TicketWireFormat = postcard::from_bytes(bytes)?;
        let TicketWireFormat::Variant0(Variant0N0desTicket { secret, addr }) = res;
        Ok(Self {
            secret,
            remote: EndpointAddr {
                id: addr.endpoint_id,
                addrs: addr.info.addrs.clone(),
            },
        })
    }
}

impl FromStr for ApiSecret {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        iroh_tickets::Ticket::deserialize(s)
    }
}

impl ApiSecret {
    /// Creates a new ticket.
    pub fn new(secret: SecretKey, remote: impl Into<EndpointAddr>) -> Self {
        Self {
            secret,
            remote: remote.into(),
        }
    }

    /// The [`EndpointAddr`] of the provider for this ticket.
    pub fn addr(&self) -> &EndpointAddr {
        &self.remote
    }
}
