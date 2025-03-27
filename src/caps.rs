use std::time::Duration;

use anyhow::{Context, Result};
use ed25519_dalek::{SigningKey, VerifyingKey};
use iroh::NodeId;
use rcan::{Capability, Expires, Rcan};
use serde::{Deserialize, Serialize};
use ssh_key::PrivateKey as SshPrivateKey;

#[derive(Ord, Eq, PartialOrd, PartialEq, Clone, Serialize, Deserialize, Debug)]
pub enum IpsCap {
    V1(IpsCapV1),
}

/// Potential capabilities for IPS
#[derive(Ord, Eq, PartialOrd, PartialEq, Clone, Serialize, Deserialize, Debug)]
#[repr(u8)]
pub enum IpsCapV1 {
    /// API tokens, used in the RPC
    Api,
    /// Used to authenticate users.
    Web,
}

impl Capability for IpsCap {
    fn permits(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::V1(IpsCapV1::Web), Self::V1(IpsCapV1::Web)) => true,
            (Self::V1(IpsCapV1::Api), Self::V1(IpsCapV1::Api)) => true,
            (Self::V1(_), Self::V1(_)) => false,
        }
    }
}

/// Create an rcan token for the api access.
pub fn create_api_token(
    user_ssh_key: &SshPrivateKey,
    local_node_id: NodeId,
    max_age: Duration,
) -> Result<Rcan<IpsCap>> {
    let issuer: SigningKey = user_ssh_key
        .key_data()
        .ed25519()
        .context("only Ed25519 keys supported")?
        .private
        .clone()
        .into();

    // TODO: add Into to iroh-base
    let audience = VerifyingKey::from_bytes(local_node_id.as_bytes())?;
    let can = Rcan::issuing_builder(&issuer, audience, IpsCap::V1(IpsCapV1::Api))
        .sign(Expires::valid_for(max_age));
    Ok(can)
}
