use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, Result};
use ed25519_dalek::{SigningKey, VerifyingKey};
use iroh::NodeId;
use rcan::{Capability, Expires, Rcan};
use serde::{Deserialize, Serialize};
use ssh_key::PrivateKey as SshPrivateKey;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub enum Caps {
    V0(CapSet<Cap>),
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Clone)]
pub enum Cap {
    All,
    Blobs(CapSet<BlobsCap>),
    Relay(CapSet<RelayCap>),
    Metrics(CapSet<MetricsCap>),
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Clone)]
pub enum BlobsCap {
    All,
    PutBlob,
    GetTag,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Clone)]
pub enum MetricsCap {
    PutAny,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Clone)]
pub enum RelayCap {
    UseUnlimited,
}

impl Caps {
    pub fn new(caps: impl IntoIterator<Item = Cap>) -> Self {
        Self::V0(CapSet::new(caps))
    }

    pub fn all() -> Self {
        Self::new([Cap::All])
    }
}

impl Capability for Caps {
    fn can_delegate(&self, other: &Self) -> bool {
        let Self::V0(slf) = self;
        let Self::V0(other) = other;
        slf.can_delegate(other)
    }
}

impl Cap {
    pub fn blobs(set: impl IntoIterator<Item = BlobsCap>) -> Self {
        Self::Blobs(CapSet::new(set))
    }

    pub fn metrics(set: impl IntoIterator<Item = MetricsCap>) -> Self {
        Self::Metrics(CapSet::new(set))
    }

    pub fn relay(set: impl IntoIterator<Item = RelayCap>) -> Self {
        Self::Relay(CapSet::new(set))
    }
}

impl Capability for Cap {
    fn can_delegate(&self, other: &Self) -> bool {
        match (self, other) {
            (Cap::All, _) => true,
            (Cap::Blobs(slf), Cap::Blobs(other)) => slf.can_delegate(other),
            (Cap::Blobs(_), _) => false,
            (Cap::Relay(slf), Cap::Relay(other)) => slf.can_delegate(other),
            (Cap::Relay(_), _) => false,
            (Cap::Metrics(slf), Cap::Metrics(other)) => slf.can_delegate(other),
            (Cap::Metrics(_), _) => false,
        }
    }
}

impl Capability for BlobsCap {
    fn can_delegate(&self, other: &Self) -> bool {
        match (self, other) {
            (BlobsCap::All, _) => true,
            (BlobsCap::PutBlob, BlobsCap::PutBlob) => true,
            (BlobsCap::PutBlob, _) => false,
            (BlobsCap::GetTag, BlobsCap::GetTag) => true,
            (BlobsCap::GetTag, _) => false,
        }
    }
}

impl Capability for MetricsCap {
    fn can_delegate(&self, other: &Self) -> bool {
        match (self, other) {
            (MetricsCap::PutAny, MetricsCap::PutAny) => true,
        }
    }
}

impl Capability for RelayCap {
    fn can_delegate(&self, other: &Self) -> bool {
        match (self, other) {
            (RelayCap::UseUnlimited, RelayCap::UseUnlimited) => true,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Clone)]
pub struct CapSet<C: Capability + Ord>(BTreeSet<C>);

impl<C: Capability + Ord> CapSet<C> {
    pub fn new(set: impl IntoIterator<Item = C>) -> Self {
        Self(BTreeSet::from_iter(set))
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ C> + '_ {
        self.0.iter()
    }
}

impl<C: Capability + Ord> Capability for CapSet<C> {
    fn can_delegate(&self, other: &Self) -> bool {
        other
            .iter()
            .all(|other_cap| self.iter().any(|self_cap| self_cap.can_delegate(other_cap)))
    }
}

/// Create an rcan token for the api access.
pub fn create_api_token(
    user_ssh_key: &SshPrivateKey,
    local_node_id: NodeId,
    max_age: Duration,
    capability: Caps,
) -> Result<Rcan<Caps>> {
    let issuer: SigningKey = user_ssh_key
        .key_data()
        .ed25519()
        .context("only Ed25519 keys supported")?
        .private
        .clone()
        .into();

    // TODO: add Into to iroh-base
    let audience = VerifyingKey::from_bytes(local_node_id.as_bytes())?;
    let can =
        Rcan::issuing_builder(&issuer, audience, capability).sign(Expires::valid_for(max_age));
    Ok(can)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let all_listed = Caps::new([
            Cap::blobs([BlobsCap::PutBlob, BlobsCap::GetTag]),
            Cap::relay([RelayCap::UseUnlimited]),
            Cap::metrics([MetricsCap::PutAny]),
        ]);

        let all = Caps::new([Cap::All]);

        assert!(all.can_delegate(&all));
        assert!(all.can_delegate(&all_listed));
        assert!(!all_listed.can_delegate(&all));

        let get_tags = Caps::new([Cap::blobs([BlobsCap::GetTag])]);
        let put_blobs = Caps::new([Cap::blobs([BlobsCap::PutBlob])]);
        let relay = Caps::new([Cap::relay([RelayCap::UseUnlimited])]);

        for cap in [&get_tags, &put_blobs, &relay] {
            assert!(all.can_delegate(&cap));
            assert!(all_listed.can_delegate(&cap));
            assert!(!cap.can_delegate(&all));
            assert!(!cap.can_delegate(&all_listed));
        }

        assert!(!get_tags.can_delegate(&put_blobs));
        assert!(!get_tags.can_delegate(&relay));

        let all_blobs = Caps::new([Cap::blobs([BlobsCap::All])]);
        assert!(all_blobs.can_delegate(&get_tags));
        assert!(all_blobs.can_delegate(&put_blobs));
        assert!(!put_blobs.can_delegate(&all_blobs));
    }
}
