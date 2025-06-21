use std::{collections::BTreeSet, fmt, str::FromStr, time::Duration};

use anyhow::{bail, Context, Result};
use ed25519_dalek::SigningKey;
use iroh::NodeId;
use rcan::{Capability, Expires, Rcan};
use serde::{Deserialize, Serialize};
use ssh_key::PrivateKey as SshPrivateKey;

macro_rules! cap_enum(
    ($enum:item) => {
        #[derive(
            Debug,
            Eq,
            PartialEq,
            Ord,
            PartialOrd,
            Serialize,
            Deserialize,
            Clone,
            Copy,
            strum::Display,
            strum::EnumString,
        )]
        #[strum(serialize_all = "kebab-case")]
        #[serde(rename_all = "kebab-case")]
        $enum
    }
);

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum Caps {
    V0(CapSet<Cap>),
}

impl Default for Caps {
    fn default() -> Self {
        Self::V0(CapSet::default())
    }
}

impl std::ops::Deref for Caps {
    type Target = CapSet<Cap>;

    fn deref(&self) -> &Self::Target {
        let Self::V0(ref slf) = self;
        slf
    }
}

#[derive(
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Clone,
    Copy,
    derive_more::From,
    strum::Display,
)]
#[serde(rename_all = "kebab-case")]
pub enum Cap {
    #[strum(to_string = "all")]
    All,
    #[strum(to_string = "relay:{0}")]
    Relay(RelayCap),
    #[strum(to_string = "metrics:{0}")]
    Metrics(MetricsCap),
}

impl FromStr for Cap {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s == "all" {
            Ok(Self::All)
        } else if let Some((domain, inner)) = s.split_once(":") {
            Ok(match domain {
                "metrics" => Self::Metrics(MetricsCap::from_str(inner)?),
                "relay" => Self::Relay(RelayCap::from_str(inner)?),
                _ => bail!("invalid cap domain"),
            })
        } else {
            Err(anyhow::anyhow!("invalid cap string"))
        }
    }
}

cap_enum!(
    pub enum MetricsCap {
        PutAny,
    }
);

cap_enum!(
    pub enum RelayCap {
        Use,
    }
);

impl Caps {
    pub fn new(caps: impl IntoIterator<Item = impl Into<Cap>>) -> Self {
        Self::V0(CapSet::new(caps))
    }

    pub fn all() -> Self {
        Self::new([Cap::All])
    }

    pub fn extend(self, caps: impl IntoIterator<Item = impl Into<Cap>>) -> Self {
        let Self::V0(mut set) = self;
        set.extend(caps.into_iter().map(Into::into));
        Self::V0(set)
    }

    pub fn from_strs<'a>(strs: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        Ok(Self::V0(CapSet::from_strs(strs)?))
    }

    pub fn to_strings(&self) -> Vec<String> {
        let Self::V0(ref set) = self;
        set.to_strings()
    }
}

impl Capability for Caps {
    fn permits(&self, other: &Self) -> bool {
        let Self::V0(slf) = self;
        let Self::V0(other) = other;
        slf.permits(other)
    }
}

impl From<Cap> for Caps {
    fn from(cap: Cap) -> Self {
        Self::new([cap])
    }
}

impl Capability for Cap {
    fn permits(&self, other: &Self) -> bool {
        match (self, other) {
            (Cap::All, _) => true,
            (Cap::Relay(slf), Cap::Relay(other)) => slf.permits(other),
            (Cap::Metrics(slf), Cap::Metrics(other)) => slf.permits(other),
            (_, _) => false,
        }
    }
}

impl Capability for MetricsCap {
    fn permits(&self, other: &Self) -> bool {
        match (self, other) {
            (MetricsCap::PutAny, MetricsCap::PutAny) => true,
        }
    }
}

impl Capability for RelayCap {
    fn permits(&self, other: &Self) -> bool {
        match (self, other) {
            (RelayCap::Use, RelayCap::Use) => true,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize)]
pub struct CapSet<C: Capability + Ord>(BTreeSet<C>);

impl<C: Capability + Ord> Default for CapSet<C> {
    fn default() -> Self {
        Self(BTreeSet::new())
    }
}

impl<C: Capability + Ord> CapSet<C> {
    pub fn new(set: impl IntoIterator<Item = impl Into<C>>) -> Self {
        Self(BTreeSet::from_iter(set.into_iter().map(Into::into)))
    }

    pub fn iter(&self) -> impl Iterator<Item = &'_ C> + '_ {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn contains(&self, cap: impl Into<C>) -> bool {
        let cap = cap.into();
        self.0.contains(&cap)
    }

    pub fn extend(&mut self, caps: impl IntoIterator<Item = impl Into<C>>) {
        self.0.extend(caps.into_iter().map(Into::into));
    }

    pub fn insert(&mut self, cap: impl Into<C>) -> bool {
        self.0.insert(cap.into())
    }

    pub fn from_strs<'a, E>(strs: impl IntoIterator<Item = &'a str>) -> Result<Self>
    where
        C: FromStr<Err = E>,
        Result<C, E>: anyhow::Context<C, E>,
    {
        let mut caps = Self::default();
        for s in strs {
            let cap = C::from_str(s).with_context(|| format!("Unknown capability: {s}"))?;
            caps.insert(cap);
        }
        Ok(caps)
    }

    pub fn to_strings(&self) -> Vec<String>
    where
        C: fmt::Display,
    {
        self.iter().map(ToString::to_string).collect()
    }
}

impl<C: Capability + Ord> Capability for CapSet<C> {
    fn permits(&self, other: &Self) -> bool {
        other
            .iter()
            .all(|other_cap| self.iter().any(|self_cap| self_cap.permits(other_cap)))
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

    let audience = local_node_id.public();
    let can =
        Rcan::issuing_builder(&issuer, audience, capability).sign(Expires::valid_for(max_age));
    Ok(can)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let all = Caps::default()
            .extend([BlobsCap::PutBlob, BlobsCap::GetBlob, BlobsCap::GetTag])
            .extend([RelayCap::Use])
            .extend([MetricsCap::PutAny]);

        // test to-and-from string conversion
        println!("all:     {:?}", all);
        let strings = all.to_strings();
        println!("strings: {:?}", strings);
        let parsed = Caps::from_strs(strings.iter().map(|s| s.as_str())).unwrap();
        assert_eq!(all, parsed);

        // manual parsing from strings
        let s = ["blobs:put-blob", "relay:use"];
        let caps = Caps::from_strs(s).unwrap();
        assert_eq!(caps, Caps::new([BlobsCap::PutBlob]).extend([RelayCap::Use]));

        let full = Caps::new([Cap::All]);

        assert!(full.permits(&full));
        assert!(full.permits(&all));
        assert!(!all.permits(&full));

        let get_tags = Caps::new([BlobsCap::GetTag]);
        let put_blobs = Caps::new([BlobsCap::PutBlob]);
        let relay = Caps::new([RelayCap::Use]);

        for cap in [&get_tags, &put_blobs, &relay] {
            assert!(full.permits(cap));
            assert!(all.permits(cap));
            assert!(!cap.permits(&full));
            assert!(!cap.permits(&all));
        }

        assert!(!get_tags.permits(&put_blobs));
        assert!(!get_tags.permits(&relay));

        let all_blobs = Caps::new([BlobsCap::All]);
        assert!(all_blobs.permits(&get_tags));
        assert!(all_blobs.permits(&put_blobs));
        assert!(!put_blobs.permits(&all_blobs));
    }
}
