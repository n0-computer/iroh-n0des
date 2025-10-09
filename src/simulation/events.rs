use std::{borrow::Cow, fmt};

use iroh::NodeId;

pub struct EventId<'a, T>(Option<&'a T>);

impl<'a, T> fmt::Display for EventId<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            None => Ok(()),
            Some(inner) => inner.fmt(f),
        }
    }
}

#[derive(Debug, derive_more::From, derive_more::Display)]
pub enum VisNode<'a> {
    #[display("{}", _0.fmt_short())]
    Node(NodeId),
    Other(Cow<'a, str>),
}

impl<'a> VisNode<'a> {
    pub fn to_static(self) -> VisNode<'static> {
        match self {
            VisNode::Node(n) => VisNode::Node(n),
            VisNode::Other(cow) => VisNode::Other(Cow::Owned(cow.into_owned())),
        }
    }
}

impl<'a> From<&'a str> for VisNode<'a> {
    fn from(value: &'a str) -> Self {
        Self::Other(Cow::Borrowed(value))
    }
}

impl<'a> From<String> for VisNode<'a> {
    fn from(value: String) -> Self {
        Self::Other(Cow::Owned(value))
    }
}

pub const TARGET: &str = "_n0des_event";
pub const TARGET_END: &str = "_n0des_event_end";

pub fn event<'a>(
    from: impl Into<VisNode<'a>>,
    to: impl Into<VisNode<'a>>,
    label: impl fmt::Display,
) {
    event_start(from, to, label, Option::<&str>::None);
}

pub fn event_start<'a>(
    from: impl Into<VisNode<'a>>,
    to: impl Into<VisNode<'a>>,
    label: impl fmt::Display,
    id: Option<impl fmt::Display>,
) {
    let from = from.into();
    let to = to.into();
    if let Some(ref id) = id {
        tracing::event! {
            target: TARGET,
            tracing::Level::INFO,
            %from, %to, %label, %id
        }
    } else {
        tracing::event! {
            target: TARGET,
            tracing::Level::INFO,
            %from, %to, %label
        }
    }
}

pub fn event_end<'a>(
    from: impl Into<VisNode<'a>>,
    to: impl Into<VisNode<'a>>,
    label: impl fmt::Display,
    id: impl fmt::Display,
) {
    let from = from.into();
    let to = to.into();
    tracing::event! {
        target: TARGET_END,
        tracing::Level::INFO,
        %from, %to, %label, %id
    }
}
