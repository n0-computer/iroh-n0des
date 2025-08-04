use std::{
    collections::BTreeMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, OnceLock, RwLock},
    time::Duration,
};

use anyhow::{Context as _, Result};
#[cfg(all(feature = "iroh_main", not(feature = "iroh_v035")))]
use iroh::Watcher;
use iroh_metrics::{Registry, encoding::Encoder};
use n0_future::{FuturesUnordered, TryStreamExt};
use proto::{ActiveTrace, NodeInfo, NodeInfoWithAddr, ScopeInfo, TraceClient, TraceInfo};
use tokio::sync::Semaphore;
use trace::submit_logs;
use tracing::{Instrument, Span, error, error_span};

use crate::{
    iroh::{Endpoint, NodeAddr, NodeId},
    n0des::N0de,
};

pub mod events;
pub mod proto;
mod trace;

pub const ENV_TRACE_ISOLATED: &str = "N0DES_TRACE_ISOLATED";
pub const ENV_TRACE_INIT_ONLY: &str = "N0DES_TRACE_INIT_ONLY";
pub const ENV_TRACE_SERVER: &str = "N0DES_TRACE_SERVER";
pub const ENV_TRACE_SESSION_ID: &str = "N0DES_SESSION_ID";

/// Simulation context passed to each node
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Context {
    pub round: u64,
    pub node_index: usize,
    pub addrs: Arc<BTreeMap<u32, NodeInfoWithAddr>>,
}

impl Context {
    pub fn all_other_nodes(&self, me: NodeId) -> impl Iterator<Item = &NodeAddr> + '_ {
        self.addrs
            .values()
            .filter(move |n| n.info.id != me)
            .map(|n| &n.addr)
    }

    pub fn addr(&self, idx: usize) -> Result<NodeAddr> {
        Ok(self
            .addrs
            .get(&(idx as u32))
            .cloned()
            .context("node not found")?
            .addr)
    }

    pub fn self_addr(&self) -> &NodeAddr {
        &self.addrs[&(self.node_index as u32)].addr
    }

    pub fn node_count(&self) -> usize {
        self.addrs.len()
    }
}

pub struct Simulation<N: N0de> {
    #[allow(unused)]
    name: String,
    max_rounds: u64,
    node_count: u32,
    round_fn: BoxedRoundFn<N>,
    check_fn: Option<BoxedCheckFn<N>>,
    nodes: Vec<SimNode<N>>,
    addrs: Arc<BTreeMap<u32, NodeInfoWithAddr>>,
    round: u64,
    trace_client: Option<ActiveTrace>,
    run_mode: RunMode,
}

type BoxedRoundFn<N> = Arc<
    dyn for<'a> Fn(
        &'a Context,
        &'a mut N,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>,
>;

type BoxedCheckFn<N> = Box<dyn Fn(&Context, &N) -> Result<()>>;

pub trait AsyncCallback<'a, N: 'a, T: 'a>:
    'static + Send + Fn(&'a Context, &'a mut N) -> Self::Fut
{
    type Fut: Future<Output = Result<T>> + Send;
}

impl<'a, N: 'a, T: 'a, Out, F> AsyncCallback<'a, N, T> for F
where
    Out: Send + Future<Output = Result<T>>,
    F: 'static + Send + Fn(&'a Context, &'a mut N) -> Out,
{
    type Fut = Out;
}

#[allow(unused)]
pub struct RoundOutcome {
    node_idx: usize,
    node_id: NodeId,
    duration: Duration,
}

#[derive(Debug, Copy, Clone)]
enum RunMode {
    Integrated,
    Isolated(u32),
}

impl<N: N0de> Simulation<N> {
    pub fn builder<F>(round: F) -> SimulationBuilder<N>
    where
        F: for<'a> AsyncCallback<'a, N, bool>,
    {
        let round_fn: BoxedRoundFn<N> =
            Arc::new(move |context: &Context, node: &mut N| Box::pin(round(context, node)));
        SimulationBuilder::<N> {
            max_rounds: 100,
            node_count: 2,
            round_fn,
            check_fn: None,
            run_mode: RunMode::Integrated,
            _node: PhantomData,
        }
    }

    fn context(&self, node_index: usize) -> Context {
        Context {
            addrs: self.addrs.clone(),
            node_index,
            round: self.round,
        }
    }
    pub async fn run(&mut self) -> Result<()> {
        let res = self.run_inner().await;
        if let Some(ref client) = self.trace_client {
            let rpc_res = res.as_ref().map(|_| ()).map_err(|e| e.to_string());
            client.end_trace(rpc_res).await?;
        }
        res
    }

    async fn if_isolated<R>(
        &self,
        f: impl AsyncFnOnce(&ActiveTrace) -> Result<R>,
    ) -> Result<Option<R>> {
        if let Some(ref client) = self.trace_client {
            if let RunMode::Isolated(_) = self.run_mode {
                return Ok(Some(f(client).await?));
            }
        }
        Ok(None)
    }

    async fn run_inner(&mut self) -> Result<()> {
        self.if_isolated(async |client| {
            client.wait_checkpoint(0, self.node_count).await?;
            Ok(())
        })
        .await?;

        while self.round < self.max_rounds {
            if let Err(err) = self.run_round().await {
                error!("Simulation failed at round {}: {err:#}", self.round);
                return Err(err);
            }
            self.round += 1;
        }
        Ok(())
    }

    async fn run_round(&mut self) -> Result<()> {
        println!("Round {}", self.round);

        let ctx = self.context(0);
        let round_fn = self.round_fn.clone();

        let run_fn = async |node: &mut SimNode<N>| {
            let mut ctx = ctx.clone();
            ctx.node_index = node.idx;
            let res = (round_fn)(&ctx, &mut node.node)
                .await
                .with_context(|| format!("Node {} failed in round {}", ctx.node_index, ctx.round));

            if let Err(err) = res.as_ref() {
                error!("{err:#}");
            }

            if let Some(ref client) = self.trace_client {
                let checkpoint = ctx.round + 1;
                client
                    .put_checkpoint(
                        checkpoint,
                        ctx.node_index as u32,
                        Some(format!("round {checkpoint} end")),
                        res.as_ref().map_err(|err| format!("{err:#}")).map(|_| ()),
                    )
                    .await?;

                let metrics = node.encoder.export();
                client
                    .put_metrics(node.node_id(), Some(checkpoint), metrics)
                    .await?;
            }

            anyhow::Ok(())
        };
        let futures = self.nodes.iter_mut().map(|node| {
            let span = node.span.clone();
            run_fn(node).instrument(span)
        });

        let _res: Vec<_> = n0_future::FuturesOrdered::from_iter(futures)
            .try_collect()
            .await?;

        self.if_isolated(async |client| {
            client
                .wait_checkpoint(self.round + 1, self.node_count)
                .await?;
            Ok(())
        })
        .await?;

        println!("Round {} completed", self.round);
        if let Some(check) = &self.check_fn {
            for node in self.nodes.iter() {
                let _guard = node.span.enter();
                let ctx = self.context(node.idx);
                (check)(&ctx, &node.node)?;
            }
        }

        if let Some(ref client) = self.trace_client {
            submit_logs(client).await?;
        }

        Ok(())
    }
}

pub struct SimulationBuilder<N: N0de> {
    max_rounds: u64,
    node_count: u32,
    round_fn: BoxedRoundFn<N>,
    check_fn: Option<BoxedCheckFn<N>>,
    run_mode: RunMode,
    _node: PhantomData<N>,
}

impl<N: N0de> SimulationBuilder<N> {
    pub fn max_rounds(self, count: u64) -> Self {
        Self {
            max_rounds: count,
            ..self
        }
    }

    pub fn node_count(self, count: u32) -> Self {
        Self {
            node_count: count,
            ..self
        }
    }

    pub fn check(self, check: impl Fn(&Context, &N) -> Result<()> + 'static) -> Self {
        Self {
            check_fn: Some(Box::new(check)),
            ..self
        }
    }

    pub fn isolated(mut self, idx: u32) -> Self {
        self.run_mode = RunMode::Isolated(idx);
        self
    }

    pub async fn build(self, trace_name: &str) -> Result<Simulation<N>> {
        let nodes: Vec<_> = match self.run_mode {
            RunMode::Integrated => {
                FuturesUnordered::from_iter(
                    (0usize..self.node_count as usize).map(|i| SimNode::<N>::spawn(i)),
                )
                .try_collect()
                .await?
            }
            RunMode::Isolated(idx) => {
                anyhow::ensure!(
                    idx < self.node_count,
                    "Isolated index must be lower than max node count"
                );
                vec![SimNode::<N>::spawn(idx as usize).await?]
            }
        };

        let client = if let Some(client) = client_from_env()? {
            if matches!(self.run_mode, RunMode::Integrated) {
                let info = TraceInfo {
                    name: trace_name.to_string(),
                    expected_nodes: Some(self.node_count),
                    expected_checkpoints: Some(self.max_rounds),
                };
                client.init_trace(info).await?;
            }
            let scope = match self.run_mode {
                RunMode::Integrated => {
                    let node_info = nodes.iter().map(|node| node.info()).collect();
                    ScopeInfo::Integrated(node_info)
                }
                RunMode::Isolated(idx) => {
                    let node_info = nodes[0].info();
                    debug_assert_eq!(node_info.idx, idx);
                    ScopeInfo::Isolated(node_info)
                }
            };
            let trace_client = client
                .start_trace(trace_name.to_string(), scope)
                .await
                .context("failed to connect to sim server")?;
            Some(trace_client)
        } else {
            None
        };

        let addrs = match (&client, self.run_mode) {
            (Some(client), RunMode::Isolated(_)) => {
                let node = &nodes[0];
                let info = node.info_with_addr().await?;

                client.wait_start(info, self.node_count).await?
            }
            _ => {
                let addrs: Vec<_> = FuturesUnordered::from_iter(nodes.iter().map(|n| async {
                    let info = n.info();
                    let addr = node_addr(&n.endpoint).await?;
                    anyhow::Ok(NodeInfoWithAddr { info, addr })
                }))
                .try_collect()
                .await?;
                addrs
            }
        };

        Ok(Simulation {
            name: trace_name.to_string(),
            max_rounds: self.max_rounds,
            round_fn: self.round_fn,
            check_fn: self.check_fn,
            nodes,
            addrs: Arc::new(addrs.into_iter().map(|n| (n.info.idx, n)).collect()),
            trace_client: client,
            round: 0,
            run_mode: self.run_mode,
            node_count: self.node_count,
        })
    }
}

pub(crate) struct SimNode<N: N0de> {
    idx: usize,
    node: N,
    endpoint: Endpoint,
    encoder: Encoder,
    span: Span,
    #[allow(unused)]
    spawn_time: Duration,
}

impl<N: N0de> SimNode<N> {
    async fn spawn(idx: usize) -> Result<Self> {
        let span = error_span!("sim-node", idx, round = tracing::field::Empty);
        let start = n0_future::time::Instant::now();
        let (node, endpoint, registry) = async move {
            let mut registry = Registry::default();
            let endpoint = Endpoint::builder().bind().await?;

            // TODO(Frando): enable metrics support for iroh 0.35
            // iroh@0.35 uses iroh-metrics@0.34, which is a major version down from iroh-metrics@0.35
            // which is used here, thus the traits are incompatible.
            // Add shim to iroh-metrics@0.35 to support registering metrics from iroh-metrics@0.34
            #[cfg(not(feature = "iroh_v035"))]
            registry.register_all(endpoint.metrics());

            let node = N::spawn(endpoint.clone(), &mut registry).await?;
            anyhow::Ok((node, endpoint, registry))
        }
        .instrument(span.clone())
        .await?;
        Ok(Self {
            idx,
            node,
            endpoint,
            encoder: Encoder::new(Arc::new(RwLock::new(registry))),
            span,
            spawn_time: start.elapsed(),
        })
    }

    fn node_id(&self) -> NodeId {
        self.endpoint.node_id()
    }

    fn info(&self) -> NodeInfo {
        NodeInfo {
            label: None,
            id: self.node_id(),
            idx: self.idx as u32,
        }
    }

    async fn info_with_addr(&self) -> Result<NodeInfoWithAddr> {
        let info = self.info();
        let addr = node_addr(&self.endpoint).await?;
        Ok(NodeInfoWithAddr { info, addr })
    }
}

async fn node_addr(endpoint: &Endpoint) -> anyhow::Result<NodeAddr> {
    #[cfg(feature = "iroh_v035")]
    let addr = endpoint.node_addr().await?;
    #[cfg(not(feature = "iroh_v035"))]
    let addr = endpoint.node_addr().initialized().await;
    Ok(addr)
}

pub fn is_sim_env() -> bool {
    std::env::var(ENV_TRACE_SERVER).is_ok()
}

fn client_from_env() -> Result<Option<TraceClient>> {
    static CLIENT: OnceLock<Result<Option<TraceClient>>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            // create a span so that the quinn endpoint is associated.
            let span = tracing::error_span!("trace-client");
            let _guard = span.enter();
            TraceClient::from_env()
        })
        .as_ref()
        .map_err(|err| anyhow::anyhow!("failed to init sim client: {err:#}"))
        .cloned()
}

static PERMIT: Semaphore = Semaphore::const_new(1);

#[doc(hidden)]
pub async fn run_sim_fn<F, Fut, N, E>(name: &str, sim_fn: F) -> anyhow::Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<SimulationBuilder<N>, E>>,
    N: N0de,
    E: Into<anyhow::Error>,
{
    // ensure simulations run sequentially so that we can extract logs properly.
    let permit = PERMIT.acquire().await.unwrap();

    if is_sim_env() {
        tracing::debug!("running in simulation env: ensure tracing subscriber is setup");
        self::trace::try_init_global_subscriber();
        self::trace::global_writer().clear();
    }

    let isolated: Option<u32> = match std::env::var(ENV_TRACE_ISOLATED) {
        Err(_) => None,
        Ok(s) => Some(s.parse().with_context(|| {
            format!("Failed to parse env var `{ENV_TRACE_ISOLATED}` as number")
        })?),
    };

    println!("running simulation: {name}");
    let mut builder = sim_fn()
        .await
        .map_err(|err| {
            // Not sure why, but anyhow::Error::from doesn't work here.
            let err: anyhow::Error = err.into();
            err
        })
        .with_context(|| format!("simulation builder function `{name}` failed"))?;

    if std::env::var(ENV_TRACE_INIT_ONLY).is_ok() {
        let client = client_from_env()?.expect("failed to build client");
        client
            .init_trace(TraceInfo {
                name: name.to_string(),
                expected_nodes: Some(builder.node_count),
                expected_checkpoints: Some(builder.max_rounds),
            })
            .await?;
        println!("{}", builder.node_count);
        return Ok(());
    }

    if let Some(isolated_idx) = isolated {
        builder = builder.isolated(isolated_idx);
    }

    let mut sim = builder
        .build(name)
        .await
        .with_context(|| format!("simulation `{name}` failed to build"))?;
    let res = sim.run().await;
    match &res {
        Ok(()) => println!("simulation passed"),
        Err(err) => println!("simulation failed: {err:#}"),
    };
    res.with_context(|| format!("simulation `{name}` failed to run"))?;

    drop(permit);
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use iroh_metrics::Counter;
    use rand::seq::IteratorRandom;
    use tracing::{debug, instrument};

    use super::*;
    use crate::iroh::{
        endpoint::Connection,
        protocol::{ProtocolHandler, Router},
    };

    const ALPN: &[u8] = b"iroh-n0des/test/ping/0";

    #[derive(Debug, Clone, Default)]
    struct Ping {
        metrics: Arc<Metrics>,
    }

    #[derive(Debug, Default, iroh_metrics::MetricsGroup)]
    #[metrics(name = "ping")]
    struct Metrics {
        /// count of valid ping messages sent
        pub pings_sent: Counter,
        /// count of valid ping messages received
        pub pings_recv: Counter,
    }

    #[cfg(feature = "iroh_v035")]
    impl ProtocolHandler for Ping {
        fn accept(&self, connection: Connection) -> n0_future::future::Boxed<Result<()>> {
            let this = self.clone();
            Box::pin(async move { this.accept(connection).await })
        }
    }

    #[cfg(all(feature = "iroh_main", not(feature = "iroh_v035")))]
    impl ProtocolHandler for Ping {
        async fn accept(&self, connection: Connection) -> Result<(), iroh::protocol::AcceptError> {
            self.accept(connection)
                .await
                .map_err(|err| err.into_boxed_dyn_error().into())
        }
    }

    impl Ping {
        async fn accept(&self, connection: Connection) -> Result<()> {
            debug!("ping accepted");
            let (mut send, mut recv) = connection.accept_bi().await?;
            recv.read_to_end(2).await?;
            debug!("ping received");
            self.metrics.pings_recv.inc();
            send.write_all(b"bye").await?;
            send.finish()?;
            debug!("pong sent");
            let reason = connection.closed().await;
            debug!(?reason, "connection closed");
            Ok(())
        }

        #[instrument("ping-connect", skip_all, fields(remote = tracing::field::Empty))]
        async fn ping(&self, endpoint: &Endpoint, addr: impl Into<NodeAddr>) -> Result<()> {
            let addr: NodeAddr = addr.into();
            tracing::Span::current()
                .record("remote", tracing::field::display(addr.node_id.fmt_short()));
            debug!("ping connect");
            let connection = endpoint.connect(addr, ALPN).await?;
            debug!("ping connected");
            let (mut send, mut recv) = connection.open_bi().await?;
            send.write_all(b"hi").await?;
            send.finish()?;
            debug!("ping sent");
            self.metrics.pings_sent.inc();
            recv.read_to_end(3).await?;
            debug!("ping received");
            connection.close(1u32.into(), b"");
            Ok(())
        }

        fn metrics(&self) -> &Arc<Metrics> {
            &self.metrics
        }
    }

    struct PingNode {
        router: Router,
        ping: Ping,
    }

    impl N0de for PingNode {
        async fn spawn(ep: Endpoint, metrics: &mut Registry) -> Result<Self> {
            let ping = Ping::default();
            metrics.register(ping.metrics.clone());

            let router = crate::iroh::protocol::Router::builder(ep)
                .accept(ALPN, ping.clone())
                .spawn();

            Ok(Self { ping, router })
        }

        async fn shutdown(&mut self) -> Result<()> {
            self.router.shutdown().await?;
            Ok(())
        }
    }

    impl PingNode {
        async fn ping(&self, addr: impl Into<NodeAddr>) -> Result<()> {
            self.ping.ping(self.router.endpoint(), addr).await
        }
    }

    #[crate::sim]
    async fn test_simulation() -> Result<SimulationBuilder<PingNode>> {
        const EVENT_ID: &str = "ping";
        async fn tick(ctx: &Context, node: &mut PingNode) -> Result<bool> {
            let me = ctx.self_addr().node_id;

            let target = ctx
                .all_other_nodes(me)
                .choose(&mut rand::thread_rng())
                .unwrap();
            node.ping(target.clone()).await?;

            // record event for simulation visualization.
            iroh_n0des::simulation::events::event_start(
                me.fmt_short(),
                target.node_id.fmt_short(),
                format!("send ping (round {})", ctx.round),
                Some(EVENT_ID.to_string()),
            );
            Ok(true)
        }

        fn check(ctx: &Context, node: &PingNode) -> Result<()> {
            let metrics = node.ping.metrics();
            assert_eq!(metrics.pings_sent.get(), ctx.round + 1);
            Ok(())
        }

        Ok(Simulation::builder(tick)
            .node_count(4)
            .max_rounds(3)
            .check(check))
    }

    #[tokio::test]
    async fn test_trace_connect() -> Result<()> {
        let Some(client) = client_from_env()? else {
            println!(
                "skipping trace server connect test: no server addr provided via N0DES_SIM_SERVER"
            );
            return Ok(());
        };
        super::trace::try_init_global_subscriber();
        let trace_client = client.init_and_start_trace("foo").await?;

        tracing::info!("hello world");
        let span = tracing::info_span!("req", id = 1, method = "get");
        let _guard = span.enter();
        tracing::debug!("start");
        let span2 = tracing::info_span!("inner");
        let _guard2 = span2.enter();
        tracing::debug!("inner message");
        drop(_guard2);
        tracing::debug!(time = 22, "end");
        super::trace::submit_logs(&trace_client).await?;
        tracing::info!("bazoo");
        tracing::debug!(foo = "bar", "bazza");
        super::trace::submit_logs(&trace_client).await?;
        Ok(())
    }

    #[crate::sim]
    async fn test_conn() -> Result<SimulationBuilder<PingNode>> {
        async fn tick(ctx: &Context, node: &mut PingNode) -> Result<bool> {
            let me = node.router.endpoint().node_id();
            if ctx.node_index == 1 {
                for other in ctx.all_other_nodes(me) {
                    node.ping
                        .ping(node.router.endpoint(), other.clone())
                        .await?;
                }
            }
            Ok(true)
        }

        fn check(_ctx: &Context, _node: &PingNode) -> Result<()> {
            Ok(())
        }

        Ok(Simulation::builder(tick).max_rounds(1).check(check))
    }

    #[derive(Debug, Clone)]
    struct NoopNode {
        endpoint: Endpoint,
        active_event: Option<(String, NodeId)>,
    }
    impl N0de for NoopNode {
        async fn spawn(endpoint: Endpoint, _metrics: &mut Registry) -> Result<Self>
        where
            Self: Sized,
        {
            Ok(NoopNode {
                endpoint,
                active_event: None,
            })
        }
    }

    #[crate::sim]
    async fn custom_events() -> Result<SimulationBuilder<NoopNode>> {
        async fn tick(ctx: &Context, node: &mut NoopNode) -> Result<bool> {
            let me = node.endpoint.node_id();
            if let Some((id, other_node)) = node.active_event.take() {
                self::events::event_end(me, other_node, "done", id);
            }
            let other = ctx
                .all_other_nodes(me)
                .choose(&mut rand::thread_rng())
                .unwrap();
            let id = format!("ping:n{}:r{}", ctx.node_index, ctx.round);
            self::events::event_start(
                me.fmt_short(),
                other.node_id.fmt_short(),
                format!("ping round {}", ctx.round),
                Some(&id),
            );
            node.active_event = Some((id, other.node_id));
            Ok(true)
        }

        Ok(Simulation::builder(tick).max_rounds(4).node_count(4))
    }
}
