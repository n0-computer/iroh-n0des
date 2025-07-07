use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, OnceLock, RwLock},
    time::Duration,
};

use anyhow::{Context as _, Result};
use iroh::{Endpoint, NodeAddr, NodeId, Watcher};
use iroh_metrics::{encoding::Encoder, Registry};
use time::OffsetDateTime as DateTime;

use n0_future::{FuturesUnordered, TryStreamExt};
use proto::{SimClient, SimSession};

use crate::n0des::N0de;

pub mod proto;

/// Simulation context passed to each node
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Context {
    pub round: u64,
    pub node_index: usize,
    pub addrs: Arc<Vec<NodeAddr>>,
}

impl Context {
    pub fn all_other_nodes(&self, me: NodeId) -> Vec<NodeAddr> {
        let mut other_nodes = Vec::new();
        for id in self.addrs.iter() {
            if id.node_id != me {
                other_nodes.push(id.clone());
            }
        }
        other_nodes
    }

    #[allow(unused)]
    fn self_addr(&self) -> &NodeAddr {
        &self.addrs[self.node_index]
    }
}

pub struct Simulation<N: N0de> {
    #[allow(unused)]
    name: String,
    max_rounds: u64,
    round_fn: BoxedRoundFn<N>,
    check_fn: Option<BoxedCheckFn<N>>,
    nodes: Vec<SimNode<N>>,
    addrs: Arc<Vec<NodeAddr>>,
    round: u64,
    sim_client: Option<SimSession>,
}

type BoxedRoundFn<N> = Box<
    dyn for<'a> Fn(&'a Context, &'a N) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>>,
>;

type BoxedCheckFn<N> = Box<dyn Fn(&Context, &N) -> Result<()>>;

pub trait AsyncCallback<'a, N: 'a, T: 'a>:
    'static + Send + Fn(&'a Context, &'a N) -> Self::Fut
{
    type Fut: Future<Output = Result<T>> + Send;
}

impl<'a, N: 'a, T: 'a, Out, F> AsyncCallback<'a, N, T> for F
where
    Out: Send + Future<Output = Result<T>>,
    F: 'static + Send + Fn(&'a Context, &'a N) -> Out,
{
    type Fut = Out;
}

pub struct RoundOutcome {
    round: u64,
    node_id: NodeId,
    node_index: usize,
    start_time: DateTime,
    end_time: DateTime,
    duration: Duration,
}

impl<N: N0de> Simulation<N> {
    pub fn builder<F>(round: F) -> SimulationBuilder<N>
    where
        F: for<'a> AsyncCallback<'a, N, bool>,
    {
        let round_fn: BoxedRoundFn<N> =
            Box::new(move |context: &Context, node: &N| Box::pin(round(context, node)));
        SimulationBuilder::<N> {
            max_rounds: 100,
            nodes: 2,
            round_fn,
            check_fn: None,
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
        while self.round < self.max_rounds {
            self.run_round().await?;
            self.round += 1;
        }
        Ok(())
    }

    async fn run_round(&mut self) -> Result<()> {
        println!("Round {}", self.round);
        let futures = self.nodes.iter().enumerate().map(|(i, node)| {
            let this = &self;
            let start_time = DateTime::now_utc();
            async move {
                let ctx = this.context(i);
                let start = n0_future::time::Instant::now();
                let round = ctx.round;
                (this.round_fn)(&ctx, &node.node)
                    .await
                    .with_context(|| "Node {i} failed in round {round}")?;
                let end_time = DateTime::now_utc();
                anyhow::Ok(RoundOutcome {
                    node_index: i,
                    duration: start.elapsed(),
                    start_time,
                    end_time,
                    round,
                    node_id: ctx.self_addr().node_id,
                })
            }
        });
        let res: Vec<_> = n0_future::FuturesOrdered::from_iter(futures)
            .try_collect()
            .await?;

        if let Some(ref client) = self.sim_client {
            for (outcome, node) in res.into_iter().zip(self.nodes.iter_mut()) {
                client.put_round(outcome, &mut node.encoder).await?;
            }
        }

        println!("Round {} completed", self.round);
        if let Some(check) = &self.check_fn {
            for (i, n) in self.nodes.iter().enumerate() {
                let ctx = self.context(i);
                (check)(&ctx, &n.node)?;
            }
        }
        Ok(())
    }
}

pub struct SimulationBuilder<N: N0de> {
    max_rounds: u64,
    nodes: u32,
    round_fn: BoxedRoundFn<N>,
    check_fn: Option<BoxedCheckFn<N>>,
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
            nodes: count,
            ..self
        }
    }

    pub fn check(self, check: impl Fn(&Context, &N) -> Result<()> + 'static) -> Self {
        Self {
            check_fn: Some(Box::new(check)),
            ..self
        }
    }

    pub async fn build(self, simulation_name: &str) -> Result<Simulation<N>> {
        let nodes: Vec<_> = FuturesUnordered::from_iter(
            (0..self.nodes).into_iter().map(|_i| SimNode::<N>::spawn()),
        )
        .try_collect()
        .await?;

        let addrs: Vec<_> = FuturesUnordered::from_iter(
            nodes
                .iter()
                .map(|n| async { n.endpoint.node_addr().initialized().await }),
        )
        .try_collect()
        .await?;

        let sim_client = client_from_env()?.map(|client| client.session(&simulation_name));

        Ok(Simulation {
            name: simulation_name.to_string(),
            max_rounds: self.max_rounds,
            round_fn: self.round_fn,
            check_fn: self.check_fn,
            nodes,
            addrs: Arc::new(addrs),
            sim_client,
            round: 0,
        })
    }
}

pub(crate) struct SimNode<N: N0de> {
    node: N,
    endpoint: Endpoint,
    encoder: Encoder,
}

impl<N: N0de> SimNode<N> {
    async fn spawn() -> Result<Self> {
        let mut registry = Registry::default();
        let endpoint = Endpoint::builder().bind().await?;
        registry.register_all(endpoint.metrics());
        let node = N::spawn(endpoint.clone(), &mut registry).await?;
        Ok(Self {
            node,
            endpoint,
            encoder: Encoder::new(Arc::new(RwLock::new(registry))),
        })
    }
}

fn client_from_env() -> Result<Option<SimClient>> {
    static CLIENT: OnceLock<Result<SimClient>> = OnceLock::new();

    if let Ok(addr) = std::env::var("N0DES_SIM_SERVER") {
        let client = CLIENT
            .get_or_init(|| SimClient::from_addr_str(&addr))
            .as_ref()
            .map_err(|err| anyhow::anyhow!("failed to init sim client: {err:#}"))?
            .clone();
        Ok(Some(client))
    } else {
        Ok(None)
    }
}

#[doc(hidden)]
pub async fn run_sim_fn<F, Fut, N, E>(name: &str, sim_fn: F) -> anyhow::Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<SimulationBuilder<N>, E>>,
    N: N0de,
    E: Into<anyhow::Error>,
{
    println!("running simulation: {name}");
    let builder = sim_fn().await.map_err(|err| {
        // Not sure why, but anyhow::Error::from doesn't work here.
        let err: anyhow::Error = err.into();
        err
    })?;
    let mut sim = builder.build(name).await?;
    let res = sim.run().await;
    match &res {
        Ok(()) => println!("simulation passed"),
        Err(err) => println!("simulation failed: {err:#}"),
    };
    res?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use iroh::protocol::Router;

    use super::*;
    use iroh_ping::{Ping, ALPN as PingALPN};

    struct PingNode {
        ping: Ping,
        router: Router,
    }

    impl N0de for PingNode {
        async fn spawn(ep: Endpoint, metrics: &mut Registry) -> Result<Self> {
            let ping = Ping::new();
            metrics.register(ping.metrics().clone());

            let router = iroh::protocol::Router::builder(ep)
                .accept(PingALPN, ping.clone())
                .spawn();

            Ok(Self { ping, router })
        }

        async fn shutdown(&mut self) -> Result<()> {
            self.router.shutdown().await?;
            Ok(())
        }
    }

    #[crate::sim]
    async fn test_simulation() -> Result<SimulationBuilder<PingNode>> {
        async fn tick(ctx: &Context, node: &PingNode) -> Result<bool> {
            let me = node.router.endpoint().node_id();
            let other_nodes = ctx.all_other_nodes(me);
            let ping = node.ping.clone();
            let endpoint = node.router.endpoint().clone();
            let node_index = ctx.node_index;

            if node_index % 2 == 0 {
                for other in other_nodes.iter() {
                    println!("Sending message:\n\tfrom: {me}\n\tto:  {}", other.node_id);
                    ping.ping(&endpoint, (other.clone()).into()).await?;
                }
            }
            Ok(true)
        }

        fn check(ctx: &Context, node: &PingNode) -> Result<()> {
            let metrics = node.ping.metrics();
            let node_count = ctx.addrs.len() as u64;
            match ctx.node_index % 2 {
                0 => assert_eq!(metrics.pings_sent.get(), (node_count / 2) * (ctx.round + 1)),
                _ => assert_eq!(metrics.pings_recv.get(), (node_count / 2) * (ctx.round + 1)),
            }
            Ok(())
        }

        Ok(Simulation::builder(tick).max_rounds(2).check(check))
    }
}
