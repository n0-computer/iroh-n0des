use std::{marker::PhantomData, sync::Arc};

use anyhow::Result;
use iroh::{Endpoint, NodeAddr, NodeId, Watcher};
use iroh_metrics::Registry;
use n0_future::boxed::BoxFuture;
use tokio::sync::RwLock;

use crate::n0des::N0de;

/// A registry to collect endpoint metrics for spawned nodes in a simulation
type MetricsRegistry = Arc<RwLock<Registry>>;

/// Simulation context passed to each node
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Context {
    pub round: usize,
    pub node_index: usize,
    pub addrs: Vec<NodeAddr>,
}

impl Context {
    #[allow(dead_code)]
    fn all_other_nodes(&self, me: NodeId) -> Vec<NodeAddr> {
        let mut other_nodes = Vec::new();
        for id in self.addrs.iter() {
            if id.node_id != me {
                other_nodes.push(id.clone());
            }
        }
        other_nodes
    }
}

pub struct Simulation<N: N0de> {
    max_ticks: usize,
    #[allow(clippy::type_complexity)]
    round: Box<dyn Fn(&Context, &N) -> BoxFuture<Result<bool>>>,
    #[allow(clippy::type_complexity)]
    check: Option<Box<dyn Fn(&Context, &N) -> Result<()>>>,
    nodes: Vec<N>,
    context: Context,
}

impl<N: N0de> Simulation<N> {
    #[allow(dead_code)]
    fn builder(
        round: impl Fn(&Context, &N) -> BoxFuture<Result<bool>> + 'static,
    ) -> SimulationBuilder<N> {
        SimulationBuilder::<N> {
            max_ticks: 100,
            nodes: 2,
            round: Box::new(round),
            check: None,
            _node: PhantomData,
        }
    }

    #[allow(dead_code)]
    async fn run(&self) -> Result<()> {
        for round_num in 0..self.max_ticks {
            println!("Round {round_num}");
            for (i, n) in self.nodes.iter().enumerate() {
                let mut ctx = self.context.clone();
                ctx.round = round_num;
                ctx.node_index = i;

                (self.round)(&ctx, n).await?;
            }

            println!("Round {round_num} completed");
            if let Some(check) = &self.check {
                for (i, n) in self.nodes.iter().enumerate() {
                    let mut ctx = self.context.clone();
                    ctx.round = round_num;
                    ctx.node_index = i;

                    (check)(&ctx, n)?;
                }
            }
        }
        Ok(())
    }
}

pub struct SimulationBuilder<N: N0de> {
    max_ticks: usize,
    nodes: u32,
    #[allow(clippy::type_complexity)]
    round: Box<dyn Fn(&Context, &N) -> BoxFuture<Result<bool>>>,
    #[allow(clippy::type_complexity)]
    check: Option<Box<dyn Fn(&Context, &N) -> Result<()>>>,
    _node: PhantomData<N>,
}

impl<N: N0de> SimulationBuilder<N> {
    pub fn max_ticks(self, count: usize) -> Self {
        Self {
            max_ticks: count,
            ..self
        }
    }

    pub fn check(self, check: impl Fn(&Context, &N) -> Result<()> + 'static) -> Self {
        Self {
            check: Some(Box::new(check)),
            ..self
        }
    }

    pub async fn build(self) -> Result<Simulation<N>> {
        let mut nodes = vec![];
        let mut addrs = vec![];

        let registry: MetricsRegistry = Arc::new(RwLock::new(Registry::default()));

        // initialize nodes
        for _ in 0..self.nodes {
            let ep = Endpoint::builder()
                .known_nodes(addrs.clone())
                .bind()
                .await?;
            let addr = ep.node_addr().initialized().await?;
            addrs.push(addr);

            let mut registry = registry.write().await;
            let sub = registry.sub_registry_with_label("node", ep.node_id().to_string());
            sub.register_all(ep.metrics());

            let node = N::spawn(ep, sub).await?;
            nodes.push(node);
        }

        let context = Context {
            round: 0,
            node_index: 0,
            addrs,
        };

        Ok(Simulation {
            max_ticks: self.max_ticks,
            round: self.round,
            check: self.check,
            nodes,
            context,
        })
    }
}

#[cfg(test)]
mod tests {

    use iroh::protocol::Router;

    use super::*;
    use iroh_ping::{Ping, ALPN as PingALPN};

    struct ExampleN0de {
        ping: Ping,
        router: Router,
    }

    impl N0de for ExampleN0de {
        async fn spawn(ep: Endpoint, metrics: &mut Registry) -> Result<Self> {
            let ping = Ping::new();
            metrics.register(ping.metrics().clone());

            let router = iroh::protocol::Router::builder(ep)
                .accept(PingALPN, ping.clone())
                .spawn();

            Ok(Self { ping, router })
        }

        async fn shutdown(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_simulation() {
        let tick = |ctx: &Context, node: &ExampleN0de| -> BoxFuture<Result<bool>> {
            let me = node.router.endpoint().node_id();
            let other_nodes = ctx.all_other_nodes(me);
            let ping = node.ping.clone();
            let endpoint = node.router.endpoint().clone();
            let node_index = ctx.node_index;

            Box::pin(async move {
                if node_index % 2 == 0 {
                    for other in other_nodes.iter() {
                        println!("Sending message:\n\tfrom: {me}\n\t to:   {}", other.node_id);
                        ping.ping(&endpoint, (other.clone()).into()).await?;
                    }
                }
                Ok(true)
            })
        };

        let check = |ctx: &Context, node: &ExampleN0de| -> Result<()> {
            let metrics = node.ping.metrics();
            let node_count = ctx.addrs.len() as u64;
            match ctx.node_index % 2 {
                0 => assert_eq!(metrics.pings_sent.get(), node_count / 2),
                _ => assert_eq!(metrics.pings_recv.get(), node_count / 2),
            }
            Ok(())
        };

        let sim: Simulation<ExampleN0de> = Simulation::builder(tick)
            .max_ticks(1) // currently stuck on one tick, because more ends with an "endpoint closing" error
            .check(check)
            .build()
            .await
            .unwrap();

        sim.run().await.unwrap();
    }
}
