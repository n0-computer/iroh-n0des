// #[derive(Clone, Debug)]
// struct Coordinator;

// #[derive(Clone, Debug)]
// struct Node;

// struct SimulationBuilder {}

use std::{
    any::Any,
    pin::Pin,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh::Watcher;
use iroh_metrics::encoding::Encoder;
use iroh_n0des::{
    Registry,
    iroh::{Endpoint, NodeId, SecretKey},
    simulation::proto::{ActiveTrace, NodeInfo, ScopeInfo, TraceClient, TraceInfo},
};
use n0_future::FuturesUnordered;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::task::JoinSet;

pub const ENV_TRACE_ISOLATED: &str = "N0DES_TRACE_ISOLATED";
pub const ENV_TRACE_INIT_ONLY: &str = "N0DES_TRACE_INIT_ONLY";
pub const ENV_TRACE_SERVER: &str = "N0DES_TRACE_SERVER";
pub const ENV_TRACE_SESSION_ID: &str = "N0DES_SESSION_ID";

pub trait UserData:
    Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug + 'static
{
}
impl<T> UserData for T where
    T: Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug + 'static
{
}

pub struct SpawnContext<'a, D> {
    secret_key: SecretKey,
    node_index: u32,
    data: &'a D,
    registry: &'a mut Registry,
}

impl<'a, D: UserData> SpawnContext<'a, D> {
    pub fn node_index(&self) -> u32 {
        self.node_index
    }

    pub fn user_data(&self) -> &D {
        self.data
    }

    pub fn metrics_registry(&mut self) -> &mut Registry {
        self.registry
    }

    pub fn secret_key(&self) -> SecretKey {
        self.secret_key.clone()
    }

    pub async fn bind_endpoint(&self) -> Result<Endpoint> {
        let ep = Endpoint::builder()
            .discovery_n0()
            .secret_key(self.secret_key())
            .bind()
            .await?;
        Ok(ep)
    }
}

pub struct RoundContext<'a, D> {
    round: u32,
    node_index: u32,
    data: &'a D,
}

pub trait Spawn<D> {
    type Node: Node;
    fn spawn(context: &mut SpawnContext<'_, D>) -> impl Future<Output = Result<Self::Node>> + Send
    where
        Self: Sized;
}

// impl<F, N: Node, D> Spawn<D> for F
// where
//     F: Fn(Endpoint, SpawnContext<'_, D>) -> N,
// {
//     type Node = N;

//     async fn spawn(endpoint: Endpoint, context: SpawnContext<'_, D>) -> Result<Self::Node>
//     where
//         Self: Sized,
//     {
//         F(endpoint, context).await
//     }
// }

pub trait Node: Send + 'static {
    fn endpoint(&self) -> Option<&Endpoint> {
        None
    }
    fn shutdown(&self) -> impl Future<Output = ()> + Send + '_ {
        async {}
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

type BoxNode = Box<dyn DynNode>;

trait DynNode: Send + Any + 'static {
    fn shutdown(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn endpoint(&self) -> Option<&Endpoint> {
        None
    }

    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Node + Sized> DynNode for T {
    fn shutdown(&self) -> BoxFuture<'_, ()> {
        Box::pin(<Self as Node>::shutdown(self))
    }

    fn endpoint(&self) -> Option<&Endpoint> {
        <Self as Node>::endpoint(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive()]
pub struct Builder<D> {
    setup_fn: BoxedSetupFn<D>,
    spawners: Vec<Spawner<D>>,
    rounds: u32,
}

#[derive(Clone)]
struct Spawner<D> {
    count: u32,
    spawn_fn: BoxedSpawnFn<D>,
    round_fn: BoxedRoundFn<D>,
}

#[derive(Default)]
enum NodeState<D> {
    Unspawned(BoxedSpawnFn<D>),
    Spawned(BoxNode),
    #[default]
    Placeholder,
}

struct SimNode<D> {
    node: BoxNode,
    idx: u32,
    round_fn: BoxedRoundFn<D>,
    round: u32,
    info: NodeInfo,
    metrics_encoder: Encoder,
}

impl<D: UserData> SimNode<D> {
    // async fn with_client(&self, f: impl AsyncFn(&ActiveTrace) -> Result<()>) -> Result<()> {
    //     if let Some(client) = self.client.as_ref() {
    //         (f)(client).await
    //     } else {
    //         Ok(())
    //     }
    // }

    async fn spawn_and_run(
        spawner: Spawner<D>,
        name: String,
        node_index: u32,
        data: &D,
        client: Option<TraceClient>,
        rounds: u32,
    ) -> Result<()> {
        let mut node = Self::spawn(spawner, node_index, data).await?;
        let scope = ScopeInfo::Isolated(node.info.clone());
        let client = match client {
            Some(client) => Some(client.start_trace(name, scope).await?),
            None => None,
        };
        node.run(client.as_ref(), data, rounds).await?;
        Ok(())
    }

    async fn spawn(spawner: Spawner<D>, node_index: u32, data: &D) -> Result<Self> {
        tracing::info!(idx = node_index, "run start");
        let mut registry = Registry::default();
        let mut context = SpawnContext {
            data,
            node_index,
            secret_key: SecretKey::generate(&mut rand::rngs::OsRng),
            registry: &mut registry,
        };
        let node_id = context.secret_key().public();
        let node = (spawner.spawn_fn)(&mut context).await?;
        if let Some(endpoint) = node.endpoint() {
            registry.register_all(endpoint.metrics());
        }

        let info = NodeInfo {
            node_id,
            idx: node_index,
            label: None,
        };
        Ok(Self {
            node,
            idx: node_index,
            info,
            round: 0,
            round_fn: spawner.round_fn,
            metrics_encoder: Encoder::new(Arc::new(RwLock::new(registry))),
        })
    }

    async fn run(&mut self, client: Option<&ActiveTrace>, data: &D, rounds: u32) -> Result<()> {
        tracing::info!(idx = self.idx, "run");
        while self.round < rounds {
            self.run_round(client, data).await?;
            self.round += 1;
        }
        Ok(())
    }

    fn node_id(&self) -> Option<NodeId> {
        self.node.endpoint().map(|e| e.node_id())
    }

    async fn run_round(&mut self, client: Option<&ActiveTrace>, data: &D) -> Result<()> {
        tracing::info!(idx = self.idx, "run");
        let context = RoundContext {
            round: self.round,
            node_index: self.idx,
            data,
        };

        let result = (self.round_fn)(&mut self.node, &context).await;

        if let Some(client) = client {
            let checkpoint = (context.round + 1) as u64;
            let result = to_str_err(&result);
            let label = format!("Round {} end", context.round);
            client
                .put_checkpoint(checkpoint, self.idx, Some(label), result)
                .await?;
            if let Some(node_id) = self.node_id() {
                let metrics = self.metrics_encoder.export();
                client
                    .put_metrics(node_id, Some(checkpoint), metrics)
                    .await?;
            }
        }

        if let Err(err) = result {
            return Err(err);
        }
        Ok(())
    }
}

type BoxedSetupFn<D> = Box<dyn 'static + Send + Sync + FnOnce() -> BoxFuture<'static, Result<D>>>;

type BoxedSpawnFn<D> = Arc<
    dyn 'static
        + Send
        + Sync
        + for<'a> Fn(&'a mut SpawnContext<'a, D>) -> BoxFuture<'a, Result<BoxNode>>,
>;
type BoxedRoundFn<D> = Arc<
    dyn 'static
        + Send
        + Sync
        + for<'a> Fn(&'a mut BoxNode, &'a RoundContext<'a, D>) -> BoxFuture<'a, Result<()>>,
>;

pub trait AsyncCallback<'a, A1: 'a, A2: 'a, T: 'a>:
    'static + Send + Sync + Fn(&'a mut A1, &'a A2) -> Self::Fut
{
    type Fut: Future<Output = T> + Send;
}

impl<'a, A1: 'a, A2: 'a, T: 'a, Out, F> AsyncCallback<'a, A1, A2, T> for F
where
    Out: Send + Future<Output = T>,
    F: 'static + Sync + Send + Fn(&'a mut A1, &'a A2) -> Out,
{
    type Fut = Out;
}

impl<D: UserData> Builder<D> {
    pub fn new() -> Builder<()> {
        let setup_fn: BoxedSetupFn<()> = Box::new(move || Box::pin(async move { Ok(()) }));
        Builder {
            spawners: Vec::new(),
            setup_fn,
            rounds: 0,
        }
    }

    pub fn with_setup<F, Fut>(setup_fn: F) -> Builder<D>
    where
        F: 'static + Send + Sync + FnOnce() -> Fut,
        Fut: 'static + Send + Future<Output = Result<D>>,
    {
        let setup_fn: BoxedSetupFn<D> = Box::new(move || Box::pin(setup_fn()));
        Builder {
            spawners: Vec::new(),
            setup_fn,
            rounds: 0,
        }
    }

    pub fn rounds(mut self, rounds: u32) -> Self {
        self.rounds = rounds;
        self
    }

    pub fn spawn<S: Spawn<D>>(
        mut self,
        count: u32,
        round_fn: impl for<'a> AsyncCallback<'a, S::Node, RoundContext<'a, D>, Result<()>>,
    ) -> Self {
        let spawn_fn: BoxedSpawnFn<D> = Arc::new(|context: &mut SpawnContext<'_, D>| {
            let fut = Box::pin(async move {
                let node = S::spawn(context).await?;
                let node: Box<dyn DynNode> = Box::new(node);
                anyhow::Ok(node)
            });
            let fut: BoxFuture<'_, Result<BoxNode>> = fut;
            fut
        });
        let round_fn: BoxedRoundFn<D> = Arc::new(
            move |node: &'_ mut BoxNode, context: &'_ RoundContext<'_, D>| {
                let node = node.as_any_mut();
                let node = node
                    .downcast_mut::<S::Node>()
                    .expect("unreachable: type is statically guaranteed");
                let fut: BoxFuture<'_, Result<()>> = Box::pin(round_fn(node, context));
                let fut: BoxFuture<'_, Result<()>> = fut;
                fut
            },
        );
        self.spawners.push(Spawner {
            count,
            spawn_fn,
            round_fn,
        });
        self
    }

    pub async fn build(self, name: &str) -> Result<Simulation<D>> {
        let client = TraceClient::from_env()?;
        let run_mode = RunMode::from_env()?;
        let name = name.to_string();

        let node_count = self.spawners.iter().map(|spawner| spawner.count).sum();

        let data = (self.setup_fn)().await?;

        if matches!(run_mode, RunMode::InitOnly | RunMode::Integrated) {
            let user_data = Bytes::from(postcard::to_stdvec(&data)?);
            if let Some(client) = &client {
                client
                    .init_trace(
                        TraceInfo {
                            name: name.clone(),
                            expected_nodes: Some(node_count),
                            expected_checkpoints: Some(self.rounds as u64),
                        },
                        Some(user_data),
                    )
                    .await?;
            }
        }

        let mut spawners = self
            .spawners
            .into_iter()
            .flat_map(|spawner| (0..spawner.count).map(move |_| spawner.clone()))
            .enumerate()
            .map(|(idx, spawner)| (idx as u32, spawner));

        let spawners: Vec<_> = match run_mode {
            RunMode::InitOnly => vec![],
            RunMode::Integrated => spawners.collect(),
            RunMode::Isolated(idx) => vec![
                spawners
                    .nth(idx as usize)
                    .context("invalid isolated index")?,
            ],
        };

        Ok(Simulation {
            name: name.to_string(),
            data,
            max_rounds: self.rounds,
            spawners,
            client,
            run_mode,
        })
    }
}

pub struct Simulation<D> {
    name: String,
    data: D,
    max_rounds: u32,
    spawners: Vec<(u32, Spawner<D>)>,
    client: Option<TraceClient>,
    run_mode: RunMode,
}

impl<D: UserData> Simulation<D> {
    pub async fn run(self) {
        let futs = self.spawners.into_iter().map(|(idx, spawner)| {
            let data = self.data.clone();
            let name = self.name.clone();
            let client = self.client.clone();
            let max_rounds = self.max_rounds;
            async move {
                let run_fut = SimNode::spawn_and_run(spawner, name, idx, &data, client, max_rounds);
                (idx, run_fut.await)
            }
        });
        let mut tasks = JoinSet::from_iter(futs);
        while let Some(res) = tasks.join_next().await {
            let (idx, res) = res.expect("task panicked");
            tracing::info!(%idx, ?res, "node complete");
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum RunMode {
    InitOnly,
    Integrated,
    Isolated(u32),
}

impl RunMode {
    fn from_env() -> Result<Self> {
        if std::env::var(ENV_TRACE_INIT_ONLY).is_ok() {
            Ok(Self::InitOnly)
        } else {
            match std::env::var(ENV_TRACE_ISOLATED) {
                Err(_) => Ok(Self::Integrated),
                Ok(s) => {
                    let idx = s.parse().with_context(|| {
                        format!("Failed to parse env var `{ENV_TRACE_ISOLATED}` as number")
                    })?;
                    Ok(Self::Isolated(idx))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use iroh_n0des::iroh::Endpoint;
    use serde::{Deserialize, Serialize};

    use crate::{Builder, Node, RoundContext, Spawn, SpawnContext};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct Data {
        topic_id: u64,
    }

    #[derive(Debug)]
    struct BootstrapNode;

    impl Node for BootstrapNode {}

    impl Spawn<Data> for BootstrapNode {
        type Node = Self;
        async fn spawn(_context: &mut SpawnContext<'_, Data>) -> Result<Self> {
            Ok(BootstrapNode)
        }
    }

    #[derive(Debug)]
    struct ClientNode {
        endpoint: Endpoint,
    }

    impl Node for ClientNode {
        fn endpoint(&self) -> Option<&Endpoint> {
            Some(&self.endpoint)
        }
    }

    impl Spawn<Data> for ClientNode {
        type Node = Self;
        async fn spawn(context: &mut SpawnContext<'_, Data>) -> Result<Self> {
            let endpoint = context.bind_endpoint().await?;
            Ok(ClientNode { endpoint })
        }
    }

    async fn setup() -> anyhow::Result<Builder<Data>> {
        async fn round_bootstrap(
            node: &mut BootstrapNode,
            context: &RoundContext<'_, Data>,
        ) -> Result<()> {
            tokio::time::sleep(Duration::from_millis(500)).await;
            tracing::debug!(
                "bootstrap node {} round {}",
                context.node_index,
                context.round
            );
            Ok(())
        }
        async fn round_client(
            node: &mut ClientNode,
            context: &RoundContext<'_, Data>,
        ) -> Result<()> {
            tokio::time::sleep(Duration::from_millis(200)).await;
            tracing::debug!("client node {} round {}", context.node_index, context.round);
            Ok(())
        }
        let builder = Builder::with_setup(async || Ok(Data { topic_id: 22 }))
            .spawn::<BootstrapNode>(2, round_bootstrap)
            .spawn::<ClientNode>(8, round_client)
            .rounds(4);

        Ok(builder)
    }

    #[tokio::test]
    async fn smoke() -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();
        tracing::warn!("start");
        let builder = setup().await?;
        let sim = builder.build("smoke").await?;
        sim.run().await;
        tracing::warn!("end");
        Ok(())
    }
}

// struct Builder;

// impl Builder {
//     fn phase
// }

fn to_str_err<T, E: std::fmt::Display>(res: &Result<T, E>) -> Result<(), String> {
    if let Some(err) = res.as_ref().err() {
        Err(err.to_string())
    } else {
        Ok(())
    }
}
