use std::{
    any::Any,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result};
use bytes::Bytes;
#[cfg(not(feature = "iroh_v035"))]
use iroh::Watcher;
use iroh_metrics::encoding::Encoder;
use iroh_n0des::{
    Registry,
    iroh::{Endpoint, NodeAddr, NodeId, SecretKey},
    simulation::proto::{ActiveTrace, NodeInfo, ScopeInfo, TraceClient, TraceInfo},
};
use n0_future::IterExt;
use proto::NodeInfoWithAddr;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Semaphore;
use tracing::info;

pub mod events;
pub mod proto;
pub mod trace;

/// Environment variable name for running a single isolated node by index.
pub const ENV_TRACE_ISOLATED: &str = "N0DES_TRACE_ISOLATED";
/// Environment variable name for initialization-only mode.
pub const ENV_TRACE_INIT_ONLY: &str = "N0DES_TRACE_INIT_ONLY";
/// Environment variable name for the trace server address.
pub const ENV_TRACE_SERVER: &str = "N0DES_TRACE_SERVER";
/// Environment variable name for the simulation session ID.
pub const ENV_TRACE_SESSION_ID: &str = "N0DES_SESSION_ID";

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
        + for<'a> Fn(&'a mut BoxNode, &'a RoundContext<'a, D>) -> BoxFuture<'a, Result<bool>>,
>;

type BoxedCheckFn<D> = Arc<dyn Fn(&BoxNode, &RoundContext<'_, D>) -> Result<()>>;

/// Helper trait for async functions.
///
/// This is needed because with a simple `impl Fn() -> Fut`, we can't
/// express a variadic lifetime bound from the future to the function parameter.
/// `impl AsyncFn` would allow this, but that doesn't allow to express a `Send`
/// bound on the future.
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

/// Trait for user-defined setup data that can be shared across simulation nodes.
///
/// User data must be serializable, deserializable, cloneable, and thread-safe
/// to be distributed across simulation nodes.
pub trait UserData:
    Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug + 'static
{
}
impl<T> UserData for T where
    T: Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug + 'static
{
}

/// Context provided when spawning a new simulation node.
///
/// Contains all the necessary information and resources for initializing
/// a node, including its index, the shared setup data, and a metrics registry.
pub struct SpawnContext<'a, D = ()> {
    secret_key: SecretKey,
    node_index: u32,
    data: &'a D,
    registry: &'a mut Registry,
}

impl<'a, D: UserData> SpawnContext<'a, D> {
    /// Returns the index of this node in the simulation.
    pub fn node_index(&self) -> u32 {
        self.node_index
    }

    /// Returns a reference to the setup data for this simulation.
    pub fn user_data(&self) -> &D {
        self.data
    }

    /// Returns a mutable reference to a metrics registry.
    ///
    /// Use this to register custom metrics for the node being spawned.
    pub fn metrics_registry(&mut self) -> &mut Registry {
        self.registry
    }

    /// Returns the secret key for this node.
    pub fn secret_key(&self) -> SecretKey {
        self.secret_key.clone()
    }

    /// Returns the node id of this node.
    pub fn node_id(&self) -> NodeId {
        self.secret_key.public()
    }

    /// Creates and binds a new endpoint with this node's secret key.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint fails to bind to a local address.
    pub async fn bind_endpoint(&self) -> Result<Endpoint> {
        let ep = Endpoint::builder()
            .discovery_n0()
            .secret_key(self.secret_key())
            .bind()
            .await?;
        Ok(ep)
    }
}

/// Context provided during each simulation round.
///
/// Contains information about the current round, this node's identity,
/// the shared setup data, and the addresses of all participating nodes.
pub struct RoundContext<'a, D = ()> {
    round: u32,
    node_index: u32,
    data: &'a D,
    all_nodes: &'a Vec<NodeInfoWithAddr>,
}

impl<'a, D> RoundContext<'a, D> {
    /// Returns the current round number.
    pub fn round(&self) -> u32 {
        self.round
    }

    /// Returns the index of this node in the simulation.
    pub fn node_index(&self) -> u32 {
        self.node_index
    }

    /// Returns a reference to the shared setup data for this simulation.
    pub fn user_data(&self) -> &D {
        self.data
    }

    /// Returns an iterator over the addresses of all nodes except the specified one.
    pub fn all_other_nodes(&self, me: NodeId) -> impl Iterator<Item = &NodeAddr> + '_ {
        self.all_nodes
            .iter()
            .filter(move |n| n.info.node_id != me)
            .map(|n| &n.addr)
    }

    /// Returns the address of the node with the given index.
    ///
    /// # Errors
    ///
    /// Returns an error if no node with the specified index exists.
    pub fn addr(&self, idx: u32) -> Result<NodeAddr> {
        Ok(self
            .all_nodes
            .iter()
            .find(|n| n.info.idx == idx)
            .cloned()
            .context("node not found")?
            .addr)
    }

    /// Returns the address of this node.
    ///
    /// # Panics
    ///
    /// Panics if this node's address is not found in the node list.
    pub fn self_addr(&self) -> &NodeAddr {
        if let Some(addr) = self
            .all_nodes
            .iter()
            .find(|n| n.info.idx == self.node_index)
        {
            &addr.addr
        } else {
            panic!("expected self address to be present")
        }
    }

    /// Returns the total number of nodes participating in the simulation.
    pub fn node_count(&self) -> usize {
        self.all_nodes.len()
    }
}

/// Trait for types that can be spawned as simulation nodes.
pub trait Spawn<D: UserData = ()>: Node + 'static {
    /// Spawns a new instance of this node type.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to initialize properly.
    fn spawn(context: &mut SpawnContext<'_, D>) -> impl Future<Output = Result<Self>> + Send
    where
        Self: Sized;

    /// Spawns a new instance as a dynamically-typed node.
    ///
    /// This calls `spawn` and boxes the result.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to initialize properly.
    fn spawn_dyn<'a>(context: &'a mut SpawnContext<'a, D>) -> BoxFuture<'a, Result<BoxNode>>
    where
        Self: Sized,
    {
        Box::pin(async move {
            let node = Self::spawn(context).await?;
            let node: Box<dyn DynNode> = Box::new(node);
            anyhow::Ok(node)
        })
    }

    /// Creates a new builder for this node type with the given round function.
    ///
    /// The round function will be called each simulation round and should return
    /// `Ok(true)` to continue or `Ok(false)` to stop early.
    fn builder(
        round_fn: impl for<'a> AsyncCallback<'a, Self, RoundContext<'a, D>, Result<bool>>,
    ) -> NodeBuilder<Self, D>
    where
        Self: Sized,
    {
        NodeBuilder::new(round_fn)
    }
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

/// Trait for simulation node implementations.
///
/// Provides basic functionality for nodes including optional endpoint access
/// and cleanup on shutdown.
pub trait Node: Send + 'static {
    /// Returns a reference to this node's endpoint, if any.
    ///
    /// The default implementation returns `None`.
    fn endpoint(&self) -> Option<&Endpoint> {
        None
    }

    /// Shuts down this node, performing any necessary cleanup.
    ///
    /// The default implementation does nothing and returns success.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown fails.
    fn shutdown(&mut self) -> impl Future<Output = Result<()>> + Send + '_ {
        async { Ok(()) }
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed dynamically-typed simulation node.
pub type BoxNode = Box<dyn DynNode>;

/// Trait for dynamically-typed simulation nodes.
///
/// This trait enables type erasure for nodes while preserving essential
/// functionality like shutdown, endpoint access, and type casting.
pub trait DynNode: Send + Any + 'static {
    /// Shuts down this node, performing any necessary cleanup.
    ///
    /// The default implementation does nothing and returns success.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown fails.
    fn shutdown(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    /// Returns a reference to this node's endpoint, if any.
    ///
    /// The default implementation returns `None`.
    fn endpoint(&self) -> Option<&Endpoint> {
        None
    }

    /// Returns a reference to this node as `Any` for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns a mutable reference to this node as `Any` for downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Node + Sized> DynNode for T {
    fn shutdown(&mut self) -> BoxFuture<'_, Result<()>> {
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
/// Builder for constructing simulation configurations.
///
/// Allows configuring the setup function, node spawners, and number of rounds
/// before building the final simulation.
pub struct Builder<D = ()> {
    setup_fn: BoxedSetupFn<D>,
    spawners: Vec<(u32, ErasedNodeBuilder<D>)>,
    rounds: u32,
}

#[derive(Clone)]
/// Builder for configuring individual nodes in a simulation.
///
/// Provides methods to set up spawn functions, round functions, and optional
/// check functions for a specific node type.
pub struct NodeBuilder<N, D> {
    phantom: PhantomData<N>,
    spawn_fn: BoxedSpawnFn<D>,
    round_fn: BoxedRoundFn<D>,
    check_fn: Option<BoxedCheckFn<D>>,
}

#[derive(Clone)]
struct ErasedNodeBuilder<D> {
    spawn_fn: BoxedSpawnFn<D>,
    round_fn: BoxedRoundFn<D>,
    check_fn: Option<BoxedCheckFn<D>>,
}

impl<N: Spawn<D>, D: UserData> NodeBuilder<N, D> {
    /// Creates a new node builder with the given round function.
    ///
    /// The round function will be called each simulation round and should return
    /// `Ok(true)` to continue or `Ok(false)` to stop early.
    pub fn new(
        round_fn: impl for<'a> AsyncCallback<'a, N, RoundContext<'a, D>, Result<bool>>,
    ) -> Self {
        let spawn_fn: BoxedSpawnFn<D> = Arc::new(N::spawn_dyn);
        let round_fn: BoxedRoundFn<D> = Arc::new(move |node, context| {
            let node = node
                .as_any_mut()
                .downcast_mut::<N>()
                .expect("unreachable: type is statically guaranteed");
            Box::pin(round_fn(node, context))
        });
        Self {
            phantom: PhantomData,
            spawn_fn,
            round_fn,
            check_fn: None,
        }
    }

    /// Adds a check function that will be called after each round.
    ///
    /// The check function can verify node state and return an error to fail
    /// the simulation if invariants are violated.
    ///
    /// # Errors
    ///
    /// The check function should return an error if validation fails.
    pub fn check(
        mut self,
        check_fn: impl 'static + for<'a> Fn(&'a N, &RoundContext<'a, D>) -> Result<()>,
    ) -> Self {
        let check_fn: BoxedCheckFn<D> = Arc::new(move |node, context| {
            let node = node
                .as_any()
                .downcast_ref::<N>()
                .expect("unreachable: type is statically guaranteed");
            check_fn(node, context)
        });
        self.check_fn = Some(check_fn);
        self
    }

    fn erase(self) -> ErasedNodeBuilder<D> {
        ErasedNodeBuilder {
            spawn_fn: self.spawn_fn,
            round_fn: self.round_fn,
            check_fn: self.check_fn,
        }
    }
}

struct SimNode<D> {
    node: BoxNode,
    idx: u32,
    round_fn: BoxedRoundFn<D>,
    check_fn: Option<BoxedCheckFn<D>>,
    round: u32,
    info: NodeInfo,
    metrics_encoder: Encoder,
    all_nodes: Vec<NodeInfoWithAddr>,
}

impl<D: UserData> SimNode<D> {
    // async fn spawn_and_run(
    //     spawner: NodeBuilder<D>,
    //     name: String,
    //     node_index: u32,
    //     data: &D,
    //     client: Option<TraceClient>,
    //     rounds: u32,
    // ) -> Result<()> {
    //     let mut node = Self::spawn(spawner, node_index, data).await?;
    //     let scope = ScopeInfo::Isolated(node.info.clone());
    //     let client = match client {
    //         Some(client) => Some(client.start_trace(name, scope).await?),
    //         None => None,
    //     };
    //     node.run(client.as_ref(), data, rounds).await?;
    //     Ok(())
    // }

    async fn spawn(builder: ErasedNodeBuilder<D>, node_index: u32, data: &D) -> Result<Self> {
        info!(idx = node_index, "run start");
        let mut registry = Registry::default();
        let mut context = SpawnContext {
            data,
            node_index,
            secret_key: SecretKey::generate(&mut rand::rngs::OsRng),
            registry: &mut registry,
        };
        let node_id = context.node_id();
        let node = (builder.spawn_fn)(&mut context).await?;

        // TODO(Frando): enable metrics support for iroh 0.35
        // iroh@0.35 uses iroh-metrics@0.34, which is a major version down from iroh-metrics@0.35
        // which is used here, thus the traits are incompatible.
        // Add shim to iroh-metrics@0.35 to support registering metrics from iroh-metrics@0.34
        #[cfg(not(feature = "iroh_v035"))]
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
            round_fn: builder.round_fn,
            check_fn: builder.check_fn,
            metrics_encoder: Encoder::new(Arc::new(RwLock::new(registry))),
            all_nodes: Default::default(),
        })
    }

    async fn run(&mut self, client: &ActiveTrace, data: &D, rounds: u32) -> Result<()> {
        info!(idx = self.idx, "run");

        let info = NodeInfoWithAddr {
            addr: self.my_addr().await,
            info: self.info.clone(),
        };
        let all_nodes = client.wait_start(info).await?;
        self.all_nodes = all_nodes;

        let result = self.run_rounds(client, data, rounds).await;

        client.end_trace(to_str_err(&result)).await.ok();

        result
    }

    async fn run_rounds(&mut self, client: &ActiveTrace, data: &D, rounds: u32) -> Result<()> {
        while self.round < rounds {
            self.run_round(client, data)
                .await
                .with_context(|| format!("failed at round {}", self.round))?;
            self.round += 1;
        }
        Ok(())
    }

    async fn run_round(&mut self, client: &ActiveTrace, data: &D) -> Result<()> {
        info!(idx = self.idx, "run");
        let context = RoundContext {
            round: self.round,
            node_index: self.idx,
            data,
            all_nodes: &self.all_nodes,
        };

        let result = (self.round_fn)(&mut self.node, &context).await;

        let checkpoint = (context.round + 1) as u64;
        let label = format!("Round {} end", context.round);
        client
            .put_checkpoint(checkpoint, self.idx, Some(label), to_str_err(&result))
            .await?;
        client
            .put_metrics(
                self.node_id(),
                Some(checkpoint),
                self.metrics_encoder.export(),
            )
            .await?;

        client.wait_checkpoint(checkpoint).await?;

        if let Some(check_fn) = self.check_fn.as_ref() {
            (check_fn)(&self.node, &context).with_context(|| "check function failed")?;
        }

        result.map(|_| ())
    }

    fn node_id(&self) -> NodeId {
        self.info.node_id
    }

    async fn my_addr(&self) -> NodeAddr {
        let mut addr = NodeAddr::new(self.node_id());
        if let Some(endpoint) = self.node.endpoint() {
            addr = node_addr(endpoint).await;
        }
        addr
    }
}

async fn node_addr(endpoint: &Endpoint) -> NodeAddr {
    #[cfg(feature = "iroh_v035")]
    let addr = endpoint
        .node_addr()
        .await
        .expect("node_addr mustn't fail unless endpoint has shut down");
    #[cfg(not(feature = "iroh_v035"))]
    let addr = {
        endpoint.direct_addresses().initialized().await;
        endpoint.home_relay().initialized().await;
        endpoint.node_addr().initialized().await
    };
    addr
}

impl Default for Builder<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder<()> {
    /// Creates a new simulation builder with empty setup data.
    pub fn new() -> Builder<()> {
        let setup_fn: BoxedSetupFn<()> = Box::new(move || Box::pin(async move { Ok(()) }));
        Builder {
            spawners: Vec::new(),
            setup_fn,
            rounds: 0,
        }
    }
}
impl<D: UserData> Builder<D> {
    /// Creates a new simulation builder with a setup function for user data.
    ///
    /// The setup function is called once before the simulation starts to
    /// initialize the user data that will be shared across all nodes.
    ///
    /// # Errors
    ///
    /// The setup function should return an error if initialization fails.
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

    /// Sets the number of rounds this simulation will run.
    pub fn rounds(mut self, rounds: u32) -> Self {
        self.rounds = rounds;
        self
    }

    /// Adds a group of nodes to spawn in this simulation.
    ///
    /// Each node will be created using the provided node builder configuration.
    pub fn spawn<N: Spawn<D>>(mut self, node_count: u32, node_builder: NodeBuilder<N, D>) -> Self {
        self.spawners.push((node_count, node_builder.erase()));
        self
    }

    // pub fn spawn<S: Spawn<D>>(
    //     mut self,
    //     count: u32,
    //     round_fn: impl for<'a> AsyncCallback<'a, S::Node, RoundContext<'a, D>, Result<()>>,
    //     check_fn: Option<
    //         impl 'static + for<'a> Fn(&'a S::Node, &RoundContext<'a, D>) -> Result<()>,
    //     >,
    // ) -> Self {
    //     let spawner = NodeBuilder::new::<S>(round_fn, check_fn);
    //     self.spawners.push((count, spawner));
    //     self
    // }

    /// Builds the final simulation from this configuration.
    ///
    /// This method initializes tracing, runs the setup function, and prepares
    /// all nodes for execution based on the current run mode.
    ///
    /// # Errors
    ///
    /// Returns an error if setup fails, tracing initialization fails, or
    /// the configuration is invalid for the current run mode.
    pub async fn build(self, name: &str) -> Result<Simulation<D>> {
        let client = TraceClient::from_env_or_local()?;
        let run_mode = RunMode::from_env()?;
        let name = name.to_string();

        let node_count = self.spawners.iter().map(|(count, _spawner)| count).sum();

        let data = (self.setup_fn)().await?;

        if matches!(run_mode, RunMode::InitOnly | RunMode::Integrated) {
            let user_data = Bytes::from(postcard::to_stdvec(&data)?);
            client
                .init_trace(
                    TraceInfo {
                        name: name.clone(),
                        node_count,
                        expected_checkpoints: Some(self.rounds as u64),
                    },
                    Some(user_data),
                )
                .await?;
        }

        let mut spawners = self
            .spawners
            .into_iter()
            .flat_map(|(count, spawner)| (0..count).map(move |_| spawner.clone()))
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
        })
    }
}

/// A configured simulation ready to run.
///
/// Contains all the necessary components including user data, node spawners,
/// and tracing client to execute a simulation run.
pub struct Simulation<D> {
    name: String,
    data: D,
    max_rounds: u32,
    spawners: Vec<(u32, ErasedNodeBuilder<D>)>,
    client: TraceClient,
}

impl<D: UserData> Simulation<D> {
    /// Runs this simulation to completion.
    ///
    /// Spawns all configured nodes concurrently and executes the specified
    /// number of simulation rounds.
    ///
    /// # Errors
    ///
    /// Returns an error if any node fails to spawn or if any round fails to execute.
    pub async fn run(self) -> Result<()> {
        // Spawn all nodes concurrently.
        let mut nodes: Vec<_> = self
            .spawners
            .into_iter()
            .map(async |(idx, spawner)| SimNode::spawn(spawner, idx, &self.data).await)
            .try_join_all()
            .await?;

        // Run all nodes concurrently.
        nodes
            .iter_mut()
            .map(async |node| {
                let idx = node.idx;
                let scope = ScopeInfo::Isolated(node.info.clone());
                let (client, data) = self.client.start_trace(self.name.clone(), scope).await?;
                let data = data.context("expected user data to be set, but wasn't")?;
                let data: D = postcard::from_bytes(&data).context("failed to decode user data")?;
                node.run(&client, &data, self.max_rounds)
                    .await
                    .with_context(|| format!("node {idx} failed"))
            })
            .try_join_all()
            .await?;

        Ok(())
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

static PERMIT: Semaphore = Semaphore::const_new(1);

/// Runs a simulation function with proper setup and cleanup.
///
/// This function handles tracing initialization, sequential execution (via semaphore),
/// log management, and error reporting for simulation functions.
///
/// # Errors
///
/// Returns an error if the simulation function fails, the builder fails,
/// or the simulation execution fails.
#[doc(hidden)]
pub async fn run_sim_fn<F, Fut, D, E>(name: &str, sim_fn: F) -> anyhow::Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Builder<D>, E>>,
    D: UserData,
    anyhow::Error: From<E>,
{
    // Ensure simulations run sequentially so that we can extract logs properly.
    let permit = PERMIT.acquire().await.expect("semaphore closed");

    // Init the global tracing subscriber.
    self::trace::init();
    // Clear remaining logs from previous runs.
    self::trace::global_writer().clear();

    eprintln!("running simulation: {name}");
    let result = sim_fn()
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| format!("simulation builder function `{name}` failed"))?
        .build(name)
        .await
        .with_context(|| format!("simulation `{name}` failed to build"))?
        .run()
        .await;

    match &result {
        Ok(()) => eprintln!("simulation `{name}` passed"),
        Err(err) => eprintln!("simulation `{name}` failed: {err:#}"),
    };
    result.with_context(|| format!("simulation `{name}` failed to run"))?;

    drop(permit);
    Ok(())
}

fn to_str_err<T, E: std::fmt::Display>(res: &Result<T, E>) -> Result<(), String> {
    if let Some(err) = res.as_ref().err() {
        Err(err.to_string())
    } else {
        Ok(())
    }
}
