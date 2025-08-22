use std::{
    any::Any,
    fmt::Debug,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result};
use bytes::Bytes;
#[cfg(not(feature = "iroh_v035"))]
use iroh::Watcher;
use iroh_metrics::encoding::Encoder;
use iroh_n0des::{
    Registry,
    iroh::{Endpoint, NodeAddr, NodeId, SecretKey},
    simulation::proto::{ActiveTrace, NodeInfo, TraceClient, TraceInfo},
};
use n0_future::IterExt;
use proto::{GetTraceResponse, NodeInfoWithAddr, Scope};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error_span, info, warn};
use uuid::Uuid;

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
/// The setup data must be serializable, deserializable, cloneable, and thread-safe
/// to be distributed across simulation nodes.
pub trait SetupData: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static {}
impl<T> SetupData for T where T: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static
{}

/// Context provided when spawning a new simulation node.
///
/// Contains all the necessary information and resources for initializing
/// a node, including its index, the shared setup data, and a metrics registry.
pub struct SpawnContext<'a, D = ()> {
    secret_key: SecretKey,
    node_idx: u32,
    setup_data: &'a D,
    registry: &'a mut Registry,
}

impl<'a, D: SetupData> SpawnContext<'a, D> {
    /// Returns the index of this node in the simulation.
    pub fn node_index(&self) -> u32 {
        self.node_idx
    }

    /// Returns a reference to the setup data for this simulation.
    pub fn setup_data(&self) -> &D {
        self.setup_data
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
    setup_data: &'a D,
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
    pub fn setup_data(&self) -> &D {
        self.setup_data
    }

    /// Returns an iterator over the addresses of all nodes except the specified one.
    pub fn all_other_nodes(&self, me: NodeId) -> impl Iterator<Item = &NodeAddr> + '_ {
        self.all_nodes
            .iter()
            .filter(move |n| n.info.node_id != Some(me))
            .flat_map(|n| &n.addr)
    }

    /// Returns the address of the node with the given index.
    ///
    /// # Errors
    ///
    /// Returns an error if no node with the specified index exists.
    pub fn addr(&self, idx: u32) -> Result<NodeAddr> {
        self.all_nodes
            .iter()
            .find(|n| n.info.idx == idx)
            .cloned()
            .context("node not found")?
            .addr
            .context("node has no address")
    }

    /// Returns the address of this node.
    pub fn self_addr(&self) -> Option<&NodeAddr> {
        self.all_nodes
            .iter()
            .find(|n| n.info.idx == self.node_index)
            .and_then(|info| info.addr.as_ref())
    }

    pub fn try_self_addr(&self) -> Result<&NodeAddr> {
        self.self_addr().context("missing node address")
    }

    /// Returns the total number of nodes participating in the simulation.
    pub fn node_count(&self) -> usize {
        self.all_nodes.len()
    }
}

/// Trait for types that can be spawned as simulation nodes.
///
/// This trait is generic over `D: SetupData`, which is the type returned from the
/// user-defined setup function (see [`Builder::with_setup`]). If not using the setup
/// step, `D` defaults to the unit type `()`.
///
/// Implement this trait on your node type to be able to spawn the node in a simulation
/// context. The only required method is [`Spawn::spawn`], which must return your spawned node.
pub trait Spawn<D: SetupData = ()>: Node + 'static {
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

/// Trait for simulation node implementations.
///
/// Provides basic functionality for nodes including optional endpoint access
/// and cleanup on shutdown.
///
/// For a node to be usable in a simulation, you also need to implement [`Spawn`]
/// for your node struct.
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
    node_builders: Vec<NodeBuilderWithCount<D>>,
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

impl<T, N: Spawn<D>, D: SetupData> From<T> for NodeBuilder<N, D>
where
    T: for<'a> AsyncCallback<'a, N, RoundContext<'a, D>, Result<bool>>,
{
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<N: Spawn<D>, D: SetupData> NodeBuilder<N, D> {
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
    trace_id: Uuid,
    idx: u32,
    round_fn: BoxedRoundFn<D>,
    check_fn: Option<BoxedCheckFn<D>>,
    round: u32,
    info: NodeInfo,
    metrics_encoder: Encoder,
    all_nodes: Vec<NodeInfoWithAddr>,
}

impl<D: SetupData> SimNode<D> {
    async fn spawn_and_run(
        builder: NodeBuilderWithIdx<D>,
        client: TraceClient,
        trace_id: Uuid,
        setup_data: &D,
        rounds: u32,
    ) -> Result<()> {
        let secret_key = SecretKey::generate(&mut rand::rngs::OsRng);
        let NodeBuilderWithIdx { node_idx, builder } = builder;
        let info = NodeInfo {
            // TODO: Only assign node id if endpoint was created.
            node_id: Some(secret_key.public()),
            idx: node_idx,
            label: None,
        };
        let mut registry = Registry::default();
        let mut context = SpawnContext {
            setup_data,
            node_idx,
            secret_key,
            registry: &mut registry,
        };
        let node = (builder.spawn_fn)(&mut context).await?;

        // TODO(Frando): enable metrics support for iroh 0.35
        // iroh@0.35 uses iroh-metrics@0.34, which is a major version down from iroh-metrics@0.35
        // which is used here, thus the traits are incompatible.
        // Add shim to iroh-metrics@0.35 to support registering metrics from iroh-metrics@0.34
        #[cfg(not(feature = "iroh_v035"))]
        if let Some(endpoint) = node.endpoint() {
            registry.register_all(endpoint.metrics());
        }

        let mut node = Self {
            node,
            trace_id,
            idx: node_idx,
            info,
            round: 0,
            round_fn: builder.round_fn,
            check_fn: builder.check_fn,
            metrics_encoder: Encoder::new(Arc::new(RwLock::new(registry))),
            all_nodes: Default::default(),
        };

        let res = node
            .run(&client, setup_data, rounds)
            .await
            .with_context(|| format!("node {} failed", node.idx));
        if let Err(err) = &res {
            warn!("node failed: {err:#}");
        }
        res
    }

    async fn run(&mut self, client: &TraceClient, setup_data: &D, rounds: u32) -> Result<()> {
        let client = client.start_node(self.trace_id, self.info.clone()).await?;

        info!(idx = self.idx, "start");

        let info = NodeInfoWithAddr {
            addr: self.my_addr().await,
            info: self.info.clone(),
        };
        let all_nodes = client.wait_start(info).await?;
        self.all_nodes = all_nodes;

        let result = self.run_rounds(&client, setup_data, rounds).await;

        if let Err(err) = self.node.shutdown().await {
            warn!("failure during node shutdown: {err:#}");
        }

        client.end(to_str_err(&result)).await?;

        result
    }

    async fn run_rounds(
        &mut self,
        client: &ActiveTrace,
        setup_data: &D,
        rounds: u32,
    ) -> Result<()> {
        while self.round < rounds {
            if !self
                .run_round(client, setup_data)
                .await
                .with_context(|| format!("failed at round {}", self.round))?
            {
                return Ok(());
            }
            self.round += 1;
        }
        Ok(())
    }

    #[tracing::instrument(name="round", skip_all, fields(round=self.round))]
    async fn run_round(&mut self, client: &ActiveTrace, setup_data: &D) -> Result<bool> {
        info!("start round");
        let context = RoundContext {
            round: self.round,
            node_index: self.idx,
            setup_data,
            all_nodes: &self.all_nodes,
        };

        let result = (self.round_fn)(&mut self.node, &context)
            .await
            .context("round function failed");

        let checkpoint = (context.round + 1) as u64;
        let label = format!("Round {} end", context.round);
        client
            .put_checkpoint(checkpoint, Some(label), to_str_err(&result))
            .await?;

        // TODO(Frando): Couple metrics to node idx, not node id.
        if let Some(node_id) = self.node_id() {
            client
                .put_metrics(node_id, Some(checkpoint), self.metrics_encoder.export())
                .await?;
        }

        client.wait_checkpoint(checkpoint).await?;

        match result {
            Ok(out) => {
                if let Some(check_fn) = self.check_fn.as_ref() {
                    (check_fn)(&self.node, &context).context("check function failed")?;
                }
                Ok(out)
            }
            Err(err) => Err(err),
        }
    }

    fn node_id(&self) -> Option<NodeId> {
        self.info.node_id
    }

    async fn my_addr(&self) -> Option<NodeAddr> {
        if let Some(endpoint) = self.node.endpoint() {
            Some(node_addr(endpoint).await)
        } else {
            None
        }
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
            node_builders: Vec::new(),
            setup_fn,
            rounds: 0,
        }
    }
}
impl<D: SetupData> Builder<D> {
    /// Creates a new simulation builder with a setup function for setup data.
    ///
    /// The setup function is called once before the simulation starts to
    /// initialize the setup data that will be shared across all nodes.
    ///
    /// The setup function can return any type that implements [`SetupData`],
    /// which is an auto-implemented supertrait for all types that are
    /// serializable, cloneable, and thread-safe. See [`SetupData`] for details.
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
            node_builders: Vec::new(),
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
    ///
    /// You can create a [`NodeBuilder`] from any type that implements [`Spawn<D>`] where
    /// `D` is the type returned from [`Self::with_setup`]. If you are not using the setup
    /// step, `D` defaults to the unit type `()`.
    pub fn spawn<N: Spawn<D>>(
        mut self,
        node_count: u32,
        node_builder: impl Into<NodeBuilder<N, D>>,
    ) -> Self {
        let node_builder = node_builder.into();
        self.node_builders.push(NodeBuilderWithCount {
            count: node_count,
            builder: node_builder.erase(),
        });
        self
    }

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

        debug!(%name, ?run_mode, "build simulation run");

        let (trace_id, setup_data) = if matches!(run_mode, RunMode::InitOnly | RunMode::Integrated)
        {
            let setup_data = (self.setup_fn)().await?;
            let encoded_setup_data = Bytes::from(postcard::to_stdvec(&setup_data)?);
            let node_count = self.node_builders.iter().map(|builder| builder.count).sum();
            let trace_id = client
                .init_trace(
                    TraceInfo {
                        name: name.to_string(),
                        node_count,
                        expected_checkpoints: Some(self.rounds as u64),
                    },
                    Some(encoded_setup_data),
                )
                .await?;
            info!(%name, node_count, %trace_id, "init simulation");

            (trace_id, setup_data)
        } else {
            let info = client.get_trace(name.to_string()).await?;
            let GetTraceResponse {
                trace_id,
                info,
                setup_data,
            } = info;
            info!(%name, node_count=info.node_count, %trace_id, "get simulation");
            let setup_data = setup_data.context("expected setup data to be set")?;
            let setup_data: D =
                postcard::from_bytes(&setup_data).context("failed to decode setup data")?;
            (trace_id, setup_data)
        };

        let mut node_builders = self
            .node_builders
            .into_iter()
            .flat_map(|builder| (0..builder.count).map(move |_| builder.builder.clone()))
            .enumerate()
            .map(|(node_idx, builder)| NodeBuilderWithIdx {
                node_idx: node_idx as u32,
                builder,
            });

        let node_builders: Vec<_> = match run_mode {
            RunMode::InitOnly => vec![],
            RunMode::Integrated => node_builders.collect(),
            RunMode::Isolated(idx) => vec![
                node_builders
                    .nth(idx as usize)
                    .context("invalid isolated index")?,
            ],
        };

        Ok(Simulation {
            run_mode,
            max_rounds: self.rounds,
            node_builders,
            client,
            trace_id,
            setup_data,
        })
    }
}

struct NodeBuilderWithCount<D> {
    count: u32,
    builder: ErasedNodeBuilder<D>,
}

struct NodeBuilderWithIdx<D> {
    node_idx: u32,
    builder: ErasedNodeBuilder<D>,
}

/// A configured simulation ready to run.
///
/// Contains all the necessary components including the setup data, node spawners,
/// and tracing client to execute a simulation run.
pub struct Simulation<D> {
    trace_id: Uuid,
    run_mode: RunMode,
    client: TraceClient,
    setup_data: D,
    max_rounds: u32,
    node_builders: Vec<NodeBuilderWithIdx<D>>,
}

impl<D: SetupData> Simulation<D> {
    /// Runs this simulation to completion.
    ///
    /// Spawns all configured nodes concurrently and executes the specified
    /// number of simulation rounds.
    ///
    /// # Errors
    ///
    /// Returns an error if any node fails to spawn or if any round fails to execute.
    pub async fn run(self) -> Result<()> {
        let cancel_token = CancellationToken::new();

        // Spawn a task to submit logs.
        let logs_scope = match self.run_mode {
            RunMode::Isolated(idx) => Some(Scope::Isolated(idx)),
            RunMode::Integrated => Some(Scope::Integrated),
            // Do not push logs for init-only runs.
            RunMode::InitOnly => None,
        };
        let logs_task = if let Some(scope) = logs_scope {
            Some(spawn_logs_task(
                self.client.clone(),
                self.trace_id,
                scope,
                cancel_token.clone(),
            ))
        } else {
            None
        };

        // Spawn and run all nodes concurrently.
        let result = self
            .node_builders
            .into_iter()
            .map(async |builder| {
                let span = error_span!("sim-node", idx = builder.node_idx);
                SimNode::spawn_and_run(
                    builder,
                    self.client.clone(),
                    self.trace_id,
                    &self.setup_data,
                    self.max_rounds,
                )
                .instrument(span)
                .await
            })
            .try_join_all()
            .await
            .map(|_list| ());

        cancel_token.cancel();
        if let Some(join_handle) = logs_task {
            join_handle.await?;
        }

        if matches!(self.run_mode, RunMode::Integrated) {
            self.client
                .close_trace(self.trace_id, to_str_err(&result))
                .await?;
        }

        result
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

/// Spawns a task that periodically submits the collected logs from our global
/// tracing subscriber to a trace server.
fn spawn_logs_task(
    client: TraceClient,
    trace_id: Uuid,
    scope: Scope,
    cancel_token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        loop {
            let _ = cancel_token
                .run_until_cancelled(tokio::time::sleep(Duration::from_secs(1)))
                .await;
            let lines = self::trace::get_logs();
            if lines.is_empty() {
                continue;
            }
            // 500 chosen so that we stay below ~16MB of logs (irpc's MAX_MESSAGE_SIZE limit).
            // This gives us ~32KB per log line on average.
            for lines_chunk in lines.chunks(500) {
                if let Err(e) = client.put_logs(trace_id, scope, lines_chunk.to_vec()).await {
                    eprintln!(
                        "warning: failed to submit logs due to error, stopping log submission now: {e:?}"
                    );
                    break;
                }
            }
            if cancel_token.is_cancelled() {
                break;
            }
        }
    })
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
    D: SetupData,
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
        .await
        .with_context(|| format!("simulation `{name}` failed to run"));

    match &result {
        Ok(()) => eprintln!("simulation `{name}` passed"),
        Err(err) => eprintln!("simulation `{name}` failed: {err:#}"),
    };

    drop(permit);

    result
}

fn to_str_err<T>(res: &Result<T, anyhow::Error>) -> Result<(), String> {
    if let Some(err) = res.as_ref().err() {
        Err(format!("{err:?}"))
    } else {
        Ok(())
    }
}
