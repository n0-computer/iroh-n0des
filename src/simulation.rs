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
use iroh::{Endpoint, NodeAddr, NodeId, SecretKey, Watcher};
use iroh_metrics::encoding::Encoder;
use iroh_n0des::{
    Registry,
    simulation::proto::{ActiveTrace, NodeInfo, TraceClient, TraceInfo},
};
use n0_future::IterExt;
use n0_watcher::Watchable;
use proto::{GetTraceResponse, NodeInfoWithAddr, Scope};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{sync::Semaphore, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error_span, info, warn};
use uuid::Uuid;

use crate::simulation::proto::CheckpointId;

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

const METRICS_INTERVAL: Duration = Duration::from_secs(1);

type BoxedSpawnFn<C> = Arc<
    dyn 'static
        + Send
        + Sync
        + for<'a> Fn(&'a mut SpawnContext<'a, C>) -> BoxFuture<'a, Result<BoxNode<C>>>,
>;
type BoxedRoundFn<C> = Arc<
    dyn 'static
        + Send
        + Sync
        + for<'a> Fn(&'a mut BoxNode<C>, &'a RoundContext<'a, C>) -> BoxFuture<'a, Result<bool>>,
>;

type BoxedCheckFn<C> = Arc<dyn Fn(&BoxNode<C>, &RoundContext<'_, C>) -> Result<()>>;

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

pub trait Ctx: Send + Sync + 'static {
    type Config: SetupData + Default;
    type Setup: SetupData;

    /// Runs once for each simulation environment.
    fn setup(config: &Self::Config) -> impl Future<Output = Result<Self::Setup>> + Send;

    fn round_label(_config: &Self::Config, _round: u32) -> Option<String> {
        None
    }
}

impl Ctx for () {
    type Config = ();
    type Setup = ();

    async fn setup(_config: &Self::Config) -> Result<Self::Setup> {
        Ok(())
    }
}

/// Context provided when spawning a new simulation node.
///
/// Contains all the necessary information and resources for initializing
/// a node, including its index, the shared setup data, and a metrics registry.
pub struct SpawnContext<'a, C: Ctx = ()> {
    ctx: &'a StaticCtx<C>,
    secret_key: SecretKey,
    node_idx: u32,
    registry: &'a mut Registry,
}

impl<'a, C: Ctx> SpawnContext<'a, C> {
    /// Returns the index of this node in the simulation.
    pub fn node_index(&self) -> u32 {
        self.node_idx
    }

    /// Returns a reference to the setup data for this simulation.
    pub fn setup_data(&self) -> &C::Setup {
        &self.ctx.setup
    }

    pub fn config(&self) -> &C::Config {
        &self.ctx.config
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
pub struct RoundContext<'a, C: Ctx = ()> {
    round: u32,
    node_index: u32,
    all_nodes: &'a Vec<NodeInfoWithAddr>,
    ctx: &'a StaticCtx<C>,
}

impl<'a, C: Ctx> RoundContext<'a, C> {
    /// Returns the current round number.
    pub fn round(&self) -> u32 {
        self.round
    }

    /// Returns the index of this node in the simulation.
    pub fn node_index(&self) -> u32 {
        self.node_index
    }

    /// Returns a reference to the shared setup data for this simulation.
    pub fn setup_data(&self) -> &C::Setup {
        &self.ctx.setup
    }

    pub fn config(&self) -> &C::Config {
        &self.ctx.config
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

/// Trait for simulation node implementations.
///
/// Provides basic functionality for nodes including optional endpoint access
/// and cleanup on shutdown.
///
/// For a node to be usable in a simulation, you also need to implement [`Spawn`]
/// for your node struct.
pub trait Node<C: Ctx = ()>: Send + 'static + Sized {
    /// Returns a reference to this node's endpoint, if any.
    fn endpoint(&self) -> Option<&Endpoint>;

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

    /// Spawns a new instance of this node type.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to initialize properly.
    fn spawn(context: &mut SpawnContext<'_, C>) -> impl Future<Output = Result<Self>> + Send;

    /// Spawns a new instance as a dynamically-typed node.
    ///
    /// This calls `spawn` and boxes the result.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to initialize properly.
    fn spawn_dyn<'a>(context: &'a mut SpawnContext<'_, C>) -> BoxFuture<'a, Result<BoxNode<C>>> {
        Box::pin(async {
            let node = Self::spawn(context).await?;
            let node: Box<dyn DynNode<C>> = Box::new(node);
            anyhow::Ok(node)
        })
    }

    fn node_label(&self, _context: &SpawnContext<C>) -> Option<String> {
        None
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed dynamically-typed simulation node.
pub type BoxNode<C> = Box<dyn DynNode<C> + 'static>;

/// Trait for dynamically-typed simulation nodes.
///
/// This trait enables type erasure for nodes while preserving essential
/// functionality like shutdown, endpoint access, and type casting.
pub trait DynNode<C: Ctx>: Send + Any + 'static {
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

    fn node_label(&self, _context: &SpawnContext<C>) -> Option<String> {
        None
    }

    /// Returns a reference to this node as `Any` for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns a mutable reference to this node as `Any` for downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<C: Ctx, N: Node<C> + Sized> DynNode<C> for N {
    fn shutdown(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(<Self as Node<C>>::shutdown(self))
    }

    fn endpoint(&self) -> Option<&Endpoint> {
        <Self as Node<C>>::endpoint(self)
    }

    fn node_label(&self, context: &SpawnContext<C>) -> Option<String> {
        <Self as Node<C>>::node_label(self, context)
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
pub struct Builder<C: Ctx = ()> {
    config: C::Config,
    node_builders: Vec<NodeBuilderWithCount<C>>,
    rounds: u32,
}

struct SimFns<C: Ctx> {
    spawn: BoxedSpawnFn<C>,
    round: BoxedRoundFn<C>,
    check: Option<BoxedCheckFn<C>>,
}

impl<C: Ctx> Clone for SimFns<C> {
    fn clone(&self) -> Self {
        Self {
            round: self.round.clone(),
            check: self.check.clone(),
            spawn: self.spawn.clone(),
        }
    }
}

/// Builder for configuring individual nodes in a simulation.
///
/// Provides methods to set up spawn functions, round functions, and optional
/// check functions for a specific node type.
#[derive(Clone)]
pub struct NodeBuilder<N: Node<C>, C: Ctx> {
    phantom: PhantomData<N>,
    fns: SimFns<C>,
}

struct ErasedNodeBuilder<C: Ctx> {
    fns: SimFns<C>,
}

impl<C: Ctx> Clone for ErasedNodeBuilder<C> {
    fn clone(&self) -> Self {
        Self {
            fns: self.fns.clone(),
        }
    }
}

impl<N: Node<C>, C: Ctx> NodeBuilder<N, C> {
    /// Creates a new node builder with the given round function.
    ///
    /// The round function will be called each simulation round and should return
    /// `Ok(true)` to continue or `Ok(false)` to stop early.
    pub fn new(
        round_fn: impl for<'a> AsyncCallback<'a, N, RoundContext<'a, C>, Result<bool>>,
    ) -> Self {
        let spawn_fn: BoxedSpawnFn<C> = Arc::new(N::spawn_dyn);
        let round_fn: BoxedRoundFn<C> = Arc::new(move |node, context| {
            let node = node
                .as_any_mut()
                .downcast_mut::<N>()
                .expect("unreachable: type is statically guaranteed");
            Box::pin(round_fn(node, context))
        });
        let fns = SimFns {
            spawn: spawn_fn,
            round: round_fn,
            check: None,
        };
        Self {
            phantom: PhantomData,
            fns,
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
        check_fn: impl 'static + for<'a> Fn(&'a N, &RoundContext<'a, C>) -> Result<()>,
    ) -> Self {
        let check_fn: BoxedCheckFn<C> = Arc::new(move |node, context| {
            let node = node
                .as_any()
                .downcast_ref::<N>()
                .expect("unreachable: type is statically guaranteed");
            check_fn(node, context)
        });
        self.fns.check = Some(check_fn);
        self
    }

    fn erase(self) -> ErasedNodeBuilder<C> {
        ErasedNodeBuilder { fns: self.fns }
    }
}

struct SimNode<'a, C: Ctx> {
    node: BoxNode<C>,
    ctx: &'a StaticCtx<C>,
    client: ActiveTrace,
    idx: u32,
    fns: SimFns<C>,
    round: u32,
    info: NodeInfo,
    metrics: Arc<RwLock<Registry>>,
    checkpoint_watcher: n0_watcher::Watchable<CheckpointId>,
    all_nodes: Vec<NodeInfoWithAddr>,
}

impl<'a, C: Ctx> SimNode<'a, C> {
    async fn spawn_and_run(builder: NodeBuilderWithIdx<C>, ctx: &'a StaticCtx<C>) -> Result<()> {
        let secret_key = SecretKey::generate(&mut rand::rng());
        let NodeBuilderWithIdx { node_idx, builder } = builder;
        let mut registry = Registry::default();
        let node_id = secret_key.public();
        let mut context = SpawnContext::<C> {
            ctx,
            node_idx,
            secret_key: secret_key.clone(),
            registry: &mut registry,
        };
        let node = (builder.fns.spawn)(&mut context).await?;

        // TODO: Borrow checker forces to recreate the context, I think this can be avoided but can't figure it out.
        let context = SpawnContext::<C> {
            ctx,
            node_idx,
            secret_key: secret_key.clone(),
            registry: &mut registry,
        };
        let label = node.node_label(&context);

        let info = NodeInfo {
            // TODO: Only assign node id if endpoint was created.
            node_id: Some(node_id),
            idx: node_idx,
            label,
        };

        if let Some(endpoint) = node.endpoint() {
            registry.register_all(endpoint.metrics());
        }
        let client = ctx.client.start_node(ctx.trace_id, info.clone()).await?;

        let mut node = Self {
            node,
            ctx,
            client,
            idx: node_idx,
            info,
            round: 0,
            fns: builder.fns,
            checkpoint_watcher: Watchable::new(0),
            metrics: Arc::new(RwLock::new(registry)),
            all_nodes: Default::default(),
        };

        let res = node
            .run()
            .await
            .with_context(|| format!("node {} failed", node.idx));
        if let Err(err) = &res {
            warn!("node failed: {err:#}");
        }
        res
    }

    async fn run(&mut self) -> Result<()> {
        info!(idx = self.idx, "start");

        // Spawn a task to periodically put metrics.
        if let Some(node_id) = self.node_id() {
            let client = self.client.clone();
            let mut watcher = self.checkpoint_watcher.watch();
            let mut metrics_encoder = Encoder::new(self.metrics.clone());
            tokio::task::spawn(
                async move {
                    let mut interval = tokio::time::interval(METRICS_INTERVAL);
                    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    loop {
                        let checkpoint = tokio::select! {
                            _ = interval.tick() => None,
                            checkpoint = watcher.updated() => {
                                match checkpoint {
                                    Err(_) => break,
                                    Ok(checkpoint) => Some(checkpoint)
                                }
                            }
                        };
                        if let Err(err) = client
                            .put_metrics(node_id, checkpoint, metrics_encoder.export())
                            .await
                        {
                            warn!(?err, "failed to put metrics, stop metrics task");
                            break;
                        }
                    }
                }
                .instrument(error_span!("metrics")),
            );
        }

        let info = NodeInfoWithAddr {
            addr: self.my_addr().await,
            info: self.info.clone(),
        };
        self.all_nodes = self.client.wait_start(info).await?;

        let result = self.run_rounds().await;

        if let Err(err) = self.node.shutdown().await {
            warn!("failure during node shutdown: {err:#}");
        }

        self.client.end(to_str_err(&result)).await?;

        result
    }

    async fn run_rounds(&mut self) -> Result<()> {
        while self.round < self.ctx.max_rounds {
            if !self
                .run_round()
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
    async fn run_round(&mut self) -> Result<bool> {
        let context = RoundContext {
            round: self.round,
            node_index: self.idx,
            all_nodes: &self.all_nodes,
            ctx: self.ctx,
        };

        let label = C::round_label(&self.ctx.config, self.round)
            .unwrap_or_else(|| format!("Round {}", self.round));

        info!(%label, "start round");

        let result = (self.fns.round)(&mut self.node, &context)
            .await
            .context("round function failed");

        info!(%label, "end round");

        let checkpoint = (context.round + 1) as u64;
        self.checkpoint_watcher.set(checkpoint).ok();
        self.client
            .put_checkpoint(checkpoint, Some(label), to_str_err(&result))
            .await
            .context("put checkpoint")?;

        self.client
            .wait_checkpoint(checkpoint)
            .await
            .context("wait checkpoint")?;

        match result {
            Ok(out) => {
                if let Some(check_fn) = self.fns.check.as_ref() {
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
    endpoint.online().await;
    endpoint.node_addr()
}

impl Default for Builder<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Ctx> Builder<C> {
    /// Creates a new simulation builder.
    ///
    /// The context's setup function is called once before the simulation starts to
    /// initialize the setup data that will be shared across all nodes.
    ///
    /// The setup function can return any type that implements [`SetupData`],
    /// which is an auto-implemented supertrait for all types that are
    /// serializable, cloneable, and thread-safe. See [`SetupData`] for details.
    ///
    /// # Errors
    ///
    /// The setup function should return an error if initialization fails.
    pub fn new() -> Self {
        Self::with_config(Default::default())
    }

    pub fn with_config(config: C::Config) -> Self {
        Self {
            config,
            node_builders: vec![],
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
    pub fn spawn<N: Node<C>>(
        mut self,
        node_count: u32,
        builder: impl Into<NodeBuilder<N, C>>,
    ) -> Self {
        let builder = builder.into();
        self.node_builders.push(NodeBuilderWithCount {
            count: node_count,
            builder: builder.erase(),
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
    pub async fn build(self, name: &str) -> Result<Simulation<C>> {
        let client = TraceClient::from_env_or_local()?;
        let run_mode = RunMode::from_env()?;

        debug!(%name, ?run_mode, "build simulation run");

        let (trace_id, setup_data) = if matches!(run_mode, RunMode::InitOnly | RunMode::Integrated)
        {
            let setup_data = C::setup(&self.config).await?;
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
            let setup_data: C::Setup =
                postcard::from_bytes(&setup_data).context("failed to decode setup data")?;
            (trace_id, setup_data)
        };

        // map all the builders with their count into a flat iter of (i, builder)
        let mut builders = self
            .node_builders
            .iter()
            .flat_map(|builder| (0..builder.count).map(|_| &builder.builder))
            .enumerate();

        // build the list of builders *we* want to run:
        let node_builders: Vec<_> = match run_mode {
            // init only: run nothing
            RunMode::InitOnly => vec![],
            // integrated: run all nodes
            RunMode::Integrated => builders.map(NodeBuilderWithIdx::from_tuple).collect(),
            // isolated: run single node
            RunMode::Isolated(idx) => {
                let item = builders
                    .nth(idx as usize)
                    .context("invalid isolated index")?;
                vec![NodeBuilderWithIdx::from_tuple(item)]
            }
        };

        let ctx = StaticCtx {
            setup: setup_data,
            config: self.config,
            trace_id,
            client,
            run_mode,
            max_rounds: self.rounds,
        };

        Ok(Simulation { node_builders, ctx })
    }
}

struct NodeBuilderWithCount<C: Ctx> {
    count: u32,
    builder: ErasedNodeBuilder<C>,
}

struct NodeBuilderWithIdx<C: Ctx> {
    node_idx: u32,
    builder: ErasedNodeBuilder<C>,
}

impl<C: Ctx> NodeBuilderWithIdx<C> {
    fn from_tuple((node_idx, builder): (usize, &ErasedNodeBuilder<C>)) -> Self {
        Self {
            node_idx: node_idx as u32,
            builder: builder.clone(),
        }
    }
}

/// A configured simulation ready to run.
///
/// Contains all the necessary components including the setup data, node spawners,
/// and tracing client to execute a simulation run.
pub struct Simulation<C: Ctx> {
    ctx: StaticCtx<C>,
    node_builders: Vec<NodeBuilderWithIdx<C>>,
}

impl<C: Ctx> Simulation<C> {
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
        let logs_scope = match self.ctx.run_mode {
            RunMode::Isolated(idx) => Some(Scope::Isolated(idx)),
            RunMode::Integrated => Some(Scope::Integrated),
            // Do not push logs for init-only runs.
            RunMode::InitOnly => None,
        };
        let logs_task = if let Some(scope) = logs_scope {
            Some(spawn_logs_task(
                self.ctx.client.clone(),
                self.ctx.trace_id,
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
                SimNode::spawn_and_run(builder, &self.ctx)
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

        if matches!(self.ctx.run_mode, RunMode::Integrated) {
            self.ctx
                .client
                .close_trace(self.ctx.trace_id, to_str_err(&result))
                .await?;
        }

        result
    }
}

#[derive(Debug, Clone)]
struct StaticCtx<C: Ctx> {
    setup: C::Setup,
    config: C::Config,
    trace_id: Uuid,
    client: TraceClient,
    run_mode: RunMode,
    max_rounds: u32,
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
            if cancel_token
                .run_until_cancelled(tokio::time::sleep(Duration::from_secs(1)))
                .await
                .is_none()
            {
                break;
            }
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
pub async fn run_sim_fn<F, Fut, C, E>(name: &str, sim_fn: F) -> anyhow::Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Builder<C>, E>>,
    C: Ctx,
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
        .with_context(|| format!("simulation `{name}` failed to start"))?
        .run()
        .await
        .with_context(|| format!("simulation `{name}` failed to complete"));

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
