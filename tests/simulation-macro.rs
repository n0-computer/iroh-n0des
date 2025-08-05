#[cfg(any(feature = "iroh_main", feature = "iroh_v035"))]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use iroh_metrics::Counter;
    use iroh_n0des::{
        iroh::{
            Endpoint, NodeAddr,
            endpoint::Connection,
            protocol::{ProtocolHandler, Router},
        },
        simulation::{proto::TraceClient, *},
    };
    use rand::seq::IteratorRandom;
    use tracing::{debug, instrument};

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

    impl Node for PingNode {
        fn endpoint(&self) -> Option<&Endpoint> {
            Some(self.router.endpoint())
        }

        async fn shutdown(&mut self) -> Result<()> {
            self.router.shutdown().await?;
            Ok(())
        }
    }
    impl Spawn for PingNode {
        async fn spawn(context: &mut SpawnContext<'_>) -> Result<Self> {
            let ping = Ping::default();
            context.metrics_registry().register(ping.metrics.clone());

            let endpoint = context.bind_endpoint().await?;
            let router = Router::builder(endpoint).accept(ALPN, ping.clone()).spawn();

            Ok(Self { ping, router })
        }
    }

    impl PingNode {
        async fn ping(&self, addr: impl Into<NodeAddr>) -> Result<()> {
            self.ping.ping(self.router.endpoint(), addr).await
        }
    }

    #[iroh_n0des::sim]
    async fn test_simulation() -> Result<Builder<()>> {
        const EVENT_ID: &str = "ping";
        async fn tick(node: &mut PingNode, ctx: &RoundContext<'_>) -> Result<bool> {
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
                format!("send ping (round {})", ctx.round()),
                Some(EVENT_ID.to_string()),
            );
            Ok(true)
        }

        fn check(node: &PingNode, ctx: &RoundContext<'_>) -> Result<()> {
            let metrics = node.ping.metrics();
            assert_eq!(metrics.pings_sent.get(), ctx.round() as u64 + 1);
            Ok(())
        }

        Ok(Builder::new()
            .spawn(4, PingNode::builder(tick).check(check))
            .rounds(3))
    }

    #[tokio::test]
    async fn test_trace_connect() -> Result<()> {
        use iroh_n0des::simulation::trace;

        let Some(client) = TraceClient::from_env()? else {
            println!(
                "skipping trace server connect test: no server addr provided via N0DES_SIM_SERVER"
            );
            return Ok(());
        };
        trace::init();
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
        trace::submit_logs(&trace_client).await?;
        tracing::info!("bazoo");
        tracing::debug!(foo = "bar", "bazza");
        trace::submit_logs(&trace_client).await?;
        Ok(())
    }

    #[iroh_n0des::sim]
    async fn test_conn() -> Result<Builder> {
        async fn tick(node: &mut PingNode, ctx: &RoundContext<'_>) -> Result<bool> {
            let me = node.router.endpoint().node_id();
            if ctx.node_index() == 1 {
                for other in ctx.all_other_nodes(me) {
                    node.ping
                        .ping(node.router.endpoint(), other.clone())
                        .await?;
                }
            }
            Ok(true)
        }

        fn check(_node: &PingNode, _ctx: &RoundContext<'_>) -> Result<()> {
            Ok(())
        }

        Ok(Builder::new()
            .spawn(2, NodeBuilder::new(tick).check(check))
            .rounds(1))
    }
}

// #[derive(Debug, Clone)]
// struct NoopNode {
//     endpoint: Endpoint,
//     active_event: Option<(String, NodeId)>,
// }
// impl Node for NoopNode {
//     async fn spawn(endpoint: Endpoint, _metrics: &mut Registry) -> Result<Self>
//     where
//         Self: Sized,
//     {
//         Ok(NoopNode {
//             endpoint,
//             active_event: None,
//         })
//     }
// }

// #[crate::sim]
// async fn custom_events() -> Result<SimulationBuilder<NoopNode>> {
//     async fn tick(ctx: &Context, node: &mut NoopNode) -> Result<bool> {
//         let me = node.endpoint.node_id();
//         if let Some((id, other_node)) = node.active_event.take() {
//             self::events::event_end(me, other_node, "done", id);
//         }
//         let other = ctx
//             .all_other_nodes(me)
//             .choose(&mut rand::thread_rng())
//             .unwrap();
//         let id = format!("ping:n{}:r{}", ctx.node_index, ctx.round);
//         self::events::event_start(
//             me.fmt_short(),
//             other.node_id.fmt_short(),
//             format!("ping round {}", ctx.round),
//             Some(&id),
//         );
//         node.active_event = Some((id, other.node_id));
//         Ok(true)
//     }

//     Ok(Simulation::builder(tick).max_rounds(4).node_count(4))
// }
