mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use iroh::Endpoint;
    use iroh_n0des::simulation::{Builder, Ctx, Node, NodeBuilder, RoundContext, SpawnContext};
    use serde::{Deserialize, Serialize};
    use tracing::info;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct Data {
        topic_id: u64,
    }

    impl Ctx for Data {
        type Config = ();
        type Setup = Self;

        async fn setup(_config: &Self::Config) -> Result<Self::Setup> {
            Ok(Self { topic_id: 123 })
        }
    }

    #[derive(Debug)]
    struct BootstrapNode;

    impl Node<Data> for BootstrapNode {
        fn endpoint(&self) -> Option<&Endpoint> {
            None
        }

        async fn spawn(_context: &mut SpawnContext<'_, Data>) -> Result<Self> {
            Ok(BootstrapNode)
        }
    }

    #[derive(Debug)]
    struct ClientNode {
        endpoint: Endpoint,
    }

    impl Node<Data> for ClientNode {
        fn endpoint(&self) -> Option<&Endpoint> {
            Some(&self.endpoint)
        }
        async fn spawn(context: &mut SpawnContext<'_, Data>) -> Result<Self> {
            let endpoint = context.bind_endpoint().await?;
            Ok(Self { endpoint })
        }
    }

    async fn setup() -> anyhow::Result<Builder<Data>> {
        async fn round_bootstrap(
            _node: &mut BootstrapNode,
            context: &RoundContext<'_, Data>,
        ) -> Result<bool> {
            tokio::time::sleep(Duration::from_millis(500)).await;
            tracing::debug!(
                "bootstrap node {} round {}",
                context.node_index(),
                context.round()
            );
            Ok(true)
        }
        async fn round_client(
            _node: &mut ClientNode,
            context: &RoundContext<'_, Data>,
        ) -> Result<bool> {
            tokio::time::sleep(Duration::from_millis(200)).await;
            tracing::debug!(
                "client node {} round {}",
                context.node_index(),
                context.round()
            );
            Ok(true)
        }
        let builder = Builder::new()
            .spawn(2, NodeBuilder::new(round_bootstrap))
            .spawn(8, NodeBuilder::new(round_client))
            .rounds(4);

        Ok(builder)
    }

    #[tokio::test]
    async fn smoke() -> anyhow::Result<()> {
        iroh_n0des::simulation::trace::init();
        info!("start");
        let builder = setup().await?;
        let sim = builder.build("smoke").await?;
        sim.run().await?;
        info!("end");
        Ok(())
    }
}
