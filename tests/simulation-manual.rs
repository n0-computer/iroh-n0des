#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use iroh_n0des::iroh::Endpoint;
    use serde::{Deserialize, Serialize};

    use iroh_n0des::simulation::{Builder, Node, RoundContext, Spawn, SpawnContext};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct Data {
        topic_id: u64,
    }

    #[derive(Debug)]
    struct BootstrapNode;

    impl Node for BootstrapNode {}

    impl Spawn<Data> for BootstrapNode {
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
        async fn spawn(context: &mut SpawnContext<'_, Data>) -> Result<Self> {
            let endpoint = context.bind_endpoint().await?;
            Ok(ClientNode { endpoint })
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
        let builder = Builder::with_setup(async || Ok(Data { topic_id: 22 }))
            .spawn(2, BootstrapNode::builder(round_bootstrap))
            .spawn(8, ClientNode::builder(round_client))
            .rounds(4);

        Ok(builder)
    }

    #[tokio::test]
    async fn smoke() -> anyhow::Result<()> {
        tracing_subscriber::fmt::init();
        tracing::warn!("start");
        let builder = setup().await?;
        let sim = builder.build("smoke").await?;
        sim.run().await?;
        tracing::warn!("end");
        Ok(())
    }
}
