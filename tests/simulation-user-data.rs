mod tests {
    use anyhow::Result;
    use iroh::{Endpoint, protocol::Router};
    use iroh_n0des::simulation::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct SetupData {
        shared_secret: u64,
    }

    struct MySim;
    impl Ctx for MySim {
        type Config = ();

        type Setup = SetupData;

        async fn setup(_config: &Self::Config) -> Result<Self::Setup> {
            Ok(SetupData { shared_secret: 42 })
        }
    }

    #[derive(Debug)]
    struct MyNode {
        router: Router,
    }

    impl Node<MySim> for MyNode {
        fn endpoint(&self) -> Option<&Endpoint> {
            Some(self.router.endpoint())
        }

        async fn shutdown(&mut self) -> Result<()> {
            self.router.shutdown().await?;
            Ok(())
        }

        async fn spawn(context: &mut SpawnContext<'_, MySim>) -> Result<Self> {
            let endpoint = context.bind_endpoint().await?;
            let router = Router::builder(endpoint).spawn();
            // We can access the shared data!
            let _shared_secret = context.setup_data().shared_secret;
            Ok(Self { router })
        }
    }

    #[derive(Debug)]
    struct MyTcpServer {
        // some non-iroh server
    }

    impl Node<MySim> for MyTcpServer {
        fn endpoint(&self) -> Option<&Endpoint> {
            None
        }

        async fn shutdown(&mut self) -> Result<()> {
            Ok(())
        }

        async fn spawn(_context: &mut SpawnContext<'_, MySim>) -> Result<Self> {
            // Do whatever
            Ok(MyTcpServer {})
        }
    }

    #[iroh_n0des::sim]
    async fn test_simulation_setup_data() -> Result<Builder<MySim>> {
        async fn round(_node: &mut MyNode, context: &RoundContext<'_, MySim>) -> Result<bool> {
            // we can access the shared data!
            let _shared_secret = context.setup_data().shared_secret;
            let _me = context.try_self_addr()?.node_id;
            // do whatever.
            Ok(true)
        }

        fn check(_node: &MyNode, _ctx: &RoundContext<'_, MySim>) -> Result<()> {
            Ok(())
        }

        async fn tcp_server_round(
            _node: &mut MyTcpServer,
            _context: &RoundContext<'_, MySim>,
        ) -> Result<bool> {
            Ok(true)
        }

        Ok(Builder::new()
            // spawn 1 instance of MyTcpServer
            .spawn(1, NodeBuilder::new(tcp_server_round))
            // spawn 4 instances of MyNode
            .spawn(4, NodeBuilder::new(round).check(check))
            .rounds(3))
    }
}
