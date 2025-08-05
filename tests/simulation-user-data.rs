use anyhow::Result;
use iroh_n0des::{
    iroh::{Endpoint, protocol::Router},
    simulation::*,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SetupData {
    shared_secret: u64,
}

#[derive(Debug)]
struct MyNode {
    router: Router,
}

impl Node for MyNode {
    fn endpoint(&self) -> Option<&Endpoint> {
        Some(self.router.endpoint())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

impl Spawn<SetupData> for MyNode {
    async fn spawn(context: &mut SpawnContext<'_, SetupData>) -> Result<Self> {
        let endpoint = context.bind_endpoint().await?;
        let router = Router::builder(endpoint).spawn();
        // We can access the shared data!
        let _shared_secret = context.user_data().shared_secret;
        Ok(Self { router })
    }
}

#[derive(Debug)]
struct MyTcpServer {
    // some non-iroh server
}

impl Node for MyTcpServer {
    fn endpoint(&self) -> Option<&Endpoint> {
        None
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Spawn<SetupData> for MyTcpServer {
    async fn spawn(_context: &mut SpawnContext<'_, SetupData>) -> Result<Self> {
        // Do whatever
        Ok(MyTcpServer {})
    }
}

#[iroh_n0des::sim]
async fn test_simulation() -> Result<Builder<SetupData>> {
    async fn round(_node: &mut MyNode, context: &RoundContext<'_, SetupData>) -> Result<bool> {
        // we can access the shared data!
        let _shared_secret = context.user_data().shared_secret;
        let _me = context.self_addr().node_id;
        // do whatever.
        Ok(true)
    }

    fn check(_node: &MyNode, _ctx: &RoundContext<'_, SetupData>) -> Result<()> {
        Ok(())
    }

    async fn tcp_server_round(
        _node: &mut MyTcpServer,
        _context: &RoundContext<'_, SetupData>,
    ) -> Result<bool> {
        Ok(true)
    }

    Ok(
        Builder::with_setup(async || Ok(SetupData { shared_secret: 42 }))
            // spawn 1 instance of MyTcpServer
            .spawn(1, MyTcpServer::builder(tcp_server_round))
            // spawn 4 instances of MyNode
            .spawn(4, MyNode::builder(round).check(check))
            .rounds(3),
    )
}
