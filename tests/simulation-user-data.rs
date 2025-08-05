use anyhow::Result;
use serde::{Deserialize, Serialize};

use iroh_n0des::iroh::{Endpoint, protocol::Router};
use iroh_n0des::simulation::*;

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

    Ok(
        Builder::with_setup(async || Ok(SetupData { shared_secret: 42 }))
            .spawn(4, MyNode::builder(round).check(check))
            .rounds(3),
    )
}
