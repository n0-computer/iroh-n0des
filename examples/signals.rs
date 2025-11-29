use std::time::Duration;

use anyhow::Result;
use iroh::Endpoint;
use iroh_n0des::Client;

/// Node is some struct you define in your app. it can contain whatever you need.
struct Node {
    endpoint: Endpoint,
    n0des: Client,
}

impl Node {
    async fn new(create_topic: bool) -> Result<Node> {
        let endpoint = Endpoint::bind().await?;

        // needs N0DES_API_SECRET set to an environment variable
        // client will now push endpoint metrics to n0des
        let n0des = Client::builder(&endpoint)
            .api_secret_from_env()?
            .build()
            .await?;

        // a real example would wire up more protocols, like gossip

        if create_topic {
            // again using gossip as an example, here we would create a gossip topic
            // and generate a ticket from it
            // create a signal that others can fetch
            let ticket = b"this_is_my_gossip_ticket".to_vec();
            n0des
                .create_signal(
                    Duration::from_secs(60 * 5).as_secs(),
                    "some signal".to_string(),
                    ticket,
                )
                .await?;
        }

        Ok(Node { endpoint, n0des })
    }

    /// a method that uses n0des signals, and processes them into a structure
    /// that your app wants
    async fn topics(&self) -> Result<Vec<String>> {
        // in the real world this would validate signal values parse to valid
        // things your app understands
        self.n0des
            .list_signals()
            .await?
            .iter()
            .map(|s| s.name.clone())
            .collect()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let alice = Node::new(true).await?;
    let bob = Node::new(false).await?;

    let bobs_rooms = bob.topics().await?;
    assert_eq!(bobs_rooms.len(), 1);
    Ok(())
}
