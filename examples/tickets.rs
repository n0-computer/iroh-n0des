use anyhow::{Context, Result};
use iroh::{Endpoint, EndpointId};
use iroh_n0des::{Client, PublishedTicket};
use iroh_tickets::Ticket;
use serde::{Deserialize, Serialize};

/// Node is some struct you define in your app. This usually holds a bunch of
/// business logic, in this case we're going to use tickets to back a "topics"
/// app where users can publish & join topics
struct Node {
    username: String,
    endpoint: Endpoint,
    n0des: Client,
}

impl Node {
    async fn new(username: &str) -> Result<Node> {
        let endpoint = Endpoint::bind().await?;

        // needs N0DES_API_SECRET set to an environment variable
        // client will now push endpoint metrics to n0des
        let n0des = Client::builder(&endpoint)
            .api_secret_from_env()?
            .build()
            .await?;

        // a real example would wire up more protocols here

        Ok(Node {
            username: username.to_string(),
            endpoint,
            n0des,
        })
    }

    /// Create a TopicTicket, publish it to n0des, and return it
    async fn publish_topic(&self, topic: &str) -> anyhow::Result<TopicTicket> {
        let ticket = TopicTicket {
            message: topic.to_string(),
            endpoint_id: self.endpoint.id(),
        };

        // here we intentionally try to create globally unique names
        // you might also use a UUID for the topic name, or some other
        // info to help disambiguate the ticket namespace
        self.n0des
            .publish_ticket(format!("{}_{topic}", self.username), ticket.clone())
            .await
            .context("publishing ticket")?;

        Ok(ticket)
    }

    /// a method that uses n0des signals, and processes them into a structure
    /// that your app wants
    async fn list_topics(&self) -> Result<Vec<PublishedTicket<TopicTicket>>> {
        self.n0des
            .fetch_tickets(0, 100)
            .await
            .context("fetching tickets")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TopicTicket {
    message: String,
    endpoint_id: EndpointId,
}

impl Ticket for TopicTicket {
    const KIND: &'static str = "coolapp";

    fn to_bytes(&self) -> Vec<u8> {
        postcard::to_stdvec(&self).expect("postcard serialization failed")
    }

    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, iroh_tickets::ParseError> {
        let ticket: Self = postcard::from_bytes(bytes)?;
        Ok(ticket)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let alice = Node::new("alice").await?;
    let ticket = alice.publish_topic("cool_pokemon").await?;
    println!("alice published a ticket: {:?}", ticket);

    let bob = Node::new("bob").await?;
    let bobs_topics = bob.list_topics().await?;
    assert_eq!(bobs_topics.len(), 1);
    println!("bob sees a ticket named: {}", bobs_topics[0].name);
    Ok(())
}
