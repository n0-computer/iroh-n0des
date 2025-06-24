use anyhow::Result;
use iroh::{protocol::Router, Endpoint, NodeAddr, NodeId};
use iroh_n0des::Client;
use ssh_key::Algorithm;
use tracing::debug;

#[tokio::main]
pub async fn main() -> Result<()> {
    // Create ssh key for alice
    let mut rng = rand::rngs::OsRng;

    let alice_ssh_key = ssh_key::PrivateKey::random(&mut rng, Algorithm::Ed25519)?;

    println!("SSH Key: {}", alice_ssh_key.public_key().to_openssh()?);

    let client_endpoint = Endpoint::builder().discovery_n0().bind().await?;
    let client_blobs = iroh_blobs::net_protocol::Blobs::memory().build(&client_endpoint);

    let client_router = Router::builder(client_endpoint)
        .accept(iroh_blobs::ALPN, client_blobs.clone())
        .spawn();
    let client_node_id = client_router.endpoint().node_id();
    debug!("local node: {}", client_node_id,);

    let remote_node_id: NodeId = std::env::args().nth(1).unwrap().parse()?;
    let remote_node_addr: NodeAddr = remote_node_id.into();

    println!("press ctrl+c once your sshkey is registered");
    tokio::signal::ctrl_c().await?;

    // Create iroh services client
    let mut rpc_client = Client::builder(client_router.endpoint())
        // .metrics_interval(Duration::from_secs(2))
        .ssh_key(&alice_ssh_key)?
        .build(remote_node_addr.clone())
        .await?;

    rpc_client.ping().await?;
    println!("ping OK");
    client_router.shutdown().await?;

    Ok(())
}
