use std::time::Duration;

use anyhow::Result;
use iroh::{Endpoint, NodeAddr, NodeId};
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
    let client_node_id = client_endpoint.node_id();
    debug!("local node: {}", client_node_id,);

    let remote_node_id: NodeId = std::env::args().nth(1).unwrap().parse()?;
    let remote_node_addr: NodeAddr = remote_node_id.into();

    println!("press ctrl+c once your sshkey is registered");
    tokio::signal::ctrl_c().await?;

    // Create iroh services client
    let mut rpc_client = Client::builder(&client_endpoint)
        .metrics_interval(Duration::from_secs(2))
        .ssh_key(&alice_ssh_key)?
        .build(remote_node_addr.clone())
        .await?;

    rpc_client.ping().await?;
    println!("ping OK");
    // waiting to push some metrics
    tokio::time::sleep(Duration::from_secs(4)).await;
    client_endpoint.close().await;

    Ok(())
}
