use std::time::Duration;

use anyhow::Result;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use iroh_n0des::Client;
use ssh_key::Algorithm;
use tracing::debug;

#[tokio::main]
pub async fn main() -> Result<()> {
    let alice_ssh_key = ssh_key::PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519)?;

    println!("SSH Key: {}", alice_ssh_key.public_key().to_openssh()?);

    let client_endpoint = Endpoint::builder().bind().await?;
    let client_id = client_endpoint.id();
    debug!("local endpoint: {}", client_id,);

    let remote_id: EndpointId = std::env::args().nth(1).unwrap().parse()?;
    let remote_addr: EndpointAddr = remote_id.into();

    println!("press ctrl+c once your sshkey is registered");
    tokio::signal::ctrl_c().await?;

    // Create iroh services client
    let mut rpc_client = Client::builder(&client_endpoint)
        .metrics_interval(Duration::from_secs(2))
        .ssh_key(&alice_ssh_key)?
        .build(remote_addr.clone())
        .await?;

    rpc_client.ping().await?;
    println!("ping OK");
    // waiting to push some metrics
    tokio::time::sleep(Duration::from_secs(4)).await;
    client_endpoint.close().await;

    Ok(())
}
