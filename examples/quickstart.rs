use iroh::Endpoint;
use iroh_n0des::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = Endpoint::bind().await?;

    // needs N0DES_API_SECRET set to an environment variable
    // client will now push endpoint metrics to n0des
    let client = Client::builder(&endpoint)
        .api_secret_from_env()?
        .build()
        .await?;

    // we can also ping the service just to confirm everything is working
    client.ping().await?;

    Ok(())
}
