use std::{
    path::PathBuf,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::Result;
use clap::Parser;
use ed25519_dalek::{SigningKey, VerifyingKey};
use iroh_n0des::caps::{create_api_token, Caps, Token};
use ssh_encoding::EncodePem;
use ssh_key::Algorithm;

#[derive(Parser, Debug)]
enum Cli {
    /// Parse a file. The file may be a SSH private key, SSH public key, or Rcan token.
    Parse { path: PathBuf },
    /// Creates a new Ed25519 keypair and saves it in the SSH format.
    CreateKeypair { out: PathBuf },
    /// Creates a new Rcan token.
    CreateToken {
        /// Path to the SSH private key issuing this token.
        #[clap(short, long)]
        issuer: PathBuf,

        /// Path to the SSH public key receiving this token.
        #[clap(short, long)]
        audience: PathBuf,

        /// Max age after which this token expires.
        #[clap(short, long, default_value = "30d")]
        max_age: String,

        /// Path to a previous token which the new token will extend.
        #[clap(short, long)]
        token: Option<PathBuf>,

        /// Path to write the token to.
        out: PathBuf,
    },
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Cli::parse();
    let mut rng = rand::rngs::OsRng {};
    match args {
        Cli::Parse { path } => {
            let content = tokio::fs::read_to_string(path).await?;
            if let Ok(key) = ssh_key::PublicKey::from_openssh(&content) {
                println!("Public SSH key");
                println!("");
                let raw_key: VerifyingKey = key
                    .key_data()
                    .ed25519()
                    .ok_or_else(|| anyhow::anyhow!("Only Ed25519 keys are supported."))?
                    .try_into()?;
                println!("Public key: {}", hex::encode(raw_key.to_bytes()));
            } else if let Ok(key) = ssh_key::PrivateKey::from_openssh(&content) {
                println!("Private SSH key");
                println!("");
                let raw_key: SigningKey = key
                    .key_data()
                    .ed25519()
                    .ok_or_else(|| anyhow::anyhow!("Only Ed25519 keys are supported."))?
                    .private
                    .clone()
                    .into();
                println!("Private key: {}", hex::encode(raw_key.to_bytes()));
                println!(
                    "Public key:  {}",
                    hex::encode(raw_key.verifying_key().to_bytes())
                );
            } else if let Ok(token) = Token::from_pem(&content) {
                println!("Rcan chain (length {})", token.len());
                println!("");
                println!(
                    "Root issuer:      {}",
                    hex::encode(&token.first_issuer()?.to_bytes())
                );
                println!(
                    "Final audience:   {}",
                    hex::encode(&token.final_audience()?.to_bytes())
                );
                println!("Final capability: {:?}", token.final_capability()?,);
                println!("");
                for (i, rcan) in token.iter().enumerate() {
                    println!("Rcan #{i}:");
                    println!("    Issuer:     {}", hex::encode(rcan.issuer().to_bytes()));
                    println!(
                        "    Audience:   {}",
                        hex::encode(rcan.audience().to_bytes())
                    );
                    println!("    Capability: {:?}", rcan.capability());
                    // println!("    capability: {:?}", rcan.capability());
                    let expires = match rcan.expires() {
                        rcan::Expires::Never => "never".to_string(),
                        rcan::Expires::At(ts) => {
                            let t = UNIX_EPOCH.checked_add(Duration::from_secs(*ts)).unwrap();
                            let t = time::OffsetDateTime::from(t);
                            t.format(&time::format_description::well_known::Rfc3339)?
                        }
                    };
                    println!("    Expires:    {}", expires);
                }
                println!("");
                match token.verify_chain() {
                    Ok(()) => println!("Chain is valid."),
                    Err(err) => println!("Chain is INVALID: {err}"),
                }
            }
        }
        Cli::CreateKeypair { out } => {
            let private_key = ssh_key::PrivateKey::random(&mut rng, Algorithm::Ed25519)?;
            let encoded = private_key.to_openssh(Default::default())?;
            tokio::fs::write(&out, encoded).await?;
            println!("Private key written to {}", out.to_string_lossy());

            let mut out = out;
            let file_name = format!(
                "{}.pub",
                out.file_name()
                    .expect("invalid path")
                    .to_str()
                    .expect("invalid filename")
            );
            out.set_file_name(file_name);
            let encoded = private_key.public_key().to_openssh()?;
            tokio::fs::write(&out, encoded).await?;
            println!("Public key written to {}", out.to_string_lossy());
        }
        Cli::CreateToken {
            token,
            issuer,
            audience,
            max_age,
            out,
        } => {
            let max_age = parse_duration::parse(&max_age)?;
            let token = match token {
                None => None,
                Some(path) => Some(Token::read_from_file(path).await?),
            };
            let issuer = {
                let file_content = tokio::fs::read_to_string(issuer).await?;
                ssh_key::PrivateKey::from_openssh(&file_content)?
            };
            let audience = {
                let file_content = tokio::fs::read_to_string(audience).await?;
                let public_key = ssh_key::PublicKey::from_openssh(&file_content)?;
                let raw_key: VerifyingKey = public_key
                    .key_data()
                    .ed25519()
                    .ok_or_else(|| anyhow::anyhow!("Only Ed25519 keys are supported."))?
                    .try_into()?;
                raw_key
            };
            // let cap = IpsCap::V1(IpsCapV1::Api);
            let token = create_api_token(&issuer, token.as_ref(), audience, max_age, Caps::all())?;
            let token = token.encode_pem_string(Default::default())?;
            tokio::fs::write(&out, token).await?;
            println!("Token written to {}", out.to_string_lossy());
        }
    }
    Ok(())
}
