//! Requires: jito-searcher-client, jito_protos, solana-client, solana-sdk, tokio, futures-util

use futures_util::StreamExt;
use jito_protos::bundle::bundle_result;
use jito_protos::bundle::{Bundle, BundleResult};
use jito_protos::convert::proto_packet_from_versioned_tx;
use jito_protos::searcher::searcher_service_client::SearcherServiceClient;
use jito_protos::searcher::{
    GetTipAccountsRequest, SendBundleRequest, SubscribeBundleResultsRequest,
};
use jito_searcher_client::get_searcher_client_auth;
use jito_searcher_client::token_authenticator::ClientInterceptor;
use rand::Rng;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use solana_sdk::system_instruction::transfer;
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use tonic::Streaming;

use std::collections::HashSet;
use std::env;
use std::fs;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod error {
    use std::fmt;

    #[derive(Debug)]
    pub enum SearcherError {
        IoError(String),
        GrpcError(String),
        ConfigError(String),
        BundleError(String),
    }

    impl fmt::Display for SearcherError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self {
                SearcherError::IoError(s) => write!(f, "IO error: {}", s),
                SearcherError::GrpcError(s) => write!(f, "gRPC error: {}", s),
                SearcherError::ConfigError(s) => write!(f, "Config error: {}", s),
                SearcherError::BundleError(s) => write!(f, "Bundle error: {}", s),
            }
        }
    }

    impl std::error::Error for SearcherError {}
}

use error::SearcherError;

type Result<T> = std::result::Result<T, SearcherError>;

struct Config {
    keypair_path: String,
    block_engine_url: String,
    rpc_url: String,
    tip_lamports: u64,
    bundle_count: u32,
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Config {
            keypair_path: env::var("KEYPAIR_PATH")
                .unwrap_or_else(|_| "~/.config/solana/id.json".to_string()),
            block_engine_url: env::var("BLOCK_ENGINE_URL")
                .map_err(|_| SearcherError::ConfigError("BLOCK_ENGINE_URL not set".into()))?,
            rpc_url: env::var("RPC_URL")
                .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into()),
            tip_lamports: env::var("TIP_LAMPORTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
            bundle_count: env::var("BUNDLES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
        })
    }
}

fn load_keypair(path: &str) -> Result<Keypair> {
    let expanded = path.replace('~', &env::var("HOME").unwrap_or_default());
    let _ = fs::metadata(&expanded)
        .map_err(|e| SearcherError::IoError(format!("keypair file not found: {}", e)))?;

    read_keypair_file(&expanded)
        .map_err(|e| SearcherError::IoError(format!("invalid keypair: {}", e)))
}

async fn latest_blockhash(rpc: &RpcClient) -> Result<Hash> {
    rpc.get_latest_blockhash()
        .await
        .map_err(|e| SearcherError::GrpcError(format!("failed to fetch blockhash: {}", e)))
}

async fn fetch_tip_accounts(client: &mut SearcherClientAuth) -> Result<Vec<Pubkey>> {
    let resp = client
        .inner
        .get_tip_accounts(GetTipAccountsRequest {})
        .await
        .map_err(|e| SearcherError::GrpcError(format!("get_tip_accounts failed: {}", e)))?
        .into_inner();

    resp.accounts
        .iter()
        .map(|a| {
            Pubkey::from_str(a)
                .map_err(|e| SearcherError::ConfigError(format!("bad tip account {}: {}", a, e)))
        })
        .collect()
}

struct SearcherClientAuth {
    inner: SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    payer: Keypair,
}

impl SearcherClientAuth {
    async fn new(url: &str, auth_keypair: Arc<Keypair>, payer: Keypair) -> Result<Self> {
        let inner = get_searcher_client_auth(url, &auth_keypair)
            .await
            .map_err(|e| SearcherError::GrpcError(format!("auth failed: {}", e)))?;

        Ok(SearcherClientAuth { inner, payer })
    }

    async fn send_bundle(&mut self, txs: &[VersionedTransaction]) -> Result<String> {
        let packets = txs.iter().map(proto_packet_from_versioned_tx).collect();

        let resp = self
            .inner
            .send_bundle(SendBundleRequest {
                bundle: Some(Bundle {
                    header: None,
                    packets,
                }),
            })
            .await
            .map_err(|e| SearcherError::BundleError(format!("send_bundle failed: {}", e)))?
            .into_inner();

        Ok(resp.uuid)
    }

    async fn subscribe_results(&mut self) -> Result<Streaming<BundleResult>> {
        self.inner
            .subscribe_bundle_results(SubscribeBundleResultsRequest {})
            .await
            .map(|r| r.into_inner())
            .map_err(|e| {
                SearcherError::GrpcError(format!("subscribe_bundle_results failed: {}", e))
            })
    }
}

fn build_bundle_txs(
    payer: &Keypair,
    tip_acct: Pubkey,
    tip_lamports: u64,
    blockhash: Hash,
) -> Vec<VersionedTransaction> {
    let trade = Transaction::new_signed_with_payer(
        &[spl_memo::build_memo(
            format!(
                "jito {}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )
            .as_bytes(),
            &[&payer.pubkey()],
        )],
        Some(&payer.pubkey()),
        &[payer],
        blockhash,
    );

    let tip = Transaction::new_signed_with_payer(
        &[transfer(&payer.pubkey(), &tip_acct, tip_lamports)],
        Some(&payer.pubkey()),
        &[payer],
        blockhash,
    );

    vec![
        VersionedTransaction::from(trade),
        VersionedTransaction::from(tip),
    ]
}

struct Searcher {
    client: SearcherClientAuth,
    config: Config,
    landed: Arc<AtomicU64>,
    sent: Arc<AtomicU64>,
    rpc_client: RpcClient,
}

impl Searcher {
    async fn new(config: Config) -> Result<Self> {
        let auth_kp = Arc::new(load_keypair(&config.keypair_path)?);
        let payer = load_keypair(&config.keypair_path)?;
        let client = SearcherClientAuth::new(&config.block_engine_url, auth_kp, payer).await?;

        let rpc_client = RpcClient::new(config.rpc_url.clone());

        Ok(Searcher {
            client,
            config,
            landed: Arc::new(AtomicU64::new(0)),
            sent: Arc::new(AtomicU64::new(0)),
            rpc_client,
        })
    }

    async fn run(&mut self) -> Result<()> {
        let tip_accounts = fetch_tip_accounts(&mut self.client).await?;

        let mut results_stream = self.client.subscribe_results().await?;
        let landed = self.landed.clone();
        let resulted_ids = Arc::new(Mutex::new(HashSet::new()));
        let resulted_ids_task = resulted_ids.clone();
        tokio::spawn(async move {
            let mut landed_ids: HashSet<String> = HashSet::new();
            while let Some(Ok(result)) = results_stream.next().await {
                resulted_ids_task
                    .lock()
                    .unwrap()
                    .insert(result.bundle_id.clone());
                let did_land = matches!(
                    result.result,
                    Some(bundle_result::Result::Processed(_))
                        | Some(bundle_result::Result::Finalized(_))
                );
                if did_land && landed_ids.insert(result.bundle_id.clone()) {
                    landed.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let mut sent_uuids = Vec::new();

        for i in 0..self.config.bundle_count {
            let blockhash = match latest_blockhash(&self.rpc_client).await {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("bundle {} blockhash fetch failed: {}", i, e);
                    continue;
                }
            };
            let tip_acct = tip_accounts[rand::thread_rng().gen_range(0..tip_accounts.len())];
            let txs = build_bundle_txs(
                &self.client.payer,
                tip_acct,
                self.config.tip_lamports,
                blockhash,
            );

            match self.client.send_bundle(&txs).await {
                Ok(uuid) => {
                    self.sent.fetch_add(1, Ordering::Relaxed);
                    println!("bundle {} sent: {}", i, uuid);
                    sent_uuids.push(uuid);
                }
                Err(e) => eprintln!("bundle {} failed: {}", i, e),
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let all_resulted = {
                let ids = resulted_ids.lock().unwrap();
                sent_uuids.iter().all(|id| ids.contains(id))
            };
            if all_resulted || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let sent_count = self.sent.load(Ordering::Relaxed);
        let landed_count = self.landed.load(Ordering::Relaxed);

        println!(
            "Sent: {}, Landed: {}, Land rate: {:.2}%",
            sent_count,
            landed_count,
            (landed_count as f64 / sent_count.max(1) as f64) * 100.0
        );

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::from_env()?;
    let mut searcher = Searcher::new(cfg).await?;
    searcher.run().await
}
