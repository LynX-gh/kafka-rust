use std::env;
use tokio::net::TcpListener;
use tokio::task;
use tokio::sync::OnceCell;

pub mod kafka_client;
pub mod load_config;

use kafka_client::{handle_client, read_cluster_metadata::RecordBatch};
use load_config::KafkaConfig;

static CONFIG: OnceCell<KafkaConfig> = OnceCell::const_new();
static METADATA: OnceCell<Vec<RecordBatch>> = OnceCell::const_new();

#[tokio::main]
async fn main() {
    env::set_var("RUST_BACKTRACE", "1");
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();
    handle_incoming_connections(listener).await;
}

async fn handle_incoming_connections(listener: TcpListener) {
    while let Ok((mut stream, _)) = listener.accept().await {
        task::spawn(async move {
            if let Err(e) = handle_client(&mut stream).await {
                println!("error: {}", e);
            }
        });
    }
}
