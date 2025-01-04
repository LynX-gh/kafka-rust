#![allow(unused_imports)]
use std::iter::Once;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task;
use tokio::sync::OnceCell;

pub mod kafka_client;
pub mod util;

use kafka_client::handle_client;
use util::KafkaConfig;

static CONFIG: OnceCell<Arc<KafkaConfig>> = OnceCell::const_new();

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    CONFIG.set(Arc::new(KafkaConfig::new())).unwrap();

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
