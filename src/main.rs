#![allow(unused_imports)]
use std::thread;
use tokio::net::TcpListener;
use tokio::task;

pub mod kafka_client;

use kafka_client::handle_client;

#[tokio::main]
async fn main() {
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