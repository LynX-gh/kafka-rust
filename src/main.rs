#![allow(unused_imports)]
use std::f32::consts::E;
use std::thread;
use std::net::TcpListener;

pub mod kafka_client;

use kafka_client::handle_client;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    handle_incoming_connections(listener);
}

fn handle_incoming_connections(listener: TcpListener) {
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                thread::spawn(move || {
                    if let Err(e) = handle_client(&mut stream) {
                        println!("error: {}", e);
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}