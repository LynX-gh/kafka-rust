#![allow(unused_imports)]
use std::io::Write;
use std::net::TcpListener;
use bytes::BufMut;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                let message_size:i32 = 0;
                let correlation_id:i32 = 7;

                let mut response = vec![];
                response.put_i32(message_size);
                response.put_i32(correlation_id);

                _stream.write_all(&response).unwrap();

                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
