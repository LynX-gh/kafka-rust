#![allow(unused_imports)]
use std::io::{Cursor, Read, Write};
use std::net::TcpListener;
use bytes::BufMut;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                handle_client(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client<T: Read + Write>(stream: &mut T) {
    let mut response_msg = vec![];
    let mut response_len = vec![];
    let mut len_buf = [0_u8; 4];

    if let Err(e) = stream.read_exact(&mut len_buf) {
        println!("error: {}", e);
        return;
    }

    println!("Incoming Message Length : {:?}", len_buf);
    let msg_len = i32::from_be_bytes(len_buf);

    let mut msg_buf = vec![0_u8; msg_len as usize];
    if let Err(e) = stream.read_exact(&mut msg_buf) {
        println!("error: {}", e);
        return;
    }

    println!("Incoming Message Buffer : {:?}", msg_buf);

    // let message_size: i32 = 20;
    // response_msg.put_i32(message_size);
    response_msg.extend(&msg_buf[4..8]);
    response_msg.put_i16(35_i16);

    println!("{}", response_msg.len());
    let message_size = response_msg.len() * 2;
    response_len.put_i32(message_size as i32);

    if let Err(e) = stream.write_all(&response_len) {
        println!("error: {}", e);
        return;
    }

    if let Err(e) = stream.write(&response_msg) {
        println!("error: {}", e);
        return;
    }

    let mut remaining_buf = vec![];
    if let Err(e) = stream.read_to_end(&mut remaining_buf) {
        println!("error: {}", e);
        return;
    }

    println!("accepted new connection");
}
