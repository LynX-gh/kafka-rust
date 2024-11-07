#![allow(unused_imports)]
use std::io::{Cursor, Read, Write, Error};
use std::thread;
use std::net::{TcpListener, TcpStream};
use bytes::BufMut;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                thread::spawn(move || {
                    handle_client(&mut stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(stream: &mut TcpStream) {
    while stream.peek(&mut [0; 4]).is_ok() {

        let mut len_buf = [0_u8; 4];

        if let Err(e) = stream.read_exact(&mut len_buf) {
            println!("error: {}", e);
            return;
        }
        let msg_len = i32::from_be_bytes(len_buf);
        println!("Incoming Message Length : {:?}", msg_len);

        let mut msg_buf = vec![0_u8; msg_len as usize];
        if let Err(e) = stream.read_exact(&mut msg_buf) {
            println!("error: {}", e);
            return;
        }
        println!("Incoming Message Buffer : {:?}", msg_buf);

        let api_key = i16::from_be_bytes(msg_buf[0..2].try_into().expect("API Key Failed Lmao"));
        let api_version = i16::from_be_bytes(msg_buf[2..4].try_into().expect("API Version Failed Lmao"));

        println!("API Key : {}", api_key);
        println!("API Version : {}", api_version);

        match api_key {
            1 => {
                if let Err(e) = stream.write(&handle_fetch_request(&msg_buf)) {
                    println!("error: {}", e);
                    return;
                }
            },
            18 => {
                if let Err(e) = stream.write(&handle_apiversions_request(&msg_buf)) {
                    println!("error: {}", e);
                    return;
                }
            },
            _ => {
                if let Err(e) = stream.write(&handle_invalid_request(&msg_buf)) {
                    println!("error: {}", e);
                    return;
                }
            }
        }

        // println!("accepted new connection");
    }
}

fn handle_apiversions_request(msg_buf: &Vec<u8>) -> Vec<u8> {
    let mut response_len = vec![];
    let mut response_msg = vec![];

    let correlation_id = i32::from_be_bytes(msg_buf[4..8].try_into().expect("Correlation ID Failed Lmao"));
    let api_version = i16::from_be_bytes(msg_buf[2..4].try_into().expect("API Version Failed Lmao"));

    // Add cid
    response_msg.put_i32(correlation_id);

    // Add error code to resp
    if api_version > 0 && api_version <= 4 {
        response_msg.put_i16(0);
    }
    else {
        response_msg.put_i16(35);
    }

    // Add data
    response_msg.put_i8(3); // num api key records + 1

    // Fetch Record = 1 [0:16]
    response_msg.put_i16(1); // api key
    response_msg.put_i16(0); // min version
    response_msg.put_i16(16); // max version
    response_msg.put_i8(0); // TAG_BUFFER length

    // APIVersions Record = 18 [0:4]
    response_msg.put_i16(18); // api key
    response_msg.put_i16(0); // min version
    response_msg.put_i16(4); // max version
    response_msg.put_i8(0); // TAG_BUFFER length

    // Close Array
    response_msg.put_i32(420); // throttle time ms
    response_msg.put_i8(0); // TAG_BUFFER length

    // calc msg size
    let message_size = response_msg.len();
    response_len.put_i32(message_size as i32);
    response_len.extend(response_msg);

    response_len

    // let mut remaining_buf = vec![];
    // if let Err(e) = stream.read_to_end(&mut remaining_buf) {
    //     println!("error: {}", e);
    //     return;
    // }
}

fn handle_fetch_request(msg_buf: &Vec<u8>) -> Vec<u8>{
    let mut response_len = vec![];
    let mut response_msg = vec![];

    let correlation_id = i32::from_be_bytes(msg_buf[4..8].try_into().expect("Correlation ID Failed Lmao"));

    // Add cid
    response_msg.put_i32(correlation_id);

    // Add error code to resp
    response_msg.put_i16(35);

    // calc msg size
    let message_size = response_msg.len();
    response_len.put_i32(message_size as i32);
    response_len.extend(response_msg);

    response_len
}

fn handle_invalid_request(msg_buf: &Vec<u8>) -> Vec<u8> {
    let mut response_len = vec![];
    let mut response_msg = vec![];

    let correlation_id = i32::from_be_bytes(msg_buf[4..8].try_into().expect("Correlation ID Failed Lmao"));

    // Add cid
    response_msg.put_i32(correlation_id);

    // Add error code to resp
    response_msg.put_i16(42);

    // calc msg size
    let message_size = response_msg.len();
    response_len.put_i32(message_size as i32);
    response_len.extend(response_msg);

    response_len
}