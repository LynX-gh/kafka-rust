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
            Ok(mut _stream) => {
                let mut response = vec![];
                let mut len_buf = [0_u8; 4];
                match _stream.read_exact(&mut len_buf) {
                    Ok(_len) => {
                        println!("Incoming Message Length : {:?}", len_buf);
                        let msg_len = i32::from_be_bytes(len_buf);

                        let mut msg_buf = vec![0_u8; msg_len as usize];
                        match _stream.read_exact(&mut msg_buf) {
                            Ok(_len) => {
                                println!("Incoming Message Buffer : {:?}", msg_buf);

                                let message_size:i32 = 20;

                                response.put_i32(message_size);
                                response.extend(&msg_buf[4..8]);
                                response.put_i16(35_i16);
                            }
                            Err(e) => {
                                println!("error: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("error: {}", e);
                    }
                }

                // println!("RESP : {:?}", response);

                _stream.write(&response).unwrap();
                let mut _remaining_buf = vec![];
                _stream.read_to_end(&mut _remaining_buf).unwrap();

                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
