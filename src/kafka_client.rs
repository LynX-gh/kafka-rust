use std::io::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
// use bytes::{Bytes, BytesMut, Buf, BufMut};

pub mod apiversions;
pub mod fetch;
pub mod invalid;
pub mod describetopicpartitions;
pub mod read_cluster_metadata;

use apiversions::handle_apiversions_request;
use fetch::handle_fetch_request;
use invalid::handle_invalid_request;
use describetopicpartitions::handle_describetopicpartitions_request;

// use crate::CONFIG;

pub async fn handle_client(stream: &mut TcpStream) -> Result<(), Error>{
    while stream.peek(&mut [0; 4]).await.is_ok() {

        // println!("Config : {:#?}", CONFIG);

        let msg_len = read_message_length(stream).await?;
        println!("Incoming Message Length : {:?}", msg_len);

        let msg_buf = read_message_data(stream, msg_len).await?;
        println!("Incoming Message Buffer : {:?}", msg_buf);

        let (api_key, api_version) = parse_message_header(&msg_buf);
        println!("API Key : {}", api_key);
        println!("API Version : {}", api_version);

        match api_key {
            1 => {
                let response = &handle_fetch_request(&msg_buf);
                stream.write(response).await?;
            },
            18 => {
                let response = &handle_apiversions_request(&msg_buf).await;
                stream.write(response).await?;
            },
            75 => {
                let response = &handle_describetopicpartitions_request(&msg_buf).await;
                stream.write(response).await?;
            },
            _ => {
                let response = &handle_invalid_request(&msg_buf);
                stream.write(response).await?;
            }
        }

        // println!("handled new connection");
    }
    Ok(())
}

async fn read_message_length(stream: &mut TcpStream) -> Result<i32, Error> {
    let mut len_buf = [0_u8; 4];
    stream.read_exact(&mut len_buf).await?;
    Ok(i32::from_be_bytes(len_buf))
}

async fn read_message_data(stream: &mut TcpStream, msg_len: i32) -> Result<Vec<u8>, Error> {
    let mut msg_buf = vec![0_u8; msg_len as usize];
    stream.read_exact(&mut msg_buf).await?;
    Ok(msg_buf)
}

fn parse_message_header(msg_buf: &[u8]) -> (i16, i16) {
    let api_key = i16::from_be_bytes(msg_buf[0..2].try_into().expect("API Key Failed"));
    let api_version = i16::from_be_bytes(msg_buf[2..4].try_into().expect("API Version Failed"));
    (api_key, api_version)
}