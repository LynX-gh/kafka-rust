// use std::io::{Cursor, Read, Write, Error};
use bytes::BufMut;

use crate::{CONFIG, KafkaConfig};

use super::utils;

pub async fn handle_apiversions_request(mut msg_buf: &[u8]) -> Vec<u8> {
    let mut response_msg = vec![];

    // Read Request Header V0
    let (_, api_version, correlation_id) = utils::read_request_header_v0(&mut msg_buf);

    // Resp Header V0
    utils::write_resp_header_v0(&mut response_msg, correlation_id);

    // Add error code to resp
    if api_version > 0 && api_version <= 4 {
        response_msg.put_i16(0);
    }
    else {
        response_msg.put_i16(35);
    }

    // Get config
    let config = CONFIG.get_or_init(KafkaConfig::new).await;

    // Add data
    response_msg.put_u8(config.api_key.key.len() as u8 + 1);

    for key in config.api_key.key.iter() {
        response_msg.put_i16(key.key); // api key
        response_msg.put_i16(key.min_version); // min version
        response_msg.put_i16(key.max_version); // max version
        response_msg.put_u8(0); // TAG_BUFFER length
    }

    // Close Array
    response_msg.put_i32(420); // throttle time ms
    response_msg.put_i8(0); // TAG_BUFFER length

    // calc msg size and return response
    utils::append_msg_len(&mut response_msg)
}
