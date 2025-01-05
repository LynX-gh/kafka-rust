// use std::io::{Cursor, Read, Write, Error};
use bytes::{Buf, BufMut};

use crate::CONFIG;

pub fn handle_apiversions_request(mut msg_buf: &[u8]) -> Vec<u8> {
    let mut response_len = vec![];
    let mut response_msg = vec![];

    let _api_key = msg_buf.get_i16();
    let api_version = msg_buf.get_i16();
    let correlation_id = msg_buf.get_i32();

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
    if let Some(config) = CONFIG.get() {
        response_msg.put_u8(config.api_key.key.len() as u8 + 1);

        for key in config.api_key.key.iter() {
            response_msg.put_i16(key.key); // api key
            response_msg.put_i16(key.min_version); // min version
            response_msg.put_i16(key.max_version); // max version
            response_msg.put_u8(0); // TAG_BUFFER length
        }
    }

    // Close Array
    response_msg.put_i32(420); // throttle time ms
    response_msg.put_i8(0); // TAG_BUFFER length

    // calc msg size
    let message_size = response_msg.len();
    response_len.put_i32(message_size as i32);
    response_len.extend(response_msg);

    response_len
}
