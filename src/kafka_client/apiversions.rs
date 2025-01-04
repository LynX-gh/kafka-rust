use std::io::{Cursor, Read, Write, Error};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use toml;

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
    response_msg.put_u8(4); // num api key records + 1

    // Fetch Record = 1 [0:16]
    response_msg.put_i16(1); // api key
    response_msg.put_i16(0); // min version
    response_msg.put_i16(16); // max version
    response_msg.put_u8(0); // TAG_BUFFER length

    // APIVersions Record = 18 [0:4]
    response_msg.put_i16(18); // api key
    response_msg.put_i16(0); // min version
    response_msg.put_i16(4); // max version
    response_msg.put_i8(0); // TAG_BUFFER length
    
    // DescribeTopicPartitions Record = 75 [0:0]
    response_msg.put_i16(75); // api key
    response_msg.put_i16(0); // min version
    response_msg.put_i16(0); // max version
    response_msg.put_i8(0); // TAG_BUFFER length

    // Close Array
    response_msg.put_i32(420); // throttle time ms
    response_msg.put_i8(0); // TAG_BUFFER length

    // calc msg size
    let message_size = response_msg.len();
    response_len.put_i32(message_size as i32);
    response_len.extend(response_msg);

    response_len
}
