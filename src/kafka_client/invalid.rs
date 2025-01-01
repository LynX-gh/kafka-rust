use std::io::{Cursor, Read, Write, Error};
use bytes::{Bytes, BytesMut, Buf, BufMut};

pub fn handle_invalid_request(mut msg_buf: &[u8]) -> Vec<u8> {
    let mut response_len = vec![];
    let mut response_msg = vec![];

    let _api_key = msg_buf.get_i16();
    let _api_version = msg_buf.get_i16();
    let correlation_id = msg_buf.get_i32();

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