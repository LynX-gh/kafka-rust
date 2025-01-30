// use std::io::{Cursor, Read, Write, Error};
use bytes::BufMut;

use super::utils;

pub fn handle_invalid_request(mut msg_buf: &[u8]) -> Vec<u8> {
    let mut response_msg = vec![];

    // Read Request Header V0
    let (_, _, correlation_id) = utils::read_request_header_v0(&mut msg_buf);

    // Resp Header V0
    utils::write_resp_header_v0(&mut response_msg, correlation_id);

    // Add error code to resp
    response_msg.put_i16(42);

    // calc msg size
    utils::append_msg_len(&mut response_msg)
}