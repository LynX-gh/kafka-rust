// use std::io::{Cursor, Read, Write, Error};
use bytes::{Buf, BufMut};

use crate::kafka_client::utils;

pub fn handle_fetch_request(mut msg_buf: &[u8]) -> Vec<u8>{
    let mut response_msg = vec![];

    // Read Fetch Request Header
    let (_, api_version, correlation_id, client_id) = utils::read_request_header_v2(&mut msg_buf);
    
    // Read Fetch Request Body
    let _max_wait_ms = msg_buf.get_i32();
    let _min_bytes= msg_buf.get_i32();
    let _max_bytes= msg_buf.get_i32();
    let _isolation_level = msg_buf.get_i8();
    let _session_id = msg_buf.get_i32();
    let _session_epoch = msg_buf.get_i32();

    let topic_count = msg_buf.get_u8().saturating_sub(1);
    let mut topics = Vec::new();
    println!("Client ID - {:?}", client_id);
    println!("Corr ID - {}", correlation_id);
    println!("Topic Cnt - {}", topic_count);
    for _ in 0..topic_count {
        let topic_id = msg_buf.get_i128();
        let partition_count = msg_buf.get_u8().saturating_sub(1);
        for _ in 0..partition_count {
            let _partition = msg_buf.get_i32();
            let _current_leader_epoch = msg_buf.get_i32();
            let _fetch_offset = msg_buf.get_i64();
            let _last_fetched_epoch = msg_buf.get_i32();
            let _log_start_offset = msg_buf.get_i64();
            let _partition_max_bytes = msg_buf.get_i32();
            msg_buf.advance(1); // TAG_BUFFER
        }
        msg_buf.advance(1); // TAG_BUFFER
        topics.push(topic_id);
    }

    println!("Topics - {:?}", topics);

    // Resp Header
    utils::write_resp_header_v1(&mut response_msg, correlation_id);

    // Resp Body
    response_msg.put_i32(0); // throttle time ms

    // Add error code to resp
    if api_version > 0 && api_version <= 16 {
        response_msg.put_i16(0);
    }
    else {
        response_msg.put_i16(35);
    }

    response_msg.put_i32(0); // session_id
    response_msg.put_u8(topic_count+1); // num topics + 1

    // Responses Array
    for topic in topics {
        response_msg.put_i128(topic);

        // Partitions Array
        response_msg.put_u8(2); // num partitions + 1
        response_msg.put_i32(0); // partition_index
        response_msg.put_i16(0); // error_code
        response_msg.put_i64(0); // high_watermark
        response_msg.put_i64(0); // last_stable_offset
        response_msg.put_i64(0); // log_start_offset

        // Aborted Transactions Array
        response_msg.put_u8(0); // num aborted_transactions

        // Continue Partitions Array
        response_msg.put_i32(0); // preferred_read_replica

        // Records Compact Array
        response_msg.put_u8(1); // num records + 1

        // Continue Partitions Array
        response_msg.put_i8(0); // TAG_BUFFER

        // Continue Responses Array
        response_msg.put_i8(0); // TAG_BUFFER
    }

    response_msg.put_i8(0); // TAG_BUFFER length

    // calc msg size
    utils::append_msg_len(&mut response_msg);

    response_msg
}