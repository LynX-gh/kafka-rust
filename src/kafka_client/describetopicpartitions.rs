use std::io::{Cursor, Read, Write, Error};
use bytes::{Bytes, BytesMut, Buf, BufMut};

use crate::CONFIG;

pub fn handle_describetopicpartitions_request(mut msg_buf: &[u8]) -> Vec<u8> {
    let mut response_len = vec![];
    let mut response_msg = vec![];

    // Read DescribeTopicPartitions Request Header
    let _api_key = msg_buf.get_i16();
    let _api_version = msg_buf.get_i16();
    let correlation_id = msg_buf.get_i32();
    // println!("Corr ID - {}", correlation_id);
    let client_id_len = msg_buf.get_i16();
    let mut client_id = Vec::new();
    if client_id_len != -1 {
        for _ in 0..client_id_len {
            client_id.push(msg_buf.get_i8());
        }
    }
    msg_buf.advance(1); // TAG_BUFFER

    // println!("Client ID Len - {}", client_id_len);
    // println!("Client ID - {:?}", client_id);

    // Read DescribeTopicPartitions Request Body
    let topic_count = msg_buf.get_u8().saturating_sub(1);
    let mut topics = Vec::new();
    // println!("Topics Cnt - {:?}", topic_count);
    for _ in 0..topic_count { 
        let topic_name_len = msg_buf.get_u8().saturating_sub(1);

        let mut topic_name = Vec::new();
        for _ in 0..topic_name_len {
            topic_name.push(msg_buf.get_i8());
        }
        topics.push(topic_name);
        
        msg_buf.advance(1); // TAG_BUFFER
    }
    // println!("Topics - {:?}", topics);

    let _response_partition_limit = msg_buf.get_i32();
    msg_buf.advance(1); // CURSOR
    msg_buf.advance(1); // TAG_BUFFER

    // Resp Header
    response_msg.put_i32(correlation_id); // Add cid
    response_msg.put_i8(0); // TAG_BUFFER

    // Resp Body
    response_msg.put_i32(0); // throttle time ms
    response_msg.put_u8(topic_count+1); // num topics + 1

    // Topics Array
    for topic in topics {
        response_msg.put_i16(3); // error_code UNKNOWN_TOPIC_OR_PARTITION
        response_msg.put_u8((topic.len()+1) as u8); // Topic Name Len
        println!("Topics - {:?}", topic.len() as u8);
        for char in topic {
            response_msg.put_i8(char); // Topic Name
        }
        response_msg.put_i128(0); // Topic ID = 0
        response_msg.put_i8(1); // is_internal
        
        // Partitions Array
        response_msg.put_u8(1); // num partitions + 1

        // Continue Topics Array
        response_msg.put_i32(3576); // topic_authorized_operations

        response_msg.put_i8(0); // TAG_BUFFER
    }

    response_msg.put_u8(255); // CURSOR
    response_msg.put_i8(0); // TAG_BUFFER

    // calc msg size
    let message_size = response_msg.len();
    response_len.put_i32(message_size as i32);
    response_len.extend(response_msg);

    response_len
}