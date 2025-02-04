// use std::io::{Cursor, Read, Write, Error};
use bytes::{Buf, BufMut};

use crate::METADATA;
use crate::kafka_client::read_cluster_metadata::return_topic_name;

use super::{read_cluster_metadata::{read_cluster_metadata, check_topic_id_exists}, utils, read_message_log};

#[derive(Debug, Clone)]
struct FetchTopics {
    topic_id: i128,
    partitions: Vec<Partition>
}

#[derive(Debug, Clone)]
struct Partition {
    partition_idx: i32,
    _current_leader_epoch: i32,
    _fetch_offset: i64,
    _last_fetched_epoch: i32,
    log_start_offset: i64,
    _partition_max_bytes: i32,
}

impl FetchTopics {
    fn new (buf: &mut &[u8]) -> Self {
        let mut res = Self {
            topic_id: buf.get_i128(),
            partitions: Vec::new(),
        };

        let partition_count = buf.get_u8().saturating_sub(1);
        for _ in 0..partition_count {
            res.partitions.push(Partition::new(buf));
        }
        res
    }
}

impl Partition {
    fn new(buf: &mut &[u8]) -> Self {
        let res = Self {
            partition_idx: buf.get_i32(),
            _current_leader_epoch: buf.get_i32(),
            _fetch_offset: buf.get_i64(),
            _last_fetched_epoch: buf.get_i32(),
            log_start_offset: buf.get_i64(),
            _partition_max_bytes: buf.get_i32(),
        };

        buf.advance(1); // TAG_BUFFER
        res
    }
}

pub async fn handle_fetch_request(mut msg_buf: &[u8]) -> Vec<u8>{
    let mut response_msg = vec![];

    // Read Fetch Request Header
    let (_, api_version, correlation_id, _) = utils::read_request_header_v2(&mut msg_buf);
    
    // Read Fetch Request Body
    let _max_wait_ms = msg_buf.get_i32();
    let _min_bytes= msg_buf.get_i32();
    let _max_bytes= msg_buf.get_i32();
    let _isolation_level = msg_buf.get_i8();
    let _session_id = msg_buf.get_i32();
    let _session_epoch = msg_buf.get_i32();

    let topic_count = msg_buf.get_u8().saturating_sub(1);
    let mut topics = Vec::new();

    for _ in 0..topic_count {
        topics.push(FetchTopics::new(&mut msg_buf));
        msg_buf.advance(1); // TAG_BUFFER
    }

    println!("Topics - {:?}", topics);

    let data = METADATA.get_or_init(read_cluster_metadata).await;
    // println!("Data - \n{:#?}", data);

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
        response_msg.put_i128(topic.topic_id);

        // Partitions Array
        response_msg.put_u8(topic.partitions.len() as u8 + 1); // num partitions + 1

        for partition in topic.partitions {
            response_msg.put_i32(partition.partition_idx); // partition_index

            if check_topic_id_exists(&data, topic.topic_id) {
                response_msg.put_i16(0); // error_code
            } else {
                response_msg.put_i16(100); // error_code
            }

            response_msg.put_i64(0); // high_watermark
            response_msg.put_i64(0); // last_stable_offset
            response_msg.put_i64(partition.log_start_offset); // log_start_offset

            // Aborted Transactions Array
            response_msg.put_u8(0); // num aborted_transactions

            // Continue Partitions Array
            response_msg.put_i32(0); // preferred_read_replica

            // Records Compact Array from File
            let mut records_data = Vec::new();
            match return_topic_name(&data, topic.topic_id) {
                Some(Ok(topic_name)) => {
                    match read_message_log::read_messages(&topic_name, 0).await {
                        Some(record_batch) => {
                            records_data.extend(record_batch);
                        },
                        None => {
                            records_data.put_u8(1); // num records + 1
                        }
                    }
                },
                _ => {
                    records_data.put_u8(1); // num records + 1
                }
            }

            response_msg.put_u8(records_data.len() as u8 + 1); // total size of records + 1
            response_msg.extend(records_data);

            // Continue Partitions Array
            response_msg.put_i8(0); // TAG_BUFFER
        }
        

        // Continue Responses Array
        response_msg.put_i8(0); // TAG_BUFFER
    }

    response_msg.put_i8(0); // TAG_BUFFER length

    // calc msg size
    utils::append_msg_len(&mut response_msg)
}