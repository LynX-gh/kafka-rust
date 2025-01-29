// use std::io::{Cursor, Read, Write, Error};
use bytes::{Buf, BufMut};

use super::read_cluster_metadata::{read_cluster_metadata, return_topic_uuid, describe_metadata_topic_partitions};

pub async fn handle_describetopicpartitions_request(mut msg_buf: &[u8]) -> Vec<u8> {
    let mut response_len = vec![];
    let mut response_msg = vec![];

    // Read DescribeTopicPartitions Request Header
    let _api_key = msg_buf.get_i16();
    let _api_version = msg_buf.get_i16();
    let correlation_id = msg_buf.get_i32();

    let client_id_len = msg_buf.get_i16();
    let mut client_id = Vec::new();
    if client_id_len != -1 {
        for _ in 0..client_id_len {
            client_id.push(msg_buf.get_i8());
        }
    }
    msg_buf.advance(1); // TAG_BUFFER

    // Read DescribeTopicPartitions Request Body
    let topic_count = msg_buf.get_u8().saturating_sub(1);

    let mut topics = Vec::new();
    for _ in 0..topic_count { 
        let topic_name_len = msg_buf.get_u8().saturating_sub(1); // !! COMPACT NULLABLE IN RETURN BUT NOT NULLABLE IN REQUEST WTF !!

        let mut topic_name = Vec::new();
        for _ in 0..topic_name_len {
            topic_name.push(msg_buf.get_u8());
        }
        topics.push(topic_name);
        
        msg_buf.advance(1); // TAG_BUFFER
    }
    // println!("Topics - {:?}", topics);

    let _response_partition_limit = msg_buf.get_i32();

    msg_buf.advance(1); // CURSOR
    msg_buf.advance(1); // TAG_BUFFER

    let data = read_cluster_metadata().await.expect("Failed to Read File");
    // for topic_name in &topics {
    //     match return_topic_uuid(&data, topic_name) {
    //         Some(uuid) => {
    //             let partition_data = describe_metadata_topic_partitions(&data, uuid);
    //             println!("{partition_data:?}");
    //         },
    //         None => {
    //             println!("Topic Partitions Not Available")
    //         }
    //     }
    // }

    // Resp Header
    response_msg.put_i32(correlation_id); // Add cid
    response_msg.put_i8(0); // TAG_BUFFER

    // Resp Body
    response_msg.put_i32(0); // throttle time ms
    response_msg.put_u8(topic_count+1); // num topics + 1

    // Topics Array
    for topic in &topics {
        // response_msg.put_i16(3); // error_code UNKNOWN_TOPIC_OR_PARTITION

        match return_topic_uuid(&data, topic) {
            Some(topic_uuid) => {
                response_msg.put_i16(0);
                response_msg.put_u8((topic.len()+1) as u8); // Topic Name Len !! COMPACT NULLABLE IN RETURN BUT NOT NULLABLE IN REQUEST WTF !!
                for char in topic {
                    response_msg.put_u8(*char); // Topic Name
                }
                response_msg.put_i128(topic_uuid); // Topic ID
                response_msg.put_i8(1); // is_internal
                
                // Partitions Array
                match describe_metadata_topic_partitions(&data, topic_uuid) {
                    Some(partition_data) => {
                        println!("{partition_data:?}");
                        response_msg.put_u8(partition_data.len() as u8 + 1);

                        for data in partition_data {
                            response_msg.put_i16(0); // Error Code
                            response_msg.put_u32(data.partition_id); // Partition Nndex
                            response_msg.put_u32(data.leader); // Leader ID
                            response_msg.put_u32(data.leader_epoch); // Leader Epoch

                            // Replica Nodes Array
                            response_msg.put_u8(data.replicas.len() as u8 + 1); // 
                            for replica in &data.replicas {
                                response_msg.put_u64(*replica);
                            }

                            // ISE Nodes Array
                            response_msg.put_u8(data.isr.len() as u8 + 1);
                            for isr in &data.isr {
                                response_msg.put_u64(*isr);
                            }
                            
                            response_msg.put_u8(1); // Eligible Leader Replicas
                            response_msg.put_u8(1); // Last Known ELR
                            response_msg.put_u8(1); // Offline Replicas
                            response_msg.put_i8(0); // TAG_BUFFER
                        }
                    },
                    None => {
                        println!("Topic Partitions Not Available");
                        response_msg.put_u8(1); // num partitions + 1
                    }
                }
            },
            None => {
                response_msg.put_i16(3);
                response_msg.put_u8((topic.len()+1) as u8); // Topic Name Len !! COMPACT NULLABLE IN RETURN BUT NOT NULLABLE IN REQUEST WTF !!
                for char in topic {
                    response_msg.put_u8(*char); // Topic Name
                }
                response_msg.put_i128(0); // Topic ID = 0
                response_msg.put_i8(1); // is_internal

                // Partitions Array
                response_msg.put_u8(1); // num partitions + 1
            }
        }

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