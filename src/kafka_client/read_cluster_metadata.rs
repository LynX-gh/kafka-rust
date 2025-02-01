use std::io::Error;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use bytes::Buf;
use crate::{CONFIG, KafkaConfig};

#[derive(Debug)]
pub struct RecordBatch {
    pub offset: u64,
    pub length: u32,
    pub parition_leader_epoch: u32,
    pub magic_byte: u8,
    pub crc: i32,
    pub attributes: u16,
    pub last_offset_delta: u32,
    pub base_timestamp: u64,
    pub last_timestamp: u64,
    pub producer_id: u64,
    pub producer_epoch: u16,
    pub base_sequence: i32,
    pub records_length: i32,
    pub records: Vec<Record>
}


#[derive(Debug)]
pub struct Record {
    pub length: i8,
    pub attributes: u8,
    pub timestamp_delta: u8,
    pub offset_delta: i8,
    pub key_length: i8,
    pub key: Option<Vec<u8>>,
    pub value_length: i8,
    pub value: RecordValue,
    pub headers_array_count: u8,
}


#[derive(Debug)]
pub enum RecordValue {
    FeatureLevelRecord(FeatureLevelRecord),
    TopicRecord(TopicRecord),
    PartitionRecord(PartitionRecord),
}


#[derive(Debug)]
pub struct FeatureLevelRecord {
    pub frame_version: u8,
    pub value_type: u8,
    pub version: u8,
    pub name: Vec<u8>,
    pub feature_level: u16,
    pub tagged_fields_count: u8
}


#[derive(Debug)]
pub struct TopicRecord {
    pub frame_version: u8,
    pub value_type: u8,
    pub version: u8,
    pub topic_name: Vec<u8>,
    pub topic_uuid: i128,
    pub tagged_fields_count: u8
}


#[derive(Debug)]
pub struct PartitionRecord {
    pub frame_version: u8,
    pub value_type: u8,
    pub version: u8,
    pub partition_id: u32,
    pub topic_uuid: i128,
    pub replica_array: Vec<u64>,
    pub in_sync_replica_array: Vec<u64>,
    pub removing_replicas_array: Vec<u64>,
    pub adding_replicas_array: Vec<u64>,
    pub leader: u32,
    pub leader_epoch: u32,
    pub partition_epoch: u32,
    pub directories_array: Vec<i128>,
    pub tagged_fields_count: u8,
}

impl RecordBatch {
    pub fn new(mut buf: &[u8], batch_len: u32, batch_offset: u64) -> Result<Self, Error>{
        // buf.advance(3);
        let parition_leader_epoch = buf.get_u32();
        let magic_byte = buf.get_u8();
        let crc = buf.get_i32();
        let attributes = buf.get_u16();
        let last_offset_delta = buf.get_u32();
        let base_timestamp = buf.get_u64();
        let last_timestamp = buf.get_u64();
        let producer_id = buf.get_u64();
        let producer_epoch = buf.get_u16();
        let base_sequence = buf.get_i32();
        let records_length = buf.get_i32();

        let mut records = Vec::new();
        println!("Records Len : {records_length:?}");
        for _ in 0..records_length {
            records.push(Record::new(&mut buf).expect("Metadata Records Read Failed"));
        }

        Ok(RecordBatch {
            offset: batch_offset,
            length: batch_len,
            parition_leader_epoch,
            magic_byte,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            last_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_length,
            records,
        })
    }
}

impl Record {
    fn new(buf: &mut &[u8]) -> Result<Self, Error>{
        let length = get_varint(buf)?;
        let attributes = buf.get_u8();
        let timestamp_delta = buf.get_u8();
        let offset_delta = buf.get_i8();
        let key_length = get_varint(buf)?;
        let key = if key_length == -1 {
            None
        } else {
            let mut key = vec![0; key_length as usize];
            buf.copy_to_slice(&mut key);
            Some(key)
        };
        let value_length = get_varint(buf)?;
        let value = Self::new_record_value(buf).expect("Metadata Record Value Read Failed");
        let headers_array_count = buf.get_u8();

        Ok(Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key_length,
            key,
            value_length,
            value,
            headers_array_count,
        })
    }

    fn new_record_value(buf: &mut &[u8]) -> Result<RecordValue, Error> {
        let frame_version = buf.get_u8();
        let value_type = buf.get_u8();
        let version = buf.get_u8();

        match value_type {
            2 => Ok(RecordValue::TopicRecord(TopicRecord {
                frame_version,
                value_type,
                version,
                topic_name: {
                    let mut topic_name = vec![0; buf.get_u8().saturating_sub(1) as usize];
                    buf.copy_to_slice(&mut topic_name);
                    topic_name
                },
                topic_uuid: buf.get_i128(),
                tagged_fields_count: buf.get_u8(),
            })),
            3 => Ok(RecordValue::PartitionRecord(PartitionRecord {
                frame_version,
                value_type,
                version,
                partition_id: buf.get_u32(),
                topic_uuid: buf.get_i128(),
                replica_array: (0..buf.get_u8().saturating_sub(1)).map(|_| buf.get_u64()).collect(),
                in_sync_replica_array: (0..buf.get_u8().saturating_sub(1)).map(|_| buf.get_u64()).collect(),
                removing_replicas_array: (0..buf.get_u8().saturating_sub(1)).map(|_| buf.get_u64()).collect(),
                adding_replicas_array: (0..buf.get_u8().saturating_sub(1)).map(|_| buf.get_u64()).collect(),
                leader: buf.get_u32(),
                leader_epoch: buf.get_u32(),
                partition_epoch: buf.get_u32(),
                directories_array: (0..buf.get_u8().saturating_sub(1)).map(|_| buf.get_i128()).collect(),
                tagged_fields_count: buf.get_u8(),
            })),
            12 => Ok(RecordValue::FeatureLevelRecord(FeatureLevelRecord {
                frame_version,
                value_type,
                version,
                name: {
                    let mut topic_name = vec![0; buf.get_u8().saturating_sub(1) as usize];
                    buf.copy_to_slice(&mut topic_name);
                    topic_name
                },
                feature_level: buf.get_u16(),
                tagged_fields_count: buf.get_u8(),
            })),
            _ => Err(Error::new(std::io::ErrorKind::InvalidData, "Invalid Record Type")),
        }
    }
}

#[derive(Debug)]
pub struct PartitionMetadata {
    pub partition_id: u32,
    pub leader: u32,
    pub leader_epoch: u32,
    pub replicas: Vec<u64>,
    pub isr: Vec<u64>,
}

pub async fn read_cluster_metadata() -> Result<Vec<RecordBatch>, Error>{
    // Get config
    let config = CONFIG.get_or_init(KafkaConfig::new).await;

    let mut file = File::open(config.metadata.path.clone()).await?;

    let mut record_batch = Vec::new();
    let mut offset_buf = [0_u8; 8];
    let mut len_buf = [0_u8; 4];

    loop {
        let data_offset = file.read(&mut offset_buf).await.expect("Metadata Read Failed");
        let data_len = file.read(&mut len_buf).await.expect("Metadata Len Read Failed");
        if data_offset == 0 || data_len == 0 {
            break;
        }
        
        let mut msg_buf = vec![0_u8; i32::from_be_bytes(len_buf) as usize];
        file.read_exact(&mut msg_buf).await.expect("Metadata Record Read Failed");
        record_batch.push(RecordBatch::new(&mut msg_buf, u32::from_be_bytes(len_buf), u64::from_be_bytes(offset_buf)).expect("Metadata Record Batch Read Failed"));
    }

    Ok(record_batch)
}

pub fn check_topic_id_exists(metadata: &Vec<RecordBatch>, topic_uuid: i128) -> bool {
    for batch in metadata {
        for record in &batch.records {
            if let RecordValue::TopicRecord(topic_record) = &record.value {
                if topic_record.topic_uuid == topic_uuid {
                    return true
                }
            }
        }
    }
    false
}

pub fn return_topic_uuid(metadata: &Vec<RecordBatch>, topic_name: &[u8]) -> Option<i128> {
    for batch in metadata {
        for record in &batch.records {
            if let RecordValue::TopicRecord(topic_record) = &record.value {
                if topic_record.topic_name == topic_name {
                    return Some(topic_record.topic_uuid);
                }
            }
        }
    }
    None
}

pub fn describe_metadata_topic_partitions(metadata: &Vec<RecordBatch>, topic_uuid: i128) -> Option<Vec<PartitionMetadata>> {
    let mut partitions_metadata = Vec::new();

    for batch in metadata {
        for record in &batch.records {
            if let RecordValue::PartitionRecord(partition_record) = &record.value {
                if partition_record.topic_uuid == topic_uuid {
                    partitions_metadata.push(PartitionMetadata {
                        partition_id: partition_record.partition_id,
                        leader: partition_record.leader,
                        leader_epoch: partition_record.leader_epoch,
                        replicas: partition_record.replica_array.clone(),
                        isr: partition_record.in_sync_replica_array.clone(),
                    });
                }
            }
        }
    }

    if partitions_metadata.is_empty() {
        None
    } else {
        Some(partitions_metadata)
    }
}

fn get_varint(buf: &mut &[u8]) -> Result<i8, Error>{
    let buffer = [buf.get_u8()];
    let x = i8::from_be_bytes(buffer);
    if x < 0 {
        let _buffer = [buf.get_u8()];
        let y = (x as u8) as i16;
        let c = (y >> 1) ^ -(y & 1);
        return Ok(c as i8);
    }
    Ok((x >> 1) ^ -(x & 1))
}