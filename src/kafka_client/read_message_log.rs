use crate::{CONFIG, KafkaConfig};
use tokio::fs;

async fn get_topic_log_path(topic_name: &str, partition: i32) -> String {
    let config = CONFIG.get_or_init(KafkaConfig::new).await;
    format!("{}/{}-{}/00000000000000000000.log", config.metadata.directory, topic_name, partition)
}

pub async fn read_messages(topic_id: &str, partition: i32) -> Option<Vec<u8>> {
    let path = get_topic_log_path(topic_id, partition).await;
    println!("[DEBUG] Looking for file at path: {:?}", path);
    
    match fs::read(&path).await {
        Ok(content) => {
            println!("Successfully read {} bytes from file", content.len());
            println!("File content (hex): {:02x?}", content);
            Some(content)
        },
        Err(e) => {
            println!("Failed to read file: {}", e);
            None
        }
    }
}