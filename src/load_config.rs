use serde::Deserialize;
use tokio::fs;

#[derive(Debug, Deserialize)]
pub struct ApiKeyDetail {
    pub key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

#[derive(Debug, Deserialize)]
pub struct ApiKey {
    pub supported_keys: Vec<i16>,
    pub key: Vec<ApiKeyDetail>,
}

#[derive(Debug, Deserialize)]
pub struct ApiConfig {
    pub version: String,
    pub endpoint: String,
}

#[derive(Debug, Deserialize)]
pub struct Metadata {
    pub directory: String,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub api: ApiConfig,
    pub api_key: ApiKey,
    pub metadata: Metadata,
}

impl KafkaConfig {
    pub async fn new() -> Self {
        let config_str = fs::read_to_string("src/KafkaConfig.toml").await.expect("Failed to read config file");
        toml::from_str(&config_str).expect("Failed to create config")
    }
}