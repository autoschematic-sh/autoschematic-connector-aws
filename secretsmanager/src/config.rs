use std::path::Path;

use autoschematic_connector_aws_core::config::AwsConnectorConfig;
use autoschematic_core::util::RON;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct SecretsManagerConnectorConfig {
    pub enabled_regions: Vec<String>,
}

impl Default for SecretsManagerConnectorConfig {
    fn default() -> Self {
        Self {
            enabled_regions: vec![
                // "af-south-1",
                // "ap-east-1",
                // "ap-northeast-1",
                // "ap-northeast-2",
                // "ap-northeast-3",
                // "ap-south-1",
                // "ap-south-2",
                // "ap-southeast-1",
                // "ap-southeast-2",
                // "ap-southeast-3",
                // "ap-southeast-4",
                // "ap-southeast-5",
                // "ca-central-1",
                // "ca-west-1",
                // "cn-north-1",
                // "cn-northwest-1",
                // "eu-central-1",
                // "eu-central-2",
                // "eu-north-1",
                // "eu-south-1",
                // "eu-south-2",
                "eu-west-1",
                "eu-west-2",
                // "eu-west-3",
                // "il-central-1",
                // "me-central-1",
                // "me-south-1",
                // "sa-east-1",
                "us-east-1",
                "us-east-2",
                // "us-gov-east-1",
                // "us-gov-west-1",
                "us-west-1",
                "us-west-2",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        }
    }
}

impl SecretsManagerConnectorConfig {
    pub fn try_load(prefix: &Path) -> anyhow::Result<Option<SecretsManagerConnectorConfig>> {
        let config_path = prefix.join("aws/secretsmanager/config.ron");
        if config_path.is_file() {
            tracing::info!("Loading SecretsManagerConnector config file at {:?}", config_path);
            let config: SecretsManagerConnectorConfig = RON.from_str(&std::fs::read_to_string(config_path)?)?;
            return Ok(Some(config));
        } else {
            tracing::info!(
                "SecretsManagerConnector config file at {:?} not present, skipping.",
                config_path
            );
            return Ok(None);
        }
    }
    pub fn from_aws_config(cfg: &AwsConnectorConfig) -> Self {
        Self {
            enabled_regions: cfg.enabled_regions.clone(),
        }
    }
}
