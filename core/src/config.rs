use std::path::Path;

use anyhow::bail;
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::RON;

#[derive(Serialize, Deserialize, Debug)]
pub struct TimeoutConfig {}

#[derive(Serialize, Deserialize, Debug)]
pub struct AwsConnectorConfig {
    pub account_id:      Option<String>,
    pub endpoint_url:    Option<String>,
    pub timeout_config:  Option<TimeoutConfig>,
    pub sts_region:      String,
    pub enabled_regions: Vec<String>,
}

impl Default for AwsConnectorConfig {
    fn default() -> Self {
        Self {
            account_id:      Default::default(),
            endpoint_url:    Default::default(),
            timeout_config:  Default::default(),
            sts_region:      String::from("us-east-1"),
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

impl AwsConnectorConfig {
    pub fn try_load(prefix: &Path) -> anyhow::Result<AwsConnectorConfig> {
        let config_path = prefix.join("aws/config.ron");
        if config_path.is_file() {
            tracing::info!("Loading AwsConnector config file at {:?}", config_path);
            let config: AwsConnectorConfig = RON.from_str(&std::fs::read_to_string(config_path)?)?;
            Ok(config)
        } else {
            tracing::info!("AwsConnector config file at {:?} not present, skipping.", config_path);
            Ok(AwsConnectorConfig::default())
        }
    }

    pub async fn verify_sts(&self) -> anyhow::Result<()> {
        let sts_config = aws_config::defaults(BehaviorVersion::latest())
            .region(RegionProviderChain::first_try(Region::new(self.sts_region.clone())))
            .load()
            .await;

        let sts_client = aws_sdk_sts::Client::new(&sts_config);
        let caller_identity = sts_client.get_caller_identity().send().await;

        match caller_identity {
            Ok(caller_identity) => {
                let Some(account_id) = caller_identity.account else {
                    bail!("Failed to get current account ID!");
                };

                if let Some(ref config_account_id) = self.account_id
                    && *config_account_id != account_id {
                        bail!(
                            "Credentials do not match configured account id: creds = {}, aws/config.ron = {}",
                            account_id,
                            config_account_id
                        );
                    }

                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to call sts:GetCallerIdentity: {}", e);
                Err(e.into())
            }
        }
    }

    // pub async fn to_sdk_config(&self) -> anyhow::Result<aws_config::SdkConfig> {
    //     let timeout_builder = aws_config::timeout::TimeoutConfig::builder();

    //     let config = aws_config::defaults(BehaviorVersion::latest())
    //         .timeout_config(
    //                 .connect_timeout(Duration::from_secs(30))
    //                 .operation_timeout(Duration::from_secs(30))
    //                 .operation_attempt_timeout(Duration::from_secs(30))
    //                 .read_timeout(Duration::from_secs(30))
    //                 .build(),
    //         )
    //         .load()
    //         .await;
    //     Ok(config)
    // }
}

pub async fn verify_sts_account_id(sts_region: String, account_id: Option<String>) -> anyhow::Result<String> {
    let sts_config = aws_config::defaults(BehaviorVersion::latest())
        .region(RegionProviderChain::first_try(Region::new(sts_region)))
        .load()
        .await;

    let sts_client = aws_sdk_sts::Client::new(&sts_config);
    let caller_identity = sts_client.get_caller_identity().send().await;

    match caller_identity {
        Ok(caller_identity) => {
            let Some(caller_account_id) = caller_identity.account else {
                bail!("Failed to get current account ID!");
            };

            if let Some(account_id) = account_id
                && caller_account_id != account_id {
                    bail!(
                        "AWS: Account ID mismatch. Configured to use account ID {account_id}, \nbut credentials provided are for account ID {caller_account_id}."
                    )
                }
            Ok(caller_account_id)
        }
        Err(e) => {
            tracing::error!("Failed to call sts:GetCallerIdentity: {}", e);
            Err(e.into())
        }
    }
}

pub trait AwsServiceConfig: From<AwsConnectorConfig> {
    async fn try_load(prefix: &Path) -> anyhow::Result<Self>;
    async fn verify_sts(&self) -> anyhow::Result<String>;
}

#[macro_export]
macro_rules! impl_aws_config {
    ($type:ty, $path:expr) => {
        impl From<AwsConnectorConfig> for $type {
            fn from(value: AwsConnectorConfig) -> Self {
                Self {
                    account_id:      value.account_id,
                    endpoint_url:    value.endpoint_url,
                    timeout_config:  value.timeout_config,
                    sts_region:      value.sts_region,
                    enabled_regions: value.enabled_regions,
                }
            }
        }

        impl Default for $type {
            fn default() -> Self {
                Self::from(AwsConnectorConfig::default())
            }
        }

        impl AwsServiceConfig for $type {
            async fn try_load(prefix: &Path) -> anyhow::Result<Self> {
                let config_path = prefix.join($path);
                if config_path.is_file() {
                    let config: $type = RON.from_str(&std::fs::read_to_string(config_path)?)?;
                    return Ok(config);
                } else {
                    return Ok(<$type>::from(AwsConnectorConfig::try_load(prefix)?));
                }
            }

            async fn verify_sts(&self) -> anyhow::Result<String> {
                verify_sts_account_id(self.sts_region.clone(), self.account_id.clone()).await
            }
        }
    };
}
