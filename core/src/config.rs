use std::path::Path;

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion, Region};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::RON;

#[derive(Serialize, Deserialize, Debug)]
pub struct TimeoutConfig {}

#[derive(Serialize, Deserialize, Debug)]
pub struct AwsConnectorConfig {
    pub account_id: String,
    pub endpoint_url: Option<String>,
    pub timeout_config: Option<TimeoutConfig>,
    pub sts_region: String,
    pub enabled_regions: Vec<String>,
}

impl AwsConnectorConfig {
    pub fn try_load(prefix: &Path) -> anyhow::Result<Option<AwsConnectorConfig>> {
        let config_path = prefix.join("aws/config.ron");
        if config_path.is_file() {
            tracing::info!("Loading AwsConnector config file at {:?}", config_path);
            let config: AwsConnectorConfig =
                RON.from_str(&std::fs::read_to_string(config_path)?)?;
            return Ok(Some(config));
        } else {
            tracing::info!(
                "AwsConnector config file at {:?} not present, skipping.",
                config_path
            );
            return Ok(None);
        }
    }

    pub async fn verify_sts(&self) -> anyhow::Result<()> {
        let sts_config = aws_config::defaults(BehaviorVersion::latest())
            .region(RegionProviderChain::first_try(Region::new(
                self.sts_region.clone(),
            )))
            .load()
            .await;

        let sts_client = aws_sdk_sts::Client::new(&sts_config);
        let caller_identity = sts_client.get_caller_identity().send().await?;

        if let Some(account_id) = caller_identity.account {}

        Ok(())
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
