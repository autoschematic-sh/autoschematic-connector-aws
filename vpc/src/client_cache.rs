use std::{sync::Arc, time::Duration};

use anyhow::bail;
use aws_config::{meta::region::RegionProviderChain, timeout::TimeoutConfig, BehaviorVersion};

use super::VpcConnector;


impl VpcConnector {
    pub async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_ec2::Client>> {
        let mut cache = self.client_cache.lock().await;

        if !cache.contains_key(region_s) {
            let region =
                RegionProviderChain::first_try(aws_config::Region::new(region_s.to_owned()));

            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .timeout_config(
                    TimeoutConfig::builder()
                        .connect_timeout(Duration::from_secs(30))
                        .operation_timeout(Duration::from_secs(30))
                        .operation_attempt_timeout(Duration::from_secs(30))
                        .read_timeout(Duration::from_secs(30))
                        .build(),
                )
                .load()
                .await;
            let client = aws_sdk_ec2::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }
}