use crate::addr::CloudFrontResourceAddress;
use crate::op::CloudFrontConnectorOp;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::config::CloudFrontConnectorConfig;
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource,
        ResourceAddress, SkeletonOutput,
    },
    diag::DiagnosticOutput,
};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct CloudFrontConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_cloudfront::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<CloudFrontConnectorConfig>,
    prefix: PathBuf,
}

impl CloudFrontConnector {
    pub async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_cloudfront::Client>> {
        let mut cache = self.client_cache.lock().await;

        if !cache.contains_key(region_s) {
            let region = RegionProviderChain::first_try(Region::new(region_s.to_owned()));

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
            let client = aws_sdk_cloudfront::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }
}

#[async_trait]
impl Connector for CloudFrontConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_) = CloudFrontResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(CloudFrontConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let config: CloudFrontConnectorConfig = CloudFrontConnectorConfig::try_load(&self.prefix).await?;

        let account_id = config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = config;
        *self.account_id.lock().await = account_id;
        Ok(())
    }

    async fn list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let results = Vec::<PathBuf>::new();

        Ok(results)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr_option = CloudFrontResourceAddress::from_path(addr)?;

        Ok(None)
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr_option = CloudFrontResourceAddress::from_path(addr)?;
        Ok(vec![])
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr_option = CloudFrontResourceAddress::from_path(addr)?;
        let op = CloudFrontConnectorOp::from_str(op)?;

        bail!("Invalid resource address");
    }

    // async fn addr_virt_to_phy(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
    //     let Some(addr) = CloudFrontResourceAddress::from_path(addr)? else {
    //         return Ok(None);
    //     };

    //     let Some(outputs) = get_outputs(&self.prefix, &addr)? else {
    //         return Ok(None);
    //     };

    //     match addr {
    //         CloudFrontResourceAddress::Secret(region, secret_name) => {
    //             let secret_name = get_output_or_bail(&outputs, "secret_name")?;
    //             Ok(Some(
    //                 CloudFrontResourceAddress::Secret(region, secret_name).to_path_buf(),
    //             ))
    //         }
    //         CloudFrontResourceAddress::SecretPolicy(region, secret_name) => {
    //             let Some(secret_outputs) = get_outputs(
    //                 &self.prefix,
    //                 &CloudFrontResourceAddress::Secret(region.clone(), secret_name),
    //             )?
    //             else {
    //                 return Ok(None);
    //             };

    //             let secret_name = get_output_or_bail(&secret_outputs, "secret_name")?;
    //             Ok(Some(
    //                 CloudFrontResourceAddress::Secret(region, secret_name).to_path_buf(),
    //             ))
    //         }
    //         _ => Ok(Some(addr.to_path_buf())),
    //     }
    // }

    // async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
    //     let Some(addr) = CloudFrontResourceAddress::from_path(addr)? else {
    //         return Ok(None);
    //     };

    //     match &addr {
    //         CloudFrontResourceAddress::Secret(_, _) => {
    //             if let Some(secret_addr) = output_phy_to_virt(&self.prefix, &addr)? {
    //                 return Ok(Some(secret_addr.to_path_buf()));
    //             }
    //         }
    //         CloudFrontResourceAddress::SecretPolicy(_, _) => {
    //             if let Some(secret_addr) = output_phy_to_virt(&self.prefix, &addr)? {
    //                 return Ok(Some(secret_addr.to_path_buf()));
    //             }
    //         }
    //         _ => {
    //             return Ok(Some(addr.to_path_buf()));
    //         }
    //     }
    //     Ok(Some(addr.to_path_buf()))
    // }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let res = Vec::new();

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;

        match addr {}
        // CloudFrontResourceAddressdd::Bucket { .. } => ron_check_eq::<resource::S3Bucket>(a, b),
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;
        // CloudFrontResourceAddressdd::Bucket { .. } => ron_check_eq::<resource::S3Bucket>(a, b),
        match addr {}
    }
}
