// pub mod addr;
// pub mod config;
// pub mod op;
// pub mod op_impl;
// pub mod resource;
// pub mod tags;

pub use addr::ApiGatewayV2ResourceAddress;
use autoschematic_connector_aws_core::config::{AwsConnectorConfig, AwsServiceConfig};
pub use op::ApiGatewayV2ConnectorOp;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::bail;
use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource,
        ResourceAddress, SkeletonOutput,
    },
    diag::DiagnosticOutput,
};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use config::ApiGatewayV2ConnectorConfig;
use tokio::sync::Mutex;

use crate::{
    addr,
    config::{self},
    op,
};

#[derive(Default)]
pub struct ApiGatewayV2Connector {
    client_cache: tokio::sync::Mutex<HashMap<String, Arc<aws_sdk_apigatewayv2::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<ApiGatewayV2ConnectorConfig>,
    prefix: PathBuf,
}

impl ApiGatewayV2Connector {
    pub async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_apigatewayv2::Client>> {
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
            let client = aws_sdk_apigatewayv2::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }
}

#[async_trait]
impl Connector for ApiGatewayV2Connector {
    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_addr) = ApiGatewayV2ResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(ApiGatewayV2Connector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let secrets_config: ApiGatewayV2ConnectorConfig = ApiGatewayV2ConnectorConfig::try_load(&self.prefix).await?;

        let account_id = secrets_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = secrets_config;
        *self.account_id.lock().await = account_id;
        Ok(())
    }

    async fn list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let results = Vec::<PathBuf>::new();

        Ok(results)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr_option = ApiGatewayV2ResourceAddress::from_path(addr)?;

        Ok(None)
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;
        Ok(vec![])
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;
        let op = ApiGatewayV2ConnectorOp::from_str(op)?;

        bail!("Invalid resource address");
    }

    // async fn addr_virt_to_phy(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
    //     let Some(addr) = ApiGatewayV2ResourceAddress::from_path(addr)? else {
    //         return Ok(None);
    //     };

    //     let Some(outputs) = get_outputs(&self.prefix, &addr)? else {
    //         return Ok(None);
    //     };

    //     match addr {
    //         ApiGatewayV2ResourceAddress::Secret(region, secret_name) => {
    //             let secret_name = get_output_or_bail(&outputs, "secret_name")?;
    //             Ok(Some(
    //                 ApiGatewayV2ResourceAddress::Secret(region, secret_name).to_path_buf(),
    //             ))
    //         }
    //         ApiGatewayV2ResourceAddress::SecretPolicy(region, secret_name) => {
    //             let Some(secret_outputs) = get_outputs(
    //                 &self.prefix,
    //                 &ApiGatewayV2ResourceAddress::Secret(region.clone(), secret_name),
    //             )?
    //             else {
    //                 return Ok(None);
    //             };

    //             let secret_name = get_output_or_bail(&secret_outputs, "secret_name")?;
    //             Ok(Some(
    //                 ApiGatewayV2ResourceAddress::Secret(region, secret_name).to_path_buf(),
    //             ))
    //         }
    //         _ => Ok(Some(addr.to_path_buf())),
    //     }
    // }

    // async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
    //     let Some(addr) = ApiGatewayV2ResourceAddress::from_path(addr)? else {
    //         return Ok(None);
    //     };

    //     match &addr {
    //         ApiGatewayV2ResourceAddress::Secret(_, _) => {
    //             if let Some(secret_addr) = output_phy_to_virt(&self.prefix, &addr)? {
    //                 return Ok(Some(secret_addr.to_path_buf()));
    //             }
    //         }
    //         ApiGatewayV2ResourceAddress::SecretPolicy(_, _) => {
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
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;

        match addr {
            // ApiGatewayV2ResourceAddress:: { .. } => ron_check_eq::<resource::S3Bucket>(a, b),
        }
        Ok(true)
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;

        match addr {
            // S3ResourceAddress::Bucket { .. } => ron_check_syntax::<resource::S3Bucket>(a),
        }
        Ok(DiagnosticOutput { diagnostics: Vec::new() })
    }
}
