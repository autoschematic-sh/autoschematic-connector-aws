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
        Connector, ConnectorOp, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, ResourceAddress,
        SkeletonOutput,
    },
    diag::DiagnosticOutput,
};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use aws_sdk_apigatewayv2::{
    error::SdkError,
    operation::{
        get_apis::GetApisError, get_authorizers::GetAuthorizersError, get_integrations::GetIntegrationsError,
        get_routes::GetRoutesError, get_stages::GetStagesError,
    },
};
use config::ApiGatewayV2ConnectorConfig;
use tokio::sync::Mutex;

use crate::{
    addr,
    config::{self},
    op,
};

pub use addr::ApiGatewayV2ResourceAddress;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
pub use op::ApiGatewayV2ConnectorOp;

pub mod get;
pub mod list;
pub mod op_exec;
pub mod plan;
pub mod util;

#[derive(Default)]
pub struct ApiGatewayV2Connector {
    client_cache: tokio::sync::Mutex<HashMap<String, Arc<aws_sdk_apigatewayv2::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<ApiGatewayV2ConnectorConfig>,
    prefix: PathBuf,
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
        let region = _subpath
            .components()
            .nth(2)
            .and_then(|s| s.as_os_str().to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid subpath: {:?}", _subpath))?;

        let mut results = Vec::new();

        let apis = self.list_apis(region).await?;
        results.extend(apis);

        let mut api_resource_addresses = Vec::new();
        for api in &results {
            if let Ok(ApiGatewayV2ResourceAddress::Api { region: r, api_id: id }) = ApiGatewayV2ResourceAddress::from_path(api)
            {
                api_resource_addresses.push((r, id));
            }
        }

        for (region, api_id) in api_resource_addresses {
            let routes = self.list_routes(&region, &api_id).await?;
            results.extend(routes);

            let integrations = self.list_integrations(&region, &api_id).await?;
            results.extend(integrations);

            let stages = self.list_stages(&region, &api_id).await?;
            results.extend(stages);

            let authorizers = self.list_authorizers(&region, &api_id).await?;
            results.extend(authorizers);
        }

        Ok(results)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
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

        // match addr {
        //     // ApiGatewayV2ResourceAddress:: { .. } => ron_check_eq::<resource::S3Bucket>(a, b),
        // }
        Ok(true)
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;

        // match addr {
        //     // S3ResourceAddress::Bucket { .. } => ron_check_syntax::<resource::S3Bucket>(a),
        // }
        Ok(DiagnosticOutput { diagnostics: Vec::new() })
    }
}
