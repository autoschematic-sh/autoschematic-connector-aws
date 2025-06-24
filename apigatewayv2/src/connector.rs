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
        ResourceAddress, SkeletonOutput, VirtToPhyOutput,
    },
    connector_util::{get_output_or_bail, load_resource_output_key, load_resource_outputs, output_phy_to_virt},
    diag::DiagnosticOutput,
    read_outputs::ReadOutput,
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
    resource::{Api, ApiGatewayV2Resource, Authorizer, Integration, Route, Stage},
};

pub use addr::ApiGatewayV2ResourceAddress;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
use autoschematic_core::skeleton;
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

    async fn addr_virt_to_phy(&self, addr: &Path) -> anyhow::Result<VirtToPhyOutput> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;

        let Some(outputs) = load_resource_outputs(&self.prefix, &addr)? else {
            return Ok(VirtToPhyOutput::NotPresent);
        };

        match &addr {
            ApiGatewayV2ResourceAddress::Api { region, .. } => {
                let region = region.clone();
                let api_id = get_output_or_bail(&outputs, "api_id")?;

                Ok(VirtToPhyOutput::Present(
                    ApiGatewayV2ResourceAddress::Api { region, api_id }.to_path_buf(),
                ))
            }
            ApiGatewayV2ResourceAddress::Route { region, api_id, .. } => {
                let region = region.clone();
                let api_id = api_id.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                let Some(api_id) = load_resource_output_key(&self.prefix, &parent_api, "api_id")? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        path: parent_api.to_path_buf(),
                        key:  String::from("api_id"),
                    }]));
                };

                let Some(route_id) = load_resource_output_key(&self.prefix, &addr, "route_id")? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };

                Ok(VirtToPhyOutput::Present(
                    ApiGatewayV2ResourceAddress::Route {
                        region,
                        api_id,
                        route_id,
                    }
                    .to_path_buf(),
                ))
            }
            ApiGatewayV2ResourceAddress::Integration { region, api_id, .. } => {
                let region = region.clone();
                let api_id = api_id.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                let Some(api_id) = load_resource_output_key(&self.prefix, &parent_api, "api_id")? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        path: parent_api.to_path_buf(),
                        key:  String::from("api_id"),
                    }]));
                };

                let Some(integration_id) = load_resource_output_key(&self.prefix, &addr, "integration_id")? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };

                Ok(VirtToPhyOutput::Present(
                    ApiGatewayV2ResourceAddress::Integration {
                        region,
                        api_id,
                        integration_id,
                    }
                    .to_path_buf(),
                ))
            }
            ApiGatewayV2ResourceAddress::Stage {
                region,
                api_id,
                stage_name,
            } => {
                let region = region.clone();
                let api_id = api_id.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                let Some(api_id) = load_resource_output_key(&self.prefix, &parent_api, "api_id")? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        path: parent_api.to_path_buf(),
                        key:  String::from("api_id"),
                    }]));
                };

                Ok(VirtToPhyOutput::Present(
                    ApiGatewayV2ResourceAddress::Stage {
                        region,
                        api_id,
                        stage_name: stage_name.clone(),
                    }
                    .to_path_buf(),
                ))
            }
            ApiGatewayV2ResourceAddress::Authorizer { region, api_id, .. } => {
                let region = region.clone();
                let api_id = api_id.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                let Some(api_id) = load_resource_output_key(&self.prefix, &parent_api, "api_id")? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        path: parent_api.to_path_buf(),
                        key:  String::from("api_id"),
                    }]));
                };

                let Some(authorizer_id) = load_resource_output_key(&self.prefix, &addr, "authorizer_id")? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };

                Ok(VirtToPhyOutput::Present(
                    ApiGatewayV2ResourceAddress::Authorizer {
                        region,
                        api_id,
                        authorizer_id,
                    }
                    .to_path_buf(),
                ))
            }
        }
    }

    async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;

        match &addr {
            ApiGatewayV2ResourceAddress::Api { .. } => {
                if let Some(virt_addr) = output_phy_to_virt(&self.prefix, &addr)? {
                    return Ok(Some(virt_addr.to_path_buf()));
                }
            }
            ApiGatewayV2ResourceAddress::Route { region, api_id, .. } => {
                let region = region.clone();
                let api_id = api_id.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                if let Some(ApiGatewayV2ResourceAddress::Api {
                    region,
                    api_id: virt_api_id,
                }) = output_phy_to_virt(&self.prefix, &parent_api)?
                {
                    if let Some(ApiGatewayV2ResourceAddress::Route { route_id, .. }) = output_phy_to_virt(&self.prefix, &addr)?
                    {
                        return Ok(Some(
                            ApiGatewayV2ResourceAddress::Route {
                                region,
                                api_id: virt_api_id,
                                route_id,
                            }
                            .to_path_buf(),
                        ));
                    }
                }
            }
            ApiGatewayV2ResourceAddress::Integration { region, api_id, .. } => {
                let region = region.clone();
                let api_id = api_id.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                if let Some(ApiGatewayV2ResourceAddress::Api {
                    region,
                    api_id: virt_api_id,
                }) = output_phy_to_virt(&self.prefix, &parent_api)?
                {
                    if let Some(ApiGatewayV2ResourceAddress::Integration { integration_id, .. }) =
                        output_phy_to_virt(&self.prefix, &addr)?
                    {
                        return Ok(Some(
                            ApiGatewayV2ResourceAddress::Integration {
                                region,
                                api_id: virt_api_id,
                                integration_id,
                            }
                            .to_path_buf(),
                        ));
                    }
                }
            }
            ApiGatewayV2ResourceAddress::Authorizer { region, api_id, .. } => {
                let region = region.clone();
                let api_id = api_id.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                if let Some(ApiGatewayV2ResourceAddress::Api {
                    region,
                    api_id: virt_api_id,
                }) = output_phy_to_virt(&self.prefix, &parent_api)?
                {
                    if let Some(ApiGatewayV2ResourceAddress::Authorizer { authorizer_id, .. }) =
                        output_phy_to_virt(&self.prefix, &addr)?
                    {
                        return Ok(Some(
                            ApiGatewayV2ResourceAddress::Authorizer {
                                region,
                                api_id: virt_api_id,
                                authorizer_id,
                            }
                            .to_path_buf(),
                        ));
                    }
                }
            }
            ApiGatewayV2ResourceAddress::Stage {
                region,
                api_id,
                stage_name,
            } => {
                let region = region.clone();
                let api_id = api_id.clone();
                let stage_name = stage_name.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                if let Some(ApiGatewayV2ResourceAddress::Api {
                    region,
                    api_id: virt_api_id,
                }) = output_phy_to_virt(&self.prefix, &parent_api)?
                {
                    return Ok(Some(
                        ApiGatewayV2ResourceAddress::Stage {
                            region,
                            api_id: virt_api_id,
                            stage_name,
                        }
                        .to_path_buf(),
                    ));
                }
            }
        }
        Ok(None)
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        let region = String::from("[region]");
        let api_id = String::from("[api_id]");

        // API Gateway V2 API
        res.push(skeleton!(
            ApiGatewayV2ResourceAddress::Api {
                region: region.clone(),
                api_id: api_id.clone(),
            },
            ApiGatewayV2Resource::Api(Api {
                name: String::from("[api_name]"),
                protocol_type: String::from("HTTP"),
                api_endpoint: None,
                tags: None,
            })
        ));

        // Route
        let route_id = String::from("[route_id]");
        res.push(skeleton!(
            ApiGatewayV2ResourceAddress::Route {
                region: region.clone(),
                api_id: api_id.clone(),
                route_id
            },
            ApiGatewayV2Resource::Route(Route {
                route_key: String::from("GET /path"),
                target:    Some(String::from("integrations/[integration_id]")),
            })
        ));

        // Integration
        let integration_id = String::from("[integration_id]");
        res.push(skeleton!(
            ApiGatewayV2ResourceAddress::Integration {
                region: region.clone(),
                api_id: api_id.clone(),
                integration_id
            },
            ApiGatewayV2Resource::Integration(Integration {
                integration_type: String::from("AWS_PROXY"),
                integration_uri:  String::from("arn:aws:lambda:[region]:[account_id]:function:[function_name]"),
            })
        ));

        // Stage
        let stage_name = String::from("[stage_name]");
        res.push(skeleton!(
            ApiGatewayV2ResourceAddress::Stage {
                region: region.clone(),
                api_id: api_id.clone(),
                stage_name
            },
            ApiGatewayV2Resource::Stage(Stage {
                stage_name: String::from("[stage_name]"),
                auto_deploy: true,
                tags: None,
            })
        ));

        // Authorizer
        let authorizer_id = String::from("[authorizer_id]");
        res.push(skeleton!(
            ApiGatewayV2ResourceAddress::Authorizer {
                region: region.clone(),
                api_id: api_id.clone(),
                authorizer_id
            },
            ApiGatewayV2Resource::Authorizer(Authorizer {
                authorizer_type: String::from("JWT"),
                authorizer_uri:  String::from("https://[issuer_url]/.well-known/jwks.json"),
                identity_source: vec![String::from("$request.header.Authorization")],
            })
        ));

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
