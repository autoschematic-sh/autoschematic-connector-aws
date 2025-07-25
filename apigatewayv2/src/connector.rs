use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, FilterResponse, GetResourceResponse, OpExecResponse, PlanResponseElement, Resource,
        ResourceAddress, SkeletonResponse, VirtToPhyResponse,
    },
    diag::DiagnosticResponse,
    glob::addr_matches_filter,
    template::ReadOutput,
    util::{ron_check_eq, ron_check_syntax},
};
use config::ApiGatewayV2ConnectorConfig;
use tokio::sync::RwLock;

use crate::{
    addr,
    config::{self},
    op,
    resource::{self, Api, ApiGatewayV2Resource, Authorizer, Integration, Route, Stage},
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
    account_id: RwLock<String>,
    config: RwLock<ApiGatewayV2ConnectorConfig>,
    prefix: PathBuf,
}

#[async_trait]
impl Connector for ApiGatewayV2Connector {
    async fn filter(&self, addr: &Path) -> Result<FilterResponse, anyhow::Error> {
        if let Ok(_addr) = ApiGatewayV2ResourceAddress::from_path(addr) {
            Ok(FilterResponse::Resource)
        } else {
            Ok(FilterResponse::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(ApiGatewayV2Connector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let secrets_config: ApiGatewayV2ConnectorConfig = ApiGatewayV2ConnectorConfig::try_load(&self.prefix).await?;

        let account_id = secrets_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.write().await = secrets_config;
        *self.account_id.write().await = account_id;
        Ok(())
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let enabled_regions = self.config.read().await.enabled_regions.clone();

        let mut results = Vec::new();

        for region in enabled_regions {
            if !addr_matches_filter(&PathBuf::from(format!("aws/apigatewayv2/{region}")), subpath) {
                continue;
            }

            let apis = self.list_apis(&region).await?;
            results.extend(apis);

            let mut api_resource_addresses = Vec::new();
            for api in &results {
                if let Ok(ApiGatewayV2ResourceAddress::Api { region: r, api_id: id }) =
                    ApiGatewayV2ResourceAddress::from_path(api)
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
        }
        Ok(results)
    }

    async fn subpaths(&self) -> anyhow::Result<Vec<PathBuf>> {
        let mut res = Vec::new();

        for region in &self.config.read().await.enabled_regions {
            res.push(PathBuf::from(format!("aws/apigatewayv2/{region}")));
        }

        Ok(res)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceResponse>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn addr_virt_to_phy(&self, addr: &Path) -> anyhow::Result<VirtToPhyResponse> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;

        match &addr {
            ApiGatewayV2ResourceAddress::Api { region, .. } => {
                let region = region.clone();
                if let Some(api_id) = addr.get_output(&self.prefix, "api_id")? {
                    Ok(VirtToPhyResponse::Present(
                        ApiGatewayV2ResourceAddress::Api { region, api_id }.to_path_buf(),
                    ))
                } else {
                    Ok(VirtToPhyResponse::NotPresent)
                }
            }
            ApiGatewayV2ResourceAddress::Route { region, api_id, .. } => {
                let region = region.clone();
                let api_id = api_id.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                let Some(api_id) = parent_api.get_output(&self.prefix, "api_id")? else {
                    return Ok(VirtToPhyResponse::Deferred(vec![ReadOutput {
                        addr: parent_api.to_path_buf(),
                        key:  String::from("api_id"),
                    }]));
                };

                if let Some(route_id) = addr.get_output(&self.prefix, "route_id")? {
                    return Ok(VirtToPhyResponse::Present(
                        ApiGatewayV2ResourceAddress::Route {
                            region,
                            api_id,
                            route_id,
                        }
                        .to_path_buf(),
                    ));
                } else {
                    return Ok(VirtToPhyResponse::NotPresent);
                };
            }
            ApiGatewayV2ResourceAddress::Integration { region, api_id, .. } => {
                let region = region.clone();
                let api_id = api_id.clone();

                let parent_api = ApiGatewayV2ResourceAddress::Api {
                    region: region.clone(),
                    api_id,
                };

                let Some(api_id) = parent_api.get_output(&self.prefix, "api_id")? else {
                    return Ok(VirtToPhyResponse::Deferred(vec![ReadOutput {
                        addr: parent_api.to_path_buf(),
                        key:  String::from("api_id"),
                    }]));
                };

                let Some(integration_id) = addr.get_output(&self.prefix, "integration_id")? else {
                    return Ok(VirtToPhyResponse::NotPresent);
                };

                Ok(VirtToPhyResponse::Present(
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

                let Some(api_id) = parent_api.get_output(&self.prefix, "api_id")? else {
                    return Ok(VirtToPhyResponse::Deferred(vec![ReadOutput {
                        addr: parent_api.to_path_buf(),
                        key:  String::from("api_id"),
                    }]));
                };

                Ok(VirtToPhyResponse::Present(
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

                let Some(api_id) = parent_api.get_output(&self.prefix, "api_id")? else {
                    return Ok(VirtToPhyResponse::Deferred(vec![ReadOutput {
                        addr: parent_api.to_path_buf(),
                        key:  String::from("api_id"),
                    }]));
                };

                let Some(authorizer_id) = addr.get_output(&self.prefix, "authorizer_id")? else {
                    return Ok(VirtToPhyResponse::NotPresent);
                };

                Ok(VirtToPhyResponse::Present(
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
                if let Some(virt_addr) = addr.phy_to_virt(&self.prefix)? {
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
                }) = parent_api.phy_to_virt(&self.prefix)?
                    && let Some(ApiGatewayV2ResourceAddress::Route { route_id, .. }) = addr.phy_to_virt(&self.prefix)? {
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
                }) = parent_api.phy_to_virt(&self.prefix)?
                    && let Some(ApiGatewayV2ResourceAddress::Integration { integration_id, .. }) =
                        addr.phy_to_virt(&self.prefix)?
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
                }) = parent_api.phy_to_virt(&self.prefix)?
                    && let Some(ApiGatewayV2ResourceAddress::Authorizer { authorizer_id, .. }) =
                        addr.phy_to_virt(&self.prefix)?
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
                }) = parent_api.phy_to_virt(&self.prefix)?
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

    async fn get_skeletons(&self) -> Result<Vec<SkeletonResponse>, anyhow::Error> {
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
        match addr {
            ApiGatewayV2ResourceAddress::Api { .. } => ron_check_eq::<resource::Api>(a, b),
            ApiGatewayV2ResourceAddress::Route { .. } => ron_check_eq::<resource::Route>(a, b),
            ApiGatewayV2ResourceAddress::Integration { .. } => ron_check_eq::<resource::Integration>(a, b),
            ApiGatewayV2ResourceAddress::Stage { .. } => ron_check_eq::<resource::Stage>(a, b),
            ApiGatewayV2ResourceAddress::Authorizer { .. } => ron_check_eq::<resource::Authorizer>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<Option<DiagnosticResponse>, anyhow::Error> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;

        match addr {
            ApiGatewayV2ResourceAddress::Api { .. } => ron_check_syntax::<resource::Api>(a),
            ApiGatewayV2ResourceAddress::Route { .. } => ron_check_syntax::<resource::Route>(a),
            ApiGatewayV2ResourceAddress::Integration { .. } => ron_check_syntax::<resource::Integration>(a),
            ApiGatewayV2ResourceAddress::Stage { .. } => ron_check_syntax::<resource::Stage>(a),
            ApiGatewayV2ResourceAddress::Authorizer { .. } => ron_check_syntax::<resource::Authorizer>(a),
        }
    }
}
