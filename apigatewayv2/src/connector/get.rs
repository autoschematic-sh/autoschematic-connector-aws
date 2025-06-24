use std::path::Path;

use anyhow::{Result, bail};
use autoschematic_core::{
    connector::{GetResourceOutput, Resource, ResourceAddress},
    get_resource_output,
};

use anyhow::Context;

use crate::{addr::ApiGatewayV2ResourceAddress, resource::ApiGatewayV2Resource};

use super::ApiGatewayV2Connector;

impl ApiGatewayV2Connector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;

        match addr {
            ApiGatewayV2ResourceAddress::Api { region, api_id } => {
                let client = self.get_or_init_client(&region).await?;
                let resp = client.get_api().api_id(api_id).send().await;

                match resp {
                    Ok(output) => {
                        let api = crate::resource::Api {
                            name: output.name().unwrap_or_default().to_string(),
                            protocol_type: output.protocol_type().map(|x| x.as_str().to_string()).unwrap_or_default(),
                            api_endpoint: output.api_endpoint().map(|x| x.to_string()),
                            tags: output.tags,
                        };

                        return get_resource_output!(ApiGatewayV2Resource::Api(api));
                    }
                    Err(e) => Ok(None),
                }
            }
            ApiGatewayV2ResourceAddress::Route {
                region,
                api_id,
                route_id,
            } => {
                let client = self.get_or_init_client(&region).await?;
                let resp = client.get_route().api_id(api_id).route_id(route_id).send().await;

                match resp {
                    Ok(output) => {
                        let route = crate::resource::Route {
                            route_key: output.route_key().unwrap_or_default().to_string(),
                            target:    output.target().map(|x| x.to_string()),
                        };
                        return get_resource_output!(ApiGatewayV2Resource::Route(route));
                    }
                    Err(e) => Ok(None),
                }
            }
            ApiGatewayV2ResourceAddress::Integration {
                region,
                api_id,
                integration_id,
            } => {
                let client = self.get_or_init_client(&region).await?;
                let resp = client
                    .get_integration()
                    .api_id(api_id)
                    .integration_id(integration_id)
                    .send()
                    .await;

                match resp {
                    Ok(output) => {
                        let integration = crate::resource::Integration {
                            integration_type: output.integration_type().map(|x| x.as_str().to_string()).unwrap_or_default(),
                            integration_uri:  output.integration_uri().unwrap_or_default().to_string(),
                        };

                        return get_resource_output!(ApiGatewayV2Resource::Integration(integration));
                    }
                    Err(e) => Ok(None),
                }
            }
            ApiGatewayV2ResourceAddress::Stage {
                region,
                api_id,
                stage_name,
            } => {
                let client = self.get_or_init_client(&region).await?;
                let resp = client.get_stage().api_id(api_id).stage_name(stage_name).send().await;

                match resp {
                    Ok(output) => {
                        let stage = crate::resource::Stage {
                            stage_name: output.stage_name().unwrap_or_default().to_string(),
                            auto_deploy: output.auto_deploy().unwrap_or_default(),
                            tags: output.tags,
                        };

                        return get_resource_output!(ApiGatewayV2Resource::Stage(stage));
                    }
                    Err(e) => Ok(None),
                }
            }
            ApiGatewayV2ResourceAddress::Authorizer {
                region,
                api_id,
                authorizer_id,
            } => {
                let client = self.get_or_init_client(&region).await?;
                let resp = client
                    .get_authorizer()
                    .api_id(api_id)
                    .authorizer_id(authorizer_id)
                    .send()
                    .await;

                match resp {
                    Ok(output) => {
                        let authorizer = crate::resource::Authorizer {
                            authorizer_type: output.authorizer_type().map(|x| x.as_str().to_string()).unwrap_or_default(),
                            authorizer_uri:  output.authorizer_uri().unwrap_or_default().to_string(),
                            identity_source: output.identity_source().to_vec(),
                        };
                        return get_resource_output!(ApiGatewayV2Resource::Authorizer(authorizer));
                    }
                    Err(e) => Ok(None),
                }
            }
        }
    }
}
