use std::path::Path;

use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    error_util::invalid_op,
};

use crate::{
    addr::ApiGatewayV2ResourceAddress,
    op::ApiGatewayV2ConnectorOp,
    op_impl::{
        create_api, create_authorizer, create_integration, create_route, create_stage, delete_api, delete_authorizer,
        delete_integration, delete_route, delete_stage, update_api, update_api_tags, update_authorizer, update_integration,
        update_route, update_stage,
    },
};

use super::ApiGatewayV2Connector;

impl ApiGatewayV2Connector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;
        let op = ApiGatewayV2ConnectorOp::from_str(op)?;

        let account_id = self.account_id.read().await.clone();

        match &addr {
            ApiGatewayV2ResourceAddress::Api { region, api_id } => {
                let client = self.get_or_init_client(region).await?;
                match op {
                    ApiGatewayV2ConnectorOp::CreateApi(api) => create_api(&client, &account_id, region, api).await,
                    ApiGatewayV2ConnectorOp::UpdateApi(old_api, new_api) => {
                        update_api(&client, &account_id, region, api_id, old_api, new_api).await
                    }
                    ApiGatewayV2ConnectorOp::UpdateApiTags(old_tags, new_tags) => {
                        update_api_tags(&client, &account_id, region, api_id, old_tags, new_tags).await
                    }
                    ApiGatewayV2ConnectorOp::DeleteApi => delete_api(&client, region, api_id).await,
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            ApiGatewayV2ResourceAddress::Route {
                region,
                api_id,
                route_id,
            } => {
                let client = self.get_or_init_client(region).await?;
                match op {
                    ApiGatewayV2ConnectorOp::CreateRoute(route) => create_route(&client, region, api_id, route).await,
                    ApiGatewayV2ConnectorOp::UpdateRoute(old_route, new_route) => {
                        update_route(&client, region, api_id, route_id, old_route, new_route).await
                    }
                    ApiGatewayV2ConnectorOp::DeleteRoute => delete_route(&client, region, api_id, route_id).await,
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            ApiGatewayV2ResourceAddress::Integration {
                region,
                api_id,
                integration_id,
            } => {
                let client = self.get_or_init_client(region).await?;
                match op {
                    ApiGatewayV2ConnectorOp::CreateIntegration(integration) => {
                        create_integration(&client, region, api_id, integration).await
                    }
                    ApiGatewayV2ConnectorOp::UpdateIntegration(old_integration, new_integration) => {
                        update_integration(&client, region, api_id, integration_id, old_integration, new_integration).await
                    }
                    ApiGatewayV2ConnectorOp::DeleteIntegration => {
                        delete_integration(&client, region, api_id, integration_id).await
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            ApiGatewayV2ResourceAddress::Stage {
                region,
                api_id,
                stage_name,
            } => {
                let client = self.get_or_init_client(region).await?;
                match op {
                    ApiGatewayV2ConnectorOp::CreateStage(stage) => create_stage(&client, region, api_id, stage).await,
                    ApiGatewayV2ConnectorOp::UpdateStage(old_stage, new_stage) => {
                        update_stage(&client, region, api_id, stage_name, old_stage, new_stage).await
                    }
                    ApiGatewayV2ConnectorOp::DeleteStage => delete_stage(&client, region, api_id, stage_name).await,
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            ApiGatewayV2ResourceAddress::Authorizer {
                region,
                api_id,
                authorizer_id,
            } => {
                let client = self.get_or_init_client(region).await?;
                match op {
                    ApiGatewayV2ConnectorOp::CreateAuthorizer(authorizer) => {
                        create_authorizer(&client, region, api_id, authorizer).await
                    }
                    ApiGatewayV2ConnectorOp::UpdateAuthorizer(old_authorizer, new_authorizer) => {
                        update_authorizer(&client, region, api_id, authorizer_id, old_authorizer, new_authorizer).await
                    }
                    ApiGatewayV2ConnectorOp::DeleteAuthorizer => {
                        delete_authorizer(&client, region, api_id, authorizer_id).await
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
        }
    }
}
