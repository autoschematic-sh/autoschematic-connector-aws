use std::path::Path;

use autoschematic_core::{
    connector::{ConnectorOp, OpPlanOutput, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values},
};

use crate::{
    addr::ApiGatewayV2ResourceAddress,
    op::ApiGatewayV2ConnectorOp,
    resource::{Api, Authorizer, Integration, Route, Stage},
};

use super::ApiGatewayV2Connector;

impl ApiGatewayV2Connector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr = ApiGatewayV2ResourceAddress::from_path(addr)?;
        let mut res = Vec::new();

        match addr {
            ApiGatewayV2ResourceAddress::Api { region: _, api_id } => match (current, desired) {
                (None, None) => {}
                (None, Some(new_api_bytes)) => {
                    let new_api: Api = RON.from_bytes(&new_api_bytes)?;
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::CreateApi(new_api.clone()),
                        format!("Create new API Gateway V2 API `{}`", new_api.name)
                    ));
                }
                (Some(_old_api_bytes), None) => {
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::DeleteApi,
                        format!("Delete API Gateway V2 API `{}`", api_id)
                    ));
                }
                (Some(old_api_bytes), Some(new_api_bytes)) => {
                    let old_api: Api = RON.from_bytes(&old_api_bytes)?;
                    let new_api: Api = RON.from_bytes(&new_api_bytes)?;

                    if old_api != new_api {
                        if old_api.name != new_api.name
                            || old_api.protocol_type != new_api.protocol_type
                            || old_api.api_endpoint != new_api.api_endpoint
                        {
                            let diff = diff_ron_values(&old_api, &new_api).unwrap_or_default();
                            res.push(connector_op!(
                                ApiGatewayV2ConnectorOp::UpdateApi(old_api.clone(), new_api.clone()),
                                format!("Modify API Gateway V2 API `{}`\n{}", old_api.name, diff)
                            ));
                        }

                        if old_api.tags != new_api.tags {
                            let diff = diff_ron_values(&old_api.tags, &new_api.tags).unwrap_or_default();
                            res.push(connector_op!(
                                ApiGatewayV2ConnectorOp::UpdateApiTags(
                                    old_api.tags.unwrap_or_default(),
                                    new_api.tags.unwrap_or_default()
                                ),
                                format!("Modify tags for API Gateway V2 API `{}`\n{}", old_api.name, diff)
                            ));
                        }
                    }
                }
            },
            ApiGatewayV2ResourceAddress::Route {
                region,
                api_id,
                route_id,
            } => match (current, desired) {
                (None, None) => {}
                (None, Some(new_route_bytes)) => {
                    let new_route: Route = RON.from_bytes(&new_route_bytes)?;
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::CreateRoute(new_route.clone()),
                        format!(
                            "Create new APIGatewayV2 Route for API ID `{}`, target = `{:#?}`",
                            api_id, new_route.target
                        )
                    ));
                }
                (Some(_old_route_bytes), None) => {
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::DeleteRoute,
                        format!("Delete API Gateway V2 Route `{}` for API `{}`", route_id, api_id)
                    ));
                }
                (Some(old_route_bytes), Some(new_route_bytes)) => {
                    let old_route: Route = RON.from_bytes(&old_route_bytes)?;
                    let new_route: Route = RON.from_bytes(&new_route_bytes)?;

                    if old_route != new_route {
                        let diff = diff_ron_values(&old_route, &new_route).unwrap_or_default();
                        res.push(connector_op!(
                            ApiGatewayV2ConnectorOp::UpdateRoute(old_route, new_route),
                            format!("Modify API Gateway V2 Route `{}` for API `{}`\n{}", route_id, api_id, diff)
                        ));
                    }
                }
            },
            ApiGatewayV2ResourceAddress::Integration {
                region,
                api_id,
                integration_id,
            } => match (current, desired) {
                (None, None) => {}
                (None, Some(new_integration_bytes)) => {
                    let new_integration: Integration = RON.from_bytes(&new_integration_bytes)?;
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::CreateIntegration(new_integration.clone()),
                        format!(
                            "Create new API Gateway V2 Integration `{}` for API `{}`",
                            new_integration.integration_type, api_id
                        )
                    ));
                }
                (Some(_old_integration_bytes), None) => {
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::DeleteIntegration,
                        format!("Delete API Gateway V2 Integration `{}` for API `{}`", integration_id, api_id)
                    ));
                }
                (Some(old_integration_bytes), Some(new_integration_bytes)) => {
                    let old_integration: Integration = RON.from_bytes(&old_integration_bytes)?;
                    let new_integration: Integration = RON.from_bytes(&new_integration_bytes)?;

                    if old_integration != new_integration {
                        let diff = diff_ron_values(&old_integration, &new_integration).unwrap_or_default();
                        res.push(connector_op!(
                            ApiGatewayV2ConnectorOp::UpdateIntegration(old_integration, new_integration),
                            format!(
                                "Modify API Gateway V2 Integration `{}` for API `{}`\n{}",
                                integration_id, api_id, diff
                            )
                        ));
                    }
                }
            },
            ApiGatewayV2ResourceAddress::Stage {
                region,
                api_id,
                stage_name,
            } => match (current, desired) {
                (None, None) => {}
                (None, Some(new_stage_bytes)) => {
                    let new_stage: Stage = RON.from_bytes(&new_stage_bytes)?;
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::CreateStage(new_stage.clone()),
                        format!(
                            "Create new API Gateway V2 Stage `{}` for API `{}`",
                            new_stage.stage_name, api_id
                        )
                    ));
                }
                (Some(_old_stage_bytes), None) => {
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::DeleteStage,
                        format!("Delete API Gateway V2 Stage `{}` for API `{}`", stage_name, api_id)
                    ));
                }
                (Some(old_stage_bytes), Some(new_stage_bytes)) => {
                    let old_stage: Stage = RON.from_bytes(&old_stage_bytes)?;
                    let new_stage: Stage = RON.from_bytes(&new_stage_bytes)?;

                    if old_stage != new_stage {
                        if old_stage.stage_name != new_stage.stage_name || old_stage.auto_deploy != new_stage.auto_deploy {
                            let diff = diff_ron_values(&old_stage, &new_stage).unwrap_or_default();
                            res.push(connector_op!(
                                ApiGatewayV2ConnectorOp::UpdateStage(old_stage.clone(), new_stage.clone()),
                                format!("Modify API Gateway V2 Stage `{}` for API `{}`\n{}", stage_name, api_id, diff)
                            ));
                        }

                        if old_stage.tags != new_stage.tags {
                            let diff = diff_ron_values(&old_stage.tags, &new_stage.tags).unwrap_or_default();
                            res.push(connector_op!(
                                ApiGatewayV2ConnectorOp::UpdateStageTags(
                                    old_stage.tags.unwrap_or_default(),
                                    new_stage.tags.unwrap_or_default()
                                ),
                                format!(
                                    "Modify tags for API Gateway V2 Stage `{}` for API `{}`\n{}",
                                    stage_name, api_id, diff
                                )
                            ));
                        }
                    }
                }
            },
            ApiGatewayV2ResourceAddress::Authorizer {
                region,
                api_id,
                authorizer_id,
            } => match (current, desired) {
                (None, None) => {}
                (None, Some(new_authorizer_bytes)) => {
                    let new_authorizer: Authorizer = RON.from_bytes(&new_authorizer_bytes)?;
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::CreateAuthorizer(new_authorizer.clone()),
                        format!(
                            "Create new API Gateway V2 Authorizer `{}` for API `{}`",
                            new_authorizer.authorizer_type, api_id
                        )
                    ));
                }
                (Some(_old_authorizer_bytes), None) => {
                    res.push(connector_op!(
                        ApiGatewayV2ConnectorOp::DeleteAuthorizer,
                        format!("Delete API Gateway V2 Authorizer `{}` for API `{}`", authorizer_id, api_id)
                    ));
                }
                (Some(old_authorizer_bytes), Some(new_authorizer_bytes)) => {
                    let old_authorizer: Authorizer = RON.from_bytes(&old_authorizer_bytes)?;
                    let new_authorizer: Authorizer = RON.from_bytes(&new_authorizer_bytes)?;

                    if old_authorizer != new_authorizer {
                        let diff = diff_ron_values(&old_authorizer, &new_authorizer).unwrap_or_default();
                        res.push(connector_op!(
                            ApiGatewayV2ConnectorOp::UpdateAuthorizer(old_authorizer, new_authorizer),
                            format!(
                                "Modify API Gateway V2 Authorizer `{}` for API `{}`\n{}",
                                authorizer_id, api_id, diff
                            )
                        ));
                    }
                }
            },
        }

        Ok(res)
    }
}
