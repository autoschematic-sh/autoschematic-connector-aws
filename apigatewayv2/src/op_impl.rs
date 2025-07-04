use anyhow::{Context, bail};
use autoschematic_core::connector::OpExecOutput;
use aws_sdk_apigatewayv2::Client;
use std::collections::HashMap;

use crate::{
    resource::{Api, Authorizer, Integration, Route, Stage},
    tags::tag_diff,
};

use autoschematic_core::op_exec_output;

pub async fn create_api(client: &Client, account_id: &str, region: &str, api: Api) -> Result<OpExecOutput, anyhow::Error> {
    let create_api_output = client
        .create_api()
        .name(&api.name)
        .protocol_type(aws_sdk_apigatewayv2::types::ProtocolType::from(api.protocol_type.as_str()))
        .set_tags(api.tags)
        .send()
        .await
        .context("Failed to create API")?;

    let api_id = create_api_output.api_id.context("API ID not returned after creation")?;

    op_exec_output!(
        Some([("api_id", Some(api_id.clone()))]),
        format!("Created API Gateway V2 API `{}` in region `{}`", api.name, region)
    )
}

pub async fn update_api(
    client: &Client,
    _account_id: &str,
    region: &str,
    api_id: &str,
    old_api: Api,
    new_api: Api,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut update_api_builder = client.update_api().api_id(api_id);

    if old_api.name != new_api.name {
        update_api_builder = update_api_builder.name(new_api.name);
    }

    if old_api.protocol_type != new_api.protocol_type {
        bail!("Cannot update protocol type for an existing API. Delete and recreate this API to make this change.")
    }

    if old_api.api_endpoint != new_api.api_endpoint {
        bail!("Cannot update `api_endpoint` for an existing API. Delete and recreate this API to make this change.")
    }

    update_api_builder.send().await.context("Failed to update API")?;

    op_exec_output!(format!(
        "Updated API Gateway V2 API `{}` in region `{}`",
        old_api.name, region
    ))
}

pub async fn update_api_tags(
    client: &Client,
    account_id: &str,
    region: &str,
    api_id: &str,
    old_tags: HashMap<String, String>,
    new_tags: HashMap<String, String>,
) -> Result<OpExecOutput, anyhow::Error> {
    let (untag_keys, new_tagset) = tag_diff(&old_tags, &new_tags).context("Failed to generate tag diff")?;

    if !untag_keys.is_empty() {
        client
            .untag_resource()
            .resource_arn(format!(
                "arn:aws:apigatewayv2:{}:{}/apis/{}/tags",
                region, "ACCOUNT_ID_PLACEHOLDER", api_id
            )) // TODO: Get actual account ID
            .set_tag_keys(Some(untag_keys))
            .send()
            .await
            .context("Failed to remove tags from API")?;
    }

    if !new_tagset.is_empty() {
        client
            .tag_resource()
            .resource_arn(format!("arn:aws:apigatewayv2:{region}:{account_id}/apis/{api_id}")) // TODO: Get actual account ID
            .set_tags(Some(new_tagset))
            .send()
            .await
            .context("Failed to add new tags to API")?;
    }

    op_exec_output!(format!(
        "Updated tags for API Gateway V2 API `{}` in region `{}`",
        api_id, region
    ))
}

pub async fn delete_api(client: &Client, region: &str, api_id: &str) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_api()
        .api_id(api_id)
        .send()
        .await
        .context("Failed to delete API")?;

    op_exec_output!(format!("Deleted API Gateway V2 API `{}` in region `{}`", api_id, region))
}

pub async fn create_route(client: &Client, region: &str, api_id: &str, route: Route) -> Result<OpExecOutput, anyhow::Error> {
    let create_route_output = client
        .create_route()
        .api_id(api_id)
        .route_key(&route.route_key)
        .set_target(route.target)
        .send()
        .await
        .context("Failed to create route")?;

    let route_id = create_route_output.route_id.context("Route ID not returned after creation")?;

    op_exec_output!(
        Some([("route_id", Some(route_id.clone()))]),
        format!(
            "Created API Gateway V2 Route `{}` for API `{}` in region `{}`",
            route.route_key, api_id, region
        )
    )
}

pub async fn update_route(
    client: &Client,
    region: &str,
    api_id: &str,
    route_id: &str,
    old_route: Route,
    new_route: Route,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut update_route_builder = client.update_route().api_id(api_id).route_id(route_id);

    if old_route.route_key != new_route.route_key {
        update_route_builder = update_route_builder.route_key(new_route.route_key);
    }

    if old_route.target != new_route.target {
        update_route_builder = update_route_builder.set_target(new_route.target);
    }

    update_route_builder.send().await.context("Failed to update route")?;

    op_exec_output!(format!(
        "Updated API Gateway V2 Route `{}` for API `{}` in region `{}`",
        route_id, api_id, region
    ))
}

pub async fn delete_route(client: &Client, region: &str, api_id: &str, route_id: &str) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_route()
        .api_id(api_id)
        .route_id(route_id)
        .send()
        .await
        .context("Failed to delete route")?;

    op_exec_output!(format!(
        "Deleted API Gateway V2 Route `{}` for API `{}` in region `{}`",
        route_id, api_id, region
    ))
}

pub async fn create_integration(
    client: &Client,
    region: &str,
    api_id: &str,
    integration: Integration,
) -> Result<OpExecOutput, anyhow::Error> {
    let create_integration_output = client
        .create_integration()
        .api_id(api_id)
        .integration_type(aws_sdk_apigatewayv2::types::IntegrationType::from(
            integration.integration_type.as_str(),
        ))
        .integration_uri(&integration.integration_uri)
        .send()
        .await
        .context("Failed to create integration")?;

    let integration_id = create_integration_output
        .integration_id
        .context("Integration ID not returned after creation")?;

    op_exec_output!(
        Some([("integration_id", Some(integration_id.clone()))]),
        format!(
            "Created API Gateway V2 Integration `{}` for API `{}` in region `{}`",
            integration.integration_type, api_id, region
        )
    )
}

pub async fn update_integration(
    client: &Client,
    region: &str,
    api_id: &str,
    integration_id: &str,
    old_integration: Integration,
    new_integration: Integration,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut update_integration_builder = client.update_integration().api_id(api_id).integration_id(integration_id);

    if old_integration.integration_type != new_integration.integration_type {
        update_integration_builder = update_integration_builder.integration_type(
            aws_sdk_apigatewayv2::types::IntegrationType::from(new_integration.integration_type.as_str()),
        );
    }

    if old_integration.integration_uri != new_integration.integration_uri {
        update_integration_builder = update_integration_builder.integration_uri(new_integration.integration_uri);
    }

    update_integration_builder
        .send()
        .await
        .context("Failed to update integration")?;

    op_exec_output!(format!(
        "Updated API Gateway V2 Integration `{}` for API `{}` in region `{}`",
        integration_id, api_id, region
    ))
}

pub async fn delete_integration(
    client: &Client,
    region: &str,
    api_id: &str,
    integration_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_integration()
        .api_id(api_id)
        .integration_id(integration_id)
        .send()
        .await
        .context("Failed to delete integration")?;

    op_exec_output!(format!(
        "Deleted API Gateway V2 Integration `{}` for API `{}` in region `{}`",
        integration_id, api_id, region
    ))
}

pub async fn create_stage(client: &Client, region: &str, api_id: &str, stage: Stage) -> Result<OpExecOutput, anyhow::Error> {
    client
        .create_stage()
        .api_id(api_id)
        .stage_name(&stage.stage_name)
        .auto_deploy(stage.auto_deploy)
        .set_tags(stage.tags)
        .send()
        .await
        .context("Failed to create stage")?;

    op_exec_output!(format!(
        "Created API Gateway V2 Stage `{}` for API `{}` in region `{}`",
        stage.stage_name, api_id, region
    ))
}

pub async fn update_stage(
    client: &Client,
    region: &str,
    api_id: &str,
    stage_name: &str,
    old_stage: Stage,
    new_stage: Stage,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut update_stage_builder = client.update_stage().api_id(api_id).stage_name(stage_name);

    if old_stage.auto_deploy != new_stage.auto_deploy {
        update_stage_builder = update_stage_builder.auto_deploy(new_stage.auto_deploy);
    }

    update_stage_builder.send().await.context("Failed to update stage")?;

    op_exec_output!(format!(
        "Updated API Gateway V2 Stage `{}` for API `{}` in region `{}`",
        stage_name, api_id, region
    ))
}

pub async fn delete_stage(
    client: &Client,
    region: &str,
    api_id: &str,
    stage_name: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_stage()
        .api_id(api_id)
        .stage_name(stage_name)
        .send()
        .await
        .context("Failed to delete stage")?;

    op_exec_output!(format!(
        "Deleted API Gateway V2 Stage `{}` for API `{}` in region `{}`",
        stage_name, api_id, region
    ))
}

pub async fn create_authorizer(
    client: &Client,
    region: &str,
    api_id: &str,
    authorizer: Authorizer,
) -> Result<OpExecOutput, anyhow::Error> {
    let create_authorizer_output = client
        .create_authorizer()
        .api_id(api_id)
        .authorizer_type(aws_sdk_apigatewayv2::types::AuthorizerType::from(
            authorizer.authorizer_type.as_str(),
        ))
        .authorizer_uri(&authorizer.authorizer_uri)
        .set_identity_source(Some(authorizer.identity_source))
        .send()
        .await
        .context("Failed to create authorizer")?;

    let authorizer_id = create_authorizer_output
        .authorizer_id
        .context("Authorizer ID not returned after creation")?;

    op_exec_output!(
        Some([("authorizer_id", Some(authorizer_id.clone()))]),
        format!(
            "Created API Gateway V2 Authorizer `{}` for API `{}` in region `{}`",
            authorizer.authorizer_type, api_id, region
        )
    )
}

pub async fn update_authorizer(
    client: &Client,
    region: &str,
    api_id: &str,
    authorizer_id: &str,
    old_authorizer: Authorizer,
    new_authorizer: Authorizer,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut update_authorizer_builder = client.update_authorizer().api_id(api_id).authorizer_id(authorizer_id);

    if old_authorizer.authorizer_type != new_authorizer.authorizer_type {
        update_authorizer_builder = update_authorizer_builder.authorizer_type(
            aws_sdk_apigatewayv2::types::AuthorizerType::from(new_authorizer.authorizer_type.as_str()),
        );
    }

    if old_authorizer.authorizer_uri != new_authorizer.authorizer_uri {
        update_authorizer_builder = update_authorizer_builder.authorizer_uri(new_authorizer.authorizer_uri);
    }

    if old_authorizer.identity_source != new_authorizer.identity_source {
        update_authorizer_builder = update_authorizer_builder.set_identity_source(Some(new_authorizer.identity_source));
    }

    update_authorizer_builder
        .send()
        .await
        .context("Failed to update authorizer")?;

    op_exec_output!(format!(
        "Updated API Gateway V2 Authorizer `{}` for API `{}` in region `{}`",
        authorizer_id, api_id, region
    ))
}

pub async fn delete_authorizer(
    client: &Client,
    region: &str,
    api_id: &str,
    authorizer_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_authorizer()
        .api_id(api_id)
        .authorizer_id(authorizer_id)
        .send()
        .await
        .context("Failed to delete authorizer")?;

    op_exec_output!(format!(
        "Deleted API Gateway V2 Authorizer `{}` for API `{}` in region `{}`",
        authorizer_id, api_id, region
    ))
}
