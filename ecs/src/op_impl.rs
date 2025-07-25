use anyhow::Context;
use aws_sdk_ecs::{
    Client,
    types::{
        CapacityProviderStrategyItem, ClusterSetting, ContainerDefinition, DeploymentCircuitBreaker, DeploymentConfiguration,
        KeyValuePair, LoadBalancer, NetworkConfiguration, PlacementConstraint, PlacementStrategy, ServiceRegistry, Tag,
        TaskDefinitionPlacementConstraint, TaskOverride,
    },
};
use std::{collections::HashMap, str::FromStr};

use super::{
    op::{NetworkConfigurationRequest, TaskOverride as OpTaskOverride},
    resource::{Cluster as EcsCluster, Service, TaskDefinition},
    tags::Tags,
    util::{get_cluster, get_service},
};
use autoschematic_core::connector::OpExecResponse;

// Cluster Operations

/// Creates a new ECS cluster
pub async fn create_cluster(client: &Client, cluster: &EcsCluster, cluster_name: &str) -> Result<OpExecResponse, anyhow::Error> {
    let mut create_cluster = client.create_cluster();

    create_cluster = create_cluster.cluster_name(cluster_name);

    // Add capacity providers if specified
    if !cluster.capacity_providers.is_empty() {
        create_cluster = create_cluster.set_capacity_providers(Some(cluster.capacity_providers.clone()));
    }

    // Add default capacity provider strategy if specified
    if !cluster.default_capacity_provider_strategy.is_empty() {
        let mut strategy_items = Vec::new();

        for item in &cluster.default_capacity_provider_strategy {
            let mut builder = CapacityProviderStrategyItem::builder().capacity_provider(&item.capacity_provider);

            if let Some(weight) = item.weight {
                builder = builder.weight(weight);
            }

            if let Some(base) = item.base {
                builder = builder.base(base);
            }

            if let Ok(strategy_item) = builder.build() {
                strategy_items.push(strategy_item);
            }
        }

        if !strategy_items.is_empty() {
            create_cluster = create_cluster.set_default_capacity_provider_strategy(Some(strategy_items));
        }
    }

    // Add settings if specified
    if !cluster.settings.is_empty() {
        let mut settings = Vec::new();

        for setting in &cluster.settings {
            // Convert setting name to ClusterSettingName
            let setting_name = match setting.name.as_str() {
                "containerInsights" => aws_sdk_ecs::types::ClusterSettingName::ContainerInsights,
                _ => continue,
            };

            let setting_builder = ClusterSetting::builder().name(setting_name).value(&setting.value);

            settings.push(setting_builder.build());
        }

        if !settings.is_empty() {
            create_cluster = create_cluster.set_settings(Some(settings));
        }
    }

    // Apply tags
    let aws_tags: Option<Vec<Tag>> = cluster.tags.clone().into();

    if let Some(tags) = aws_tags
        && !tags.is_empty()
    {
        create_cluster = create_cluster.set_tags(Some(tags));
    }

    // Create the cluster
    let resp = create_cluster.send().await?;
    let cluster = resp.cluster.context("No cluster returned from create_cluster")?;
    let cluster_arn = cluster.cluster_arn.context("No cluster ARN returned")?;

    let mut outputs = HashMap::new();
    outputs.insert(String::from("arn"), Some(cluster_arn.clone()));

    Ok(OpExecResponse {
        outputs: Some(outputs),
        friendly_message: Some(format!("Created ECS cluster {cluster_name}")),
    })
}

/// Updates tags for an existing cluster
pub async fn update_cluster_tags(
    client: &Client,
    cluster_name: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecResponse, anyhow::Error> {
    // Get the cluster to retrieve the ARN
    let cluster = get_cluster(client, cluster_name)
        .await?
        .context(format!("Cluster {cluster_name} not found"))?;

    let cluster_arn = cluster.cluster_arn.context("No cluster ARN returned")?;

    // Use tag_diff utility to determine tags to add and remove
    let (tag_keys_to_remove, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Add tags if needed
    if !tags_to_add.is_empty() {
        client
            .tag_resource()
            .resource_arn(&cluster_arn)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    // Remove tags if needed
    if !tag_keys_to_remove.is_empty() {
        client
            .untag_resource()
            .resource_arn(&cluster_arn)
            .set_tag_keys(Some(tag_keys_to_remove))
            .send()
            .await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated tags for ECS cluster {cluster_name}")),
    })
}

/// Updates settings for an existing cluster
pub async fn update_cluster_settings(
    client: &Client,
    cluster_name: &str,
    settings: Vec<(String, String)>,
) -> Result<OpExecResponse, anyhow::Error> {
    let mut cluster_settings = Vec::new();

    for (name, value) in settings {
        // Convert setting name to ClusterSettingName
        let setting_name = match name.as_str() {
            "containerInsights" => aws_sdk_ecs::types::ClusterSettingName::ContainerInsights,
            _ => continue,
        };

        let setting_builder = ClusterSetting::builder().name(setting_name).value(value);

        cluster_settings.push(setting_builder.build());
    }

    if !cluster_settings.is_empty() {
        client
            .update_cluster_settings()
            .cluster(cluster_name)
            .set_settings(Some(cluster_settings))
            .send()
            .await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated settings for ECS cluster {cluster_name}")),
    })
}

/// Updates capacity providers for a cluster
pub async fn update_cluster_capacity_providers(
    client: &Client,
    cluster_name: &str,
    add_capacity_providers: Vec<String>,
    remove_capacity_providers: Vec<String>,
    default_strategy: Vec<(String, Option<i32>, Option<i32>)>,
) -> Result<OpExecResponse, anyhow::Error> {
    // Get current capacity providers
    let cluster = get_cluster(client, cluster_name)
        .await?
        .context(format!("Cluster {cluster_name} not found"))?;

    let mut capacity_providers = cluster.capacity_providers.unwrap_or_default();

    // Remove capacity providers
    for provider in &remove_capacity_providers {
        capacity_providers.retain(|p| p != provider);
    }

    // Add new capacity providers
    for provider in &add_capacity_providers {
        if !capacity_providers.contains(provider) {
            capacity_providers.push(provider.clone());
        }
    }

    // Build the default strategy
    let mut strategy_items = Vec::new();

    for (provider, weight, base) in default_strategy {
        let mut builder = CapacityProviderStrategyItem::builder().capacity_provider(provider);

        if let Some(weight) = weight {
            builder = builder.weight(weight);
        }

        if let Some(base) = base {
            builder = builder.base(base);
        }

        if let Ok(item) = builder.build() {
            strategy_items.push(item);
        }
    }

    client
        .put_cluster_capacity_providers()
        .cluster(cluster_name)
        .set_capacity_providers(Some(capacity_providers))
        .set_default_capacity_provider_strategy(Some(strategy_items))
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated capacity providers for ECS cluster {cluster_name}")),
    })
}

/// Deletes an ECS cluster
pub async fn delete_cluster(client: &Client, cluster_name: &str) -> Result<OpExecResponse, anyhow::Error> {
    client.delete_cluster().cluster(cluster_name).send().await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Deleted ECS cluster {cluster_name}")),
    })
}

// Service Operations

/// Creates a new ECS service
pub async fn create_service(
    client: &Client,
    cluster_name: &str,
    service: &Service,
    service_name: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    // Get service name from tags

    let mut create_service = client
        .create_service()
        .service_name(service_name)
        .cluster(cluster_name)
        .task_definition(&service.task_definition)
        .desired_count(service.desired_count);

    // Set launch type if specified
    if let Some(launch_type) = &service.launch_type {
        match launch_type.as_str() {
            "EC2" => create_service = create_service.launch_type(aws_sdk_ecs::types::LaunchType::Ec2),
            "FARGATE" => create_service = create_service.launch_type(aws_sdk_ecs::types::LaunchType::Fargate),
            "EXTERNAL" => create_service = create_service.launch_type(aws_sdk_ecs::types::LaunchType::External),
            _ => {}
        }
    }

    // Set capacity provider strategy if specified
    if !service.capacity_provider_strategy.is_empty() {
        let mut strategy_items = Vec::new();

        for item in &service.capacity_provider_strategy {
            let mut builder = CapacityProviderStrategyItem::builder().capacity_provider(&item.capacity_provider);

            if let Some(weight) = item.weight {
                builder = builder.weight(weight);
            }

            if let Some(base) = item.base {
                builder = builder.base(base);
            }

            if let Ok(strategy_item) = builder.build() {
                strategy_items.push(strategy_item);
            }
        }

        if !strategy_items.is_empty() {
            create_service = create_service.set_capacity_provider_strategy(Some(strategy_items));
        }
    }

    // Set platform version if specified
    if let Some(platform_version) = &service.platform_version {
        create_service = create_service.platform_version(platform_version);
    }

    // Set deployment configuration if specified
    if let Some(deployment_config) = &service.deployment_configuration {
        let mut builder = DeploymentConfiguration::builder();

        if let Some(max_percent) = deployment_config.maximum_percent {
            builder = builder.maximum_percent(max_percent);
        }

        if let Some(min_percent) = deployment_config.minimum_healthy_percent {
            builder = builder.minimum_healthy_percent(min_percent);
        }

        if let Some(circuit_breaker) = &deployment_config.deployment_circuit_breaker {
            builder = builder.deployment_circuit_breaker(
                DeploymentCircuitBreaker::builder()
                    .enable(circuit_breaker.enable)
                    .rollback(circuit_breaker.rollback)
                    .build(),
            );
        }

        create_service = create_service.deployment_configuration(builder.build());
    }

    // Set network configuration if specified
    if let Some(network_config) = &service.network_configuration
        && let Some(awsvpc_config) = &network_config.awsvpc_configuration
    {
        let mut builder = aws_sdk_ecs::types::AwsVpcConfiguration::builder()
            .set_subnets(Some(awsvpc_config.subnets.clone()))
            .set_security_groups(Some(awsvpc_config.security_groups.clone()));

        if let Some(assign_public_ip) = &awsvpc_config.assign_public_ip {
            match assign_public_ip.as_str() {
                "ENABLED" => builder = builder.assign_public_ip(aws_sdk_ecs::types::AssignPublicIp::Enabled),
                "DISABLED" => builder = builder.assign_public_ip(aws_sdk_ecs::types::AssignPublicIp::Disabled),
                _ => {}
            }
        }

        if let Ok(vpc_config) = builder.build() {
            let network_config = NetworkConfiguration::builder().awsvpc_configuration(vpc_config).build();

            create_service = create_service.network_configuration(network_config);
        }
    }

    // Set placement constraints if specified
    if !service.placement_constraints.is_empty() {
        let mut constraints = Vec::new();

        for constraint in &service.placement_constraints {
            let mut builder = PlacementConstraint::builder().r#type(constraint.r#type.as_str().into());

            if let Some(expression) = &constraint.expression {
                builder = builder.expression(expression);
            }

            constraints.push(builder.build());
        }

        if !constraints.is_empty() {
            create_service = create_service.set_placement_constraints(Some(constraints));
        }
    }

    // Set placement strategy if specified
    if !service.placement_strategy.is_empty() {
        let mut strategies = Vec::new();

        for strategy in &service.placement_strategy {
            let mut builder = PlacementStrategy::builder().r#type(strategy.r#type.as_str().into());

            if let Some(field) = &strategy.field {
                builder = builder.field(field);
            }

            strategies.push(builder.build());
        }

        if !strategies.is_empty() {
            create_service = create_service.set_placement_strategy(Some(strategies));
        }
    }

    // Set load balancers if specified
    if !service.load_balancers.is_empty() {
        let mut load_balancers = Vec::new();

        for lb in &service.load_balancers {
            let mut builder = LoadBalancer::builder();

            if let Some(target_group_arn) = &lb.target_group_arn {
                builder = builder.target_group_arn(target_group_arn);
            }

            if let Some(lb_name) = &lb.load_balancer_name {
                builder = builder.load_balancer_name(lb_name);
            }

            if let Some(container_name) = &lb.container_name {
                builder = builder.container_name(container_name);
            }

            if let Some(container_port) = lb.container_port {
                builder = builder.container_port(container_port);
            }

            load_balancers.push(builder.build());
        }

        if !load_balancers.is_empty() {
            create_service = create_service.set_load_balancers(Some(load_balancers));
        }
    }

    // Set service registries if specified
    if !service.service_registries.is_empty() {
        let mut registries = Vec::new();

        for reg in &service.service_registries {
            let mut builder = ServiceRegistry::builder();

            if let Some(registry_arn) = &reg.registry_arn {
                builder = builder.registry_arn(registry_arn);
            }

            if let Some(port) = reg.port {
                builder = builder.port(port);
            }

            if let Some(container_name) = &reg.container_name {
                builder = builder.container_name(container_name);
            }

            if let Some(container_port) = reg.container_port {
                builder = builder.container_port(container_port);
            }

            registries.push(builder.build());
        }

        if !registries.is_empty() {
            create_service = create_service.set_service_registries(Some(registries));
        }
    }

    // Set scheduling strategy if specified
    if let Some(scheduling_strategy) = &service.scheduling_strategy {
        match scheduling_strategy.as_str() {
            "REPLICA" => create_service = create_service.scheduling_strategy(aws_sdk_ecs::types::SchedulingStrategy::Replica),
            "DAEMON" => create_service = create_service.scheduling_strategy(aws_sdk_ecs::types::SchedulingStrategy::Daemon),
            _ => {}
        }
    }

    // Set enable_ecs_managed_tags if specified
    if let Some(enable_ecs_managed_tags) = service.enable_ecs_managed_tags {
        create_service = create_service.enable_ecs_managed_tags(enable_ecs_managed_tags);
    }

    // Set propagate_tags if specified
    if let Some(propagate_tags) = &service.propagate_tags {
        match propagate_tags.as_str() {
            "SERVICE" => create_service = create_service.propagate_tags(aws_sdk_ecs::types::PropagateTags::Service),
            "TASK_DEFINITION" => {
                create_service = create_service.propagate_tags(aws_sdk_ecs::types::PropagateTags::TaskDefinition)
            }
            _ => {}
        }
    }

    // Set enable_execute_command if specified
    if let Some(enable_execute_command) = service.enable_execute_command {
        create_service = create_service.enable_execute_command(enable_execute_command);
    }

    // Apply tags
    let aws_tags: Option<Vec<Tag>> = service.tags.clone().into();

    if let Some(tags) = aws_tags
        && !tags.is_empty()
    {
        create_service = create_service.set_tags(Some(tags));
    }

    // Create the service
    let resp = create_service.send().await?;
    let service = resp.service.context("No service returned from create_service")?;
    let service_arn = service.service_arn.context("No service ARN returned")?;
    let service_name = service.service_name.context("No service name returned")?;

    let mut outputs = HashMap::new();
    outputs.insert(String::from("arn"), Some(service_arn.clone()));
    outputs.insert(String::from("service_name"), Some(service_name.clone()));

    Ok(OpExecResponse {
        outputs: Some(outputs),
        friendly_message: Some(format!("Created ECS service {service_name} in cluster {cluster_name}")),
    })
}

/// Updates tags for an existing service
pub async fn update_service_tags(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecResponse, anyhow::Error> {
    // Get the service to retrieve the ARN
    let service = get_service(client, cluster_name, service_name)
        .await?
        .context(format!("Service {service_name} not found in cluster {cluster_name}"))?;

    let service_arn = service.service_arn.context("No service ARN returned")?;

    // Use tag_diff utility to determine tags to add and remove
    let (tag_keys_to_remove, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Add tags if needed
    if !tags_to_add.is_empty() {
        client
            .tag_resource()
            .resource_arn(&service_arn)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    // Remove tags if needed
    if !tag_keys_to_remove.is_empty() {
        client
            .untag_resource()
            .resource_arn(&service_arn)
            .set_tag_keys(Some(tag_keys_to_remove))
            .send()
            .await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated tags for ECS service {service_name} in cluster {cluster_name}"
        )),
    })
}

/// Updates the desired count for a service
pub async fn update_service_desired_count(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    desired_count: i32,
) -> Result<OpExecResponse, anyhow::Error> {
    client
        .update_service()
        .cluster(cluster_name)
        .service(service_name)
        .desired_count(desired_count)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated desired count to {desired_count} for ECS service {service_name} in cluster {cluster_name}"
        )),
    })
}

/// Updates the task definition for a service
pub async fn update_service_task_definition(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    task_definition: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    client
        .update_service()
        .cluster(cluster_name)
        .service(service_name)
        .task_definition(task_definition)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated task definition to {task_definition} for ECS service {service_name} in cluster {cluster_name}"
        )),
    })
}

/// Updates deployment configuration for a service
pub async fn update_service_deployment_configuration(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    maximum_percent: Option<i32>,
    minimum_healthy_percent: Option<i32>,
    enable_circuit_breaker: Option<bool>,
    enable_rollback: Option<bool>,
) -> Result<OpExecResponse, anyhow::Error> {
    let mut builder = DeploymentConfiguration::builder();

    if let Some(max_percent) = maximum_percent {
        builder = builder.maximum_percent(max_percent);
    }

    if let Some(min_percent) = minimum_healthy_percent {
        builder = builder.minimum_healthy_percent(min_percent);
    }

    // Both circuit breaker and rollback need to be set together
    if let (Some(enable_circuit), Some(enable_roll)) = (enable_circuit_breaker, enable_rollback) {
        builder = builder.deployment_circuit_breaker(
            DeploymentCircuitBreaker::builder()
                .enable(enable_circuit)
                .rollback(enable_roll)
                .build(),
        );
    }

    let deployment_config = builder.build();
    client
        .update_service()
        .cluster(cluster_name)
        .service(service_name)
        .deployment_configuration(deployment_config)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated deployment configuration for ECS service {service_name} in cluster {cluster_name}"
        )),
    })
}

/// Updates load balancers for a service
pub async fn update_service_load_balancers(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    _old_load_balancers: Vec<super::resource::LoadBalancer>,
    new_load_balancers: Vec<super::resource::LoadBalancer>,
) -> Result<OpExecResponse, anyhow::Error> {
    // Convert our LoadBalancer structs to AWS SDK LoadBalancer structs
    let mut aws_load_balancers = Vec::new();

    for lb in &new_load_balancers {
        let mut builder = LoadBalancer::builder();

        if let Some(target_group_arn) = &lb.target_group_arn {
            builder = builder.target_group_arn(target_group_arn);
        }

        if let Some(lb_name) = &lb.load_balancer_name {
            builder = builder.load_balancer_name(lb_name);
        }

        if let Some(container_name) = &lb.container_name {
            builder = builder.container_name(container_name);
        }

        if let Some(container_port) = lb.container_port {
            builder = builder.container_port(container_port);
        }

        aws_load_balancers.push(builder.build());
    }

    // Update the service with the new load balancers
    client
        .update_service()
        .cluster(cluster_name)
        .service(service_name)
        .set_load_balancers(Some(aws_load_balancers))
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated load balancers for ECS service {service_name} in cluster {cluster_name}"
        )),
    })
}

/// Enables or disables execute command for a service
pub async fn enable_execute_command(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    enable: bool,
) -> Result<OpExecResponse, anyhow::Error> {
    client
        .update_service()
        .cluster(cluster_name)
        .service(service_name)
        .enable_execute_command(enable)
        .send()
        .await?;

    let action = if enable { "Enabled" } else { "Disabled" };

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "{action} execute command for ECS service {service_name} in cluster {cluster_name}"
        )),
    })
}

/// Deletes an ECS service
pub async fn delete_service(client: &Client, cluster_name: &str, service_name: &str) -> Result<OpExecResponse, anyhow::Error> {
    client
        .delete_service()
        .cluster(cluster_name)
        .service(service_name)
        .force(true) // Use force to allow deleting even if it has instances
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Deleted ECS service {service_name} from cluster {cluster_name}")),
    })
}

// TaskDefinition Operations

/// Registers a new task definition
pub async fn register_task_definition(
    client: &Client,
    family: &str,
    task_definition: &TaskDefinition,
) -> Result<OpExecResponse, anyhow::Error> {
    let mut register_task_def = client.register_task_definition().family(family);

    // Set task role ARN if specified
    if let Some(task_role_arn) = &task_definition.task_role_arn {
        register_task_def = register_task_def.task_role_arn(task_role_arn);
    }

    // Set execution role ARN if specified
    if let Some(execution_role_arn) = &task_definition.execution_role_arn {
        register_task_def = register_task_def.execution_role_arn(execution_role_arn);
    }

    // Set network mode if specified
    if let Some(network_mode) = &task_definition.network_mode {
        match network_mode.as_str() {
            "bridge" => register_task_def = register_task_def.network_mode(aws_sdk_ecs::types::NetworkMode::Bridge),
            "host" => register_task_def = register_task_def.network_mode(aws_sdk_ecs::types::NetworkMode::Host),
            "awsvpc" => register_task_def = register_task_def.network_mode(aws_sdk_ecs::types::NetworkMode::Awsvpc),
            "none" => register_task_def = register_task_def.network_mode(aws_sdk_ecs::types::NetworkMode::None),
            _ => {}
        }
    }

    // Set container definitions
    let mut container_defs = Vec::new();

    for container in &task_definition.container_definitions {
        let mut builder = ContainerDefinition::builder().name(&container.name).image(&container.image);

        // Set CPU if specified
        if let Some(cpu) = container.cpu {
            builder = builder.cpu(cpu);
        }

        // Set memory if specified
        if let Some(memory) = container.memory {
            builder = builder.memory(memory);
        }

        // Set memory reservation if specified
        if let Some(memory_reservation) = container.memory_reservation {
            builder = builder.memory_reservation(memory_reservation);
        }

        // Set links if specified
        if !container.links.is_empty() {
            builder = builder.set_links(Some(container.links.clone()));
        }

        // Set port mappings if specified
        if !container.port_mappings.is_empty() {
            let mut port_mappings = Vec::new();

            for pm in &container.port_mappings {
                let mut pm_builder = aws_sdk_ecs::types::PortMapping::builder();

                if let Some(container_port) = pm.container_port {
                    pm_builder = pm_builder.container_port(container_port);
                }

                if let Some(host_port) = pm.host_port {
                    pm_builder = pm_builder.host_port(host_port);
                }

                if let Some(protocol) = &pm.protocol {
                    match protocol.as_str() {
                        "tcp" => pm_builder = pm_builder.protocol(aws_sdk_ecs::types::TransportProtocol::Tcp),
                        "udp" => pm_builder = pm_builder.protocol(aws_sdk_ecs::types::TransportProtocol::Udp),
                        _ => {}
                    }
                }

                port_mappings.push(pm_builder.build());
            }

            if !port_mappings.is_empty() {
                builder = builder.set_port_mappings(Some(port_mappings));
            }
        }

        if let Some(essential) = container.essential {
            builder = builder.essential(essential);
        }

        if !container.entry_point.is_empty() {
            builder = builder.set_entry_point(Some(container.entry_point.clone()));
        }

        if !container.command.is_empty() {
            builder = builder.set_command(Some(container.command.clone()));
        }

        if !container.environment.is_empty() {
            let mut env_vars = Vec::new();

            for env in &container.environment {
                let mut env_builder = aws_sdk_ecs::types::KeyValuePair::builder();

                if let Some(name) = &env.name {
                    env_builder = env_builder.name(name);
                }

                if let Some(value) = &env.value {
                    env_builder = env_builder.value(value);
                }

                env_vars.push(env_builder.build());
            }

            if !env_vars.is_empty() {
                builder = builder.set_environment(Some(env_vars));
            }
        }

        // Set environment files if specified
        if !container.environment_files.is_empty() {
            let mut env_files = Vec::new();

            for env_file in &container.environment_files {
                let mut env_file_builder = aws_sdk_ecs::types::EnvironmentFile::builder().value(&env_file.value);

                match env_file.r#type.as_str() {
                    "s3" => env_file_builder = env_file_builder.r#type(aws_sdk_ecs::types::EnvironmentFileType::S3),
                    _ => continue,
                }

                if let Ok(env_file) = env_file_builder.build() {
                    env_files.push(env_file);
                }
            }

            if !env_files.is_empty() {
                builder = builder.set_environment_files(Some(env_files));
            }
        }

        // Set mount points if specified
        if !container.mount_points.is_empty() {
            let mut mount_points = Vec::new();

            for mp in &container.mount_points {
                let mut mp_builder = aws_sdk_ecs::types::MountPoint::builder();

                if let Some(source_volume) = &mp.source_volume {
                    mp_builder = mp_builder.source_volume(source_volume);
                }

                if let Some(container_path) = &mp.container_path {
                    mp_builder = mp_builder.container_path(container_path);
                }

                if let Some(read_only) = mp.read_only {
                    mp_builder = mp_builder.read_only(read_only);
                }

                mount_points.push(mp_builder.build());
            }

            if !mount_points.is_empty() {
                builder = builder.set_mount_points(Some(mount_points));
            }
        }

        // Set volumes from if specified
        if !container.volumes_from.is_empty() {
            let mut volumes_from = Vec::new();

            for vf in &container.volumes_from {
                let mut vf_builder = aws_sdk_ecs::types::VolumeFrom::builder();

                if let Some(source_container) = &vf.source_container {
                    vf_builder = vf_builder.source_container(source_container);
                }

                if let Some(read_only) = vf.read_only {
                    vf_builder = vf_builder.read_only(read_only);
                }

                volumes_from.push(vf_builder.build());
            }

            if !volumes_from.is_empty() {
                builder = builder.set_volumes_from(Some(volumes_from));
            }
        }

        // Set linux parameters if specified
        if let Some(linux_params) = &container.linux_parameters {
            let mut linux_builder = aws_sdk_ecs::types::LinuxParameters::builder();

            // Set capabilities
            if let Some(capabilities) = &linux_params.capabilities {
                let mut cap_builder = aws_sdk_ecs::types::KernelCapabilities::builder();

                if !capabilities.add.is_empty() {
                    cap_builder = cap_builder.set_add(Some(capabilities.add.clone()));
                }

                if !capabilities.drop.is_empty() {
                    cap_builder = cap_builder.set_drop(Some(capabilities.drop.clone()));
                }

                linux_builder = linux_builder.capabilities(cap_builder.build());
            }

            // Set devices
            if !linux_params.devices.is_empty() {
                let mut devices = Vec::new();

                for device in &linux_params.devices {
                    let mut device_builder = aws_sdk_ecs::types::Device::builder().host_path(&device.host_path);

                    if let Some(container_path) = &device.container_path {
                        device_builder = device_builder.container_path(container_path);
                    }

                    if !device.permissions.is_empty() {
                        let mut perms = Vec::new();
                        for perm in &device.permissions {
                            match perm.as_str() {
                                "read" => perms.push(aws_sdk_ecs::types::DeviceCgroupPermission::Read),
                                "write" => perms.push(aws_sdk_ecs::types::DeviceCgroupPermission::Write),
                                "mknod" => perms.push(aws_sdk_ecs::types::DeviceCgroupPermission::Mknod),
                                _ => continue,
                            }
                        }
                        device_builder = device_builder.set_permissions(Some(perms));
                    }

                    if let Ok(device) = device_builder.build() {
                        devices.push(device);
                    }
                }

                if !devices.is_empty() {
                    linux_builder = linux_builder.set_devices(Some(devices));
                }
            }

            // Set init process enabled
            if let Some(init_process_enabled) = linux_params.init_process_enabled {
                linux_builder = linux_builder.init_process_enabled(init_process_enabled);
            }

            // Set shared memory size
            if let Some(shared_memory_size) = linux_params.shared_memory_size {
                linux_builder = linux_builder.shared_memory_size(shared_memory_size);
            }

            // Set tmpfs
            if !linux_params.tmpfs.is_empty() {
                let mut tmpfs_list = Vec::new();

                for tmpfs in &linux_params.tmpfs {
                    let mut tmpfs_builder = aws_sdk_ecs::types::Tmpfs::builder()
                        .container_path(&tmpfs.container_path)
                        .size(tmpfs.size);

                    if !tmpfs.mount_options.is_empty() {
                        tmpfs_builder = tmpfs_builder.set_mount_options(Some(tmpfs.mount_options.clone()));
                    }

                    if let Ok(tmpfs) = tmpfs_builder.build() {
                        tmpfs_list.push(tmpfs);
                    }
                }

                if !tmpfs_list.is_empty() {
                    linux_builder = linux_builder.set_tmpfs(Some(tmpfs_list));
                }
            }

            // Set max swap
            if let Some(max_swap) = linux_params.max_swap {
                linux_builder = linux_builder.max_swap(max_swap);
            }

            // Set swappiness
            if let Some(swappiness) = linux_params.swappiness {
                linux_builder = linux_builder.swappiness(swappiness);
            }

            builder = builder.linux_parameters(linux_builder.build());
        }

        // Set secrets if specified
        if !container.secrets.is_empty() {
            let mut secrets = Vec::new();

            for secret in &container.secrets {
                let secret_builder = aws_sdk_ecs::types::Secret::builder()
                    .name(&secret.name)
                    .value_from(&secret.value_from);

                if let Ok(secret) = secret_builder.build() {
                    secrets.push(secret);
                }
            }

            if !secrets.is_empty() {
                builder = builder.set_secrets(Some(secrets));
            }
        }

        // Set container dependencies if specified
        if !container.depends_on.is_empty() {
            let mut depends_on = Vec::new();

            for dependency in &container.depends_on {
                let mut dep_builder =
                    aws_sdk_ecs::types::ContainerDependency::builder().container_name(&dependency.container_name);

                match dependency.condition.as_str() {
                    "START" => dep_builder = dep_builder.condition(aws_sdk_ecs::types::ContainerCondition::Start),
                    "COMPLETE" => dep_builder = dep_builder.condition(aws_sdk_ecs::types::ContainerCondition::Complete),
                    "SUCCESS" => dep_builder = dep_builder.condition(aws_sdk_ecs::types::ContainerCondition::Success),
                    "HEALTHY" => dep_builder = dep_builder.condition(aws_sdk_ecs::types::ContainerCondition::Healthy),
                    _ => continue,
                }

                if let Ok(dependency) = dep_builder.build() {
                    depends_on.push(dependency);
                }
            }

            if !depends_on.is_empty() {
                builder = builder.set_depends_on(Some(depends_on));
            }
        }

        // Set start timeout if specified
        if let Some(start_timeout) = container.start_timeout {
            builder = builder.start_timeout(start_timeout);
        }

        // Set stop timeout if specified
        if let Some(stop_timeout) = container.stop_timeout {
            builder = builder.stop_timeout(stop_timeout);
        }

        // Set hostname if specified
        if let Some(hostname) = &container.hostname {
            builder = builder.hostname(hostname);
        }

        // Set user if specified
        if let Some(user) = &container.user {
            builder = builder.user(user);
        }

        // Set working directory if specified
        if let Some(working_directory) = &container.working_directory {
            builder = builder.working_directory(working_directory);
        }

        // Set disable networking if specified
        if let Some(disable_networking) = container.disable_networking {
            builder = builder.disable_networking(disable_networking);
        }

        // Set privileged if specified
        if let Some(privileged) = container.privileged {
            builder = builder.privileged(privileged);
        }

        // Set readonly root filesystem if specified
        if let Some(readonly_root_filesystem) = container.readonly_root_filesystem {
            builder = builder.readonly_root_filesystem(readonly_root_filesystem);
        }

        // Set DNS servers if specified
        if !container.dns_servers.is_empty() {
            builder = builder.set_dns_servers(Some(container.dns_servers.clone()));
        }

        // Set DNS search domains if specified
        if !container.dns_search_domains.is_empty() {
            builder = builder.set_dns_search_domains(Some(container.dns_search_domains.clone()));
        }

        // Set extra hosts if specified
        if !container.extra_hosts.is_empty() {
            let mut extra_hosts = Vec::new();

            for host_entry in &container.extra_hosts {
                let host_entry_builder = aws_sdk_ecs::types::HostEntry::builder()
                    .hostname(&host_entry.hostname)
                    .ip_address(&host_entry.ip_address);

                if let Ok(host_entry) = host_entry_builder.build() {
                    extra_hosts.push(host_entry);
                }
            }

            if !extra_hosts.is_empty() {
                builder = builder.set_extra_hosts(Some(extra_hosts));
            }
        }

        // Set docker security options if specified
        if !container.docker_security_options.is_empty() {
            builder = builder.set_docker_security_options(Some(container.docker_security_options.clone()));
        }

        // Set interactive if specified
        if let Some(interactive) = container.interactive {
            builder = builder.interactive(interactive);
        }

        // Set pseudo terminal if specified
        if let Some(pseudo_terminal) = container.pseudo_terminal {
            builder = builder.pseudo_terminal(pseudo_terminal);
        }

        // Set docker labels if specified
        if !container.docker_labels.is_empty() {
            builder = builder.set_docker_labels(Some(container.docker_labels.clone()));
        }

        // Set ulimits if specified
        if !container.ulimits.is_empty() {
            let mut ulimits = Vec::new();

            for ulimit in &container.ulimits {
                let ulimit_builder = aws_sdk_ecs::types::Ulimit::builder()
                    .name(ulimit.name.as_str().into())
                    .hard_limit(ulimit.hard_limit)
                    .soft_limit(ulimit.soft_limit);

                if let Ok(ulimit) = ulimit_builder.build() {
                    ulimits.push(ulimit);
                }
            }

            if !ulimits.is_empty() {
                builder = builder.set_ulimits(Some(ulimits));
            }
        }

        // Set log configuration if specified
        if let Some(log_config) = &container.log_configuration {
            let mut log_config_builder =
                aws_sdk_ecs::types::LogConfiguration::builder().log_driver(log_config.log_driver.as_str().into());

            if !log_config.options.is_empty() {
                log_config_builder = log_config_builder.set_options(Some(log_config.options.clone()));
            }

            if !log_config.secret_options.is_empty() {
                let mut secret_options = Vec::new();

                for secret in &log_config.secret_options {
                    let secret_builder = aws_sdk_ecs::types::Secret::builder()
                        .name(&secret.name)
                        .value_from(&secret.value_from);

                    if let Ok(secret) = secret_builder.build() {
                        secret_options.push(secret);
                    }
                }

                if !secret_options.is_empty() {
                    log_config_builder = log_config_builder.set_secret_options(Some(secret_options));
                }
            }

            if let Ok(log_config) = log_config_builder.build() {
                builder = builder.log_configuration(log_config);
            }
        }

        // Set health check if specified
        if let Some(health_check) = &container.health_check {
            let mut health_check_builder =
                aws_sdk_ecs::types::HealthCheck::builder().set_command(Some(health_check.command.clone()));

            if let Some(interval) = health_check.interval {
                health_check_builder = health_check_builder.interval(interval);
            }

            if let Some(timeout) = health_check.timeout {
                health_check_builder = health_check_builder.timeout(timeout);
            }

            if let Some(retries) = health_check.retries {
                health_check_builder = health_check_builder.retries(retries);
            }

            if let Some(start_period) = health_check.start_period {
                health_check_builder = health_check_builder.start_period(start_period);
            }

            if let Ok(health_check) = health_check_builder.build() {
                builder = builder.health_check(health_check);
            }
        }

        // Set system controls if specified
        if !container.system_controls.is_empty() {
            let mut system_controls = Vec::new();

            for system_control in &container.system_controls {
                let mut sc_builder = aws_sdk_ecs::types::SystemControl::builder();

                if let Some(namespace) = &system_control.namespace {
                    sc_builder = sc_builder.namespace(namespace);
                }

                if let Some(value) = &system_control.value {
                    sc_builder = sc_builder.value(value);
                }

                system_controls.push(sc_builder.build());
            }

            if !system_controls.is_empty() {
                builder = builder.set_system_controls(Some(system_controls));
            }
        }

        // Set resource requirements if specified
        if !container.resource_requirements.is_empty() {
            let mut resource_requirements = Vec::new();

            for resource_req in &container.resource_requirements {
                let mut rr_builder = aws_sdk_ecs::types::ResourceRequirement::builder().value(&resource_req.value);

                match resource_req.r#type.as_str() {
                    "GPU" => rr_builder = rr_builder.r#type(aws_sdk_ecs::types::ResourceType::Gpu),
                    "InferenceAccelerator" => {
                        rr_builder = rr_builder.r#type(aws_sdk_ecs::types::ResourceType::InferenceAccelerator)
                    }
                    _ => continue,
                }

                if let Ok(resource_req) = rr_builder.build() {
                    resource_requirements.push(resource_req);
                }
            }

            if !resource_requirements.is_empty() {
                builder = builder.set_resource_requirements(Some(resource_requirements));
            }
        }

        // Set firelens configuration if specified
        if let Some(firelens_config) = &container.firelens_configuration {
            let mut firelens_builder = aws_sdk_ecs::types::FirelensConfiguration::builder();

            match firelens_config.r#type.as_str() {
                "fluentd" => firelens_builder = firelens_builder.r#type(aws_sdk_ecs::types::FirelensConfigurationType::Fluentd),
                "fluentbit" => {
                    firelens_builder = firelens_builder.r#type(aws_sdk_ecs::types::FirelensConfigurationType::Fluentbit)
                }
                _ => {}
            }

            if !firelens_config.options.is_empty() {
                firelens_builder = firelens_builder.set_options(Some(firelens_config.options.clone()));
            }

            if let Ok(firelens_config) = firelens_builder.build() {
                builder = builder.firelens_configuration(firelens_config);
            }
        }

        container_defs.push(builder.build());
    }

    if !container_defs.is_empty() {
        register_task_def = register_task_def.set_container_definitions(Some(container_defs));
    }

    // Set volumes if specified
    if !task_definition.volumes.is_empty() {
        let mut volumes = Vec::new();

        for vol in &task_definition.volumes {
            let mut vol_builder = aws_sdk_ecs::types::Volume::builder().name(&vol.name);

            // Set host volume properties if specified
            if let Some(host) = &vol.host {
                let mut host_builder = aws_sdk_ecs::types::HostVolumeProperties::builder();

                if let Some(source_path) = &host.source_path {
                    host_builder = host_builder.source_path(source_path);
                }

                vol_builder = vol_builder.host(host_builder.build());
            }

            // Set docker volume configuration if specified
            if let Some(docker_volume) = &vol.docker_volume_configuration {
                let mut docker_builder = aws_sdk_ecs::types::DockerVolumeConfiguration::builder();

                if let Some(scope) = &docker_volume.scope {
                    match scope.as_str() {
                        "task" => docker_builder = docker_builder.scope(aws_sdk_ecs::types::Scope::Task),
                        "shared" => docker_builder = docker_builder.scope(aws_sdk_ecs::types::Scope::Shared),
                        _ => {}
                    }
                }

                if let Some(autoprovision) = docker_volume.autoprovision {
                    docker_builder = docker_builder.autoprovision(autoprovision);
                }

                if let Some(driver) = &docker_volume.driver {
                    docker_builder = docker_builder.driver(driver);
                }

                if !docker_volume.driver_opts.is_empty() {
                    docker_builder = docker_builder.set_driver_opts(Some(docker_volume.driver_opts.clone()));
                }

                if !docker_volume.labels.is_empty() {
                    docker_builder = docker_builder.set_labels(Some(docker_volume.labels.clone()));
                }

                vol_builder = vol_builder.docker_volume_configuration(docker_builder.build());
            }

            // Set EFS volume configuration if specified
            if let Some(efs_volume) = &vol.efs_volume_configuration {
                let mut efs_builder =
                    aws_sdk_ecs::types::EfsVolumeConfiguration::builder().file_system_id(&efs_volume.file_system_id);

                if let Some(root_directory) = &efs_volume.root_directory {
                    efs_builder = efs_builder.root_directory(root_directory);
                }

                if let Some(transit_encryption) = &efs_volume.transit_encryption {
                    match transit_encryption.as_str() {
                        "ENABLED" => {
                            efs_builder = efs_builder.transit_encryption(aws_sdk_ecs::types::EfsTransitEncryption::Enabled)
                        }
                        "DISABLED" => {
                            efs_builder = efs_builder.transit_encryption(aws_sdk_ecs::types::EfsTransitEncryption::Disabled)
                        }
                        _ => {}
                    }
                }

                if let Some(transit_encryption_port) = efs_volume.transit_encryption_port {
                    efs_builder = efs_builder.transit_encryption_port(transit_encryption_port);
                }

                if let Some(auth_config) = &efs_volume.authorization_config {
                    let mut auth_builder = aws_sdk_ecs::types::EfsAuthorizationConfig::builder();

                    if let Some(iam) = &auth_config.iam {
                        match iam.as_str() {
                            "ENABLED" => {
                                auth_builder = auth_builder.iam(aws_sdk_ecs::types::EfsAuthorizationConfigIam::Enabled)
                            }
                            "DISABLED" => {
                                auth_builder = auth_builder.iam(aws_sdk_ecs::types::EfsAuthorizationConfigIam::Disabled)
                            }
                            _ => {}
                        }
                    }

                    if let Some(access_point_id) = &auth_config.access_point_id {
                        auth_builder = auth_builder.access_point_id(access_point_id);
                    }

                    efs_builder = efs_builder.authorization_config(auth_builder.build());
                }

                if let Ok(efs_config) = efs_builder.build() {
                    vol_builder = vol_builder.efs_volume_configuration(efs_config);
                }
            }

            volumes.push(vol_builder.build());
        }

        if !volumes.is_empty() {
            register_task_def = register_task_def.set_volumes(Some(volumes));
        }
    }

    // Set placement constraints if specified
    if !task_definition.placement_constraints.is_empty() {
        let mut constraints = Vec::new();

        for constraint in &task_definition.placement_constraints {
            let mut builder = TaskDefinitionPlacementConstraint::builder().r#type(constraint.r#type.as_str().into());

            if let Some(expression) = &constraint.expression {
                builder = builder.expression(expression);
            }

            constraints.push(builder.build());
        }

        if !constraints.is_empty() {
            register_task_def = register_task_def.set_placement_constraints(Some(constraints));
        }
    }

    // Set requires compatibilities if specified
    if !task_definition.requires_compatibilities.is_empty() {
        let mut compatibilities = Vec::new();

        for compat in &task_definition.requires_compatibilities {
            match compat.as_str() {
                "EC2" => compatibilities.push(aws_sdk_ecs::types::Compatibility::Ec2),
                "FARGATE" => compatibilities.push(aws_sdk_ecs::types::Compatibility::Fargate),
                "EXTERNAL" => compatibilities.push(aws_sdk_ecs::types::Compatibility::External),
                _ => continue,
            }
        }

        if !compatibilities.is_empty() {
            register_task_def = register_task_def.set_requires_compatibilities(Some(compatibilities));
        }
    }

    // Set CPU if specified
    if let Some(cpu) = &task_definition.cpu {
        register_task_def = register_task_def.cpu(cpu);
    }

    // Set memory if specified
    if let Some(memory) = &task_definition.memory {
        register_task_def = register_task_def.memory(memory);
    }

    // Set PID mode if specified
    if let Some(pid_mode) = &task_definition.pid_mode {
        match pid_mode.as_str() {
            "host" => register_task_def = register_task_def.pid_mode(aws_sdk_ecs::types::PidMode::Host),
            "task" => register_task_def = register_task_def.pid_mode(aws_sdk_ecs::types::PidMode::Task),
            _ => {}
        }
    }

    // Set IPC mode if specified
    if let Some(ipc_mode) = &task_definition.ipc_mode {
        match ipc_mode.as_str() {
            "host" => register_task_def = register_task_def.ipc_mode(aws_sdk_ecs::types::IpcMode::Host),
            "task" => register_task_def = register_task_def.ipc_mode(aws_sdk_ecs::types::IpcMode::Task),
            "none" => register_task_def = register_task_def.ipc_mode(aws_sdk_ecs::types::IpcMode::None),
            _ => {}
        }
    }

    // Set proxy configuration if specified
    if let Some(proxy_config) = &task_definition.proxy_configuration {
        let mut proxy_builder = aws_sdk_ecs::types::ProxyConfiguration::builder().container_name(&proxy_config.container_name);

        if let Some(proxy_type) = &proxy_config.r#type {
            match proxy_type.as_str() {
                "APPMESH" => proxy_builder = proxy_builder.r#type(aws_sdk_ecs::types::ProxyConfigurationType::Appmesh),
                _ => {}
            }
        }

        if !proxy_config.properties.is_empty() {
            let mut properties = Vec::new();

            for prop in &proxy_config.properties {
                let mut prop_builder = aws_sdk_ecs::types::KeyValuePair::builder();

                if let Some(name) = &prop.name {
                    prop_builder = prop_builder.name(name);
                }

                if let Some(value) = &prop.value {
                    prop_builder = prop_builder.value(value);
                }

                properties.push(prop_builder.build());
            }

            if !properties.is_empty() {
                proxy_builder = proxy_builder.set_properties(Some(properties));
            }
        }

        register_task_def = register_task_def.proxy_configuration(proxy_builder.build()?);
    }

    // Set runtime platform if specified
    if let Some(runtime_platform) = &task_definition.runtime_platform {
        let mut runtime_builder = aws_sdk_ecs::types::RuntimePlatform::builder();

        if let Some(cpu_architecture) = &runtime_platform.cpu_architecture {
            match cpu_architecture.as_str() {
                "X86_64" => runtime_builder = runtime_builder.cpu_architecture(aws_sdk_ecs::types::CpuArchitecture::X8664),
                "ARM64" => runtime_builder = runtime_builder.cpu_architecture(aws_sdk_ecs::types::CpuArchitecture::Arm64),
                _ => {}
            }
        }

        if let Some(operating_system_family) = &runtime_platform.operating_system_family {
            runtime_builder = runtime_builder.operating_system_family(aws_sdk_ecs::types::OsFamily::from_str(&operating_system_family).context("Parsing operating_system_family")?);
        }

        register_task_def = register_task_def.runtime_platform(runtime_builder.build());
    }

    // Register the task definition
    let resp = register_task_def.send().await?;
    let task_def = resp
        .task_definition
        .context("No task definition returned from register_task_definition")?;
    let task_def_arn = task_def.task_definition_arn.context("No task definition ARN returned")?;

    let mut outputs = HashMap::new();
    outputs.insert(String::from("arn"), Some(task_def_arn.clone()));

    Ok(OpExecResponse {
        outputs: Some(outputs),
        friendly_message: Some(format!("Registered task definition {task_def_arn}")),
    })
}

/// Updates tags for a task definition
pub async fn update_task_definition_tags(
    client: &Client,
    task_definition_arn: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecResponse, anyhow::Error> {
    // Use tag_diff utility to determine tags to add and remove
    let (tag_keys_to_remove, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Add tags if needed
    if !tags_to_add.is_empty() {
        client
            .tag_resource()
            .resource_arn(task_definition_arn)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    // Remove tags if needed
    if !tag_keys_to_remove.is_empty() {
        client
            .untag_resource()
            .resource_arn(task_definition_arn)
            .set_tag_keys(Some(tag_keys_to_remove))
            .send()
            .await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated tags for task definition {task_definition_arn}")),
    })
}

/// Deregisters a task definition
pub async fn deregister_task_definition(client: &Client, task_definition: &str) -> Result<OpExecResponse, anyhow::Error> {
    client
        .deregister_task_definition()
        .task_definition(task_definition)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Deregistered task definition {task_definition}")),
    })
}

// Task Operations

/// Runs a task with specified configuration
pub async fn run_task(
    client: &Client,
    cluster: &str,
    task_definition: &str,
    count: i32,
    launch_type: Option<String>,
    platform_version: Option<String>,
    network_configuration: Option<NetworkConfigurationRequest>,
    overrides: Option<OpTaskOverride>,
    tags: &Tags,
) -> Result<OpExecResponse, anyhow::Error> {
    let mut run_task = client
        .run_task()
        .cluster(cluster)
        .task_definition(task_definition)
        .count(count);

    // Set launch type if specified
    if let Some(launch_type_str) = launch_type {
        match launch_type_str.as_str() {
            "EC2" => run_task = run_task.launch_type(aws_sdk_ecs::types::LaunchType::Ec2),
            "FARGATE" => run_task = run_task.launch_type(aws_sdk_ecs::types::LaunchType::Fargate),
            "EXTERNAL" => run_task = run_task.launch_type(aws_sdk_ecs::types::LaunchType::External),
            _ => {}
        }
    }

    // Set platform version if specified
    if let Some(platform_version) = platform_version {
        run_task = run_task.platform_version(platform_version);
    }

    // Set network configuration if specified
    if let Some(network_config) = network_configuration {
        let mut builder = aws_sdk_ecs::types::AwsVpcConfiguration::builder()
            .set_subnets(Some(network_config.subnets))
            .set_security_groups(Some(network_config.security_groups));

        if let Some(assign_public_ip) = network_config.assign_public_ip {
            builder = builder.assign_public_ip(if assign_public_ip {
                aws_sdk_ecs::types::AssignPublicIp::Enabled
            } else {
                aws_sdk_ecs::types::AssignPublicIp::Disabled
            });
        }

        if let Ok(vpc_config) = builder.build() {
            let network_config = NetworkConfiguration::builder().awsvpc_configuration(vpc_config).build();

            run_task = run_task.network_configuration(network_config);
        }
    }

    // Set overrides if specified
    if let Some(override_config) = overrides {
        let mut task_override_builder = TaskOverride::builder();

        // Set container overrides
        if !override_config.container_overrides.is_empty() {
            let mut container_overrides = Vec::new();

            for container in &override_config.container_overrides {
                let mut builder = aws_sdk_ecs::types::ContainerOverride::builder().name(&container.name);

                // Set command if specified
                if let Some(command) = &container.command {
                    builder = builder.set_command(Some(command.clone()));
                }

                // Set environment if specified
                if !container.environment.is_empty() {
                    let mut env_vars = Vec::new();

                    for (name, value) in &container.environment {
                        let kv = KeyValuePair::builder().name(name).value(value).build();

                        env_vars.push(kv);
                    }

                    builder = builder.set_environment(Some(env_vars));
                }

                // Set CPU if specified
                if let Some(cpu) = container.cpu {
                    builder = builder.cpu(cpu);
                }

                // Set memory if specified
                if let Some(memory) = container.memory {
                    builder = builder.memory(memory);
                }

                // Set memory reservation if specified
                if let Some(memory_reservation) = container.memory_reservation {
                    builder = builder.memory_reservation(memory_reservation);
                }

                container_overrides.push(builder.build());
            }

            if !container_overrides.is_empty() {
                task_override_builder = task_override_builder.set_container_overrides(Some(container_overrides));
            }
        }

        // Set CPU if specified
        if let Some(cpu) = &override_config.cpu {
            task_override_builder = task_override_builder.cpu(cpu);
        }

        // Set memory if specified
        if let Some(memory) = &override_config.memory {
            task_override_builder = task_override_builder.memory(memory);
        }

        // Set execution role ARN if specified
        if let Some(execution_role_arn) = &override_config.execution_role_arn {
            task_override_builder = task_override_builder.execution_role_arn(execution_role_arn);
        }

        // Set task role ARN if specified
        if let Some(task_role_arn) = &override_config.task_role_arn {
            task_override_builder = task_override_builder.task_role_arn(task_role_arn);
        }

        run_task = run_task.overrides(task_override_builder.build());
    }

    // Apply tags
    let aws_tags: Option<Vec<Tag>> = tags.clone().into();

    if let Some(tag_list) = aws_tags
        && !tag_list.is_empty()
    {
        run_task = run_task.set_tags(Some(tag_list));
    }

    // Run the task
    let resp = run_task.send().await?;
    let tasks = resp.tasks.context("No tasks returned from run_task")?;

    let mut outputs = HashMap::new();
    let mut task_arns = Vec::new();

    for (i, task) in tasks.iter().enumerate() {
        if let Some(task_arn) = &task.task_arn {
            task_arns.push(task_arn.clone());
            outputs.insert(format!("task_arn_{i}"), Some(task_arn.clone()));
        }
    }

    let task_count = tasks.len();
    let task_message = if task_count == 1 {
        format!("Started 1 task in cluster {cluster}")
    } else {
        format!("Started {task_count} tasks in cluster {cluster}")
    };

    Ok(OpExecResponse {
        outputs: Some(outputs),
        friendly_message: Some(task_message),
    })
}

/// Stops a task
pub async fn stop_task(
    client: &Client,
    cluster: &str,
    task_id: &str,
    reason: Option<String>,
) -> Result<OpExecResponse, anyhow::Error> {
    let mut stop_task = client.stop_task().cluster(cluster).task(task_id);

    if let Some(reason_str) = reason {
        stop_task = stop_task.reason(reason_str);
    }

    stop_task.send().await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Stopped task {task_id} in cluster {cluster}")),
    })
}

/// Updates tags for a task
pub async fn update_task_tags(
    client: &Client,
    task_arn: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecResponse, anyhow::Error> {
    // Use tag_diff utility to determine tags to add and remove
    let (tag_keys_to_remove, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Add tags if needed
    if !tags_to_add.is_empty() {
        client
            .tag_resource()
            .resource_arn(task_arn)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    // Remove tags if needed
    if !tag_keys_to_remove.is_empty() {
        client
            .untag_resource()
            .resource_arn(task_arn)
            .set_tag_keys(Some(tag_keys_to_remove))
            .send()
            .await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated tags for task {task_arn}")),
    })
}

// ContainerInstance Operations

/// Registers a container instance
pub async fn register_container_instance(
    client: &Client,
    cluster: &str,
    instance_identity_document: &str,
    attributes: Vec<(String, Option<String>)>,
    tags: &Tags,
) -> Result<OpExecResponse, anyhow::Error> {
    let mut register_container_instance = client
        .register_container_instance()
        .cluster(cluster)
        .instance_identity_document(instance_identity_document);

    // Set attributes if specified
    if !attributes.is_empty() {
        let mut container_attributes = Vec::new();

        for (name, value) in &attributes {
            let mut builder = aws_sdk_ecs::types::Attribute::builder().name(name);

            if let Some(val) = value {
                builder = builder.value(val);
            }

            if let Ok(attribute) = builder.build() {
                container_attributes.push(attribute);
            }
        }

        if !container_attributes.is_empty() {
            register_container_instance = register_container_instance.set_attributes(Some(container_attributes));
        }
    }

    // Apply tags
    let aws_tags: Option<Vec<Tag>> = tags.clone().into();

    if let Some(tag_list) = aws_tags
        && !tag_list.is_empty()
    {
        register_container_instance = register_container_instance.set_tags(Some(tag_list));
    }

    // Register the container instance
    let resp = register_container_instance.send().await?;
    let container_instance = resp
        .container_instance
        .context("No container instance returned from register_container_instance")?;
    let container_instance_arn = container_instance
        .container_instance_arn
        .context("No container instance ARN returned")?;

    let mut outputs = HashMap::new();
    outputs.insert(String::from("arn"), Some(container_instance_arn.clone()));

    Ok(OpExecResponse {
        outputs: Some(outputs),
        friendly_message: Some(format!("Registered container instance in cluster {cluster}")),
    })
}

/// Updates container instance attributes
pub async fn update_container_instance_attributes(
    client: &Client,
    cluster: &str,
    container_instance_id: &str,
    attributes: Vec<(String, Option<String>)>,
    remove_attributes: Vec<String>,
) -> Result<OpExecResponse, anyhow::Error> {
    let mut put_attributes = client.put_attributes().cluster(cluster);

    // Set attributes to add/update
    if !attributes.is_empty() {
        let mut container_attributes = Vec::new();

        for (name, value) in &attributes {
            let mut builder = aws_sdk_ecs::types::Attribute::builder()
                .name(name)
                .target_id(container_instance_id);

            // Set target type to container-instance
            builder = builder.target_type(aws_sdk_ecs::types::TargetType::ContainerInstance);

            if let Some(val) = value {
                builder = builder.value(val);
            }

            if let Ok(attribute) = builder.build() {
                container_attributes.push(attribute);
            }
        }

        if !container_attributes.is_empty() {
            put_attributes = put_attributes.set_attributes(Some(container_attributes));
        }
    }

    // Process attributes to remove
    if !remove_attributes.is_empty() {
        let mut to_remove = Vec::new();

        for name in &remove_attributes {
            let builder = aws_sdk_ecs::types::Attribute::builder()
                .name(name)
                .target_id(container_instance_id)
                .target_type(aws_sdk_ecs::types::TargetType::ContainerInstance);

            if let Ok(attribute) = builder.build() {
                to_remove.push(attribute);
            }
        }

        if !to_remove.is_empty() {
            // Use a separate call for deleting attributes if needed
            client
                .put_attributes()
                .cluster(cluster)
                .set_attributes(Some(to_remove))
                .send()
                .await?;
        }
    }

    if !attributes.is_empty() {
        put_attributes.send().await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated attributes for container instance {container_instance_id} in cluster {cluster}"
        )),
    })
}

/// Updates tags for a container instance
pub async fn update_container_instance_tags(
    client: &Client,
    container_instance_arn: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecResponse, anyhow::Error> {
    // Use tag_diff utility to determine tags to add and remove
    let (tag_keys_to_remove, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Add tags if needed
    if !tags_to_add.is_empty() {
        client
            .tag_resource()
            .resource_arn(container_instance_arn)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    // Remove tags if needed
    if !tag_keys_to_remove.is_empty() {
        client
            .untag_resource()
            .resource_arn(container_instance_arn)
            .set_tag_keys(Some(tag_keys_to_remove))
            .send()
            .await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated tags for container instance {container_instance_arn}")),
    })
}

/// Deregisters a container instance
pub async fn deregister_container_instance(
    client: &Client,
    cluster: &str,
    container_instance_id: &str,
    force: bool,
) -> Result<OpExecResponse, anyhow::Error> {
    client
        .deregister_container_instance()
        .cluster(cluster)
        .container_instance(container_instance_id)
        .force(force)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Deregistered container instance {container_instance_id} from cluster {cluster}"
        )),
    })
}
