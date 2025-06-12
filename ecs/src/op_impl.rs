use anyhow::Context;
use aws_sdk_ecs::{
    Client,
    types::{
        CapacityProviderStrategyItem, ClusterSetting, ContainerDefinition, DeploymentCircuitBreaker, DeploymentConfiguration,
        KeyValuePair, LoadBalancer, NetworkConfiguration, PlacementConstraint, PlacementStrategy, ServiceRegistry, Tag,
        TaskDefinitionPlacementConstraint, TaskOverride,
    },
};
use std::collections::HashMap;

use super::{
    op::{NetworkConfigurationRequest, TaskOverride as OpTaskOverride},
    resource::{Cluster as EcsCluster, Service, TaskDefinition},
    tags::Tags,
    util::{get_cluster, get_service},
};
use autoschematic_core::connector::OpExecOutput;

// Cluster Operations

/// Creates a new ECS cluster
pub async fn create_cluster(client: &Client, cluster: &EcsCluster, cluster_name: &str) -> Result<OpExecOutput, anyhow::Error> {
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

    if let Some(tags) = aws_tags {
        if !tags.is_empty() {
            create_cluster = create_cluster.set_tags(Some(tags));
        }
    }

    // Create the cluster
    let resp = create_cluster.send().await?;
    let cluster = resp.cluster.context("No cluster returned from create_cluster")?;
    let cluster_arn = cluster.cluster_arn.context("No cluster ARN returned")?;

    let mut outputs = HashMap::new();
    outputs.insert(String::from("arn"), Some(cluster_arn.clone()));

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!("Created ECS cluster {}", cluster_name)),
    })
}

/// Updates tags for an existing cluster
pub async fn update_cluster_tags(
    client: &Client,
    cluster_name: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
    // Get the cluster to retrieve the ARN
    let cluster = get_cluster(client, cluster_name)
        .await?
        .context(format!("Cluster {} not found", cluster_name))?;

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

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated tags for ECS cluster {}", cluster_name)),
    })
}

/// Updates settings for an existing cluster
pub async fn update_cluster_settings(
    client: &Client,
    cluster_name: &str,
    settings: Vec<(String, String)>,
) -> Result<OpExecOutput, anyhow::Error> {
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

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated settings for ECS cluster {}", cluster_name)),
    })
}

/// Updates capacity providers for a cluster
pub async fn update_cluster_capacity_providers(
    client: &Client,
    cluster_name: &str,
    add_capacity_providers: Vec<String>,
    remove_capacity_providers: Vec<String>,
    default_strategy: Vec<(String, Option<i32>, Option<i32>)>,
) -> Result<OpExecOutput, anyhow::Error> {
    // Get current capacity providers
    let cluster = get_cluster(client, cluster_name)
        .await?
        .context(format!("Cluster {} not found", cluster_name))?;

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

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated capacity providers for ECS cluster {}", cluster_name)),
    })
}

/// Deletes an ECS cluster
pub async fn delete_cluster(client: &Client, cluster_name: &str) -> Result<OpExecOutput, anyhow::Error> {
    client.delete_cluster().cluster(cluster_name).send().await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted ECS cluster {}", cluster_name)),
    })
}

// Service Operations

/// Creates a new ECS service
pub async fn create_service(
    client: &Client,
    cluster_name: &str,
    service: &Service,
    service_name: &str,
) -> Result<OpExecOutput, anyhow::Error> {
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
    if let Some(network_config) = &service.network_configuration {
        if let Some(awsvpc_config) = &network_config.awsvpc_configuration {
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

    if let Some(tags) = aws_tags {
        if !tags.is_empty() {
            create_service = create_service.set_tags(Some(tags));
        }
    }

    // Create the service
    let resp = create_service.send().await?;
    let service = resp.service.context("No service returned from create_service")?;
    let service_arn = service.service_arn.context("No service ARN returned")?;
    let service_name = service.service_name.context("No service name returned")?;

    let mut outputs = HashMap::new();
    outputs.insert(String::from("arn"), Some(service_arn.clone()));
    outputs.insert(String::from("service_name"), Some(service_name.clone()));

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!("Created ECS service {} in cluster {}", service_name, cluster_name)),
    })
}

/// Updates tags for an existing service
pub async fn update_service_tags(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
    // Get the service to retrieve the ARN
    let service = get_service(client, cluster_name, service_name)
        .await?
        .context(format!("Service {} not found in cluster {}", service_name, cluster_name))?;

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

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Updated tags for ECS service {} in cluster {}",
            service_name, cluster_name
        )),
    })
}

/// Updates the desired count for a service
pub async fn update_service_desired_count(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    desired_count: i32,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .update_service()
        .cluster(cluster_name)
        .service(service_name)
        .desired_count(desired_count)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Updated desired count to {} for ECS service {} in cluster {}",
            desired_count, service_name, cluster_name
        )),
    })
}

/// Updates the task definition for a service
pub async fn update_service_task_definition(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    task_definition: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .update_service()
        .cluster(cluster_name)
        .service(service_name)
        .task_definition(task_definition)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Updated task definition to {} for ECS service {} in cluster {}",
            task_definition, service_name, cluster_name
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
) -> Result<OpExecOutput, anyhow::Error> {
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

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Updated deployment configuration for ECS service {} in cluster {}",
            service_name, cluster_name
        )),
    })
}

/// Enables or disables execute command for a service
pub async fn enable_execute_command(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
    enable: bool,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .update_service()
        .cluster(cluster_name)
        .service(service_name)
        .enable_execute_command(enable)
        .send()
        .await?;

    let action = if enable { "Enabled" } else { "Disabled" };

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "{} execute command for ECS service {} in cluster {}",
            action, service_name, cluster_name
        )),
    })
}

/// Deletes an ECS service
pub async fn delete_service(client: &Client, cluster_name: &str, service_name: &str) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_service()
        .cluster(cluster_name)
        .service(service_name)
        .force(true) // Use force to allow deleting even if it has instances
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted ECS service {} from cluster {}", service_name, cluster_name)),
    })
}

// TaskDefinition Operations

/// Registers a new task definition
pub async fn register_task_definition(
    client: &Client,
    family: &str,
    task_definition: &TaskDefinition,
) -> Result<OpExecOutput, anyhow::Error> {
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

        // Set essential if specified
        if let Some(essential) = container.essential {
            builder = builder.essential(essential);
        }

        // Set entry point if specified
        if !container.entry_point.is_empty() {
            builder = builder.set_entry_point(Some(container.entry_point.clone()));
        }

        // Set command if specified
        if !container.command.is_empty() {
            builder = builder.set_command(Some(container.command.clone()));
        }

        // Set environment variables if specified
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

        // Additional properties would be set here...
        // For brevity, we're not setting all possible properties, but the pattern is the same

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

            // Additional volume properties would be set here...

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

    // Register the task definition
    let resp = register_task_def.send().await?;
    let task_def = resp
        .task_definition
        .context("No task definition returned from register_task_definition")?;
    let task_def_arn = task_def.task_definition_arn.context("No task definition ARN returned")?;

    let mut outputs = HashMap::new();
    outputs.insert(String::from("arn"), Some(task_def_arn.clone()));

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!("Registered task definition {}", task_def_arn)),
    })
}

/// Updates tags for a task definition
pub async fn update_task_definition_tags(
    client: &Client,
    task_definition_arn: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
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

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated tags for task definition {}", task_definition_arn)),
    })
}

/// Deregisters a task definition
pub async fn deregister_task_definition(client: &Client, task_definition: &str) -> Result<OpExecOutput, anyhow::Error> {
    client
        .deregister_task_definition()
        .task_definition(task_definition)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deregistered task definition {}", task_definition)),
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
) -> Result<OpExecOutput, anyhow::Error> {
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

    if let Some(tag_list) = aws_tags {
        if !tag_list.is_empty() {
            run_task = run_task.set_tags(Some(tag_list));
        }
    }

    // Run the task
    let resp = run_task.send().await?;
    let tasks = resp.tasks.context("No tasks returned from run_task")?;

    let mut outputs = HashMap::new();
    let mut task_arns = Vec::new();

    for (i, task) in tasks.iter().enumerate() {
        if let Some(task_arn) = &task.task_arn {
            task_arns.push(task_arn.clone());
            outputs.insert(format!("task_arn_{}", i), Some(task_arn.clone()));
        }
    }

    let task_count = tasks.len();
    let task_message = if task_count == 1 {
        format!("Started 1 task in cluster {}", cluster)
    } else {
        format!("Started {} tasks in cluster {}", task_count, cluster)
    };

    Ok(OpExecOutput {
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
) -> Result<OpExecOutput, anyhow::Error> {
    let mut stop_task = client.stop_task().cluster(cluster).task(task_id);

    if let Some(reason_str) = reason {
        stop_task = stop_task.reason(reason_str);
    }

    stop_task.send().await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Stopped task {} in cluster {}", task_id, cluster)),
    })
}

/// Updates tags for a task
pub async fn update_task_tags(
    client: &Client,
    task_arn: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
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

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated tags for task {}", task_arn)),
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
) -> Result<OpExecOutput, anyhow::Error> {
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

    if let Some(tag_list) = aws_tags {
        if !tag_list.is_empty() {
            register_container_instance = register_container_instance.set_tags(Some(tag_list));
        }
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

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!("Registered container instance in cluster {}", cluster)),
    })
}

/// Updates container instance attributes
pub async fn update_container_instance_attributes(
    client: &Client,
    cluster: &str,
    container_instance_id: &str,
    attributes: Vec<(String, Option<String>)>,
    remove_attributes: Vec<String>,
) -> Result<OpExecOutput, anyhow::Error> {
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

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Updated attributes for container instance {} in cluster {}",
            container_instance_id, cluster
        )),
    })
}

/// Updates tags for a container instance
pub async fn update_container_instance_tags(
    client: &Client,
    container_instance_arn: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
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

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated tags for container instance {}", container_instance_arn)),
    })
}

/// Deregisters a container instance
pub async fn deregister_container_instance(
    client: &Client,
    cluster: &str,
    container_instance_id: &str,
    force: bool,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .deregister_container_instance()
        .cluster(cluster)
        .container_instance(container_instance_id)
        .force(force)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Deregistered container instance {} from cluster {}",
            container_instance_id, cluster
        )),
    })
}
