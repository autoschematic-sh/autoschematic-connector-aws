use anyhow::Context;
use aws_sdk_ecs::Client;

/// Gets a cluster by name
pub async fn get_cluster(
    client: &Client,
    cluster_name: &str,
) -> Result<Option<aws_sdk_ecs::types::Cluster>, anyhow::Error> {
    let resp = client
        .describe_clusters()
        .clusters(cluster_name)
        .send()
        .await?;

    let clusters = resp.clusters.context("Failed to get clusters")?;

    if clusters.is_empty() {
        return Ok(None);
    }

    Ok(Some(clusters[0].clone()))
}

/// Gets a service by name in a specific cluster
pub async fn get_service(
    client: &Client,
    cluster_name: &str,
    service_name: &str,
) -> Result<Option<aws_sdk_ecs::types::Service>, anyhow::Error> {
    let resp = client
        .describe_services()
        .cluster(cluster_name)
        .services(service_name)
        .send()
        .await?;

    let services = resp.services.context("Failed to get services")?;

    if services.is_empty() {
        return Ok(None);
    }

    Ok(Some(services[0].clone()))
}

/// Gets a task definition by ARN or family:revision
pub async fn get_task_definition(
    client: &Client,
    task_definition: &str,
) -> Result<Option<aws_sdk_ecs::types::TaskDefinition>, anyhow::Error> {
    let resp = client
        .describe_task_definition()
        .task_definition(task_definition)
        .send()
        .await?;

    let task_def = resp.task_definition.context("Failed to get task definition")?;

    Ok(Some(task_def))
}

/// Gets a task by ID in a specific cluster
pub async fn get_task(
    client: &Client,
    cluster_name: &str,
    task_id: &str,
) -> Result<Option<aws_sdk_ecs::types::Task>, anyhow::Error> {
    let resp = client
        .describe_tasks()
        .cluster(cluster_name)
        .tasks(task_id)
        .send()
        .await?;

    let tasks = resp.tasks.context("Failed to get tasks")?;

    if tasks.is_empty() {
        return Ok(None);
    }

    Ok(Some(tasks[0].clone()))
}

/// Gets a container instance by ID in a specific cluster
pub async fn get_container_instance(
    client: &Client,
    cluster_name: &str,
    container_instance_id: &str,
) -> Result<Option<aws_sdk_ecs::types::ContainerInstance>, anyhow::Error> {
    let resp = client
        .describe_container_instances()
        .cluster(cluster_name)
        .container_instances(container_instance_id)
        .send()
        .await?;

    let container_instances = resp.container_instances.context("Failed to get container instances")?;

    if container_instances.is_empty() {
        return Ok(None);
    }

    Ok(Some(container_instances[0].clone()))
}
