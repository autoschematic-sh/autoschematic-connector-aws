use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum EcsResourceAddress {
    Cluster(String, String),                   // (region, cluster_name)
    Service(String, String, String),           // (region, cluster_name, service_name)
    TaskDefinition(String, String),            // (region, task_family:revision)
    Task(String, String, String),              // (region, cluster_name, task_id)
    ContainerInstance(String, String, String), // (region, cluster_name, container_instance_id)
}

impl ResourceAddress for EcsResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            EcsResourceAddress::Cluster(region, cluster_name) => {
                PathBuf::from(format!("aws/ecs/{}/clusters/{}.ron", region, cluster_name))
            }
            EcsResourceAddress::Service(region, cluster_name, service_name) => PathBuf::from(format!(
                "aws/ecs/{}/clusters/{}/services/{}.ron",
                region, cluster_name, service_name
            )),
            EcsResourceAddress::TaskDefinition(region, task_def_id) => {
                PathBuf::from(format!("aws/ecs/{}/task_definitions/{}.ron", region, task_def_id))
            }
            EcsResourceAddress::Task(region, cluster_name, task_id) => {
                PathBuf::from(format!("aws/ecs/{}/clusters/{}/tasks/{}.ron", region, cluster_name, task_id))
            }
            EcsResourceAddress::ContainerInstance(region, cluster_name, container_instance_id) => PathBuf::from(format!(
                "aws/ecs/{}/clusters/{}/container_instances/{}.ron",
                region, cluster_name, container_instance_id
            )),
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "ecs", region, "clusters", cluster_name] if cluster_name.ends_with(".ron") => {
                let cluster_name = cluster_name.strip_suffix(".ron").unwrap().to_string();
                Ok(EcsResourceAddress::Cluster(region.to_string(), cluster_name))
            }
            ["aws", "ecs", region, "clusters", cluster_name, "services", service_name] if service_name.ends_with(".ron") => {
                let service_name = service_name.strip_suffix(".ron").unwrap().to_string();
                Ok(EcsResourceAddress::Service(
                    region.to_string(),
                    cluster_name.to_string(),
                    service_name,
                ))
            }
            ["aws", "ecs", region, "task_definitions", task_def_id] if task_def_id.ends_with(".ron") => {
                let task_def_id = task_def_id.strip_suffix(".ron").unwrap().to_string();
                Ok(EcsResourceAddress::TaskDefinition(region.to_string(), task_def_id))
            }
            ["aws", "ecs", region, "clusters", cluster_name, "tasks", task_id] if task_id.ends_with(".ron") => {
                let task_id = task_id.strip_suffix(".ron").unwrap().to_string();
                Ok(EcsResourceAddress::Task(
                    region.to_string(),
                    cluster_name.to_string(),
                    task_id,
                ))
            }
            [
                "aws",
                "ecs",
                region,
                "clusters",
                cluster_name,
                "container_instances",
                container_instance_id,
            ] if container_instance_id.ends_with(".ron") => {
                let container_instance_id = container_instance_id.strip_suffix(".ron").unwrap().to_string();
                Ok(EcsResourceAddress::ContainerInstance(
                    region.to_string(),
                    cluster_name.to_string(),
                    container_instance_id,
                ))
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
