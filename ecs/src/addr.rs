use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum EcsResourceAddress {
    Cluster(String, String),         // (region, cluster_name)
    Service(String, String, String), // (region, cluster_name, service_name)
    TaskDefinition(String, String),  // (region, task_family:revision)
}

impl ResourceAddress for EcsResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            EcsResourceAddress::Cluster(region, cluster_name) => {
                PathBuf::from(format!("aws/ecs/{region}/clusters/{cluster_name}.ron"))
            }
            EcsResourceAddress::Service(region, cluster_name, service_name) => PathBuf::from(format!(
                "aws/ecs/{region}/clusters/{cluster_name}/services/{service_name}.ron"
            )),
            EcsResourceAddress::TaskDefinition(region, task_def_id) => {
                PathBuf::from(format!("aws/ecs/{region}/task_definitions/{task_def_id}.ron"))
            }
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
            _ => Err(invalid_addr_path(path)),
        }
    }
}
