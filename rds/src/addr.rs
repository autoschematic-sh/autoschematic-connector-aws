use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum RdsResourceAddress {
    DBInstance { region: String, id: String },
    DBCluster { region: String, id: String },
    DBSubnetGroup { region: String, name: String },
    DBParameterGroup { region: String, name: String },
}

impl ResourceAddress for RdsResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            RdsResourceAddress::DBInstance { region, id } => PathBuf::from(format!("aws/rds/{}/instances/{}.ron", region, id)),
            RdsResourceAddress::DBCluster { region, id } => PathBuf::from(format!("aws/rds/{}/clusters/{}.ron", region, id)),
            RdsResourceAddress::DBSubnetGroup { region, name } => {
                PathBuf::from(format!("aws/rds/{}/subnet-groups/{}.ron", region, name))
            }
            RdsResourceAddress::DBParameterGroup { region, name } => {
                PathBuf::from(format!("aws/rds/{}/parameter-groups/{}.ron", region, name))
            }
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "rds", region, "instances", instance_id] if instance_id.ends_with(".ron") => {
                let instance_id = instance_id.strip_suffix(".ron").unwrap().to_string();
                Ok(RdsResourceAddress::DBInstance {
                    region: region.to_string(),
                    id: instance_id,
                })
            }
            ["aws", "rds", region, "clusters", cluster_id] if cluster_id.ends_with(".ron") => {
                let cluster_id = cluster_id.strip_suffix(".ron").unwrap().to_string();
                Ok(RdsResourceAddress::DBCluster {
                    region: region.to_string(),
                    id: cluster_id,
                })
            }
            ["aws", "rds", region, "subnet-groups", group_name] if group_name.ends_with(".ron") => {
                let group_name = group_name.strip_suffix(".ron").unwrap().to_string();
                Ok(RdsResourceAddress::DBSubnetGroup {
                    region: region.to_string(),
                    name: group_name,
                })
            }
            ["aws", "rds", region, "parameter-groups", group_name] if group_name.ends_with(".ron") => {
                let group_name = group_name.strip_suffix(".ron").unwrap().to_string();
                Ok(RdsResourceAddress::DBParameterGroup {
                    region: region.to_string(),
                    name: group_name,
                })
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
