use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

type Region = String;
type LoadBalancerName = String;
type ListenerId = String;
type TargetGroupName = String;

#[derive(Debug, Clone)]
pub enum ElbResourceAddress {
    LoadBalancer(Region, LoadBalancerName),                  // (region, load_balancer_name)
    TargetGroup(Region, TargetGroupName),                   // (region, target_group_name)
    Listener(Region, LoadBalancerName, ListenerId),          // (region, load_balancer_name, listener_id)
}

impl ResourceAddress for ElbResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            ElbResourceAddress::LoadBalancer(region, name) => {
                PathBuf::from(format!("aws/elb/{}/load_balancers/{}.ron", region, name))
            }
            ElbResourceAddress::TargetGroup(region, name) => {
                PathBuf::from(format!("aws/elb/{}/target_groups/{}.ron", region, name))
            }
            ElbResourceAddress::Listener(region, lb_name, listener_id) => PathBuf::from(format!(
                "aws/elb/{}/load_balancers/{}/listeners/{}.ron",
                region, lb_name, listener_id
            )),
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path
            .components()
            .into_iter()
            .map(|s| s.as_os_str().to_str().unwrap())
            .collect();

        match &path_components[..] {
            ["aws", "elb", region, "load_balancers", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(ElbResourceAddress::LoadBalancer(
                    region.to_string(),
                    name,
                ))
            }
            ["aws", "elb", region, "target_groups", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(ElbResourceAddress::TargetGroup(
                    region.to_string(),
                    name,
                ))
            }
            ["aws", "elb", region, "load_balancers", lb_name, "listeners", listener_id]
                if listener_id.ends_with(".ron") =>
            {
                let listener_id = listener_id.strip_suffix(".ron").unwrap().to_string();
                Ok(ElbResourceAddress::Listener(
                    region.to_string(),
                    lb_name.to_string(),
                    listener_id,
                ))
            }
            _ => Err(invalid_addr_path(path))
        }
    }
}
