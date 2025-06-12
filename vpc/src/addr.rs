use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum VpcResourceAddress {
    Vpc {
        region: String,
        vpc_id: String,
    },
    Subnet {
        region:    String,
        vpc_id:    String,
        subnet_id: String,
    },
    InternetGateway {
        region: String,
        igw_id: String,
    },
    RouteTable {
        region: String,
        vpc_id: String,
        rt_id:  String,
    },
    SecurityGroup {
        region: String,
        vpc_id: String,
        sg_id:  String,
    },
}

impl ResourceAddress for VpcResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            VpcResourceAddress::Vpc { region, vpc_id } => PathBuf::from(format!("aws/vpc/{}/vpcs/{}.ron", region, vpc_id)),
            VpcResourceAddress::Subnet {
                region,
                vpc_id,
                subnet_id,
            } => PathBuf::from(format!("aws/vpc/{}/vpcs/{}/subnets/{}.ron", region, vpc_id, subnet_id)),
            VpcResourceAddress::InternetGateway { region, igw_id } => {
                PathBuf::from(format!("aws/vpc/{}/internet_gateways/{}.ron", region, igw_id))
            }
            VpcResourceAddress::RouteTable { region, vpc_id, rt_id } => {
                PathBuf::from(format!("aws/vpc/{}/vpcs/{}/route_tables/{}.ron", region, vpc_id, rt_id))
            }
            VpcResourceAddress::SecurityGroup { region, vpc_id, sg_id } => {
                PathBuf::from(format!("aws/vpc/{}/vpcs/{}/security_groups/{}.ron", region, vpc_id, sg_id))
            }
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "vpc", region, "vpcs", vpc_id] if vpc_id.ends_with(".ron") => {
                let vpc_id = vpc_id.strip_suffix(".ron").unwrap().to_string();
                Ok(VpcResourceAddress::Vpc {
                    region: region.to_string(),
                    vpc_id,
                })
            }
            ["aws", "vpc", region, "vpcs", vpc_id, "subnets", subnet_id] if subnet_id.ends_with(".ron") => {
                let subnet_id = subnet_id.strip_suffix(".ron").unwrap().to_string();
                Ok(VpcResourceAddress::Subnet {
                    region: region.to_string(),
                    vpc_id: vpc_id.to_string(),
                    subnet_id,
                })
            }
            ["aws", "vpc", region, "internet_gateways", igw_id] if igw_id.ends_with(".ron") => {
                let igw_id = igw_id.strip_suffix(".ron").unwrap().to_string();
                Ok(VpcResourceAddress::InternetGateway {
                    region: region.to_string(),
                    igw_id,
                })
            }
            ["aws", "vpc", region, "vpcs", vpc_id, "route_tables", rt_id] if rt_id.ends_with(".ron") => {
                let rt_id = rt_id.strip_suffix(".ron").unwrap().to_string();
                Ok(VpcResourceAddress::RouteTable {
                    region: region.to_string(),
                    vpc_id: vpc_id.to_string(),
                    rt_id,
                })
            }
            ["aws", "vpc", region, "vpcs", vpc_id, "security_groups", sg_id] if sg_id.ends_with(".ron") => {
                let sg_id = sg_id.strip_suffix(".ron").unwrap().to_string();
                Ok(VpcResourceAddress::SecurityGroup {
                    region: region.to_string(),
                    vpc_id: vpc_id.to_string(),
                    sg_id,
                })
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
