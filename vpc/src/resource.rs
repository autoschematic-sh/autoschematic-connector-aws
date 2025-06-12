
use autoschematic_core::connector::{Resource, ResourceAddress};
use autoschematic_core::util::{PrettyConfig, RON};
use serde::{Deserialize, Serialize};

use super::{addr::VpcResourceAddress, tags::Tags};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Vpc {
    // #need(plan, Vpc.cidr_block)
    pub cidr_block: String,
    // #need(plan, Vpc.instance_tenancy)
    pub instance_tenancy: Option<String>,
    // #need(plan, Vpc.enable_dns_support)
    pub enable_dns_support: bool,
    // #need(plan, Vpc.dhcpOptionsId)
    pub dhcp_options_id: Option<String>,
    // // #_need(plan, Vpc.cidr_block_association_set)
    // pub cidr_block_association_set: Option<HashSet<String>>,
    // // #_need(plan, Vpc.ipv6_cidr_block_association_set)
    // pub ipv6_cidr_block_association_set: Option<HashSet<String>>,
    // #need(plan, Vpc.enable_dns_hostnames)
    pub enable_dns_hostnames: bool,
    // #need(plan, Vpc.tags)
    pub tags: Tags,
}

pub enum CidrBlockAssociation {
    Ipv6AmazonProvided { border_group: Option<String> },
    Ipv4IpamPool { id: String, netmask_length: i32 },
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Subnet {
    pub cidr_block: String,
    pub availability_zone: String,
    pub map_public_ip_on_launch: bool,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct InternetGateway {
    pub vpc_id: Option<String>,
    pub tags:   Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct RouteTable {
    pub routes: Vec<Route>,
    pub associations: Vec<String>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Route {
    pub destination_cidr_block: Option<String>,
    pub destination_ipv6_cidr_block: Option<String>,
    pub gateway_id: Option<String>,
    pub instance_id: Option<String>,
    pub nat_gateway_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SecurityGroup {
    pub description: String,
    pub ingress_rules: Vec<SecurityGroupRule>,
    pub egress_rules: Vec<SecurityGroupRule>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SecurityGroupRule {
    pub protocol: String,
    pub from_port: Option<i32>,
    pub to_port: Option<i32>,
    pub cidr_blocks: Vec<String>,
    pub security_group_ids: Vec<String>,
}

pub enum VpcResource {
    Vpc(Vpc),
    Subnet(Subnet),
    InternetGateway(InternetGateway),
    RouteTable(RouteTable),
    SecurityGroup(SecurityGroup),
}

impl Resource for VpcResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        // .extensions(ron::extensions::Extensions::IMPLICIT_SOME);
        match self {
            VpcResource::Vpc(vpc) => match RON.to_string_pretty(&vpc, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            VpcResource::Subnet(subnet) => match RON.to_string_pretty(&subnet, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            VpcResource::InternetGateway(igw) => match RON.to_string_pretty(&igw, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            VpcResource::RouteTable(rt) => match RON.to_string_pretty(&rt, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            VpcResource::SecurityGroup(sg) => match RON.to_string_pretty(&sg, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = VpcResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;
        match addr {
            VpcResourceAddress::Vpc { region, vpc_id } => Ok(VpcResource::Vpc(RON.from_str(s)?)),
            VpcResourceAddress::Subnet {
                region,
                vpc_id,
                subnet_id,
            } => Ok(VpcResource::Subnet(RON.from_str(s)?)),
            VpcResourceAddress::InternetGateway { region, igw_id } => Ok(VpcResource::InternetGateway(RON.from_str(s)?)),
            VpcResourceAddress::RouteTable { region, vpc_id, rt_id } => Ok(VpcResource::RouteTable(RON.from_str(s)?)),
            VpcResourceAddress::SecurityGroup { region, vpc_id, sg_id } => Ok(VpcResource::SecurityGroup(RON.from_str(s)?)),
        }
    }
}
