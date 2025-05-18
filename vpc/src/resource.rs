use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;

use anyhow::bail;
use autoschematic_core::connector::{Resource, ResourceAddress};
use autoschematic_core::util::{PrettyConfig, RON};
use serde::{Deserialize, Serialize};

use super::{addr::VpcResourceAddress, tags::Tags};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Vpc {
    pub cidr_block: String,
    pub instance_tenancy: String,
    pub enable_dns_support: bool,
    pub enable_dns_hostnames: bool,
    pub tags: Tags,
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
    pub tags: Tags,
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
    fn to_os_string(&self) -> Result<OsString, anyhow::Error> {
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

    fn from_os_str(addr: &impl ResourceAddress, s: &OsStr) -> Result<Option<Self>, anyhow::Error>
    where
        Self: Sized,
    {
        let Some(addr) = VpcResourceAddress::from_path(&addr.to_path_buf())? else {
            return Ok(None);
        };
        let s = str::from_utf8(s.as_bytes())?;
        match addr {
            VpcResourceAddress::Vpc(_region, _vpc_id) => {
                return Ok(Some(VpcResource::Vpc(RON.from_str(s)?)));
            }
            VpcResourceAddress::Subnet(_region, _vpc_id, _subnet_id) => {
                return Ok(Some(VpcResource::Subnet(RON.from_str(s)?)));
            }
            VpcResourceAddress::InternetGateway(_region, _igw_id) => {
                return Ok(Some(VpcResource::InternetGateway(RON.from_str(s)?)));
            }
            VpcResourceAddress::RouteTable(_region, _vpc_id, _rt_id) => {
                return Ok(Some(VpcResource::RouteTable(RON.from_str(s)?)));
            }
            VpcResourceAddress::SecurityGroup(_region, _vpc_id, _sg_id) => {
                return Ok(Some(VpcResource::SecurityGroup(RON.from_str(s)?)));
            }
        }
    }
}
