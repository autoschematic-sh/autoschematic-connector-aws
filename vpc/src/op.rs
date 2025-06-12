use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};

use super::{
    resource::{InternetGateway, Route, RouteTable, SecurityGroup, SecurityGroupRule, Subnet, Vpc},
    tags::Tags,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum VpcConnectorOp {
    // VPC operations
    CreateVpc(Vpc),
    UpdateVpcTags(Tags, Tags),
    UpdateVpcCidrBlock(String),
    UpdateVpcInstanceTenancy(String),
    UpdateVpcAttributes {
        enable_dns_support: Option<bool>,
        enable_dns_hostnames: Option<bool>,
    },
    DeleteVpc,

    // Subnet operations
    CreateSubnet(Subnet),
    UpdateSubnetTags(Tags, Tags),
    UpdateSubnetAttributes {
        map_public_ip_on_launch: Option<bool>,
    },
    DeleteSubnet,

    // Internet Gateway operations
    CreateInternetGateway(InternetGateway),
    AttachInternetGateway {
        vpc_id: String,
    },
    DetachInternetGateway {
        vpc_id: String,
    },
    UpdateInternetGatewayTags(Tags, Tags),
    DeleteInternetGateway,

    // Route Table operations
    CreateRouteTable(RouteTable),
    UpdateRouteTableTags(Tags, Tags),
    CreateRoute(Route),
    DeleteRoute(Route),
    AssociateRouteTable {
        subnet_id: String,
    },
    DisassociateRouteTable {
        association_id: String,
    },
    DeleteRouteTable,

    // Security Group operations
    CreateSecurityGroup(SecurityGroup),
    UpdateSecurityGroupTags(Tags, Tags),
    AuthorizeSecurityGroupIngress(SecurityGroupRule),
    AuthorizeSecurityGroupEgress(SecurityGroupRule),
    RevokeSecurityGroupIngress(SecurityGroupRule),
    RevokeSecurityGroupEgress(SecurityGroupRule),
    DeleteSecurityGroup,
}

impl ConnectorOp for VpcConnectorOp {
    fn to_string(&self) -> Result<String, anyhow::Error> {
        Ok(RON.to_string(self)?)
    }

    fn from_str(s: &str) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(RON.from_str(s)?)
    }
}
