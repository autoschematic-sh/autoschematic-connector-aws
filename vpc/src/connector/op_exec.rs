use crate::addr::VpcResourceAddress;

use super::VpcConnector;

use std::path::Path;

use crate::{op::VpcConnectorOp, op_impl};
use autoschematic_core::{
    connector::{ConnectorOp, OpExecResponse, ResourceAddress},
    error_util::invalid_op,
};

use crate::util::{
    get_phy_internet_gateway_id, get_phy_route_table_id, get_phy_security_group_id, get_phy_subnet_id, get_phy_vpc_id,
};

impl VpcConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        let addr = VpcResourceAddress::from_path(addr)?;
        let op = VpcConnectorOp::from_str(op)?;

        match &addr {
            VpcResourceAddress::Vpc { region, vpc_id } => {
                let vpc_id = get_phy_vpc_id(&self.prefix, region, vpc_id)?.unwrap_or(vpc_id.into());

                let client = self.get_or_init_client(region).await?;

                match op {
                    VpcConnectorOp::CreateVpc(vpc) => op_impl::create_vpc(&client, &vpc).await,
                    VpcConnectorOp::UpdateVpcTags(old_tags, new_tags) => {
                        op_impl::update_vpc_tags(&client, &vpc_id, &old_tags, &new_tags).await
                    }
                    // VpcConnectorOp::UpdateVpcCidrBlock(cidr) => {
                    //     // op_impl::up
                    // }
                    VpcConnectorOp::UpdateVpcAttributes {
                        enable_dns_support,
                        enable_dns_hostnames,
                    } => op_impl::update_vpc_attributes(&client, &vpc_id, enable_dns_support, enable_dns_hostnames).await,
                    VpcConnectorOp::DeleteVpc => op_impl::delete_vpc(&client, &vpc_id).await,
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            VpcResourceAddress::Subnet {
                region,
                vpc_id,
                subnet_id,
            } => {
                let vpc_id = get_phy_vpc_id(&self.prefix, region, vpc_id)?.unwrap_or(vpc_id.into());
                let subnet_id = get_phy_subnet_id(&self.prefix, region, &vpc_id, subnet_id)?.unwrap_or(subnet_id.into());

                let client = self.get_or_init_client(region).await?;

                match op {
                    VpcConnectorOp::CreateSubnet(subnet) => op_impl::create_subnet(&client, &vpc_id, &subnet).await,
                    VpcConnectorOp::UpdateSubnetTags(old_tags, new_tags) => {
                        op_impl::update_subnet_tags(&client, &subnet_id, &old_tags, &new_tags).await
                    }
                    VpcConnectorOp::UpdateSubnetAttributes { map_public_ip_on_launch } => {
                        op_impl::update_subnet_attributes(&client, &subnet_id, map_public_ip_on_launch).await
                    }
                    VpcConnectorOp::DeleteSubnet => op_impl::delete_subnet(&client, &subnet_id).await,
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            VpcResourceAddress::InternetGateway { region, igw_id } => {
                let client = self.get_or_init_client(region).await?;
                let igw_id = get_phy_internet_gateway_id(&self.prefix, region, igw_id)?.unwrap_or(igw_id.clone());

                match op {
                    VpcConnectorOp::CreateInternetGateway(igw) => op_impl::create_internet_gateway(&client, &igw).await,
                    VpcConnectorOp::AttachInternetGateway { vpc_id } => {
                        let vpc_id = get_phy_vpc_id(&self.prefix, region, &vpc_id)?.unwrap_or(vpc_id);
                        op_impl::attach_internet_gateway(&client, &igw_id, &vpc_id).await
                    }
                    VpcConnectorOp::DetachInternetGateway { vpc_id } => {
                        let vpc_id = get_phy_vpc_id(&self.prefix, region, &vpc_id)?.unwrap_or(vpc_id);
                        op_impl::detach_internet_gateway(&client, &igw_id, &vpc_id).await
                    }
                    VpcConnectorOp::UpdateInternetGatewayTags(old_tags, new_tags) => {
                        op_impl::update_internet_gateway_tags(&client, &igw_id, &old_tags, &new_tags).await
                    }
                    VpcConnectorOp::DeleteInternetGateway => op_impl::delete_internet_gateway(&client, &igw_id).await,
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            VpcResourceAddress::RouteTable { region, vpc_id, rt_id } => {
                let client = self.get_or_init_client(region).await?;

                let vpc_id = get_phy_vpc_id(&self.prefix, region, vpc_id)?.unwrap_or(vpc_id.clone());
                let rt_id = get_phy_route_table_id(&self.prefix, region, &vpc_id, rt_id)?.unwrap_or(rt_id.clone());

                match op {
                    VpcConnectorOp::CreateRouteTable(rt) => op_impl::create_route_table(&client, &rt, &vpc_id).await,
                    VpcConnectorOp::UpdateRouteTableTags(old_tags, new_tags) => {
                        op_impl::update_route_table_tags(&client, &rt_id, &old_tags, &new_tags).await
                    }
                    VpcConnectorOp::CreateRoute(route) => op_impl::create_route(&client, &rt_id, &route).await,
                    VpcConnectorOp::DeleteRoute(route) => op_impl::delete_route(&client, &rt_id, &route).await,
                    VpcConnectorOp::AssociateRouteTable { subnet_id } => {
                        op_impl::associate_route_table(&client, &rt_id, &subnet_id).await
                    }
                    VpcConnectorOp::DisassociateRouteTable { association_id } => {
                        op_impl::disassociate_route_table(&client, &association_id).await
                    }
                    VpcConnectorOp::DeleteRouteTable => op_impl::delete_route_table(&client, &rt_id).await,
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            VpcResourceAddress::SecurityGroup { region, vpc_id, sg_id } => {
                let client = self.get_or_init_client(region).await?;
                let vpc_id = get_phy_vpc_id(&self.prefix, region, vpc_id)?.unwrap_or(vpc_id.clone());
                let sg_id = get_phy_security_group_id(&self.prefix, region, &vpc_id, sg_id)?.unwrap_or(sg_id.clone());

                match op {
                    VpcConnectorOp::CreateSecurityGroup(sg) => {
                        op_impl::create_security_group(&client, &sg, &vpc_id, &sg_id).await
                    }
                    VpcConnectorOp::UpdateSecurityGroupTags(old_tags, new_tags) => {
                        op_impl::update_security_group_tags(&client, &sg_id, &old_tags, &new_tags).await
                    }
                    VpcConnectorOp::AuthorizeSecurityGroupIngress(rule) => {
                        op_impl::authorize_security_group_ingress(&client, &sg_id, &rule).await
                    }
                    VpcConnectorOp::AuthorizeSecurityGroupEgress(rule) => {
                        op_impl::authorize_security_group_egress(&client, &sg_id, &rule).await
                    }
                    VpcConnectorOp::RevokeSecurityGroupIngress(rule) => {
                        op_impl::revoke_security_group_ingress(&client, &sg_id, &rule).await
                    }
                    VpcConnectorOp::RevokeSecurityGroupEgress(rule) => {
                        op_impl::revoke_security_group_egress(&client, &sg_id, &rule).await
                    }
                    VpcConnectorOp::DeleteSecurityGroup => op_impl::delete_security_group(&client, &sg_id).await,
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
        }
    }
}
