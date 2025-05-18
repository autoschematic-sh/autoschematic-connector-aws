use crate::addr::VpcResourceAddress;

use super::VpcConnector;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::{
    op::VpcConnectorOp,
    op_impl,
    resource::{
        InternetGateway, Route, RouteTable, SecurityGroup, SecurityGroupRule, Subnet, Vpc,
        VpcResource,
    },
    tags::Tags,
};
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsConnectorConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, GetResourceOutput, OpExecOutput, OpPlanOutput,
        Resource, ResourceAddress, SkeletonOutput, VirtToPhyOutput,
    },
    connector_op,
    connector_util::{get_output_or_bail, load_resource_outputs, output_phy_to_virt},
    diag::DiagnosticOutput,
    read_outputs::ReadOutput,
    skeleton,
    util::{diff_ron_values, ron_check_eq, ron_check_syntax, RON},
};

use aws_config::{meta::region::RegionProviderChain, timeout::TimeoutConfig, BehaviorVersion};
use aws_sdk_ec2::{config::Region, types::Filter};
use tokio::sync::Mutex;

use crate::util::{
    get_igw, get_phy_internet_gateway_id, get_phy_route_table_id, get_phy_security_group_id,
    get_phy_subnet_id, get_phy_vpc_id, get_route_table, get_security_group, get_subnet, get_vpc,
};

impl VpcConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = VpcResourceAddress::from_path(addr)?;
        let op = VpcConnectorOp::from_str(op)?;

        match addr {
            Some(VpcResourceAddress::Vpc(region, vpc_id)) => {
                let vpc_id = get_phy_vpc_id(&self.prefix, &region, &vpc_id)?.unwrap_or(vpc_id);

                let client = self.get_or_init_client(&region).await?;

                match op {
                    VpcConnectorOp::CreateVpc(vpc) => op_impl::create_vpc(&client, &vpc).await,
                    VpcConnectorOp::UpdateVpcTags(old_tags, new_tags) => {
                        op_impl::update_vpc_tags(&client, &vpc_id, &old_tags, &new_tags).await
                    }
                    VpcConnectorOp::UpdateVpcAttributes {
                        enable_dns_support,
                        enable_dns_hostnames,
                    } => {
                        op_impl::update_vpc_attributes(
                            &client,
                            &vpc_id,
                            enable_dns_support,
                            enable_dns_hostnames,
                        )
                        .await
                    }
                    VpcConnectorOp::DeleteVpc => op_impl::delete_vpc(&client, &vpc_id).await,
                    _ => bail!("Invalid operation for VPC resource"),
                }
            }
            Some(VpcResourceAddress::Subnet(region, vpc_id, subnet_id)) => {
                let vpc_id = get_phy_vpc_id(&self.prefix, &region, &vpc_id)?.unwrap_or(vpc_id);
                let subnet_id = get_phy_subnet_id(&self.prefix, &region, &vpc_id, &subnet_id)?
                    .unwrap_or(subnet_id);

                let client = self.get_or_init_client(&region).await?;

                match op {
                    VpcConnectorOp::CreateSubnet(subnet) => {
                        op_impl::create_subnet(&client, &vpc_id, &subnet).await
                    }
                    VpcConnectorOp::UpdateSubnetTags(old_tags, new_tags) => {
                        op_impl::update_subnet_tags(&client, &subnet_id, &old_tags, &new_tags).await
                    }
                    VpcConnectorOp::UpdateSubnetAttributes {
                        map_public_ip_on_launch,
                    } => {
                        op_impl::update_subnet_attributes(
                            &client,
                            &subnet_id,
                            map_public_ip_on_launch,
                        )
                        .await
                    }
                    VpcConnectorOp::DeleteSubnet => {
                        op_impl::delete_subnet(&client, &subnet_id).await
                    }
                    _ => bail!("Invalid operation for Subnet resource"),
                }
            }
            Some(VpcResourceAddress::InternetGateway(region, igw_id)) => {
                let client = self.get_or_init_client(&region).await?;
                let igw_id =
                    get_phy_internet_gateway_id(&self.prefix, &region, &igw_id)?.unwrap_or(igw_id);

                match op {
                    VpcConnectorOp::CreateInternetGateway(igw) => {
                        op_impl::create_internet_gateway(&client, &igw).await
                    }
                    VpcConnectorOp::AttachInternetGateway { vpc_id } => {
                        let vpc_id =
                            get_phy_vpc_id(&self.prefix, &region, &vpc_id)?.unwrap_or(vpc_id);
                        op_impl::attach_internet_gateway(&client, &igw_id, &vpc_id).await
                    }
                    VpcConnectorOp::DetachInternetGateway { vpc_id } => {
                        let vpc_id =
                            get_phy_vpc_id(&self.prefix, &region, &vpc_id)?.unwrap_or(vpc_id);
                        op_impl::detach_internet_gateway(&client, &igw_id, &vpc_id).await
                    }
                    VpcConnectorOp::UpdateInternetGatewayTags(old_tags, new_tags) => {
                        op_impl::update_internet_gateway_tags(
                            &client, &igw_id, &old_tags, &new_tags,
                        )
                        .await
                    }
                    VpcConnectorOp::DeleteInternetGateway => {
                        op_impl::delete_internet_gateway(&client, &igw_id).await
                    }
                    _ => bail!("Invalid operation for Internet Gateway resource"),
                }
            }
            Some(VpcResourceAddress::RouteTable(region, vpc_id, rt_id)) => {
                let client = self.get_or_init_client(&region).await?;

                let vpc_id = get_phy_vpc_id(&self.prefix, &region, &vpc_id)?.unwrap_or(vpc_id);
                let rt_id = get_phy_route_table_id(&self.prefix, &region, &vpc_id, &rt_id)?
                    .unwrap_or(rt_id);

                match op {
                    VpcConnectorOp::CreateRouteTable(rt) => {
                        op_impl::create_route_table(&client, &rt, &vpc_id).await
                    }
                    VpcConnectorOp::UpdateRouteTableTags(old_tags, new_tags) => {
                        op_impl::update_route_table_tags(&client, &rt_id, &old_tags, &new_tags)
                            .await
                    }
                    VpcConnectorOp::CreateRoute(route) => {
                        op_impl::create_route(&client, &rt_id, &route).await
                    }
                    VpcConnectorOp::DeleteRoute(route) => {
                        op_impl::delete_route(&client, &rt_id, &route).await
                    }
                    VpcConnectorOp::AssociateRouteTable { subnet_id } => {
                        op_impl::associate_route_table(&client, &rt_id, &subnet_id).await
                    }
                    VpcConnectorOp::DisassociateRouteTable { association_id } => {
                        op_impl::disassociate_route_table(&client, &association_id).await
                    }
                    VpcConnectorOp::DeleteRouteTable => {
                        op_impl::delete_route_table(&client, &rt_id).await
                    }
                    _ => bail!("Invalid operation for Route Table resource"),
                }
            }
            Some(VpcResourceAddress::SecurityGroup(region, vpc_id, sg_id)) => {
                let client = self.get_or_init_client(&region).await?;
                let vpc_id = get_phy_vpc_id(&self.prefix, &region, &vpc_id)?.unwrap_or(vpc_id);
                let sg_id = get_phy_security_group_id(&self.prefix, &region, &vpc_id, &sg_id)?
                    .unwrap_or(sg_id);

                match op {
                    VpcConnectorOp::CreateSecurityGroup(sg) => {
                        op_impl::create_security_group(&client, &sg, &vpc_id, &sg_id).await
                    }
                    VpcConnectorOp::UpdateSecurityGroupTags(old_tags, new_tags) => {
                        op_impl::update_security_group_tags(&client, &sg_id, &old_tags, &new_tags)
                            .await
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
                    VpcConnectorOp::DeleteSecurityGroup => {
                        op_impl::delete_security_group(&client, &sg_id).await
                    }
                    _ => bail!("Invalid operation for Security Group resource"),
                }
            }
            None => bail!("Invalid resource address"),
        }
    }
}
