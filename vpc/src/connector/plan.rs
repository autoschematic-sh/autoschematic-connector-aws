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
    resource::{InternetGateway, Route, RouteTable, SecurityGroup, SecurityGroupRule, Subnet, Vpc, VpcResource},
    tags::Tags,
};
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsConnectorConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource, ResourceAddress,
        SkeletonOutput, VirtToPhyOutput,
    },
    connector_op,
    connector_util::{get_output_or_bail, load_resource_outputs, output_phy_to_virt},
    diag::DiagnosticOutput,
    read_outputs::ReadOutput,
    skeleton,
    util::{RON, diff_ron_values, ron_check_eq, ron_check_syntax},
};

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use aws_sdk_ec2::{config::Region, types::Filter};
use tokio::sync::Mutex;

impl VpcConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<String>,
        desired: Option<String>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr = VpcResourceAddress::from_path(addr)?;
        match addr {
            VpcResourceAddress::Vpc(_region, vpc_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_vpc)) => {
                        let new_vpc: Vpc = RON.from_str(&new_vpc)?;
                        Ok(vec![connector_op!(
                            VpcConnectorOp::CreateVpc(new_vpc),
                            format!("Create new VPC {}", vpc_id)
                        )])
                    }
                    (Some(_old_vpc), None) => {
                        Ok(vec![connector_op!(
                            VpcConnectorOp::DeleteVpc,
                            format!("DELETE VPC {}", vpc_id)
                        )])
                    }
                    (Some(old_vpc), Some(new_vpc)) => {
                        let old_vpc: Vpc = RON.from_str(&old_vpc)?;
                        let new_vpc: Vpc = RON.from_str(&new_vpc)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_vpc.tags != new_vpc.tags {
                            let diff = diff_ron_values(&old_vpc.tags, &new_vpc.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                VpcConnectorOp::UpdateVpcTags(old_vpc.tags, new_vpc.tags,),
                                format!("Modify tags for VPC `{}`\n{}", vpc_id, diff)
                            ));
                        }

                        // Check for DNS settings changes
                        if old_vpc.enable_dns_support != new_vpc.enable_dns_support
                            || old_vpc.enable_dns_hostnames != new_vpc.enable_dns_hostnames
                        {
                            ops.push(connector_op!(
                                VpcConnectorOp::UpdateVpcAttributes {
                                    enable_dns_support: Some(new_vpc.enable_dns_support),
                                    enable_dns_hostnames: Some(new_vpc.enable_dns_hostnames),
                                },
                                format!("Modify DNS settings for VPC `{}`", vpc_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
            VpcResourceAddress::Subnet(_region, vpc_id, subnet_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_subnet)) => {
                        let new_subnet: Subnet = RON.from_str(&new_subnet)?;
                        Ok(vec![connector_op!(
                            VpcConnectorOp::CreateSubnet(new_subnet),
                            format!("Create new Subnet {}", subnet_id)
                        )])
                    }
                    (Some(_old_subnet), None) => {
                        Ok(vec![connector_op!(
                            VpcConnectorOp::DeleteSubnet,
                            format!("DELETE Subnet {}", subnet_id)
                        )])
                    }
                    (Some(old_subnet), Some(new_subnet)) => {
                        let old_subnet: Subnet = RON.from_str(&old_subnet)?;
                        let new_subnet: Subnet = RON.from_str(&new_subnet)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_subnet.tags != new_subnet.tags {
                            let diff = diff_ron_values(&old_subnet.tags, &new_subnet.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                VpcConnectorOp::UpdateSubnetTags(old_subnet.tags, new_subnet.tags,),
                                format!("Modify tags for Subnet `{}`\n{}", subnet_id, diff)
                            ));
                        }

                        // Check for map_public_ip_on_launch changes
                        if old_subnet.map_public_ip_on_launch != new_subnet.map_public_ip_on_launch {
                            ops.push(connector_op!(
                                VpcConnectorOp::UpdateSubnetAttributes {
                                    map_public_ip_on_launch: Some(new_subnet.map_public_ip_on_launch,),
                                },
                                format!("Modify public IP mapping for Subnet `{}`", subnet_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
            VpcResourceAddress::InternetGateway(_region, igw_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_igw)) => {
                        let new_igw: InternetGateway = RON.from_str(&new_igw)?;
                        let mut ops = Vec::new();

                        // Create internet gateway
                        ops.push(connector_op!(
                            VpcConnectorOp::CreateInternetGateway(InternetGateway {
                                vpc_id: new_igw.vpc_id.clone(),
                                tags: new_igw.tags.clone(),
                            }),
                            format!("Create new Internet Gateway {}", igw_id)
                        ));

                        // Attach to VPC if specified
                        if let Some(vpc_id) = &new_igw.vpc_id {
                            ops.push(connector_op!(
                                VpcConnectorOp::AttachInternetGateway { vpc_id: vpc_id.clone() },
                                format!("Attach Internet Gateway {} to VPC {}", igw_id, vpc_id)
                            ));
                        }

                        Ok(ops)
                    }
                    (Some(_old_igw), None) => {
                        Ok(vec![connector_op!(
                            VpcConnectorOp::DeleteInternetGateway,
                            format!("DELETE Internet Gateway {}", igw_id)
                        )])
                    }
                    (Some(old_igw), Some(new_igw)) => {
                        let old_igw: InternetGateway = RON.from_str(&old_igw)?;
                        let new_igw: InternetGateway = RON.from_str(&new_igw)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_igw.tags != new_igw.tags {
                            let diff = diff_ron_values(&old_igw.tags, &new_igw.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                VpcConnectorOp::UpdateInternetGatewayTags(old_igw.tags, new_igw.tags,),
                                format!("Modify tags for Internet Gateway `{}`\n{}", igw_id, diff)
                            ));
                        }

                        // Check for VPC attachment changes
                        match (&old_igw.vpc_id, &new_igw.vpc_id) {
                            (Some(old_vpc_id), Some(new_vpc_id)) if old_vpc_id != new_vpc_id => {
                                // Detach from old VPC
                                ops.push(connector_op!(
                                    VpcConnectorOp::DetachInternetGateway {
                                        vpc_id: old_vpc_id.clone(),
                                    },
                                    format!("Detach Internet Gateway `{}` from VPC `{}`", igw_id, old_vpc_id)
                                ));

                                // Attach to new VPC
                                ops.push(connector_op!(
                                    VpcConnectorOp::AttachInternetGateway {
                                        vpc_id: new_vpc_id.clone(),
                                    },
                                    format!("Attach Internet Gateway `{}` to VPC `{}`", igw_id, new_vpc_id)
                                ));
                            }
                            (Some(old_vpc_id), None) => {
                                // Detach from VPC
                                ops.push(connector_op!(
                                    VpcConnectorOp::DetachInternetGateway {
                                        vpc_id: old_vpc_id.clone(),
                                    },
                                    format!("Detach Internet Gateway `{}` from VPC `{}`", igw_id, old_vpc_id)
                                ));
                            }
                            (None, Some(new_vpc_id)) => {
                                // Attach to VPC
                                ops.push(connector_op!(
                                    VpcConnectorOp::AttachInternetGateway {
                                        vpc_id: new_vpc_id.clone(),
                                    },
                                    format!("Attach Internet Gateway `{}` to VPC `{}`", igw_id, new_vpc_id)
                                ));
                            }
                            _ => {} // No change in VPC attachment
                        }

                        Ok(ops)
                    }
                }
            }
            VpcResourceAddress::RouteTable(_region, vpc_id, rt_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_rt)) => {
                        let new_rt: RouteTable = RON.from_str(&new_rt)?;
                        Ok(vec![connector_op!(
                            VpcConnectorOp::CreateRouteTable(new_rt),
                            format!("Create new Route Table {}", rt_id)
                        )])
                    }
                    (Some(_old_rt), None) => {
                        Ok(vec![connector_op!(
                            VpcConnectorOp::DeleteRouteTable,
                            format!("DELETE Route Table {}", rt_id)
                        )])
                    }
                    (Some(old_rt), Some(new_rt)) => {
                        let old_rt: RouteTable = RON.from_str(&old_rt)?;
                        let new_rt: RouteTable = RON.from_str(&new_rt)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_rt.tags != new_rt.tags {
                            let diff = diff_ron_values(&old_rt.tags, &new_rt.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                VpcConnectorOp::UpdateRouteTableTags(old_rt.tags, new_rt.tags,),
                                format!("Modify tags for Route Table `{}`\n{}", rt_id, diff)
                            ));
                        }

                        // Compare routes - find routes to add
                        for new_route in &new_rt.routes {
                            let existing_route = old_rt
                                .routes
                                .iter()
                                .find(|r| r.destination_cidr_block == new_route.destination_cidr_block);

                            if existing_route.is_none() || existing_route != Some(new_route) {
                                // Either the route is new or has changed
                                ops.push(connector_op!(
                                    VpcConnectorOp::CreateRoute(Route {
                                        destination_cidr_block: new_route.destination_cidr_block.clone(),
                                        destination_ipv6_cidr_block: new_route.destination_ipv6_cidr_block.clone(),
                                        gateway_id: new_route.gateway_id.clone(),
                                        instance_id: new_route.instance_id.clone(),
                                        nat_gateway_id: new_route.nat_gateway_id.clone(),
                                    }),
                                    format!("Create route in Route Table `{}`", rt_id)
                                ));
                            }
                        }

                        // Find routes to delete
                        for old_route in &old_rt.routes {
                            let still_exists = new_rt.routes.iter().any(|r| r == old_route);

                            if !still_exists {
                                ops.push(connector_op!(
                                    VpcConnectorOp::DeleteRoute(old_route.clone()),
                                    format!("Delete route from Route Table `{}`", rt_id)
                                ));
                            }
                        }

                        // Handle association changes - very simplified, in a real implementation
                        // we would need more context to properly handle this
                        for new_assoc in &new_rt.associations {
                            if !old_rt.associations.contains(new_assoc) {
                                // This is a simplification - in reality we'd need to determine if it's a subnet association
                                if new_assoc.starts_with("subnet-") {
                                    ops.push(connector_op!(
                                        VpcConnectorOp::AssociateRouteTable {
                                            subnet_id: new_assoc.clone(),
                                        },
                                        format!("Associate Route Table `{}` with subnet `{}`", rt_id, new_assoc)
                                    ));
                                }
                            }
                        }

                        for old_assoc in &old_rt.associations {
                            if !new_rt.associations.contains(old_assoc) {
                                ops.push(connector_op!(
                                    VpcConnectorOp::DisassociateRouteTable {
                                        association_id: old_assoc.clone(),
                                    },
                                    format!("Disassociate Route Table `{}` from association `{}`", rt_id, old_assoc)
                                ));
                            }
                        }

                        Ok(ops)
                    }
                }
            }
            VpcResourceAddress::SecurityGroup(_region, vpc_id, sg_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_sg)) => {
                        let new_sg: SecurityGroup = RON.from_str(&new_sg)?;
                        Ok(vec![connector_op!(
                            VpcConnectorOp::CreateSecurityGroup(new_sg),
                            format!("Create new Security Group {}", sg_id)
                        )])
                    }
                    (Some(_old_sg), None) => {
                        Ok(vec![connector_op!(
                            VpcConnectorOp::DeleteSecurityGroup,
                            format!("DELETE Security Group {}", sg_id)
                        )])
                    }
                    (Some(old_sg), Some(new_sg)) => {
                        let old_sg: SecurityGroup = RON.from_str(&old_sg)?;
                        let new_sg: SecurityGroup = RON.from_str(&new_sg)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_sg.tags != new_sg.tags {
                            let diff = diff_ron_values(&old_sg.tags, &new_sg.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                VpcConnectorOp::UpdateSecurityGroupTags(old_sg.tags, new_sg.tags,),
                                format!("Modify tags for Security Group `{}`\n{}", sg_id, diff)
                            ));
                        }

                        // Compare ingress rules - find rules to add
                        for new_rule in &new_sg.ingress_rules {
                            let rule_exists = old_sg.ingress_rules.iter().any(|r| r == new_rule);

                            if !rule_exists {
                                ops.push(connector_op!(
                                    VpcConnectorOp::AuthorizeSecurityGroupIngress(SecurityGroupRule {
                                        protocol: new_rule.protocol.clone(),
                                        from_port: new_rule.from_port.clone(),
                                        to_port: new_rule.to_port.clone(),
                                        cidr_blocks: new_rule.cidr_blocks.clone(),
                                        security_group_ids: new_rule.security_group_ids.clone(),
                                    },),
                                    format!("Add ingress rule in Security Group `{}`", sg_id)
                                ));
                            }
                        }

                        // Find ingress rules to delete
                        for old_rule in &old_sg.ingress_rules {
                            let rule_exists = new_sg.ingress_rules.iter().any(|r| r == old_rule);

                            if !rule_exists {
                                ops.push(connector_op!(
                                    VpcConnectorOp::RevokeSecurityGroupIngress(SecurityGroupRule {
                                        protocol: old_rule.protocol.clone(),
                                        from_port: old_rule.from_port.clone(),
                                        to_port: old_rule.to_port.clone(),
                                        cidr_blocks: old_rule.cidr_blocks.clone(),
                                        security_group_ids: old_rule.security_group_ids.clone(),
                                    },),
                                    format!("Remove ingress rule from Security Group `{}`", sg_id)
                                ));
                            }
                        }

                        // Compare egress rules - find rules to add
                        for new_rule in &new_sg.egress_rules {
                            let rule_exists = old_sg.egress_rules.iter().any(|r| r == new_rule);

                            if !rule_exists {
                                ops.push(connector_op!(
                                    VpcConnectorOp::AuthorizeSecurityGroupEgress(SecurityGroupRule {
                                        protocol: new_rule.protocol.clone(),
                                        from_port: new_rule.from_port.clone(),
                                        to_port: new_rule.to_port.clone(),
                                        cidr_blocks: new_rule.cidr_blocks.clone(),
                                        security_group_ids: new_rule.security_group_ids.clone(),
                                    },),
                                    format!("Add egress rule in Security Group `{}`", sg_id)
                                ));
                            }
                        }

                        // Find egress rules to delete
                        for old_rule in &old_sg.egress_rules {
                            let rule_exists = new_sg.egress_rules.iter().any(|r| r == old_rule);

                            if !rule_exists {
                                ops.push(connector_op!(
                                    VpcConnectorOp::RevokeSecurityGroupEgress(SecurityGroupRule {
                                        protocol: old_rule.protocol.clone(),
                                        from_port: old_rule.from_port.clone(),
                                        to_port: old_rule.to_port.clone(),
                                        cidr_blocks: old_rule.cidr_blocks.clone(),
                                        security_group_ids: old_rule.security_group_ids.clone(),
                                    },),
                                    format!("Remove egress rule from Security Group `{}`", sg_id)
                                ));
                            }
                        }

                        Ok(ops)
                    }
                }
            }
        }
    }
}
