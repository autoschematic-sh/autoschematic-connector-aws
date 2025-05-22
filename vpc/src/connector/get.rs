use crate::addr::VpcResourceAddress;

use super::VpcConnector;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;

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
    }, connector_op, connector_util::{get_output_or_bail, load_resource_outputs, output_phy_to_virt}, diag::DiagnosticOutput, get_resource_output, read_outputs::ReadOutput, skeleton, util::{diff_ron_values, ron_check_eq, ron_check_syntax, RON}
};

use aws_config::{meta::region::RegionProviderChain, timeout::TimeoutConfig, BehaviorVersion};
use aws_sdk_ec2::{config::Region, types::Filter};
use tokio::sync::Mutex;

use crate::util::{
    get_igw, get_phy_internet_gateway_id, get_phy_route_table_id, get_phy_security_group_id,
    get_phy_subnet_id, get_phy_vpc_id, get_route_table, get_security_group, get_subnet, get_vpc,
};

impl VpcConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = VpcResourceAddress::from_path(addr)?;

        match addr {
            VpcResourceAddress::Vpc(region, vpc_id) => {
                let client = self.get_or_init_client(&region).await?;
                let Some(vpc) = get_vpc(&client, &vpc_id).await? else {
                    return Ok(None);
                };
                return get_resource_output!(
                    VpcResource::Vpc(vpc),
                    [(String::from("vpc_id"), Some(vpc_id))]
                );
            }
            VpcResourceAddress::Subnet(region, vpc_id, subnet_id) => {
                let client = self.get_or_init_client(&region).await?;
                let Some(subnet) = get_subnet(&client, &vpc_id, &subnet_id).await? else {
                    return Ok(None);
                };
                return get_resource_output!(
                    VpcResource::Subnet(subnet),
                    [(String::from("subnet_id"), Some(subnet_id))]
                );
            }
            VpcResourceAddress::InternetGateway(region, igw_id) => {
                let client = self.get_or_init_client(&region).await?;
                let Some(igw) = get_igw(&client, &igw_id).await? else {
                    return Ok(None);
                };
                return get_resource_output!(
                    VpcResource::InternetGateway(igw),
                    [(String::from("internet_gateway_id"), Some(igw_id))]
                );
            }
            VpcResourceAddress::RouteTable(region, vpc_id, rt_id) => {
                let client = self.get_or_init_client(&region).await?;
                let Some(route_table) = get_route_table(&client, &vpc_id, &rt_id).await? else {
                    return Ok(None);
                };
                return get_resource_output!(
                    VpcResource::RouteTable(route_table),
                    [(String::from("route_table_id"), Some(rt_id))]
                );
            }
            VpcResourceAddress::SecurityGroup(region, vpc_id, sg_id) => {
                let client = self.get_or_init_client(&region).await?;
                let Some(security_group) = get_security_group(&client, &vpc_id, &sg_id).await?
                else {
                    return Ok(None);
                };
                return get_resource_output!(
                    VpcResource::SecurityGroup(security_group),
                    [(String::from("security_group_id"), Some(sg_id))]
                );
            }
        }
    }
}
