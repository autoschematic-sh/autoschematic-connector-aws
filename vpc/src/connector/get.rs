use crate::addr::VpcResourceAddress;

use super::VpcConnector;

use std::{collections::HashMap, path::Path};

use anyhow::Context;

use crate::resource::VpcResource;
use autoschematic_core::{
    connector::{GetResourceResponse, Resource, ResourceAddress},
    get_resource_response,
};

use crate::util::{get_igw, get_route_table, get_security_group, get_subnet, get_vpc};

impl VpcConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceResponse>, anyhow::Error> {
        let addr = VpcResourceAddress::from_path(addr)?;

        match addr {
            VpcResourceAddress::Vpc { region, vpc_id } => {
                let client = self.get_or_init_client(&region).await?;
                let Some(vpc) = get_vpc(&client, &vpc_id).await? else {
                    return Ok(None);
                };
                get_resource_response!(VpcResource::Vpc(vpc), [(String::from("vpc_id"), vpc_id)])
            }
            VpcResourceAddress::Subnet {
                region,
                vpc_id,
                subnet_id,
            } => {
                let client = self.get_or_init_client(&region).await?;
                let Some(subnet) = get_subnet(&client, &vpc_id, &subnet_id).await? else {
                    return Ok(None);
                };
                get_resource_response!(VpcResource::Subnet(subnet), [(String::from("subnet_id"), subnet_id)])
            }
            VpcResourceAddress::InternetGateway { region, igw_id } => {
                let client = self.get_or_init_client(&region).await?;
                let Some(igw) = get_igw(&client, &igw_id).await? else {
                    return Ok(None);
                };
                get_resource_response!(
                    VpcResource::InternetGateway(igw),
                    [(String::from("internet_gateway_id"), igw_id)]
                )
            }
            VpcResourceAddress::RouteTable { region, vpc_id, rt_id } => {
                let client = self.get_or_init_client(&region).await?;
                let Some(route_table) = get_route_table(&client, &vpc_id, &rt_id).await? else {
                    return Ok(None);
                };
                get_resource_response!(
                    VpcResource::RouteTable(route_table),
                    [(String::from("route_table_id"), rt_id)]
                )
            }
            VpcResourceAddress::SecurityGroup { region, vpc_id, sg_id } => {
                let client = self.get_or_init_client(&region).await?;
                let Some(security_group) = get_security_group(&client, &vpc_id, &sg_id).await? else {
                    return Ok(None);
                };
                get_resource_response!(
                    VpcResource::SecurityGroup(security_group),
                    [(String::from("security_group_id"), sg_id)]
                )
            }
        }
    }
}
