use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::{
    addr::VpcResourceAddress,
    resource::{InternetGateway, Route, RouteTable, SecurityGroup, SecurityGroupRule, Subnet, Vpc, VpcResource},
    tags::Tags,
};
use anyhow::{Context, bail};
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsConnectorConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource,
        ResourceAddress, SkeletonOutput, VirtToPhyOutput,
    },
    connector_util::{get_output_or_bail, load_resource_outputs, output_phy_to_virt},
    diag::DiagnosticOutput,
    read_outputs::ReadOutput,
    skeleton,
    util::{optional_string_from_utf8, ron_check_eq, ron_check_syntax},
};

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use aws_sdk_ec2::config::Region;
use tokio::sync::Mutex;

use crate::config::VpcConnectorConfig;

pub mod get;
pub mod list;
pub mod op_exec;
pub mod plan;

#[derive(Default)]
pub struct VpcConnector {
    pub client_cache: Mutex<HashMap<String, Arc<aws_sdk_ec2::Client>>>,
    pub account_id: Mutex<String>,
    pub config: Mutex<VpcConnectorConfig>,
    pub prefix: PathBuf,
}

#[async_trait]
impl Connector for VpcConnector {
    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(VpcConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> Result<(), anyhow::Error> {
        let config_file = AwsConnectorConfig::try_load(&self.prefix)?;

        let region_str = "us-east-1";
        let region = RegionProviderChain::first_try(Region::new(region_str.to_owned()));

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(30))
                    .operation_timeout(Duration::from_secs(30))
                    .operation_attempt_timeout(Duration::from_secs(30))
                    .read_timeout(Duration::from_secs(30))
                    .build(),
            )
            .load()
            .await;

        tracing::warn!("VpcConnector::new()!");

        // Get account ID from STS
        let sts_config = aws_config::defaults(BehaviorVersion::latest())
            .region(RegionProviderChain::first_try(Region::new("us-east-1".to_owned())))
            .load()
            .await;

        let sts_client = aws_sdk_sts::Client::new(&sts_config);
        let caller_identity = sts_client
            .get_caller_identity()
            .send()
            .await
            .context("Getting caller identity")?;

        let Some(account_id) = caller_identity.account else {
            bail!("Failed to get current account ID!");
        };

        if let Some(config_file) = config_file {
            if config_file.account_id != account_id {
                bail!(
                    "Credentials do not match configured account id: creds = {}, aws/config.ron = {}",
                    account_id,
                    config_file.account_id
                );
            }
        }

        let vpc_config: VpcConnectorConfig = VpcConnectorConfig::try_load(&self.prefix)?.unwrap_or_default();

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = vpc_config;
        *self.account_id.lock().await = account_id;

        Ok(())
    }

    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_addr) = VpcResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<OsString>,
        desired: Option<OsString>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        self.do_plan(addr, optional_string_from_utf8(current)?, optional_string_from_utf8(desired)?)
            .await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn addr_virt_to_phy(&self, addr: &Path) -> anyhow::Result<VirtToPhyOutput> {
        let addr = VpcResourceAddress::from_path(addr)?;

        match &addr {
            VpcResourceAddress::Vpc(region, vpc_id) => {
                let Some(outputs) = load_resource_outputs(&self.prefix, &addr)? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };
                let vpc_id = get_output_or_bail(&outputs, "vpc_id")?;
                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::Vpc(region.into(), vpc_id).to_path_buf(),
                ))
            }
            VpcResourceAddress::Subnet(region, vpc_id, subnet_id) => {
                let parent_vpc_addr = VpcResourceAddress::Vpc(region.into(), vpc_id.into());

                let Some(vpc_outputs) = load_resource_outputs(&self.prefix, &parent_vpc_addr)? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        path: parent_vpc_addr.to_path_buf(),
                        key: "vpc_id".to_string(),
                    }]));
                };

                let vpc_id = get_output_or_bail(&vpc_outputs, "vpc_id")?;

                let Some(outputs) = load_resource_outputs(&self.prefix, &addr)? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };

                let subnet_id = get_output_or_bail(&outputs, "subnet_id")?;

                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::Subnet(region.into(), vpc_id, subnet_id).to_path_buf(),
                ))
            }
            VpcResourceAddress::InternetGateway(region, igw_id) => {
                let Some(outputs) = load_resource_outputs(&self.prefix, &addr)? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };
                let igw_id = get_output_or_bail(&outputs, "internet_gateway_id")?;
                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::InternetGateway(region.into(), igw_id).to_path_buf(),
                ))
            }
            VpcResourceAddress::RouteTable(region, vpc_id, rt_id) => {
                let parent_vpc_addr = VpcResourceAddress::Vpc(region.into(), vpc_id.into());

                let Some(vpc_outputs) = load_resource_outputs(&self.prefix, &parent_vpc_addr)? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        path: parent_vpc_addr.to_path_buf(),
                        key: "vpc_id".to_string(),
                    }]));
                };

                let vpc_id = get_output_or_bail(&vpc_outputs, "vpc_id")?;

                let Some(outputs) = load_resource_outputs(&self.prefix, &addr)? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };

                let rt_id = get_output_or_bail(&outputs, "route_table_id")?;

                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::RouteTable(region.into(), vpc_id, rt_id.to_string()).to_path_buf(),
                ))
            }
            VpcResourceAddress::SecurityGroup(region, vpc_id, sg_id) => {
                let parent_vpc_addr = VpcResourceAddress::Vpc(region.into(), vpc_id.into());

                let Some(vpc_outputs) = load_resource_outputs(&self.prefix, &parent_vpc_addr)? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        path: parent_vpc_addr.to_path_buf(),
                        key: "vpc_id".to_string(),
                    }]));
                };

                let vpc_id = get_output_or_bail(&vpc_outputs, "vpc_id")?;

                let Some(outputs) = load_resource_outputs(&self.prefix, &addr)? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };

                let sg_id = get_output_or_bail(&outputs, "security_group_id")?;

                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::SecurityGroup(region.into(), vpc_id, sg_id).to_path_buf(),
                ))
            }
        }
    }

    async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
        let addr = VpcResourceAddress::from_path(addr)?;

        match &addr {
            VpcResourceAddress::Vpc(_, _) => {
                if let Some(vpc_addr) = output_phy_to_virt(&self.prefix, &addr)? {
                    return Ok(Some(vpc_addr.to_path_buf()));
                }
            }
            VpcResourceAddress::Subnet(region, vpc_id, _) => {
                if let Some(VpcResourceAddress::Vpc(_, virt_vpc_id)) =
                    output_phy_to_virt(&self.prefix, &VpcResourceAddress::Vpc(region.to_string(), vpc_id.to_string()))?
                {
                    if let Some(VpcResourceAddress::Subnet(_, _, virt_subnet_id)) = output_phy_to_virt(&self.prefix, &addr)? {
                        return Ok(Some(
                            VpcResourceAddress::Subnet(region.to_string(), virt_vpc_id, virt_subnet_id).to_path_buf(),
                        ));
                    }
                }
            }
            VpcResourceAddress::InternetGateway(_, _) => {
                if let Some(igw_addr) = output_phy_to_virt(&self.prefix, &addr)? {
                    return Ok(Some(igw_addr.to_path_buf()));
                }
            }
            VpcResourceAddress::RouteTable(region, vpc_id, _) => {
                if let Some(VpcResourceAddress::Vpc(_, virt_vpc_id)) =
                    output_phy_to_virt(&self.prefix, &VpcResourceAddress::Vpc(region.to_string(), vpc_id.to_string()))?
                {
                    if let Some(VpcResourceAddress::RouteTable(_, _, virt_rt_id)) = output_phy_to_virt(&self.prefix, &addr)? {
                        return Ok(Some(
                            VpcResourceAddress::RouteTable(region.to_string(), virt_vpc_id, virt_rt_id).to_path_buf(),
                        ));
                    }
                }
            }

            VpcResourceAddress::SecurityGroup(region, vpc_id, _) => {
                if let Some(VpcResourceAddress::Vpc(_, virt_vpc_id)) =
                    output_phy_to_virt(&self.prefix, &VpcResourceAddress::Vpc(region.to_string(), vpc_id.to_string()))?
                {
                    if let Some(VpcResourceAddress::SecurityGroup(_, _, virt_sg_id)) = output_phy_to_virt(&self.prefix, &addr)?
                    {
                        return Ok(Some(
                            VpcResourceAddress::SecurityGroup(region.to_string(), virt_vpc_id, virt_sg_id).to_path_buf(),
                        ));
                    }
                }
            }
        }

        // Ok(Some(addr.to_path_buf()))
        Ok(None)
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        res.push(skeleton!(
            VpcResourceAddress::Vpc(String::from("[region]"), String::from("[vpc_id]")),
            VpcResource::Vpc(Vpc {
                cidr_block: String::from("[cidr_block]"),
                instance_tenancy: String::from("default"),
                enable_dns_support: false,
                enable_dns_hostnames: false,
                tags: Tags::default()
            })
        ));

        res.push(skeleton!(
            VpcResourceAddress::Subnet(
                String::from("[region]"),
                String::from("[vpc_id]"),
                String::from("[subnet_id]")
            ),
            VpcResource::Subnet(Subnet {
                cidr_block: String::from("[cidr_block]"),
                tags: Tags::default(),
                availability_zone: String::from("[availability_zone]"),
                map_public_ip_on_launch: false,
            })
        ));

        res.push(skeleton!(
            VpcResourceAddress::SecurityGroup(
                String::from("[region]"),
                String::from("[vpc_id]"),
                String::from("[security_group_id]")
            ),
            VpcResource::SecurityGroup(SecurityGroup {
                description: String::from("[description]"),
                ingress_rules: vec![SecurityGroupRule {
                    protocol: String::from("TCP"),
                    from_port: Some(8080),
                    to_port: Some(8080),
                    cidr_blocks: vec![String::from("[cidr_block]")],
                    security_group_ids: vec![String::from("[security_group_id]")]
                }],
                egress_rules: vec![],
                tags: Tags::default()
            })
        ));

        res.push(skeleton!(
            VpcResourceAddress::RouteTable(
                String::from("[region]"),
                String::from("[vpc_id]"),
                String::from("[route_table_id]")
            ),
            VpcResource::RouteTable(RouteTable {
                routes: vec![Route {
                    destination_cidr_block: Some(String::from("[cidr_block]")),
                    destination_ipv6_cidr_block: None,
                    gateway_id: Some(String::from("[gateway_id]")),
                    instance_id: None,
                    nat_gateway_id: None,
                }],
                associations: vec![],
                tags: Tags::default()
            })
        ));

        res.push(skeleton!(
            VpcResourceAddress::InternetGateway(String::from("[region]"), String::from("[internet_gateway_id]")),
            VpcResource::InternetGateway(InternetGateway {
                vpc_id: None,
                tags: Tags::default()
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &OsStr, b: &OsStr) -> Result<bool, anyhow::Error> {
        let addr = VpcResourceAddress::from_path(addr)?;

        match addr {
            VpcResourceAddress::Vpc(_, _) => ron_check_eq::<Vpc>(a, b),
            VpcResourceAddress::Subnet(_, _, _) => ron_check_eq::<Subnet>(a, b),
            VpcResourceAddress::InternetGateway(_, _) => ron_check_eq::<InternetGateway>(a, b),
            VpcResourceAddress::RouteTable(_, _, _) => ron_check_eq::<RouteTable>(a, b),
            VpcResourceAddress::SecurityGroup(_, _, _) => ron_check_eq::<SecurityGroup>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &OsStr) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = VpcResourceAddress::from_path(addr)?;

        match addr {
            VpcResourceAddress::Vpc(_, _) => ron_check_syntax::<Vpc>(a),
            VpcResourceAddress::Subnet(_, _, _) => ron_check_syntax::<Subnet>(a),
            VpcResourceAddress::InternetGateway(_, _) => ron_check_syntax::<InternetGateway>(a),
            VpcResourceAddress::RouteTable(_, _, _) => ron_check_syntax::<RouteTable>(a),
            VpcResourceAddress::SecurityGroup(_, _, _) => ron_check_syntax::<SecurityGroup>(a),
        }
    }
}
