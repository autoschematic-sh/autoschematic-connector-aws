use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    addr::VpcResourceAddress,
    resource::{InternetGateway, Route, RouteTable, SecurityGroup, SecurityGroupRule, Subnet, Vpc, VpcResource},
    tags::Tags,
};
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource, ResourceAddress,
        SkeletonOutput, VirtToPhyOutput,
    },
    diag::DiagnosticOutput,
    template::ReadOutput,
    skeleton,
    util::{optional_string_from_utf8, ron_check_eq, ron_check_syntax},
};

use tokio::sync::{Mutex, RwLock};

use crate::config::VpcConnectorConfig;

pub mod get;
pub mod list;
pub mod op_exec;
pub mod plan;

#[derive(Default)]
pub struct VpcConnector {
    pub client_cache: Mutex<HashMap<String, Arc<aws_sdk_ec2::Client>>>,
    pub account_id: Mutex<String>,
    pub config: RwLock<VpcConnectorConfig>,
    pub prefix: PathBuf,
}

#[async_trait]
impl Connector for VpcConnector {
    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(VpcConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> Result<(), anyhow::Error> {
        let vpc_config = VpcConnectorConfig::try_load(&self.prefix).await?;

        let account_id = vpc_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.write().await = vpc_config;
        *self.account_id.lock().await = account_id;

        Ok(())
    }

    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_addr) = VpcResourceAddress::from_path(addr) {
            eprintln!("VpcConnector::filter({}) = true", addr.display());
            Ok(FilterOutput::Resource)
        } else {
            eprintln!("VpcConnector::filter({}) = false", addr.display());
            Ok(FilterOutput::None)
        }
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn subpaths(&self) -> anyhow::Result<Vec<PathBuf>> {
        let mut res = Vec::new();

        for region in &self.config.read().await.enabled_regions {
            res.push(PathBuf::from(format!("aws/vpc/{}", region)));
        }

        Ok(res)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
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
            VpcResourceAddress::Vpc { region, .. } => {
                let Some(vpc_id) = addr.get_output(&self.prefix, "vpc_id")? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };
                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::Vpc {
                        region: region.into(),
                        vpc_id,
                    }
                    .to_path_buf(),
                ))
            }
            VpcResourceAddress::Subnet { region, vpc_id, .. } => {
                let parent_vpc_addr = VpcResourceAddress::Vpc {
                    region: region.into(),
                    vpc_id: vpc_id.into(),
                };

                let Some(vpc_id) = parent_vpc_addr.get_output(&self.prefix, "vpc_id")? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        addr: parent_vpc_addr.to_path_buf(),
                        key:  "vpc_id".to_string(),
                    }]));
                };

                let Some(subnet_id) = addr.get_output(&self.prefix, "subnet_id")? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };

                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::Subnet {
                        region: region.into(),
                        vpc_id,
                        subnet_id,
                    }
                    .to_path_buf(),
                ))
            }
            VpcResourceAddress::InternetGateway { region, .. } => {
                let Some(igw_id) = addr.get_output(&self.prefix, "internet_gateway_id")? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };
                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::InternetGateway {
                        region: region.into(),
                        igw_id,
                    }
                    .to_path_buf(),
                ))
            }
            VpcResourceAddress::RouteTable { region, vpc_id, .. } => {
                let parent_vpc_addr = VpcResourceAddress::Vpc {
                    region: region.into(),
                    vpc_id: vpc_id.into(),
                };

                let Some(vpc_id) = parent_vpc_addr.get_output(&self.prefix, "vpc_id")? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        addr: parent_vpc_addr.to_path_buf(),
                        key:  "vpc_id".to_string(),
                    }]));
                };

                let Some(rt_id) = addr.get_output(&self.prefix, "route_table_id")? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };

                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::RouteTable {
                        region: region.into(),
                        vpc_id,
                        rt_id,
                    }
                    .to_path_buf(),
                ))
            }
            VpcResourceAddress::SecurityGroup { region, vpc_id, .. } => {
                let parent_vpc_addr = VpcResourceAddress::Vpc {
                    region: region.into(),
                    vpc_id: vpc_id.into(),
                };

                let Some(vpc_id) = parent_vpc_addr.get_output(&self.prefix, "vpc_id")? else {
                    return Ok(VirtToPhyOutput::Deferred(vec![ReadOutput {
                        addr: parent_vpc_addr.to_path_buf(),
                        key:  "vpc_id".to_string(),
                    }]));
                };

                let Some(sg_id) = addr.get_output(&self.prefix, "security_group_id")? else {
                    return Ok(VirtToPhyOutput::NotPresent);
                };

                Ok(VirtToPhyOutput::Present(
                    VpcResourceAddress::SecurityGroup {
                        region: region.into(),
                        vpc_id,
                        sg_id,
                    }
                    .to_path_buf(),
                ))
            }
        }
    }

    async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
        let addr = VpcResourceAddress::from_path(addr)?;

        match &addr {
            VpcResourceAddress::Vpc { .. } => {
                if let Some(vpc_addr) = addr.phy_to_virt(&self.prefix)? {
                    return Ok(Some(vpc_addr.to_path_buf()));
                }
            }
            VpcResourceAddress::Subnet { region, vpc_id, .. } => {
                let parent_vpc_addr = VpcResourceAddress::Vpc {
                    region: region.into(),
                    vpc_id: vpc_id.into(),
                };

                if let Some(VpcResourceAddress::Vpc { vpc_id: virt_vpc_id, .. }) = parent_vpc_addr.phy_to_virt(&self.prefix)? {
                    if let Some(VpcResourceAddress::Subnet {
                        subnet_id: virt_subnet_id,
                        ..
                    }) = addr.phy_to_virt(&self.prefix)?
                    {
                        return Ok(Some(
                            VpcResourceAddress::Subnet {
                                region:    region.to_string(),
                                vpc_id:    virt_vpc_id,
                                subnet_id: virt_subnet_id,
                            }
                            .to_path_buf(),
                        ));
                    }
                }
            }
            VpcResourceAddress::InternetGateway { region, igw_id } => {
                if let Some(igw_addr) = addr.phy_to_virt(&self.prefix)? {
                    return Ok(Some(igw_addr.to_path_buf()));
                }
            }
            VpcResourceAddress::RouteTable { region, vpc_id, .. } => {
                let parent_vpc_addr = VpcResourceAddress::Vpc {
                    region: region.into(),
                    vpc_id: vpc_id.into(),
                };

                if let Some(VpcResourceAddress::Vpc { vpc_id: virt_vpc_id, .. }) = parent_vpc_addr.phy_to_virt(&self.prefix)? {
                    if let Some(VpcResourceAddress::RouteTable {
                        region,
                        vpc_id,
                        rt_id: virt_rt_id,
                    }) = addr.phy_to_virt(&self.prefix)?
                    {
                        return Ok(Some(
                            VpcResourceAddress::RouteTable {
                                region: region.to_string(),
                                vpc_id: virt_vpc_id,
                                rt_id:  virt_rt_id,
                            }
                            .to_path_buf(),
                        ));
                    }
                }
            }

            VpcResourceAddress::SecurityGroup { region, vpc_id, .. } => {
                let parent_vpc_addr = VpcResourceAddress::Vpc {
                    region: region.into(),
                    vpc_id: vpc_id.into(),
                };

                if let Some(VpcResourceAddress::Vpc { vpc_id: virt_vpc_id, .. }) = parent_vpc_addr.phy_to_virt(&self.prefix)? {
                    if let Some(VpcResourceAddress::SecurityGroup { sg_id: virt_sg_id, .. }) = addr.phy_to_virt(&self.prefix)? {
                        return Ok(Some(
                            VpcResourceAddress::SecurityGroup {
                                region: region.to_string(),
                                vpc_id: virt_vpc_id,
                                sg_id:  virt_sg_id,
                            }
                            .to_path_buf(),
                        ));
                    }
                }
            }
        }

        Ok(None)
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        let region = String::from("[region]");
        let vpc_id = String::from("[vpc_id]");
        let sg_id = String::from("[security_group_id]");
        let rt_id = String::from("[route_table_id]");
        let igw_id = String::from("[internet_gateway_id]");

        res.push(skeleton!(
            VpcResourceAddress::Vpc { region, vpc_id },
            VpcResource::Vpc(Vpc {
                cidr_block: String::from("[cidr_block]"),
                instance_tenancy: None,
                enable_dns_support: false,
                enable_dns_hostnames: false,
                dhcp_options_id: None,
                tags: Tags::default(),
            })
        ));

        let region = String::from("[region]");
        let vpc_id = String::from("[vpc_id]");
        let subnet_id = String::from("[subnet_id]");
        res.push(skeleton!(
            VpcResourceAddress::Subnet {
                region,
                vpc_id,
                subnet_id
            },
            VpcResource::Subnet(Subnet {
                cidr_block: String::from("[cidr_block]"),
                tags: Tags::default(),
                availability_zone: String::from("[availability_zone]"),
                map_public_ip_on_launch: false,
            })
        ));

        let region = String::from("[region]");
        let vpc_id = String::from("[vpc_id]");
        res.push(skeleton!(
            VpcResourceAddress::SecurityGroup { region, vpc_id, sg_id },
            VpcResource::SecurityGroup(SecurityGroup {
                description: String::from("[description]"),
                ingress_rules: vec![SecurityGroupRule {
                    protocol: String::from("TCP"),
                    from_port: Some(8080),
                    to_port: Some(8080),
                    cidr_blocks: vec![String::from("[cidr_block]")],
                    security_group_ids: vec![String::from("[security_group_id]")],
                }],
                egress_rules: vec![],
                tags: Tags::default(),
            })
        ));

        let region = String::from("[region]");
        let vpc_id = String::from("[vpc_id]");
        res.push(skeleton!(
            VpcResourceAddress::RouteTable { region, vpc_id, rt_id },
            VpcResource::RouteTable(RouteTable {
                routes: vec![Route {
                    destination_cidr_block: Some(String::from("[cidr_block]")),
                    destination_ipv6_cidr_block: None,
                    gateway_id: Some(String::from("[gateway_id]")),
                    instance_id: None,
                    nat_gateway_id: None,
                }],
                associations: vec![],
                tags: Tags::default(),
            })
        ));

        let region = String::from("[region]");
        res.push(skeleton!(
            VpcResourceAddress::InternetGateway { region, igw_id },
            VpcResource::InternetGateway(InternetGateway {
                vpc_id: None,
                tags:   Tags::default(),
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> Result<bool, anyhow::Error> {
        let addr = VpcResourceAddress::from_path(addr)?;

        match addr {
            VpcResourceAddress::Vpc { .. } => ron_check_eq::<Vpc>(a, b),
            VpcResourceAddress::Subnet { .. } => ron_check_eq::<Subnet>(a, b),
            VpcResourceAddress::InternetGateway { .. } => ron_check_eq::<InternetGateway>(a, b),
            VpcResourceAddress::RouteTable { .. } => ron_check_eq::<RouteTable>(a, b),
            VpcResourceAddress::SecurityGroup { .. } => ron_check_eq::<SecurityGroup>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = VpcResourceAddress::from_path(addr)?;

        match addr {
            VpcResourceAddress::Vpc { .. } => ron_check_syntax::<Vpc>(a),
            VpcResourceAddress::Subnet { .. } => ron_check_syntax::<Subnet>(a),
            VpcResourceAddress::InternetGateway { .. } => ron_check_syntax::<InternetGateway>(a),
            VpcResourceAddress::RouteTable { .. } => ron_check_syntax::<RouteTable>(a),
            VpcResourceAddress::SecurityGroup { .. } => ron_check_syntax::<SecurityGroup>(a),
        }
    }
}
