use crate::addr::VpcResourceAddress;

use super::VpcConnector;

use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, glob::addr_matches_filter};

use aws_sdk_ec2::types::Filter;

impl VpcConnector {
    pub async fn do_list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();
        let config = self.config.read().await;

        for region_name in &config.enabled_regions {
            if !addr_matches_filter(&PathBuf::from(format!("aws/vpc/{}", region_name)), subpath) {
                continue;
            }
            let client = self.get_or_init_client(region_name).await.unwrap();

            let vpcs_resp = client.describe_vpcs().send().await?;
            if let Some(vpcs) = vpcs_resp.vpcs {
                for vpc in vpcs {
                    let Some(vpc_id) = vpc.vpc_id else {
                        continue;
                    };
                    results.push(
                        VpcResourceAddress::Vpc {
                            region: region_name.to_string(),
                            vpc_id: vpc_id.clone(),
                        }
                        .to_path_buf(),
                    );

                    let vpc_filter = Filter::builder().name("vpc-id").values(&vpc_id).build();

                    // List Subnets
                    let subnets_resp = client.describe_subnets().filters(vpc_filter.clone()).send().await?;
                    if let Some(subnets) = subnets_resp.subnets {
                        for subnet in subnets {
                            if let Some(subnet_id) = subnet.subnet_id {
                                results.push(
                                    VpcResourceAddress::Subnet {
                                        region: region_name.to_string(),
                                        vpc_id: vpc_id.clone(),
                                        subnet_id,
                                    }
                                    .to_path_buf(),
                                );
                            }
                        }
                    }

                    // List Route Tables
                    let route_tables_resp = client.describe_route_tables().filters(vpc_filter.clone()).send().await?;
                    if let Some(route_tables) = route_tables_resp.route_tables {
                        for rt in route_tables {
                            if let Some(rt_id) = rt.route_table_id {
                                results.push(
                                    VpcResourceAddress::RouteTable {
                                        region: region_name.clone(),
                                        vpc_id: vpc_id.clone(),
                                        rt_id,
                                    }
                                    .to_path_buf(),
                                );
                            }
                        }
                    }
                    let security_groups_resp = client.describe_security_groups().filters(vpc_filter).send().await?;
                    if let Some(security_groups) = security_groups_resp.security_groups {
                        for sg in security_groups {
                            if let Some(sg_id) = sg.group_id {
                                results.push(
                                    VpcResourceAddress::SecurityGroup {
                                        region: region_name.clone(),
                                        vpc_id: vpc_id.clone(),
                                        sg_id,
                                    }
                                    .to_path_buf(),
                                );
                            }
                        }
                    }
                }
            }

            let igws_resp = client.describe_internet_gateways().send().await?;
            if let Some(igws) = igws_resp.internet_gateways {
                for igw in igws {
                    if let Some(igw_id) = igw.internet_gateway_id {
                        results.push(
                            VpcResourceAddress::InternetGateway {
                                region: region_name.clone(),
                                igw_id,
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }
        }

        Ok(results)
    }
}
