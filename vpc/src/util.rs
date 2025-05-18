use std::path::Path;

use anyhow::{bail, Context};
use autoschematic_core::connector_util::load_resource_output_key;
use aws_sdk_ec2::types::{AttributeBooleanValue, Filter};

use super::{
    addr::VpcResourceAddress,
    resource::{InternetGateway, Route, RouteTable, SecurityGroup, SecurityGroupRule, Subnet, Vpc},
    tags::Tags,
};

pub fn bool_build(v: bool) -> AttributeBooleanValue {
    AttributeBooleanValue::builder().set_value(Some(v)).build()
}

pub fn bool_unpack(v: Option<AttributeBooleanValue>) -> bool {
    v.as_ref().and_then(|attr| attr.value).unwrap_or(false)
}

pub async fn get_vpc(client: &aws_sdk_ec2::Client, vpc_id: &str) -> anyhow::Result<Option<Vpc>> {
    let Ok(vpc_output) = client.describe_vpcs().vpc_ids(vpc_id).send().await else {
        return Ok(None);
    };

    if let Some(vpcs) = vpc_output.vpcs {
        if let Some(vpc) = vpcs.first() {
            let cidr_block = vpc.cidr_block.clone().unwrap_or_default();

            let instance_tenancy = if let Some(tenancy) = &vpc.instance_tenancy {
                tenancy.as_str().to_string()
            } else {
                "default".to_string()
            };

            // Get VPC attributes (DNS support and hostnames)
            let dns_support_resp = client
                .describe_vpc_attribute()
                .vpc_id(vpc_id)
                .attribute(aws_sdk_ec2::types::VpcAttributeName::EnableDnsSupport)
                .send()
                .await?;

            let dns_hostnames_resp = client
                .describe_vpc_attribute()
                .vpc_id(vpc_id)
                .attribute(aws_sdk_ec2::types::VpcAttributeName::EnableDnsHostnames)
                .send()
                .await?;

            let enable_dns_support = bool_unpack(dns_support_resp.enable_dns_support);

            let enable_dns_hostnames = bool_unpack(dns_hostnames_resp.enable_dns_hostnames);

            // Get tags
            let tags: Tags = vpc.tags.clone().into();

            let vpc_resource = Vpc {
                cidr_block,
                instance_tenancy,
                enable_dns_support,
                enable_dns_hostnames,
                tags,
            };

            Ok(Some(vpc_resource))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

pub async fn get_subnet(
    client: &aws_sdk_ec2::Client,
    vpc_id: &str,
    subnet_id: &str,
) -> anyhow::Result<Option<Subnet>> {
    let vpc_filter = Filter::builder().name("vpc-id").values(vpc_id).build();

    let Ok(subnet_resp) = client
        .describe_subnets()
        .filters(vpc_filter.clone())
        .subnet_ids(subnet_id)
        .send()
        .await
    else {
        // ?????? Is there seriously no error variant for "not found?? Come on amazon..."
        return Ok(None);
    };

    if let Some(subnets) = subnet_resp.subnets {
        if let Some(subnet) = subnets.first() {
            // Get subnet details
            let cidr_block = subnet.cidr_block.clone().unwrap_or_default();
            let availability_zone = subnet.availability_zone.clone().unwrap_or_default();
            let map_public_ip_on_launch = subnet.map_public_ip_on_launch.unwrap_or(false);

            let tags: Tags = subnet.tags.clone().into();

            let subnet_resource = Subnet {
                cidr_block,
                availability_zone,
                map_public_ip_on_launch,
                tags,
            };

            Ok(Some(subnet_resource))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

pub async fn get_igw(
    client: &aws_sdk_ec2::Client,
    igw_id: &str,
) -> anyhow::Result<Option<InternetGateway>> {
    let igw_resp = client
        .describe_internet_gateways()
        .internet_gateway_ids(igw_id)
        .send()
        .await?;

    if let Some(igws) = igw_resp.internet_gateways {
        if let Some(igw) = igws.first() {
            // Get VPC ID if attached
            let mut vpc_id = None;
            if let Some(attachments) = &igw.attachments {
                for attachment in attachments {
                    if attachment
                        .state
                        .as_ref()
                        .map_or(false, |state| state.as_str() == "attached")
                    {
                        vpc_id = attachment.vpc_id.clone();
                        break;
                    }
                }
            }

            // Get tags
            let tags: Tags = igw.tags.clone().into();

            let igw_resource = InternetGateway { vpc_id, tags };

            Ok(Some(igw_resource))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

pub async fn get_route_table(
    client: &aws_sdk_ec2::Client,
    vpc_id: &str,
    rt_id: &str,
) -> anyhow::Result<Option<RouteTable>> {
    let vpc_filter = Filter::builder().name("vpc-id").values(vpc_id).build();
    let Ok(rt_resp) = client
        .describe_route_tables()
        .filters(vpc_filter.clone())
        .route_table_ids(rt_id)
        .send()
        .await
    else {
        return Ok(None);
    };

    if let Some(route_tables) = rt_resp.route_tables {
        if let Some(rt) = route_tables.first() {
            // Get VPC ID

            // Get routes
            let mut routes = Vec::new();
            if let Some(aws_routes) = &rt.routes {
                for route in aws_routes {
                    let destination_cidr_block = route.destination_cidr_block.clone();
                    let destination_ipv6_cidr_block = route.destination_ipv6_cidr_block.clone();
                    let gateway_id = route.gateway_id.clone();
                    let instance_id = route.instance_id.clone();
                    let nat_gateway_id = route.nat_gateway_id.clone();

                    routes.push(Route {
                        destination_cidr_block,
                        destination_ipv6_cidr_block,
                        gateway_id,
                        instance_id,
                        nat_gateway_id,
                    });
                }
            }

            routes.sort();

            // Get associations
            let mut associations = Vec::new();
            if let Some(aws_associations) = &rt.associations {
                for assoc in aws_associations {
                    if let Some(assoc_id) = &assoc.route_table_association_id {
                        associations.push(assoc_id.clone());
                    }
                }
            }

            // Get tags
            let tags: Tags = rt.tags.clone().into();

            let rt_resource = RouteTable {
                routes,
                associations,
                tags,
            };
            Ok(Some(rt_resource))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

pub async fn get_security_group(
    client: &aws_sdk_ec2::Client,
    vpc_id: &str,
    sg_id: &str,
) -> anyhow::Result<Option<SecurityGroup>> {
    let vpc_filter = Filter::builder().name("vpc-id").values(vpc_id).build();
    let Ok(sg_resp) = client
        .describe_security_groups()
        .filters(vpc_filter)
        .group_ids(sg_id)
        .send()
        .await
    else {
        return Ok(None);
    };

    if let Some(security_groups) = sg_resp.security_groups {
        if let Some(sg) = security_groups.first() {
            // Get basic info
            let description = sg.description.clone().unwrap_or_default();

            // Get ingress rules
            let mut ingress_rules = Vec::new();
            if let Some(ip_permissions) = &sg.ip_permissions {
                for perm in ip_permissions {
                    let protocol = perm.ip_protocol.clone().unwrap_or_default();
                    let from_port = perm.from_port;
                    let to_port = perm.to_port;

                    let mut cidr_blocks = Vec::new();
                    if let Some(ip_ranges) = &perm.ip_ranges {
                        for ip_range in ip_ranges {
                            if let Some(cidr) = &ip_range.cidr_ip {
                                cidr_blocks.push(cidr.clone());
                            }
                        }
                    }

                    let mut security_group_ids = Vec::new();
                    if let Some(sg_references) = &perm.user_id_group_pairs {
                        for sg_ref in sg_references {
                            if let Some(sg_id) = &sg_ref.group_id {
                                security_group_ids.push(sg_id.clone());
                            }
                        }
                    }

                    ingress_rules.push(SecurityGroupRule {
                        protocol,
                        from_port,
                        to_port,
                        cidr_blocks,
                        security_group_ids,
                    });
                }
            }

            // Get egress rules
            let mut egress_rules = Vec::new();
            if let Some(ip_permissions_egress) = &sg.ip_permissions_egress {
                for perm in ip_permissions_egress {
                    let protocol = perm.ip_protocol.clone().unwrap_or_default();
                    let from_port = perm.from_port;
                    let to_port = perm.to_port;

                    let mut cidr_blocks = Vec::new();
                    if let Some(ip_ranges) = &perm.ip_ranges {
                        for ip_range in ip_ranges {
                            if let Some(cidr) = &ip_range.cidr_ip {
                                cidr_blocks.push(cidr.clone());
                            }
                        }
                    }

                    let mut security_group_ids = Vec::new();
                    if let Some(sg_references) = &perm.user_id_group_pairs {
                        for sg_ref in sg_references {
                            if let Some(sg_id) = &sg_ref.group_id {
                                security_group_ids.push(sg_id.clone());
                            }
                        }
                    }

                    egress_rules.push(SecurityGroupRule {
                        protocol,
                        from_port,
                        to_port,
                        cidr_blocks,
                        security_group_ids,
                    });
                }
            }

            // Get tags
            let tags: Tags = sg.tags.clone().into();

            let sg_resource = SecurityGroup {
                description,
                ingress_rules,
                egress_rules,
                tags,
            };
            Ok(Some(sg_resource))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

pub fn get_phy_vpc_id(
    prefix: &Path,
    region: &str,
    virt_vpc_id: &str,
) -> anyhow::Result<Option<String>> {
    let addr = VpcResourceAddress::Vpc(region.to_string(), virt_vpc_id.to_string());

    Ok(load_resource_output_key(prefix, &addr, "vpc_id")?)
}

pub fn get_phy_security_group_id(
    prefix: &Path,
    region: &str,
    virt_vpc_id: &str,
    virt_sg_id: &str,
) -> anyhow::Result<Option<String>> {
    let addr = VpcResourceAddress::SecurityGroup(
        region.to_string(),
        virt_vpc_id.to_string(),
        virt_sg_id.to_string(),
    );

    Ok(load_resource_output_key(prefix, &addr, "security_group_id")?)
}

pub fn get_phy_subnet_id(
    prefix: &Path,
    region: &str,
    virt_vpc_id: &str,
    virt_subnet_id: &str,
) -> anyhow::Result<Option<String>> {
    let addr = VpcResourceAddress::Subnet(
        region.to_string(),
        virt_vpc_id.to_string(),
        virt_subnet_id.to_string(),
    );

    Ok(load_resource_output_key(prefix, &addr, "subnet_id")?)
}

pub fn get_phy_route_table_id(
    prefix: &Path,
    region: &str,
    virt_vpc_id: &str,
    virt_route_table_id: &str,
) -> anyhow::Result<Option<String>> {
    let addr = VpcResourceAddress::RouteTable(
        region.to_string(),
        virt_vpc_id.to_string(),
        virt_route_table_id.to_string(),
    );

    Ok(load_resource_output_key(prefix, &addr, "route_table_id")?)
}

pub fn get_phy_internet_gateway_id(
    prefix: &Path,
    region: &str,
    virt_igw_id: &str,
) -> anyhow::Result<Option<String>> {
    let addr = VpcResourceAddress::InternetGateway(region.to_string(), virt_igw_id.to_string());

    Ok(load_resource_output_key(prefix, &addr, "internet_gateway_id")?)
}
