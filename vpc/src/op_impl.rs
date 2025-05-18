use anyhow::{bail, Context};
use aws_sdk_ec2::types::{AttributeBooleanValue, IpPermission, IpRange, Tag, UserIdGroupPair};
use std::collections::HashMap;

use super::{
    resource::{InternetGateway, Route, RouteTable, SecurityGroup, SecurityGroupRule, Subnet, Vpc},
    tags::Tags,
};
use autoschematic_core::{
    connector::{OpExecOutput, Resource},
    op_exec_output,
};

/// Creates a VPC using the provided configuration
pub async fn create_vpc(
    client: &aws_sdk_ec2::Client,
    vpc: &Vpc,
) -> Result<OpExecOutput, anyhow::Error> {
    // Create a new VPC with the specified CIDR block and instance tenancy
    let create_vpc_resp = client
        .create_vpc()
        .cidr_block(vpc.cidr_block.clone())
        .instance_tenancy(aws_sdk_ec2::types::Tenancy::from(
            vpc.instance_tenancy.as_str(),
        ))
        .send()
        .await?;

    let Some(new_vpc) = create_vpc_resp.vpc else {
        bail!("Failed to create VPC: response did not contain VPC details");
    };

    let Some(new_vpc_id) = new_vpc.vpc_id else {
        bail!("Failed to create VPC: response did not contain VPC ID");
    };

    // Apply DNS settings
    if vpc.enable_dns_support {
        client
            .modify_vpc_attribute()
            .vpc_id(&new_vpc_id)
            .enable_dns_support(AttributeBooleanValue::builder().value(true).build())
            .send()
            .await?;
    }

    if vpc.enable_dns_hostnames {
        client
            .modify_vpc_attribute()
            .vpc_id(&new_vpc_id)
            .enable_dns_hostnames(AttributeBooleanValue::builder().value(true).build())
            .send()
            .await?;
    }

    // Apply tags
    let aws_tags: Option<Vec<Tag>> = vpc.tags.clone().into();
    let aws_tags = aws_tags.unwrap_or_default();

    if !aws_tags.is_empty() {
        client
            .create_tags()
            .resources(new_vpc_id.clone())
            .set_tags(Some(aws_tags))
            .send()
            .await?;
    }

    let mut outputs = HashMap::new();
    outputs.insert(String::from("vpc_id"), Some(new_vpc_id.clone()));

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!("Created VPC {}", new_vpc_id)),
    })
}

/// Updates VPC tags
pub async fn update_vpc_tags(
    client: &aws_sdk_ec2::Client,
    vpc_id: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
    let (delete_keys, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Convert delete_keys to Tags for delete_tags API
    let mut tags_to_remove = Vec::new();
    for key in delete_keys {
        tags_to_remove.push(
            Tag::builder()
                .key(key)
                .value("") // Value doesn't matter for delete
                .build(),
        );
    }

    // Delete tags if needed
    if !tags_to_remove.is_empty() {
        client
            .delete_tags()
            .resources(vpc_id)
            .set_tags(Some(tags_to_remove))
            .send()
            .await?;
    }

    // Add/update tags if needed
    if !tags_to_add.is_empty() {
        client
            .create_tags()
            .resources(vpc_id)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    return op_exec_output!(format!("Updated tags for VPC {}", vpc_id));
}

/// Updates VPC attributes
pub async fn update_vpc_attributes(
    client: &aws_sdk_ec2::Client,
    vpc_id: &str,
    enable_dns_support: Option<bool>,
    enable_dns_hostnames: Option<bool>,
) -> Result<OpExecOutput, anyhow::Error> {
    if let Some(enable_dns_support) = enable_dns_support {
        client
            .modify_vpc_attribute()
            .vpc_id(vpc_id)
            .enable_dns_support(
                AttributeBooleanValue::builder()
                    .value(enable_dns_support)
                    .build(),
            )
            .send()
            .await?;
    }

    if let Some(enable_dns_hostnames) = enable_dns_hostnames {
        client
            .modify_vpc_attribute()
            .vpc_id(vpc_id)
            .enable_dns_hostnames(
                AttributeBooleanValue::builder()
                    .value(enable_dns_hostnames)
                    .build(),
            )
            .send()
            .await?;
    }

    return op_exec_output!(format!("Updated attributes for VPC {}", vpc_id));
}

/// Deletes a VPC
pub async fn delete_vpc(
    client: &aws_sdk_ec2::Client,
    vpc_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client.delete_vpc().vpc_id(vpc_id).send().await?;

    return op_exec_output!(
        Some([(String::from("vpc"), Option::<String>::None)]),
        format!("Deleted VPC {}", vpc_id)
    );
}

/// Creates a subnet
pub async fn create_subnet(
    client: &aws_sdk_ec2::Client,
    vpc_id: &str,
    subnet: &Subnet,
) -> Result<OpExecOutput, anyhow::Error> {
    let create_subnet_resp = client
        .create_subnet()
        .vpc_id(vpc_id)
        .cidr_block(&subnet.cidr_block)
        .availability_zone(&subnet.availability_zone)
        .send()
        .await?;

    let Some(new_subnet) = create_subnet_resp.subnet else {
        bail!("Failed to create subnet: response did not contain subnet details");
    };

    let Some(new_subnet_id) = new_subnet.subnet_id else {
        bail!("Failed to create subnet: response did not contain subnet ID");
    };

    // Apply map_public_ip_on_launch setting if needed
    if subnet.map_public_ip_on_launch {
        client
            .modify_subnet_attribute()
            .subnet_id(&new_subnet_id)
            .map_public_ip_on_launch(
                AttributeBooleanValue::builder()
                    .value(subnet.map_public_ip_on_launch)
                    .build(),
            )
            .send()
            .await?;
    }

    // Apply tags
    let aws_tags: Option<Vec<Tag>> = subnet.tags.clone().into();
    let aws_tags = aws_tags.unwrap_or_default();

    if !aws_tags.is_empty() {
        client
            .create_tags()
            .resources(new_subnet_id.clone())
            .set_tags(Some(aws_tags))
            .send()
            .await?;
    }

    let mut outputs = HashMap::new();
    outputs.insert(String::from("subnet_id"), Some(new_subnet_id.clone()));

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!(
            "Created subnet {} in VPC {}",
            new_subnet_id, vpc_id
        )),
    })
}

/// Updates subnet tags
pub async fn update_subnet_tags(
    client: &aws_sdk_ec2::Client,
    subnet_id: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
    let (delete_keys, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Convert delete_keys to Tags for delete_tags API
    let mut tags_to_remove = Vec::new();
    for key in delete_keys {
        tags_to_remove.push(
            Tag::builder()
                .key(key)
                .value("") // Value doesn't matter for delete
                .build(),
        );
    }

    // Delete tags if needed
    if !tags_to_remove.is_empty() {
        client
            .delete_tags()
            .resources(subnet_id)
            .set_tags(Some(tags_to_remove))
            .send()
            .await?;
    }

    // Add/update tags if needed
    if !tags_to_add.is_empty() {
        client
            .create_tags()
            .resources(subnet_id)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated tags for subnet {}", subnet_id)),
    })
}

/// Updates subnet attributes
pub async fn update_subnet_attributes(
    client: &aws_sdk_ec2::Client,
    subnet_id: &str,
    map_public_ip_on_launch: Option<bool>,
) -> Result<OpExecOutput, anyhow::Error> {
    if let Some(map_public_ip_on_launch) = map_public_ip_on_launch {
        client
            .modify_subnet_attribute()
            .subnet_id(subnet_id)
            .map_public_ip_on_launch(
                AttributeBooleanValue::builder()
                    .value(map_public_ip_on_launch)
                    .build(),
            )
            .send()
            .await?;
    }

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated attributes for subnet {}", subnet_id)),
    })
}

/// Deletes a subnet
pub async fn delete_subnet(
    client: &aws_sdk_ec2::Client,
    subnet_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client.delete_subnet().subnet_id(subnet_id).send().await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted subnet {}", subnet_id)),
    })
}

/// Creates an internet gateway
pub async fn create_internet_gateway(
    client: &aws_sdk_ec2::Client,
    igw: &InternetGateway,
) -> Result<OpExecOutput, anyhow::Error> {
    let create_igw_resp = client.create_internet_gateway().send().await?;

    let Some(new_igw) = create_igw_resp.internet_gateway else {
        bail!(
            "Failed to create internet gateway: response did not contain internet gateway details"
        );
    };

    let Some(new_igw_id) = new_igw.internet_gateway_id else {
        bail!("Failed to create internet gateway: response did not contain internet gateway ID");
    };

    // Apply tags
    let aws_tags: Option<Vec<Tag>> = igw.tags.clone().into();
    let aws_tags = aws_tags.unwrap_or_default();

    if !aws_tags.is_empty() {
        client
            .create_tags()
            .resources(new_igw_id.clone())
            .set_tags(Some(aws_tags))
            .send()
            .await?;
    }

    // Attach to VPC if specified
    if let Some(vpc_id) = &igw.vpc_id {
        client
            .attach_internet_gateway()
            .internet_gateway_id(&new_igw_id)
            .vpc_id(vpc_id)
            .send()
            .await?;
    }

    let mut outputs = HashMap::new();
    outputs.insert(
        String::from("internet_gateway_id"),
        Some(new_igw_id.clone()),
    );

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!("Created internet gateway {}", new_igw_id)),
    })
}

/// Attaches an internet gateway to a VPC
pub async fn attach_internet_gateway(
    client: &aws_sdk_ec2::Client,
    igw_id: &str,
    vpc_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .attach_internet_gateway()
        .internet_gateway_id(igw_id)
        .vpc_id(vpc_id)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Attached internet gateway {} to VPC {}",
            igw_id, vpc_id
        )),
    })
}

/// Detaches an internet gateway from a VPC
pub async fn detach_internet_gateway(
    client: &aws_sdk_ec2::Client,
    igw_id: &str,
    vpc_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .detach_internet_gateway()
        .internet_gateway_id(igw_id)
        .vpc_id(vpc_id)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Detached internet gateway {} from VPC {}",
            igw_id, vpc_id
        )),
    })
}

/// Updates internet gateway tags
pub async fn update_internet_gateway_tags(
    client: &aws_sdk_ec2::Client,
    igw_id: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
    let (delete_keys, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Convert delete_keys to Tags for delete_tags API
    let mut tags_to_remove = Vec::new();
    for key in delete_keys {
        tags_to_remove.push(
            Tag::builder()
                .key(key)
                .value("") // Value doesn't matter for delete
                .build(),
        );
    }

    // Delete tags if needed
    if !tags_to_remove.is_empty() {
        client
            .delete_tags()
            .resources(igw_id)
            .set_tags(Some(tags_to_remove))
            .send()
            .await?;
    }

    // Add/update tags if needed
    if !tags_to_add.is_empty() {
        client
            .create_tags()
            .resources(igw_id)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated tags for internet gateway {}", igw_id)),
    })
}

/// Deletes an internet gateway
pub async fn delete_internet_gateway(
    client: &aws_sdk_ec2::Client,
    igw_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    // First, need to check if it's attached and detach if necessary
    let igw_resp = client
        .describe_internet_gateways()
        .internet_gateway_ids(igw_id)
        .send()
        .await?;

    if let Some(igws) = igw_resp.internet_gateways {
        if let Some(igw) = igws.first() {
            if let Some(attachments) = &igw.attachments {
                for attachment in attachments {
                    if let Some(vpc_id) = &attachment.vpc_id {
                        // Detach from VPC
                        client
                            .detach_internet_gateway()
                            .internet_gateway_id(igw_id)
                            .vpc_id(vpc_id)
                            .send()
                            .await?;
                    }
                }
            }
        }
    }

    // Now delete the internet gateway
    client
        .delete_internet_gateway()
        .internet_gateway_id(igw_id)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted internet gateway {}", igw_id)),
    })
}

/// Creates a route table
pub async fn create_route_table(
    client: &aws_sdk_ec2::Client,
    rt: &RouteTable,
    vpc_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    let create_rt_resp = client.create_route_table().vpc_id(vpc_id).send().await?;

    let Some(new_rt) = create_rt_resp.route_table else {
        bail!("Failed to create route table: response did not contain route table details");
    };

    let Some(new_rt_id) = new_rt.route_table_id else {
        bail!("Failed to create route table: response did not contain route table ID");
    };

    // Apply tags
    let aws_tags: Option<Vec<Tag>> = rt.tags.clone().into();
    let aws_tags = aws_tags.unwrap_or_default();

    if !aws_tags.is_empty() {
        client
            .create_tags()
            .resources(new_rt_id.clone())
            .set_tags(Some(aws_tags))
            .send()
            .await?;
    }

    // Create routes
    for route in &rt.routes {
        let mut create_route = client.create_route().route_table_id(&new_rt_id);

        if let Some(destination_cidr_block) = &route.destination_cidr_block {
            create_route = create_route.destination_cidr_block(destination_cidr_block);
        }

        if let Some(destination_ipv6_cidr_block) = &route.destination_ipv6_cidr_block {
            create_route = create_route.destination_ipv6_cidr_block(destination_ipv6_cidr_block);
        }

        if let Some(gateway_id) = &route.gateway_id {
            create_route = create_route.gateway_id(gateway_id);
        }

        if let Some(instance_id) = &route.instance_id {
            create_route = create_route.instance_id(instance_id);
        }

        if let Some(nat_gateway_id) = &route.nat_gateway_id {
            create_route = create_route.nat_gateway_id(nat_gateway_id);
        }

        create_route.send().await?;
    }

    // Associate with subnets
    for subnet_id in &rt.associations {
        if subnet_id.starts_with("subnet-") {
            client
                .associate_route_table()
                .route_table_id(&new_rt_id)
                .subnet_id(subnet_id)
                .send()
                .await?;
        }
    }

    let mut outputs = HashMap::new();
    outputs.insert(String::from("route_table_id"), Some(new_rt_id.clone()));

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!(
            "Created route table {} in VPC {}",
            new_rt_id, vpc_id
        )),
    })
}

/// Updates route table tags
pub async fn update_route_table_tags(
    client: &aws_sdk_ec2::Client,
    rt_id: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
    let (delete_keys, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Convert delete_keys to Tags for delete_tags API
    let mut tags_to_remove = Vec::new();
    for key in delete_keys {
        tags_to_remove.push(
            Tag::builder()
                .key(key)
                .value("") // Value doesn't matter for delete
                .build(),
        );
    }

    // Delete tags if needed
    if !tags_to_remove.is_empty() {
        client
            .delete_tags()
            .resources(rt_id)
            .set_tags(Some(tags_to_remove))
            .send()
            .await?;
    }

    // Add/update tags if needed
    if !tags_to_add.is_empty() {
        client
            .create_tags()
            .resources(rt_id)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated tags for route table {}", rt_id)),
    })
}

/// Creates a route in a route table
pub async fn create_route(
    client: &aws_sdk_ec2::Client,
    rt_id: &str,
    route: &Route,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut create_route = client.create_route().route_table_id(rt_id);

    if let Some(destination_cidr_block) = &route.destination_cidr_block {
        create_route = create_route.destination_cidr_block(destination_cidr_block);
    }

    if let Some(destination_ipv6_cidr_block) = &route.destination_ipv6_cidr_block {
        create_route = create_route.destination_ipv6_cidr_block(destination_ipv6_cidr_block);
    }

    if let Some(gateway_id) = &route.gateway_id {
        create_route = create_route.gateway_id(gateway_id);
    }

    if let Some(instance_id) = &route.instance_id {
        create_route = create_route.instance_id(instance_id);
    }

    if let Some(nat_gateway_id) = &route.nat_gateway_id {
        create_route = create_route.nat_gateway_id(nat_gateway_id);
    }

    create_route.send().await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Created route in route table {}", rt_id)),
    })
}

/// Deletes a route from a route table
pub async fn delete_route(
    client: &aws_sdk_ec2::Client,
    rt_id: &str,
    route: &Route,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut builder = client.delete_route().route_table_id(rt_id);

    if let Some(destination_cidr_block) = &route.destination_cidr_block {
        builder = builder.destination_cidr_block(destination_cidr_block);
    }

    if let Some(destination_ipv6_cidr_block) = &route.destination_ipv6_cidr_block {
        builder = builder.destination_ipv6_cidr_block(destination_ipv6_cidr_block);
    }

    builder.send().await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted route from route table {}", rt_id)),
    })
}

/// Associates a route table with a subnet
pub async fn associate_route_table(
    client: &aws_sdk_ec2::Client,
    rt_id: &str,
    subnet_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    let resp = client
        .associate_route_table()
        .route_table_id(rt_id)
        .subnet_id(subnet_id)
        .send()
        .await?;

    let association_id = resp
        .association_id
        .context("Failed to get association ID from route table association response")?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Associated route table {} with subnet {}",
            rt_id, subnet_id
        )),
    })
}

/// Disassociates a route table association
pub async fn disassociate_route_table(
    client: &aws_sdk_ec2::Client,
    association_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .disassociate_route_table()
        .association_id(association_id)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Disassociated route table association {}",
            association_id
        )),
    })
}

/// Deletes a route table
pub async fn delete_route_table(
    client: &aws_sdk_ec2::Client,
    rt_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    // First, need to disassociate any associated subnets
    let rt_resp = client
        .describe_route_tables()
        .route_table_ids(rt_id)
        .send()
        .await?;

    if let Some(route_tables) = rt_resp.route_tables {
        if let Some(rt) = route_tables.first() {
            if let Some(associations) = &rt.associations {
                for assoc in associations {
                    if let Some(assoc_id) = &assoc.route_table_association_id {
                        // Disassociate route table
                        client
                            .disassociate_route_table()
                            .association_id(assoc_id)
                            .send()
                            .await?;
                    }
                }
            }
        }
    }

    // Now delete the route table
    client
        .delete_route_table()
        .route_table_id(rt_id)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted route table {}", rt_id)),
    })
}

/// Creates a security group
pub async fn create_security_group(
    client: &aws_sdk_ec2::Client,
    sg: &SecurityGroup,
    vpc_id: &str,
    sg_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    let sg_id = if sg_id.starts_with("sg-") {
        sg_id.strip_prefix("sg-").unwrap()
    } else {
        sg_id
    };

    let create_sg_resp = client
        .create_security_group()
        .vpc_id(vpc_id)
        .group_name(sg_id)
        .description(&sg.description)
        .send()
        .await?;

    let new_sg_id = create_sg_resp
        .group_id
        .context("Failed to get security group ID from create response")?;

    // Apply tags
    let aws_tags: Option<Vec<Tag>> = sg.tags.clone().into();
    let aws_tags = aws_tags.unwrap_or_default();

    if !aws_tags.is_empty() {
        client
            .create_tags()
            .resources(new_sg_id.clone())
            .set_tags(Some(aws_tags))
            .send()
            .await?;
    }

    // Add ingress rules
    for rule in &sg.ingress_rules {
        let mut ip_permissions = IpPermission::builder().ip_protocol(&rule.protocol);

        if let Some(from_port) = rule.from_port {
            ip_permissions = ip_permissions.from_port(from_port);
        }
        if let Some(to_port) = rule.to_port {
            ip_permissions = ip_permissions.to_port(to_port);
        }

        // Add CIDR ranges
        let mut ip_ranges = Vec::new();
        for cidr in &rule.cidr_blocks {
            ip_ranges.push(IpRange::builder().cidr_ip(cidr).build());
        }
        if !ip_ranges.is_empty() {
            ip_permissions = ip_permissions.set_ip_ranges(Some(ip_ranges));
        }

        // Add security group references
        let mut user_id_group_pairs = Vec::new();
        for sg_id in &rule.security_group_ids {
            user_id_group_pairs.push(UserIdGroupPair::builder().group_id(sg_id).build());
        }
        if !user_id_group_pairs.is_empty() {
            ip_permissions = ip_permissions.set_user_id_group_pairs(Some(user_id_group_pairs));
        }

        let ip_permission = ip_permissions.build();
        client
            .authorize_security_group_ingress()
            .group_id(&new_sg_id)
            .ip_permissions(ip_permission)
            .send()
            .await?;
    }

    // Add egress rules
    for rule in &sg.egress_rules {
        let mut ip_permissions = IpPermission::builder().ip_protocol(&rule.protocol);

        if let Some(from_port) = rule.from_port {
            ip_permissions = ip_permissions.from_port(from_port);
        }
        if let Some(to_port) = rule.to_port {
            ip_permissions = ip_permissions.to_port(to_port);
        }

        // Add CIDR ranges
        let mut ip_ranges = Vec::new();
        for cidr in &rule.cidr_blocks {
            ip_ranges.push(IpRange::builder().cidr_ip(cidr).build());
        }
        if !ip_ranges.is_empty() {
            ip_permissions = ip_permissions.set_ip_ranges(Some(ip_ranges));
        }

        // Add security group references
        let mut user_id_group_pairs = Vec::new();
        for sg_id in &rule.security_group_ids {
            user_id_group_pairs.push(UserIdGroupPair::builder().group_id(sg_id).build());
        }
        if !user_id_group_pairs.is_empty() {
            ip_permissions = ip_permissions.set_user_id_group_pairs(Some(user_id_group_pairs));
        }

        let ip_permission = ip_permissions.build();
        client
            .authorize_security_group_egress()
            .group_id(&new_sg_id)
            .ip_permissions(ip_permission)
            .send()
            .await?;
    }

    let mut outputs = HashMap::new();
    outputs.insert(String::from("security_group_id"), Some(new_sg_id.clone()));

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!(
            "Created security group {} in VPC {}",
            new_sg_id, vpc_id
        )),
    })
}

/// Updates security group tags
pub async fn update_security_group_tags(
    client: &aws_sdk_ec2::Client,
    sg_id: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
    let (delete_keys, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Convert delete_keys to Tags for delete_tags API
    let mut tags_to_remove = Vec::new();
    for key in delete_keys {
        tags_to_remove.push(
            Tag::builder()
                .key(key)
                .value("") // Value doesn't matter for delete
                .build(),
        );
    }

    // Delete tags if needed
    if !tags_to_remove.is_empty() {
        client
            .delete_tags()
            .resources(sg_id)
            .set_tags(Some(tags_to_remove))
            .send()
            .await?;
    }

    // Add/update tags if needed
    if !tags_to_add.is_empty() {
        client
            .create_tags()
            .resources(sg_id)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated tags for security group {}", sg_id)),
    })
}

/// Authorizes an ingress rule for a security group
pub async fn authorize_security_group_ingress(
    client: &aws_sdk_ec2::Client,
    sg_id: &str,
    rule: &SecurityGroupRule,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut ip_permissions = IpPermission::builder().ip_protocol(&rule.protocol);

    if let Some(from_port) = rule.from_port {
        ip_permissions = ip_permissions.from_port(from_port);
    }
    if let Some(to_port) = rule.to_port {
        ip_permissions = ip_permissions.to_port(to_port);
    }

    // Add CIDR ranges
    let mut ip_ranges = Vec::new();
    for cidr in &rule.cidr_blocks {
        ip_ranges.push(IpRange::builder().cidr_ip(cidr).build());
    }
    if !ip_ranges.is_empty() {
        ip_permissions = ip_permissions.set_ip_ranges(Some(ip_ranges));
    }

    // Add security group references
    let mut user_id_group_pairs = Vec::new();
    for sg_id in &rule.security_group_ids {
        user_id_group_pairs.push(UserIdGroupPair::builder().group_id(sg_id).build());
    }
    if !user_id_group_pairs.is_empty() {
        ip_permissions = ip_permissions.set_user_id_group_pairs(Some(user_id_group_pairs));
    }

    let ip_permission = ip_permissions.build();
    client
        .authorize_security_group_ingress()
        .group_id(sg_id)
        .ip_permissions(ip_permission)
        .send()
        .await?;

    let protocol = &rule.protocol;
    let port_range = match (rule.from_port, rule.to_port) {
        (Some(from), Some(to)) if from == to => format!("port {}", from),
        (Some(from), Some(to)) => format!("ports {}-{}", from, to),
        _ => "all ports".to_string(),
    };

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Added ingress rule for {} on {} in security group {}",
            protocol, port_range, sg_id
        )),
    })
}

/// Authorizes an egress rule for a security group
pub async fn authorize_security_group_egress(
    client: &aws_sdk_ec2::Client,
    sg_id: &str,
    rule: &SecurityGroupRule,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut ip_permissions = IpPermission::builder().ip_protocol(&rule.protocol);

    if let Some(from_port) = rule.from_port {
        ip_permissions = ip_permissions.from_port(from_port);
    }
    if let Some(to_port) = rule.to_port {
        ip_permissions = ip_permissions.to_port(to_port);
    }

    // Add CIDR ranges
    let mut ip_ranges = Vec::new();
    for cidr in &rule.cidr_blocks {
        ip_ranges.push(IpRange::builder().cidr_ip(cidr).build());
    }
    if !ip_ranges.is_empty() {
        ip_permissions = ip_permissions.set_ip_ranges(Some(ip_ranges));
    }

    // Add security group references
    let mut user_id_group_pairs = Vec::new();
    for sg_id in &rule.security_group_ids {
        user_id_group_pairs.push(UserIdGroupPair::builder().group_id(sg_id).build());
    }
    if !user_id_group_pairs.is_empty() {
        ip_permissions = ip_permissions.set_user_id_group_pairs(Some(user_id_group_pairs));
    }

    let ip_permission = ip_permissions.build();
    client
        .authorize_security_group_egress()
        .group_id(sg_id)
        .ip_permissions(ip_permission)
        .send()
        .await?;

    let protocol = &rule.protocol;
    let port_range = match (rule.from_port, rule.to_port) {
        (Some(from), Some(to)) if from == to => format!("port {}", from),
        (Some(from), Some(to)) => format!("ports {}-{}", from, to),
        _ => "all ports".to_string(),
    };

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Added egress rule for {} on {} in security group {}",
            protocol, port_range, sg_id
        )),
    })
}

/// Revokes an ingress rule from a security group
pub async fn revoke_security_group_ingress(
    client: &aws_sdk_ec2::Client,
    sg_id: &str,
    rule: &SecurityGroupRule,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut ip_permissions = IpPermission::builder().ip_protocol(&rule.protocol);

    if let Some(from_port) = rule.from_port {
        ip_permissions = ip_permissions.from_port(from_port);
    }
    if let Some(to_port) = rule.to_port {
        ip_permissions = ip_permissions.to_port(to_port);
    }

    // Add CIDR ranges
    let mut ip_ranges = Vec::new();
    for cidr in &rule.cidr_blocks {
        ip_ranges.push(IpRange::builder().cidr_ip(cidr).build());
    }
    if !ip_ranges.is_empty() {
        ip_permissions = ip_permissions.set_ip_ranges(Some(ip_ranges));
    }

    // Add security group references
    let mut user_id_group_pairs = Vec::new();
    for sg_id in &rule.security_group_ids {
        user_id_group_pairs.push(UserIdGroupPair::builder().group_id(sg_id).build());
    }
    if !user_id_group_pairs.is_empty() {
        ip_permissions = ip_permissions.set_user_id_group_pairs(Some(user_id_group_pairs));
    }

    let ip_permission = ip_permissions.build();
    client
        .revoke_security_group_ingress()
        .group_id(sg_id)
        .ip_permissions(ip_permission)
        .send()
        .await?;

    let protocol = &rule.protocol;
    let port_range = match (rule.from_port, rule.to_port) {
        (Some(from), Some(to)) if from == to => format!("port {}", from),
        (Some(from), Some(to)) => format!("ports {}-{}", from, to),
        _ => "all ports".to_string(),
    };

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Removed ingress rule for {} on {} from security group {}",
            protocol, port_range, sg_id
        )),
    })
}

/// Revokes an egress rule from a security group
pub async fn revoke_security_group_egress(
    client: &aws_sdk_ec2::Client,
    sg_id: &str,
    rule: &SecurityGroupRule,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut ip_permissions = IpPermission::builder().ip_protocol(&rule.protocol);

    if let Some(from_port) = rule.from_port {
        ip_permissions = ip_permissions.from_port(from_port);
    }
    if let Some(to_port) = rule.to_port {
        ip_permissions = ip_permissions.to_port(to_port);
    }

    // Add CIDR ranges
    let mut ip_ranges = Vec::new();
    for cidr in &rule.cidr_blocks {
        ip_ranges.push(IpRange::builder().cidr_ip(cidr).build());
    }
    if !ip_ranges.is_empty() {
        ip_permissions = ip_permissions.set_ip_ranges(Some(ip_ranges));
    }

    // Add security group references
    let mut user_id_group_pairs = Vec::new();
    for sg_id in &rule.security_group_ids {
        user_id_group_pairs.push(UserIdGroupPair::builder().group_id(sg_id).build());
    }
    if !user_id_group_pairs.is_empty() {
        ip_permissions = ip_permissions.set_user_id_group_pairs(Some(user_id_group_pairs));
    }

    let ip_permission = ip_permissions.build();
    client
        .revoke_security_group_egress()
        .group_id(sg_id)
        .ip_permissions(ip_permission)
        .send()
        .await?;

    let protocol = &rule.protocol;
    let port_range = match (rule.from_port, rule.to_port) {
        (Some(from), Some(to)) if from == to => format!("port {}", from),
        (Some(from), Some(to)) => format!("ports {}-{}", from, to),
        _ => "all ports".to_string(),
    };

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Removed egress rule for {} on {} from security group {}",
            protocol, port_range, sg_id
        )),
    })
}

/// Deletes a security group
pub async fn delete_security_group(
    client: &aws_sdk_ec2::Client,
    sg_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_security_group()
        .group_id(sg_id)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted security group {}", sg_id)),
    })
}
