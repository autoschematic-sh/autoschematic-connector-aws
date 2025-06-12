use std::{collections::HashMap, path::Path};

use autoschematic_core::{
    connector::{GetResourceOutput, Resource, ResourceAddress},
    get_resource_output,
};

use crate::addr::RdsResourceAddress;
use anyhow::Context;
use aws_sdk_rds::types::DbInstance;

use super::RdsConnector;

impl RdsConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = RdsResourceAddress::from_path(addr)?;
        match addr {
            RdsResourceAddress::DBInstance { region, id } => {
                let client = self.get_or_init_client(&region).await?;

                let resp = client.describe_db_instances().db_instance_identifier(&id).send().await?;

                let db_instance = resp.db_instances.and_then(|instances| instances.first().cloned());

                match db_instance {
                    Some(db_instance) => {
                        let instance = crate::resource::RdsResource::DBInstance(map_db_instance(&db_instance)?);
                        get_resource_output!(instance, [(String::from("id"), Some(id))])
                    }
                    None => Ok(None),
                }
            }
            RdsResourceAddress::DBCluster { region, id } => {
                let client = self.get_or_init_client(&region).await?;

                let resp = client.describe_db_clusters().db_cluster_identifier(&id).send().await?;

                let Some(db_clusters) = resp.db_clusters else {
                    return Ok(None);
                };

                let Some(cluster) = db_clusters.first() else {
                    return Ok(None);
                };

                let cluster = crate::resource::RdsResource::DBCluster(map_db_cluster(cluster)?);
                get_resource_output!(cluster, [(String::from("id"), Some(id))])
            }
            RdsResourceAddress::DBSubnetGroup { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                let resp = client.describe_db_subnet_groups().db_subnet_group_name(&name).send().await?;

                let Some(db_subnet_groups) = resp.db_subnet_groups else {
                    return Ok(None);
                };

                let Some(db_subnet_group) = db_subnet_groups.first() else {
                    return Ok(None);
                };

                let group = crate::resource::RdsResource::DBSubnetGroup(map_db_subnet_group(db_subnet_group)?);
                get_resource_output!(group, [(String::from("name"), Some(name))])
            }
            RdsResourceAddress::DBParameterGroup { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                let resp = client
                    .describe_db_parameter_groups()
                    .db_parameter_group_name(&name)
                    .send()
                    .await?;

                let Some(db_parameter_groups) = resp.db_parameter_groups else {
                    return Ok(None);
                };

                let Some(db_parameter_group) = db_parameter_groups.first() else {
                    return Ok(None);
                };

                let group = crate::resource::RdsResource::DBParameterGroup(map_db_parameter_group(db_parameter_group)?);
                get_resource_output!(group, [(String::from("name"), Some(name))])
            }
        }
    }
}

fn map_db_instance(db_instance: &DbInstance) -> Result<crate::resource::RdsDBInstance, anyhow::Error> {
    Ok(crate::resource::RdsDBInstance {
        engine: db_instance.engine().unwrap_or_default().to_string(),
        instance_class: db_instance.db_instance_class().unwrap_or_default().to_string(),
        allocated_storage: db_instance.allocated_storage(),
        master_username: db_instance.master_username().map(|s| s.to_string()),
        port: db_instance.db_instance_port(),
        publicly_accessible: db_instance.publicly_accessible(),
        storage_type: db_instance.storage_type().map(|s| s.to_string()),
        backup_retention_period: db_instance.backup_retention_period(),
        preferred_backup_window: db_instance.preferred_backup_window().map(|s| s.to_string()),
        preferred_maintenance_window: db_instance.preferred_maintenance_window().map(|s| s.to_string()),
        multi_az: db_instance.multi_az(),
        storage_encrypted: db_instance.storage_encrypted(),
        tags: db_instance.tag_list.clone().into(),
    })
}

fn map_db_cluster(db_cluster: &aws_sdk_rds::types::DbCluster) -> Result<crate::resource::RdsDBCluster, anyhow::Error> {
    Ok(crate::resource::RdsDBCluster {
        engine: db_cluster.engine().unwrap_or_default().to_string(),
        engine_version: db_cluster.engine_version().map(|s| s.to_string()),
        port: db_cluster.port(),
        master_username: db_cluster.master_username().map(|s| s.to_string()),
        backup_retention_period: db_cluster.backup_retention_period(),
        preferred_backup_window: db_cluster.preferred_backup_window().map(|s| s.to_string()),
        preferred_maintenance_window: db_cluster.preferred_maintenance_window().map(|s| s.to_string()),
        storage_encrypted: db_cluster.storage_encrypted(),
        deletion_protection: db_cluster.deletion_protection(),
        tags: db_cluster.tag_list.clone().into(),
    })
}

fn map_db_subnet_group(
    db_subnet_group: &aws_sdk_rds::types::DbSubnetGroup,
) -> Result<crate::resource::RdsDBSubnetGroup, anyhow::Error> {
    Ok(crate::resource::RdsDBSubnetGroup {
        description: db_subnet_group.db_subnet_group_description().unwrap_or_default().to_string(),
        subnet_ids: db_subnet_group
            .subnets()
            .iter()
            .map(|subnet| subnet.subnet_identifier().unwrap_or_default().to_string())
            .collect(),
    })
}

fn map_db_parameter_group(
    db_parameter_group: &aws_sdk_rds::types::DbParameterGroup,
) -> Result<crate::resource::RdsDBParameterGroup, anyhow::Error> {
    Ok(crate::resource::RdsDBParameterGroup {
        description: db_parameter_group.description.clone(),
        family: db_parameter_group.db_parameter_group_family().unwrap_or_default().to_string(),
        parameters: HashMap::new(), // TODO: Implement parameter retrieval
    })
}
