use std::{collections::HashMap, path::Path};

use anyhow::Context;
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    error_util::invalid_op,
    op_exec_output,
};

use crate::{addr::RdsResourceAddress, op::RdsConnectorOp, tags::tag_diff};

use super::RdsConnector;

impl RdsConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = RdsResourceAddress::from_path(addr)?;
        let op = RdsConnectorOp::from_str(op)?;

        match &addr {
            RdsResourceAddress::DBInstance { region, id } => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    RdsConnectorOp::CreateDBInstance(instance) => {
                        let mut request = client
                            .create_db_instance()
                            .db_instance_identifier(id)
                            .engine(&instance.engine)
                            .db_instance_class(&instance.instance_class);

                        if let Some(storage) = instance.allocated_storage {
                            request = request.allocated_storage(storage);
                        }

                        if let Some(username) = &instance.master_username {
                            request = request.master_username(username);
                        }

                        if let Some(port) = instance.port {
                            request = request.port(port);
                        }

                        if let Some(public) = instance.publicly_accessible {
                            request = request.publicly_accessible(public);
                        }

                        if let Some(storage_type) = &instance.storage_type {
                            request = request.storage_type(storage_type);
                        }

                        if let Some(backup_retention) = instance.backup_retention_period {
                            request = request.backup_retention_period(backup_retention);
                        }

                        if let Some(backup_window) = &instance.preferred_backup_window {
                            request = request.preferred_backup_window(backup_window);
                        }

                        if let Some(maintenance_window) = &instance.preferred_maintenance_window {
                            request = request.preferred_maintenance_window(maintenance_window);
                        }

                        if let Some(multi_az) = instance.multi_az {
                            request = request.multi_az(multi_az);
                        }

                        if let Some(encrypted) = instance.storage_encrypted {
                            request = request.storage_encrypted(encrypted);
                        }

                        // Add tags if provided
                        request = request.set_tags(instance.tags.clone().into());

                        let response = request.send().await?;

                        let db_instance_arn = response
                            .db_instance()
                            .and_then(|db| db.db_instance_arn())
                            .context("Failed to get DB instance ARN from response")?;

                        let endpoint = response
                            .db_instance()
                            .and_then(|db| db.endpoint())
                            .and_then(|ep| ep.address())
                            .unwrap_or("pending");

                        op_exec_output!(
                            Some([
                                ("db_instance_arn", Some(db_instance_arn.to_string())),
                                ("endpoint", Some(endpoint.to_string())),
                                ("db_instance_identifier", Some(id.clone()))
                            ]),
                            format!("Created DB instance `{}`", id)
                        )
                    }
                    RdsConnectorOp::UpdateDBInstanceTags(old_tags, new_tags) => {
                        // First get the DB instance ARN
                        let response = client.describe_db_instances().db_instance_identifier(id).send().await?;

                        let db_arn = response
                            .db_instances()
                            .first()
                            .and_then(|db| db.db_instance_arn())
                            .context("DB instance not found")?;

                        let (remove_keys, add_tags) = tag_diff(&old_tags, &new_tags)?;

                        if !remove_keys.is_empty() {
                            client
                                .remove_tags_from_resource()
                                .resource_name(db_arn)
                                .set_tag_keys(Some(remove_keys))
                                .send()
                                .await?;
                        }

                        if !add_tags.is_empty() {
                            client
                                .add_tags_to_resource()
                                .resource_name(db_arn)
                                .set_tags(Some(add_tags))
                                .send()
                                .await?;
                        }

                        op_exec_output!(format!("Updated tags for DB instance `{}`", id))
                    }
                    RdsConnectorOp::DeleteDBInstance {
                        skip_final_snapshot,
                        final_snapshot_identifier,
                        delete_automated_backups,
                    } => {
                        let mut request = client
                            .delete_db_instance()
                            .db_instance_identifier(id)
                            .skip_final_snapshot(skip_final_snapshot);

                        if let Some(snapshot_id) = final_snapshot_identifier {
                            request = request.final_db_snapshot_identifier(snapshot_id);
                        }

                        if let Some(delete_backups) = delete_automated_backups {
                            request = request.delete_automated_backups(delete_backups);
                        }

                        request.send().await?;

                        op_exec_output!(format!("Deleted DB instance `{}`", id))
                    }
                    RdsConnectorOp::StartDBInstance => {
                        client.start_db_instance().db_instance_identifier(id).send().await?;

                        op_exec_output!(format!("Started DB instance `{}`", id))
                    }
                    RdsConnectorOp::StopDBInstance => {
                        client.stop_db_instance().db_instance_identifier(id).send().await?;

                        op_exec_output!(format!("Stopped DB instance `{}`", id))
                    }
                    RdsConnectorOp::RebootDBInstance { force_failover } => {
                        let mut request = client.reboot_db_instance().db_instance_identifier(id);

                        if let Some(force) = force_failover {
                            request = request.force_failover(force);
                        }

                        request.send().await?;

                        op_exec_output!(format!("Rebooted DB instance `{}`", id))
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            RdsResourceAddress::DBCluster { region, id } => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    RdsConnectorOp::CreateDBCluster(cluster) => {
                        let mut request = client.create_db_cluster().db_cluster_identifier(id).engine(&cluster.engine);

                        if let Some(version) = &cluster.engine_version {
                            request = request.engine_version(version);
                        }

                        if let Some(port) = cluster.port {
                            request = request.port(port);
                        }

                        if let Some(username) = &cluster.master_username {
                            request = request.master_username(username);
                        }

                        if let Some(backup_retention) = cluster.backup_retention_period {
                            request = request.backup_retention_period(backup_retention);
                        }

                        if let Some(backup_window) = &cluster.preferred_backup_window {
                            request = request.preferred_backup_window(backup_window);
                        }

                        if let Some(maintenance_window) = &cluster.preferred_maintenance_window {
                            request = request.preferred_maintenance_window(maintenance_window);
                        }

                        if let Some(encrypted) = cluster.storage_encrypted {
                            request = request.storage_encrypted(encrypted);
                        }

                        if let Some(deletion_protection) = cluster.deletion_protection {
                            request = request.deletion_protection(deletion_protection);
                        }

                        // Add tags if provided
                        request = request.set_tags(cluster.tags.clone().into());

                        let response = request.send().await?;

                        let cluster_arn = response
                            .db_cluster()
                            .and_then(|cluster| cluster.db_cluster_arn())
                            .context("Failed to get DB cluster ARN from response")?;

                        let endpoint = response
                            .db_cluster()
                            .and_then(|cluster| cluster.endpoint())
                            .unwrap_or("pending");

                        op_exec_output!(
                            Some([
                                ("db_cluster_arn", Some(cluster_arn.to_string())),
                                ("endpoint", Some(endpoint.to_string())),
                                ("db_cluster_identifier", Some(id.clone()))
                            ]),
                            format!("Created DB cluster `{}`", id)
                        )
                    }
                    RdsConnectorOp::UpdateDBClusterTags(old_tags, new_tags) => {
                        // First get the DB cluster ARN
                        let response = client.describe_db_clusters().db_cluster_identifier(id).send().await?;

                        let cluster_arn = response
                            .db_clusters()
                            .first()
                            .and_then(|cluster| cluster.db_cluster_arn())
                            .context("DB cluster not found")?;

                        let (remove_keys, add_tags) = tag_diff(&old_tags, &new_tags)?;

                        if !remove_keys.is_empty() {
                            client
                                .remove_tags_from_resource()
                                .resource_name(cluster_arn)
                                .set_tag_keys(Some(remove_keys))
                                .send()
                                .await?;
                        }

                        if !add_tags.is_empty() {
                            client
                                .add_tags_to_resource()
                                .resource_name(cluster_arn)
                                .set_tags(Some(add_tags))
                                .send()
                                .await?;
                        }

                        op_exec_output!(format!("Updated tags for DB cluster `{}`", id))
                    }
                    RdsConnectorOp::DeleteDBCluster {
                        skip_final_snapshot,
                        final_snapshot_identifier,
                    } => {
                        let mut request = client
                            .delete_db_cluster()
                            .db_cluster_identifier(id)
                            .skip_final_snapshot(skip_final_snapshot);

                        if let Some(snapshot_id) = final_snapshot_identifier {
                            request = request.final_db_snapshot_identifier(snapshot_id);
                        }

                        request.send().await?;

                        op_exec_output!(format!("Deleted DB cluster `{}`", id))
                    }
                    RdsConnectorOp::StartDBCluster => {
                        client.start_db_cluster().db_cluster_identifier(id).send().await?;

                        op_exec_output!(format!("Started DB cluster `{}`", id))
                    }
                    RdsConnectorOp::StopDBCluster => {
                        client.stop_db_cluster().db_cluster_identifier(id).send().await?;

                        op_exec_output!(format!("Stopped DB cluster `{}`", id))
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            RdsResourceAddress::DBSubnetGroup { region, name } => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    RdsConnectorOp::CreateDBSubnetGroup(subnet_group) => {
                        let response = client
                            .create_db_subnet_group()
                            .db_subnet_group_name(name)
                            .db_subnet_group_description(&subnet_group.description)
                            .set_subnet_ids(Some(subnet_group.subnet_ids.clone()))
                            .send()
                            .await?;

                        let subnet_group_arn = response
                            .db_subnet_group()
                            .and_then(|sg| sg.db_subnet_group_arn())
                            .context("Failed to get DB subnet group ARN from response")?;

                        op_exec_output!(
                            Some([
                                ("db_subnet_group_arn", Some(subnet_group_arn.to_string())),
                                ("db_subnet_group_name", Some(name.clone()))
                            ]),
                            format!("Created DB subnet group `{}`", name)
                        )
                    }
                    RdsConnectorOp::ModifyDBSubnetGroup { description, subnet_ids } => {
                        let mut request = client.modify_db_subnet_group().db_subnet_group_name(name);

                        if let Some(desc) = description {
                            request = request.db_subnet_group_description(desc);
                        }

                        if !subnet_ids.is_empty() {
                            request = request.set_subnet_ids(Some(subnet_ids.clone()));
                        }

                        request.send().await?;

                        op_exec_output!(format!("Modified DB subnet group `{}`", name))
                    }
                    RdsConnectorOp::DeleteDBSubnetGroup => {
                        client.delete_db_subnet_group().db_subnet_group_name(name).send().await?;

                        op_exec_output!(format!("Deleted DB subnet group `{}`", name))
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            RdsResourceAddress::DBParameterGroup { region, name } => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    RdsConnectorOp::CreateDBParameterGroup(param_group) => {
                        let mut request = client
                            .create_db_parameter_group()
                            .db_parameter_group_name(name)
                            .db_parameter_group_family(&param_group.family);

                        if let Some(desc) = &param_group.description {
                            request = request.description(desc);
                        }

                        let response = request.send().await?;

                        let param_group_arn = response
                            .db_parameter_group()
                            .and_then(|pg| pg.db_parameter_group_arn())
                            .context("Failed to get DB parameter group ARN from response")?;

                        // Apply parameters if any
                        if !param_group.parameters.is_empty() {
                            let parameters: Vec<_> = param_group
                                .parameters
                                .iter()
                                .map(|(key, value)| {
                                    aws_sdk_rds::types::Parameter::builder()
                                        .parameter_name(key)
                                        .parameter_value(value)
                                        .apply_method(aws_sdk_rds::types::ApplyMethod::Immediate)
                                        .build()
                                })
                                .collect();

                            client
                                .modify_db_parameter_group()
                                .db_parameter_group_name(name)
                                .set_parameters(Some(parameters))
                                .send()
                                .await?;
                        }

                        op_exec_output!(
                            Some([
                                ("db_parameter_group_arn", Some(param_group_arn.to_string())),
                                ("db_parameter_group_name", Some(name.clone()))
                            ]),
                            format!("Created DB parameter group `{}`", name)
                        )
                    }
                    RdsConnectorOp::ModifyDBParameterGroup { parameters } => {
                        let parameters: Vec<_> = parameters
                            .iter()
                            .map(|(key, value)| {
                                aws_sdk_rds::types::Parameter::builder()
                                    .parameter_name(key)
                                    .parameter_value(value)
                                    .apply_method(aws_sdk_rds::types::ApplyMethod::Immediate)
                                    .build()
                            })
                            .collect();

                        client
                            .modify_db_parameter_group()
                            .db_parameter_group_name(name)
                            .set_parameters(Some(parameters))
                            .send()
                            .await?;

                        op_exec_output!(format!("Modified DB parameter group `{}`", name))
                    }
                    RdsConnectorOp::ResetDBParameterGroup {
                        reset_all_parameters,
                        parameters,
                    } => {
                        let mut request = client.reset_db_parameter_group().db_parameter_group_name(name);

                        if let Some(reset_all) = reset_all_parameters {
                            request = request.reset_all_parameters(reset_all);
                        }

                        if let Some(param_names) = parameters {
                            let parameters: Vec<_> = param_names
                                .iter()
                                .map(|name| {
                                    aws_sdk_rds::types::Parameter::builder()
                                        .parameter_name(name)
                                        .apply_method(aws_sdk_rds::types::ApplyMethod::Immediate)
                                        .build()
                                })
                                .collect();

                            request = request.set_parameters(Some(parameters));
                        }

                        request.send().await?;

                        op_exec_output!(format!("Reset DB parameter group `{}`", name))
                    }
                    RdsConnectorOp::DeleteDBParameterGroup => {
                        client
                            .delete_db_parameter_group()
                            .db_parameter_group_name(name)
                            .send()
                            .await?;

                        op_exec_output!(format!("Deleted DB parameter group `{}`", name))
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
        }
    }
}
