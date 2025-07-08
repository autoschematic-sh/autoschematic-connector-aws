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
                            .manage_master_user_password(true)
                            .db_instance_class(&instance.instance_class);

                        if let Some(version) = &instance.engine_version {
                            request = request.engine_version(version);
                        }

                        if let Some(storage) = instance.allocated_storage {
                            request = request.allocated_storage(storage);
                        }

                        if let Some(max_storage) = instance.max_allocated_storage {
                            request = request.max_allocated_storage(max_storage);
                        }

                        if let Some(throughput) = instance.storage_throughput {
                            request = request.storage_throughput(throughput);
                        }

                        if let Some(iops) = instance.iops {
                            request = request.iops(iops);
                        }

                        if let Some(db_name) = &instance.db_name {
                            request = request.db_name(db_name);
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

                        if let Some(kms_key) = &instance.kms_key_id {
                            request = request.kms_key_id(kms_key);
                        }

                        if let Some(iam_auth) = instance.enable_iam_database_authentication {
                            request = request.enable_iam_database_authentication(iam_auth);
                        }

                        if let Some(performance_insights) = instance.enable_performance_insights {
                            request = request.enable_performance_insights(performance_insights);
                        }

                        if let Some(retention) = instance.performance_insights_retention_period {
                            request = request.performance_insights_retention_period(retention);
                        }

                        if let Some(insights_kms_key) = &instance.performance_insights_kms_key_id {
                            request = request.performance_insights_kms_key_id(insights_kms_key);
                        }

                        if let Some(monitoring_interval) = instance.monitoring_interval {
                            request = request.monitoring_interval(monitoring_interval);
                        }

                        if let Some(monitoring_role) = &instance.monitoring_role_arn {
                            request = request.monitoring_role_arn(monitoring_role);
                        }

                        if let Some(auto_upgrade) = instance.auto_minor_version_upgrade {
                            request = request.auto_minor_version_upgrade(auto_upgrade);
                        }

                        if let Some(deletion_protection) = instance.deletion_protection {
                            request = request.deletion_protection(deletion_protection);
                        }

                        if let Some(copy_tags) = instance.copy_tags_to_snapshot {
                            request = request.copy_tags_to_snapshot(copy_tags);
                        }

                        if let Some(az) = &instance.availability_zone {
                            request = request.availability_zone(az);
                        }

                        if let Some(subnet_group) = &instance.db_subnet_group_name {
                            request = request.db_subnet_group_name(subnet_group);
                        }

                        if let Some(security_groups) = &instance.vpc_security_group_ids {
                            request = request.set_vpc_security_group_ids(Some(security_groups.clone()));
                        }

                        if let Some(param_group) = &instance.db_parameter_group_name {
                            request = request.db_parameter_group_name(param_group);
                        }

                        if let Some(option_group) = &instance.option_group_name {
                            request = request.option_group_name(option_group);
                        }

                        if let Some(license) = &instance.license_model {
                            request = request.license_model(license);
                        }

                        if let Some(character_set) = &instance.character_set_name {
                            request = request.character_set_name(character_set);
                        }

                        if let Some(timezone) = &instance.timezone {
                            request = request.timezone(timezone);
                        }

                        if let Some(domain) = &instance.domain {
                            request = request.domain(domain);
                        }

                        if let Some(domain_role) = &instance.domain_iam_role_name {
                            request = request.domain_iam_role_name(domain_role);
                        }

                        if !instance.enabled_cloudwatch_logs_exports.is_empty() {
                            request = request
                                .set_enable_cloudwatch_logs_exports(Some(instance.enabled_cloudwatch_logs_exports.clone()));
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
                    RdsConnectorOp::ModifyDBInstance {
                        instance_class,
                        allocated_storage,
                        max_allocated_storage,
                        backup_retention_period,
                        preferred_backup_window,
                        preferred_maintenance_window,
                        multi_az,
                        storage_type,
                        publicly_accessible,
                        enable_iam_database_authentication,
                        auto_minor_version_upgrade,
                        deletion_protection,
                        apply_immediately,
                    } => {
                        let mut request = client.modify_db_instance().db_instance_identifier(id);

                        if let Some(class) = instance_class {
                            request = request.db_instance_class(class);
                        }

                        if let Some(storage) = allocated_storage {
                            request = request.allocated_storage(storage);
                        }

                        if let Some(max_storage) = max_allocated_storage {
                            request = request.max_allocated_storage(max_storage);
                        }

                        if let Some(backup_retention) = backup_retention_period {
                            request = request.backup_retention_period(backup_retention);
                        }

                        if let Some(backup_window) = preferred_backup_window {
                            request = request.preferred_backup_window(backup_window);
                        }

                        if let Some(maintenance_window) = preferred_maintenance_window {
                            request = request.preferred_maintenance_window(maintenance_window);
                        }

                        if let Some(multi_az_val) = multi_az {
                            request = request.multi_az(multi_az_val);
                        }

                        if let Some(storage_type_val) = storage_type {
                            request = request.storage_type(storage_type_val);
                        }

                        // Note: storage_encrypted cannot be modified after instance creation

                        if let Some(publicly_accessible_val) = publicly_accessible {
                            request = request.publicly_accessible(publicly_accessible_val);
                        }

                        if let Some(iam_auth) = enable_iam_database_authentication {
                            request = request.enable_iam_database_authentication(iam_auth);
                        }

                        if let Some(auto_upgrade) = auto_minor_version_upgrade {
                            request = request.auto_minor_version_upgrade(auto_upgrade);
                        }

                        if let Some(deletion_prot) = deletion_protection {
                            request = request.deletion_protection(deletion_prot);
                        }

                        if let Some(apply_now) = apply_immediately {
                            request = request.apply_immediately(apply_now);
                        }

                        request.send().await?;

                        op_exec_output!(format!("Modified DB instance `{}`", id))
                    }
                    RdsConnectorOp::ModifyDBInstanceMonitoring {
                        monitoring_interval,
                        monitoring_role_arn,
                        enable_performance_insights,
                        performance_insights_retention_period,
                    } => {
                        let mut request = client.modify_db_instance().db_instance_identifier(id);

                        if let Some(interval) = monitoring_interval {
                            request = request.monitoring_interval(interval);
                        }

                        if let Some(role_arn) = monitoring_role_arn {
                            request = request.monitoring_role_arn(role_arn);
                        }

                        if let Some(enable_insights) = enable_performance_insights {
                            request = request.enable_performance_insights(enable_insights);
                        }

                        if let Some(retention) = performance_insights_retention_period {
                            request = request.performance_insights_retention_period(retention);
                        }

                        request = request.apply_immediately(true);

                        request.send().await?;

                        op_exec_output!(format!("Modified monitoring for DB instance `{}`", id))
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
                    RdsConnectorOp::ModifyDBCluster {
                        engine_version,
                        backup_retention_period,
                        preferred_backup_window,
                        preferred_maintenance_window,
                        deletion_protection,
                        enable_iam_database_authentication,
                        backtrack_window,
                        master_user_password,
                        apply_immediately,
                    } => {
                        let mut request = client.modify_db_cluster().db_cluster_identifier(id);

                        if let Some(version) = engine_version {
                            request = request.engine_version(version);
                        }

                        if let Some(backup_retention) = backup_retention_period {
                            request = request.backup_retention_period(backup_retention);
                        }

                        if let Some(backup_window) = preferred_backup_window {
                            request = request.preferred_backup_window(backup_window);
                        }

                        if let Some(maintenance_window) = preferred_maintenance_window {
                            request = request.preferred_maintenance_window(maintenance_window);
                        }

                        if let Some(deletion_prot) = deletion_protection {
                            request = request.deletion_protection(deletion_prot);
                        }

                        if let Some(iam_auth) = enable_iam_database_authentication {
                            request = request.enable_iam_database_authentication(iam_auth);
                        }

                        if let Some(backtrack) = backtrack_window {
                            request = request.backtrack_window(backtrack);
                        }

                        if let Some(password) = master_user_password {
                            request = request.master_user_password(password);
                        }

                        if let Some(apply_now) = apply_immediately {
                            request = request.apply_immediately(apply_now);
                        }

                        request.send().await?;

                        op_exec_output!(format!("Modified DB cluster `{}`", id))
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
