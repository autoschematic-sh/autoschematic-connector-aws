use std::path::Path;

use crate::{
    addr::RdsResourceAddress,
    op::RdsConnectorOp,
    resource::{RdsDBCluster, RdsDBInstance, RdsDBParameterGroup, RdsDBSubnetGroup},
};
use autoschematic_core::{
    connector::{ConnectorOp, PlanResponseElement, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values, optional_string_from_utf8},
};

use super::RdsConnector;

impl RdsConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;

        let addr = RdsResourceAddress::from_path(addr)?;
        let mut res = Vec::new();

        match addr {
            RdsResourceAddress::DBInstance { region, id } => {
                match (current, desired) {
                    (None, None) => {}
                    (None, Some(new_instance)) => {
                        let new_instance: RdsDBInstance = RON.from_str(&new_instance)?;
                        res.push(connector_op!(
                            RdsConnectorOp::CreateDBInstance(new_instance),
                            format!("Create new RDS DB Instance {}", id)
                        ));
                    }
                    (Some(_old_instance), None) => {
                        res.push(connector_op!(
                            RdsConnectorOp::DeleteDBInstance {
                                skip_final_snapshot: false,
                                final_snapshot_identifier: Some(format!("{}-final-snapshot", id)),
                                delete_automated_backups: Some(true),
                            },
                            format!("DELETE RDS DB Instance {}", id)
                        ));
                    }
                    (Some(old_instance), Some(new_instance)) => {
                        let old_instance: RdsDBInstance = RON.from_str(&old_instance)?;
                        let new_instance: RdsDBInstance = RON.from_str(&new_instance)?;

                        if old_instance == new_instance {
                            // No changes
                        } else {
                            // Check for tag changes
                            if old_instance.tags != new_instance.tags {
                                let diff = diff_ron_values(&old_instance.tags, &new_instance.tags).unwrap_or_default();
                                res.push(connector_op!(
                                    RdsConnectorOp::UpdateDBInstanceTags(old_instance.tags.clone(), new_instance.tags.clone()),
                                    format!("Modify tags for RDS DB Instance `{}`\n{}", id, diff)
                                ));
                            }

                            // Check for instance modification
                            let needs_modification = old_instance.instance_class != new_instance.instance_class
                                || old_instance.allocated_storage != new_instance.allocated_storage
                                || old_instance.max_allocated_storage != new_instance.max_allocated_storage
                                || old_instance.backup_retention_period != new_instance.backup_retention_period
                                || old_instance.preferred_backup_window != new_instance.preferred_backup_window
                                || old_instance.preferred_maintenance_window != new_instance.preferred_maintenance_window
                                || old_instance.multi_az != new_instance.multi_az
                                || old_instance.storage_type != new_instance.storage_type
                                || old_instance.storage_encrypted != new_instance.storage_encrypted
                                || old_instance.publicly_accessible != new_instance.publicly_accessible
                                || old_instance.enable_iam_database_authentication
                                    != new_instance.enable_iam_database_authentication
                                || old_instance.enable_performance_insights != new_instance.enable_performance_insights
                                || old_instance.performance_insights_retention_period
                                    != new_instance.performance_insights_retention_period
                                || old_instance.monitoring_interval != new_instance.monitoring_interval
                                || old_instance.auto_minor_version_upgrade != new_instance.auto_minor_version_upgrade
                                || old_instance.deletion_protection != new_instance.deletion_protection;

                            if needs_modification {
                                res.push(connector_op!(
                                    RdsConnectorOp::ModifyDBInstance {
                                        instance_class: if old_instance.instance_class != new_instance.instance_class {
                                            Some(new_instance.instance_class.clone())
                                        } else {
                                            None
                                        },
                                        allocated_storage: if old_instance.allocated_storage != new_instance.allocated_storage {
                                            new_instance.allocated_storage
                                        } else {
                                            None
                                        },
                                        max_allocated_storage: if old_instance.max_allocated_storage != new_instance.max_allocated_storage {
                                            new_instance.max_allocated_storage
                                        } else {
                                            None
                                        },
                                        backup_retention_period: if old_instance.backup_retention_period
                                            != new_instance.backup_retention_period
                                        {
                                            new_instance.backup_retention_period
                                        } else {
                                            None
                                        },
                                        preferred_backup_window: if old_instance.preferred_backup_window
                                            != new_instance.preferred_backup_window
                                        {
                                            new_instance.preferred_backup_window.clone()
                                        } else {
                                            None
                                        },
                                        preferred_maintenance_window: if old_instance.preferred_maintenance_window
                                            != new_instance.preferred_maintenance_window
                                        {
                                            new_instance.preferred_maintenance_window.clone()
                                        } else {
                                            None
                                        },
                                        multi_az: if old_instance.multi_az != new_instance.multi_az {
                                            new_instance.multi_az
                                        } else {
                                            None
                                        },
                                        storage_type: if old_instance.storage_type != new_instance.storage_type {
                                            new_instance.storage_type.clone()
                                        } else {
                                            None
                                        },
                                        publicly_accessible: if old_instance.publicly_accessible
                                            != new_instance.publicly_accessible
                                        {
                                            new_instance.publicly_accessible
                                        } else {
                                            None
                                        },
                                        enable_iam_database_authentication: if old_instance.enable_iam_database_authentication
                                            != new_instance.enable_iam_database_authentication
                                        {
                                            new_instance.enable_iam_database_authentication
                                        } else {
                                            None
                                        },
                                        auto_minor_version_upgrade: if old_instance.auto_minor_version_upgrade != new_instance.auto_minor_version_upgrade {
                                            new_instance.auto_minor_version_upgrade
                                        } else {
                                            None
                                        },
                                        deletion_protection: if old_instance.deletion_protection != new_instance.deletion_protection {
                                            new_instance.deletion_protection
                                        } else {
                                            None
                                        },
                                        apply_immediately: Some(true), // Apply changes immediately by default
                                    },
                                    format!("Modify RDS DB Instance {}", id)
                                ));

                                // Handle Performance Insights changes separately
                                if old_instance.enable_performance_insights != new_instance.enable_performance_insights
                                    || old_instance.performance_insights_retention_period
                                        != new_instance.performance_insights_retention_period
                                    || old_instance.monitoring_interval != new_instance.monitoring_interval
                                {
                                    res.push(connector_op!(
                                        RdsConnectorOp::ModifyDBInstanceMonitoring {
                                            monitoring_interval: new_instance.monitoring_interval,
                                            monitoring_role_arn: new_instance.monitoring_role_arn.clone(),
                                            enable_performance_insights: new_instance.enable_performance_insights,
                                            performance_insights_retention_period: new_instance
                                                .performance_insights_retention_period,
                                        },
                                        format!("Modify monitoring and performance insights for RDS DB Instance {}", id)
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            RdsResourceAddress::DBCluster { region, id } => {
                match (current, desired) {
                    (None, None) => {}
                    (None, Some(new_cluster)) => {
                        let new_cluster: RdsDBCluster = RON.from_str(&new_cluster)?;
                        res.push(connector_op!(
                            RdsConnectorOp::CreateDBCluster(new_cluster),
                            format!("Create new RDS DB Cluster {}", id)
                        ));
                    }
                    (Some(_old_cluster), None) => {
                        res.push(connector_op!(
                            RdsConnectorOp::DeleteDBCluster {
                                skip_final_snapshot: false,
                                final_snapshot_identifier: Some(format!("{}-final-snapshot", id)),
                            },
                            format!("DELETE RDS DB Cluster {}", id)
                        ));
                    }
                    (Some(old_cluster), Some(new_cluster)) => {
                        let old_cluster: RdsDBCluster = RON.from_str(&old_cluster)?;
                        let new_cluster: RdsDBCluster = RON.from_str(&new_cluster)?;

                        if old_cluster == new_cluster {
                            // No changes
                        } else {
                            // Check for tag changes
                            if old_cluster.tags != new_cluster.tags {
                                let diff = diff_ron_values(&old_cluster.tags, &new_cluster.tags).unwrap_or_default();
                                res.push(connector_op!(
                                    RdsConnectorOp::UpdateDBClusterTags(old_cluster.tags.clone(), new_cluster.tags.clone()),
                                    format!("Modify tags for RDS DB Cluster `{}`\n{}", id, diff)
                                ));
                            }

                            // Check for cluster modification
                            let needs_modification = old_cluster.engine_version != new_cluster.engine_version
                                || old_cluster.backup_retention_period != new_cluster.backup_retention_period
                                || old_cluster.preferred_backup_window != new_cluster.preferred_backup_window
                                || old_cluster.preferred_maintenance_window != new_cluster.preferred_maintenance_window
                                || old_cluster.storage_encrypted != new_cluster.storage_encrypted
                                || old_cluster.deletion_protection != new_cluster.deletion_protection
                                || old_cluster.enable_iam_database_authentication
                                    != new_cluster.enable_iam_database_authentication
                                || old_cluster.backtrack_window != new_cluster.backtrack_window
                                || old_cluster.enabled_cloudwatch_logs_exports != new_cluster.enabled_cloudwatch_logs_exports;

                            if needs_modification {
                                res.push(connector_op!(
                                    RdsConnectorOp::ModifyDBCluster {
                                        engine_version: if old_cluster.engine_version != new_cluster.engine_version {
                                            new_cluster.engine_version.clone()
                                        } else {
                                            None
                                        },
                                        backup_retention_period: if old_cluster.backup_retention_period
                                            != new_cluster.backup_retention_period
                                        {
                                            new_cluster.backup_retention_period
                                        } else {
                                            None
                                        },
                                        preferred_backup_window: if old_cluster.preferred_backup_window
                                            != new_cluster.preferred_backup_window
                                        {
                                            new_cluster.preferred_backup_window.clone()
                                        } else {
                                            None
                                        },
                                        preferred_maintenance_window: if old_cluster.preferred_maintenance_window
                                            != new_cluster.preferred_maintenance_window
                                        {
                                            new_cluster.preferred_maintenance_window.clone()
                                        } else {
                                            None
                                        },
                                        deletion_protection: if old_cluster.deletion_protection
                                            != new_cluster.deletion_protection
                                        {
                                            new_cluster.deletion_protection
                                        } else {
                                            None
                                        },
                                        enable_iam_database_authentication: if old_cluster.enable_iam_database_authentication
                                            != new_cluster.enable_iam_database_authentication
                                        {
                                            new_cluster.enable_iam_database_authentication
                                        } else {
                                            None
                                        },
                                        backtrack_window: if old_cluster.backtrack_window != new_cluster.backtrack_window {
                                            new_cluster.backtrack_window
                                        } else {
                                            None
                                        },
                                        master_user_password: None, // Don't include password in modify operations
                                        apply_immediately: Some(true), // Apply changes immediately by default
                                    },
                                    format!("Modify RDS DB Cluster {}", id)
                                ));
                            }
                        }
                    }
                }
            }
            RdsResourceAddress::DBSubnetGroup { region, name } => match (current, desired) {
                (None, None) => {}
                (None, Some(new_subnet_group)) => {
                    let new_subnet_group: RdsDBSubnetGroup = RON.from_str(&new_subnet_group)?;
                    res.push(connector_op!(
                        RdsConnectorOp::CreateDBSubnetGroup(new_subnet_group),
                        format!("Create new RDS DB Subnet Group {}", name)
                    ));
                }
                (Some(_old_subnet_group), None) => {
                    res.push(connector_op!(
                        RdsConnectorOp::DeleteDBSubnetGroup,
                        format!("DELETE RDS DB Subnet Group {}", name)
                    ));
                }
                (Some(old_subnet_group), Some(new_subnet_group)) => {
                    let old_subnet_group: RdsDBSubnetGroup = RON.from_str(&old_subnet_group)?;
                    let new_subnet_group: RdsDBSubnetGroup = RON.from_str(&new_subnet_group)?;

                    if old_subnet_group != new_subnet_group {
                        res.push(connector_op!(
                            RdsConnectorOp::ModifyDBSubnetGroup {
                                description: if old_subnet_group.description != new_subnet_group.description {
                                    Some(new_subnet_group.description.clone())
                                } else {
                                    None
                                },
                                subnet_ids:  if old_subnet_group.subnet_ids != new_subnet_group.subnet_ids {
                                    new_subnet_group.subnet_ids.clone()
                                } else {
                                    vec![]
                                },
                            },
                            format!("Modify RDS DB Subnet Group {}", name)
                        ));
                    }
                }
            },
            RdsResourceAddress::DBParameterGroup { region, name } => match (current, desired) {
                (None, None) => {}
                (None, Some(new_parameter_group)) => {
                    let new_parameter_group: RdsDBParameterGroup = RON.from_str(&new_parameter_group)?;
                    res.push(connector_op!(
                        RdsConnectorOp::CreateDBParameterGroup(new_parameter_group),
                        format!("Create new RDS DB Parameter Group {}", name)
                    ));
                }
                (Some(_old_parameter_group), None) => {
                    res.push(connector_op!(
                        RdsConnectorOp::DeleteDBParameterGroup,
                        format!("DELETE RDS DB Parameter Group {}", name)
                    ));
                }
                (Some(old_parameter_group), Some(new_parameter_group)) => {
                    let old_parameter_group: RdsDBParameterGroup = RON.from_str(&old_parameter_group)?;
                    let new_parameter_group: RdsDBParameterGroup = RON.from_str(&new_parameter_group)?;

                    if old_parameter_group.parameters != new_parameter_group.parameters {
                        res.push(connector_op!(
                            RdsConnectorOp::ModifyDBParameterGroup {
                                parameters: new_parameter_group.parameters.clone(),
                            },
                            format!("Modify parameters for RDS DB Parameter Group {}", name)
                        ));
                    }
                }
            },
        }

        Ok(res)
    }
}
