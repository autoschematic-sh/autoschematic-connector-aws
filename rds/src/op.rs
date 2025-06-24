use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::resource::{RdsDBCluster, RdsDBInstance, RdsDBParameterGroup, RdsDBSubnetGroup};

type Tags = crate::tags::Tags;

#[derive(Debug, Serialize, Deserialize)]
pub enum RdsConnectorOp {
    // DB Instance operations
    CreateDBInstance(RdsDBInstance),
    UpdateDBInstanceTags(Tags, Tags),
    ModifyDBInstance {
        instance_class: Option<String>,
        allocated_storage: Option<i32>,
        backup_retention_period: Option<i32>,
        preferred_backup_window: Option<String>,
        preferred_maintenance_window: Option<String>,
        multi_az: Option<bool>,
        storage_type: Option<String>,
        storage_encrypted: Option<bool>,
        publicly_accessible: Option<bool>,
    },
    StartDBInstance,
    StopDBInstance,
    RebootDBInstance {
        force_failover: Option<bool>,
    },
    CreateDBSnapshot {
        snapshot_identifier: String,
        tags: Tags,
    },
    RestoreDBInstanceFromSnapshot {
        snapshot_identifier: String,
        instance_class:      Option<String>,
        subnet_group_name:   Option<String>,
    },
    DeleteDBInstance {
        skip_final_snapshot: bool,
        final_snapshot_identifier: Option<String>,
        delete_automated_backups: Option<bool>,
    },

    // DB Cluster operations
    CreateDBCluster(RdsDBCluster),
    UpdateDBClusterTags(Tags, Tags),
    ModifyDBCluster {
        engine_version: Option<String>,
        backup_retention_period: Option<i32>,
        preferred_backup_window: Option<String>,
        preferred_maintenance_window: Option<String>,
        storage_encrypted: Option<bool>,
        deletion_protection: Option<bool>,
        master_user_password: Option<String>,
    },
    StartDBCluster,
    StopDBCluster,
    CreateDBClusterSnapshot {
        snapshot_identifier: String,
        tags: Tags,
    },
    RestoreDBClusterFromSnapshot {
        snapshot_identifier: String,
        engine: Option<String>,
    },
    DeleteDBCluster {
        skip_final_snapshot: bool,
        final_snapshot_identifier: Option<String>,
    },

    // DB Subnet Group operations
    CreateDBSubnetGroup(RdsDBSubnetGroup),
    ModifyDBSubnetGroup {
        description: Option<String>,
        subnet_ids:  Vec<String>,
    },
    DeleteDBSubnetGroup,

    // DB Parameter Group operations
    CreateDBParameterGroup(RdsDBParameterGroup),
    ModifyDBParameterGroup {
        parameters: HashMap<String, String>,
    },
    ResetDBParameterGroup {
        reset_all_parameters: Option<bool>,
        parameters: Option<Vec<String>>,
    },
    DeleteDBParameterGroup,

    // DB Cluster Parameter Group operations
    CreateDBClusterParameterGroup {
        name: String,
        family: String,
        description: Option<String>,
        parameters: HashMap<String, String>,
    },
    ModifyDBClusterParameterGroup {
        parameters: HashMap<String, String>,
    },
    ResetDBClusterParameterGroup {
        reset_all_parameters: Option<bool>,
        parameters: Option<Vec<String>>,
    },
    DeleteDBClusterParameterGroup,

    // Security and networking operations
    CreateDBSecurityGroup {
        name: String,
        description: String,
        tags: Tags,
    },
    AuthorizeDBSecurityGroupIngress {
        cidrip: Option<String>,
        ec2_security_group_name: Option<String>,
        ec2_security_group_id: Option<String>,
        ec2_security_group_owner_id: Option<String>,
    },
    RevokeDBSecurityGroupIngress {
        cidrip: Option<String>,
        ec2_security_group_name: Option<String>,
        ec2_security_group_id: Option<String>,
        ec2_security_group_owner_id: Option<String>,
    },
    DeleteDBSecurityGroup,

    // Option Group operations
    CreateOptionGroup {
        name: String,
        engine_name: String,
        major_engine_version: String,
        description: Option<String>,
        tags: Tags,
    },
    ModifyOptionGroup {
        options_to_include: Option<Vec<HashMap<String, String>>>,
        options_to_remove:  Option<Vec<String>>,
        apply_immediately:  Option<bool>,
    },
    DeleteOptionGroup,

    // Performance and monitoring operations
    ModifyDBInstanceMonitoring {
        monitoring_interval: Option<i32>,
        monitoring_role_arn: Option<String>,
        enable_performance_insights: Option<bool>,
        performance_insights_retention_period: Option<i32>,
    },
    CreateDBInstanceReadReplica {
        replica_identifier: String,
        source_db_identifier: String,
        instance_class: Option<String>,
        availability_zone: Option<String>,
        port: Option<i32>,
        multi_az: Option<bool>,
        publicly_accessible: Option<bool>,
        tags: Tags,
    },
    PromoteReadReplica {
        backup_retention_period: Option<i32>,
        preferred_backup_window: Option<String>,
    },

    // Backup and maintenance operations
    CreateDBClusterEndpoint {
        endpoint_identifier: String,
        endpoint_type: String,
        static_members: Option<Vec<String>>,
        excluded_members: Option<Vec<String>>,
        tags: Tags,
    },
    ModifyDBClusterEndpoint {
        endpoint_type:    Option<String>,
        static_members:   Option<Vec<String>>,
        excluded_members: Option<Vec<String>>,
    },
    DeleteDBClusterEndpoint,

    // Cross-region operations
    CreateDBInstanceAutomatedBackup {
        source_db_instance_arn: String,
        kms_key_id: Option<String>,
    },
    DeleteDBInstanceAutomatedBackup,

    // Event subscription operations
    CreateEventSubscription {
        subscription_name: String,
        sns_topic_arn: String,
        source_type: Option<String>,
        event_categories: Option<Vec<String>>,
        source_ids: Option<Vec<String>>,
        enabled: Option<bool>,
        tags: Tags,
    },
    ModifyEventSubscription {
        sns_topic_arn: Option<String>,
        source_type: Option<String>,
        event_categories: Option<Vec<String>>,
        enabled: Option<bool>,
    },
    DeleteEventSubscription,
}

impl ConnectorOp for RdsConnectorOp {
    fn to_string(&self) -> Result<String, anyhow::Error> {
        Ok(RON.to_string(self)?)
    }

    fn from_str(s: &str) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(RON.from_str(s)?)
    }
}
