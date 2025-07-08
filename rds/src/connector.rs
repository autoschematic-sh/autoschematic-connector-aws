use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
use autoschematic_core::{
    connector::{Connector, ConnectorOutbox, Resource, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, ResourceAddress, SkeletonOutput},
    diag::DiagnosticOutput,
    skeleton,
    util::{ron_check_eq, ron_check_syntax},
};

use tokio::sync::Mutex;

use crate::{
    addr::RdsResourceAddress,
    config::RdsConnectorConfig,
    resource::{RdsDBCluster, RdsDBInstance, RdsDBParameterGroup, RdsDBSubnetGroup, RdsResource},
    tags::Tags,
};

mod get;
mod list;
mod op_exec;
mod plan;

#[derive(Default)]
pub struct RdsConnector {
    pub prefix: PathBuf,
    pub client_cache: Mutex<HashMap<String, Arc<aws_sdk_rds::Client>>>,
    pub account_id: Mutex<String>,
    pub config: Mutex<RdsConnectorConfig>,
}

#[async_trait]
impl Connector for RdsConnector {
    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        let mut conn = Self::default();

        conn.prefix = prefix.into();

        Ok(Arc::new(conn))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let secrets_config: RdsConnectorConfig = RdsConnectorConfig::try_load(&self.prefix).await?;

        let account_id = secrets_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = secrets_config;
        *self.account_id.lock().await = account_id;
        Ok(())
    }

    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if addr == PathBuf::from("aws/rds/config.ron") {
            Ok(FilterOutput::Config)
        } else if let Ok(_addr) = RdsResourceAddress::from_path(addr) {
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
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        // RDS PostgreSQL Instance skeleton
        res.push(skeleton!(
            RdsResourceAddress::DBInstance {
                region: String::from("[region]"),
                id: String::from("[db_instance_id]"),
            },
            RdsResource::DBInstance(RdsDBInstance {
                engine: String::from("postgres"),
                engine_version: Some(String::from("15.4")),
                instance_class: String::from("db.t3.micro"),
                allocated_storage: Some(20),
                max_allocated_storage: Some(100),
                storage_type: Some(String::from("gp3")),
                storage_throughput: None,
                iops: None,
                db_name: Some(String::from("[database_name]")),
                master_username: Some(String::from("[master_username]")),
                port: Some(5432),
                publicly_accessible: Some(false),
                backup_retention_period: Some(7),
                preferred_backup_window: Some(String::from("03:00-04:00")),
                preferred_maintenance_window: Some(String::from("sun:04:00-sun:05:00")),
                multi_az: Some(false),
                storage_encrypted: Some(true),
                kms_key_id: None,
                enable_iam_database_authentication: Some(true),
                enable_performance_insights: Some(true),
                performance_insights_retention_period: Some(7),
                performance_insights_kms_key_id: None,
                monitoring_interval: Some(60),
                monitoring_role_arn: None,
                auto_minor_version_upgrade: Some(true),
                deletion_protection: Some(false),
                copy_tags_to_snapshot: Some(true),
                skip_final_snapshot: Some(false),
                final_snapshot_identifier: None,
                availability_zone: None,
                db_subnet_group_name: Some(String::from("[db_subnet_group_name]")),
                vpc_security_group_ids: Some(vec![String::from("[security_group_id]")]),
                db_parameter_group_name: Some(String::from("[db_parameter_group_name]")),
                option_group_name: None,
                license_model: None,
                character_set_name: None,
                timezone: None,
                domain: None,
                domain_iam_role_name: None,
                enabled_cloudwatch_logs_exports: vec![String::from("postgresql")],
                tags: Tags::default(),
            })
        ));

        // RDS Aurora PostgreSQL Cluster skeleton
        res.push(skeleton!(
            RdsResourceAddress::DBCluster {
                region: String::from("[region]"),
                id: String::from("[db_cluster_id]"),
            },
            RdsResource::DBCluster(RdsDBCluster {
                engine: String::from("aurora-postgresql"),
                engine_version: Some(String::from("15.4")),
                port: Some(5432),
                database_name: Some(String::from("[database_name]")),
                master_username: Some(String::from("[master_username]")),
                backup_retention_period: Some(7),
                preferred_backup_window: Some(String::from("03:00-04:00")),
                preferred_maintenance_window: Some(String::from("sun:04:00-sun:05:00")),
                storage_encrypted: Some(true),
                kms_key_id: None,
                enable_iam_database_authentication: Some(true),
                deletion_protection: Some(false),
                copy_tags_to_snapshot: Some(true),
                skip_final_snapshot: Some(false),
                final_snapshot_identifier: None,
                availability_zones: Some(vec![String::from("[availability_zone_1]"), String::from("[availability_zone_2]")]),
                db_subnet_group_name: Some(String::from("[db_subnet_group_name]")),
                vpc_security_group_ids: Some(vec![String::from("[security_group_id]")]),
                db_cluster_parameter_group_name: Some(String::from("[db_cluster_parameter_group_name]")),
                backtrack_window: Some(72),
                enabled_cloudwatch_logs_exports: Some(vec![String::from("postgresql")]),
                enable_http_endpoint: Some(false),
                global_cluster_identifier: None,
                replication_source_identifier: None,
                restore_type: None,
                source_engine: None,
                source_engine_version: None,
                s3_import_configuration: None,
                serverless_v2_scaling_configuration: None,
                tags: Tags::default(),
            })
        ));

        // RDS DB Subnet Group skeleton
        res.push(skeleton!(
            RdsResourceAddress::DBSubnetGroup {
                region: String::from("[region]"),
                name: String::from("[db_subnet_group_name]"),
            },
            RdsResource::DBSubnetGroup(RdsDBSubnetGroup {
                description: String::from("Database subnet group for PostgreSQL"),
                subnet_ids: vec![
                    String::from("[subnet_id_1]"),
                    String::from("[subnet_id_2]"),
                ],
            })
        ));

        // RDS PostgreSQL Parameter Group skeleton
        let mut parameters = std::collections::HashMap::new();
        parameters.insert(String::from("shared_preload_libraries"), String::from("pg_stat_statements"));
        parameters.insert(String::from("log_statement"), String::from("all"));
        parameters.insert(String::from("log_min_duration_statement"), String::from("1000"));
        parameters.insert(String::from("work_mem"), String::from("4MB"));
        parameters.insert(String::from("maintenance_work_mem"), String::from("64MB"));

        res.push(skeleton!(
            RdsResourceAddress::DBParameterGroup {
                region: String::from("[region]"),
                name: String::from("[db_parameter_group_name]"),
            },
            RdsResource::DBParameterGroup(RdsDBParameterGroup {
                description: Some(String::from("PostgreSQL parameter group")),
                family: String::from("postgres15"),
                parameters,
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> Result<bool, anyhow::Error> {
        let addr = RdsResourceAddress::from_path(addr)?;

        match addr {
            RdsResourceAddress::DBInstance { region, id } => ron_check_eq::<RdsDBInstance>(a, b),
            RdsResourceAddress::DBCluster { region, id } => ron_check_eq::<RdsDBCluster>(a, b),
            RdsResourceAddress::DBSubnetGroup { region, name } => ron_check_eq::<RdsDBSubnetGroup>(a, b),
            RdsResourceAddress::DBParameterGroup { region, name } => ron_check_eq::<RdsDBParameterGroup>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = RdsResourceAddress::from_path(addr)?;

        match addr {
            RdsResourceAddress::DBInstance { region, id } => ron_check_syntax::<RdsDBInstance>(a),
            RdsResourceAddress::DBCluster { region, id } => ron_check_syntax::<RdsDBCluster>(a),
            RdsResourceAddress::DBSubnetGroup { region, name } => ron_check_syntax::<RdsDBSubnetGroup>(a),
            RdsResourceAddress::DBParameterGroup { region, name } => ron_check_syntax::<RdsDBParameterGroup>(a),
        }
    }
}
