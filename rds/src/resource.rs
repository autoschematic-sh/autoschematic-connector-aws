use autoschematic_core::connector::{Resource, ResourceAddress};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::{PrettyConfig, RON};

use super::addr::RdsResourceAddress;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RdsDBInstance {
    pub engine: String,
    pub engine_version: Option<String>,
    pub instance_class: String,
    pub allocated_storage: Option<i32>,
    pub max_allocated_storage: Option<i32>,
    pub storage_type: Option<String>,
    pub storage_throughput: Option<i32>,
    pub iops: Option<i32>,
    pub db_name: Option<String>,
    pub master_username: Option<String>,
    pub port: Option<i32>,
    pub publicly_accessible: Option<bool>,
    pub backup_retention_period: Option<i32>,
    pub preferred_backup_window: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub multi_az: Option<bool>,
    pub storage_encrypted: Option<bool>,
    pub kms_key_id: Option<String>,
    pub enable_iam_database_authentication: Option<bool>,
    pub enable_performance_insights: Option<bool>,
    pub performance_insights_retention_period: Option<i32>,
    pub performance_insights_kms_key_id: Option<String>,
    pub monitoring_interval: Option<i32>,
    pub monitoring_role_arn: Option<String>,
    pub auto_minor_version_upgrade: Option<bool>,
    pub deletion_protection: Option<bool>,
    pub copy_tags_to_snapshot: Option<bool>,
    pub skip_final_snapshot: Option<bool>,
    pub final_snapshot_identifier: Option<String>,
    pub availability_zone: Option<String>,
    pub db_subnet_group_name: Option<String>,
    pub vpc_security_group_ids: Option<Vec<String>>,
    pub db_parameter_group_name: Option<String>,
    pub option_group_name: Option<String>,
    pub license_model: Option<String>,
    pub character_set_name: Option<String>,
    pub timezone: Option<String>,
    pub domain: Option<String>,
    pub domain_iam_role_name: Option<String>,
    pub enabled_cloudwatch_logs_exports: Vec<String>,
    pub tags: crate::tags::Tags,
}
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RdsDBCluster {
    pub engine: String,
    pub engine_version: Option<String>,
    pub port: Option<i32>,
    pub database_name: Option<String>,
    pub master_username: Option<String>,
    pub backup_retention_period: Option<i32>,
    pub preferred_backup_window: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub storage_encrypted: Option<bool>,
    pub kms_key_id: Option<String>,
    pub enable_iam_database_authentication: Option<bool>,
    pub deletion_protection: Option<bool>,
    pub copy_tags_to_snapshot: Option<bool>,
    pub skip_final_snapshot: Option<bool>,
    pub final_snapshot_identifier: Option<String>,
    pub availability_zones: Option<Vec<String>>,
    pub db_subnet_group_name: Option<String>,
    pub vpc_security_group_ids: Option<Vec<String>>,
    pub db_cluster_parameter_group_name: Option<String>,
    pub backtrack_window: Option<i64>,
    pub enabled_cloudwatch_logs_exports: Option<Vec<String>>,
    pub enable_http_endpoint: Option<bool>,
    pub global_cluster_identifier: Option<String>,
    pub replication_source_identifier: Option<String>,
    pub restore_type: Option<String>,
    pub source_engine: Option<String>,
    pub source_engine_version: Option<String>,
    pub s3_import_configuration: Option<S3ImportConfiguration>,
    pub serverless_v2_scaling_configuration: Option<ServerlessV2ScalingConfiguration>,
    pub tags: crate::tags::Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct S3ImportConfiguration {
    pub bucket_name: String,
    pub bucket_prefix: Option<String>,
    pub ingestion_role_arn: String,
    pub source_engine: String,
    pub source_engine_version: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ServerlessV2ScalingConfiguration {
    pub max_capacity: Option<f64>,
    pub min_capacity: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RdsDBSubnetGroup {
    pub description: String,
    pub subnet_ids:  Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RdsDBParameterGroup {
    pub description: Option<String>,
    pub family:      String,
    pub parameters:  std::collections::HashMap<String, String>,
}

pub enum RdsResource {
    DBInstance(RdsDBInstance),
    DBCluster(RdsDBCluster),
    DBSubnetGroup(RdsDBSubnetGroup),
    DBParameterGroup(RdsDBParameterGroup),
}

impl Resource for RdsResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        match self {
            RdsResource::DBInstance(instance) => match RON.to_string_pretty(&instance, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            RdsResource::DBCluster(cluster) => match RON.to_string_pretty(&cluster, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            RdsResource::DBSubnetGroup(group) => match RON.to_string_pretty(&group, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            RdsResource::DBParameterGroup(group) => match RON.to_string_pretty(&group, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = RdsResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;
        match addr {
            RdsResourceAddress::DBInstance { region, id } => Ok(RdsResource::DBInstance(RON.from_str(s)?)),
            RdsResourceAddress::DBCluster { region, id } => Ok(RdsResource::DBCluster(RON.from_str(s)?)),
            RdsResourceAddress::DBSubnetGroup { region, name } => Ok(RdsResource::DBSubnetGroup(RON.from_str(s)?)),
            RdsResourceAddress::DBParameterGroup { region, name } => Ok(RdsResource::DBParameterGroup(RON.from_str(s)?)),
        }
    }
}
