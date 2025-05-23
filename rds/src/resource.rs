use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;

use autoschematic_core::connector::{Resource, ResourceAddress};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::{PrettyConfig, RON};

use super::addr::RdsResourceAddress;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RdsDBInstance {
    pub engine: String,
    pub instance_class: String,
    pub allocated_storage: Option<i32>,
    pub master_username: Option<String>,
    pub port: Option<i32>,
    pub publicly_accessible: Option<bool>,
    pub storage_type: Option<String>,
    pub backup_retention_period: Option<i32>,
    pub preferred_backup_window: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub multi_az: Option<bool>,
    pub storage_encrypted: Option<bool>,
    pub tags: crate::tags::Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RdsDBCluster {
    pub engine: String,
    pub engine_version: Option<String>,
    pub port: Option<i32>,
    pub master_username: Option<String>,
    pub backup_retention_period: Option<i32>,
    pub preferred_backup_window: Option<String>,
    pub preferred_maintenance_window: Option<String>,
    pub storage_encrypted: Option<bool>,
    pub deletion_protection: Option<bool>,
    pub tags: crate::tags::Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RdsDBSubnetGroup {
    pub description: String,
    pub subnet_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RdsDBParameterGroup {
    pub description: Option<String>,
    pub family: String,
    pub parameters: std::collections::HashMap<String, String>,
}

pub enum RdsResource {
    DBInstance(RdsDBInstance),
    DBCluster(RdsDBCluster),
    DBSubnetGroup(RdsDBSubnetGroup),
    DBParameterGroup(RdsDBParameterGroup),
}

impl Resource for RdsResource {
    fn to_os_string(&self) -> Result<OsString, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        match self {
            RdsResource::DBInstance(instance) => {
                match RON.to_string_pretty(&instance, pretty_config) {
                    Ok(s) => Ok(s.into()),
                    Err(e) => Err(e.into()),
                }
            }
            RdsResource::DBCluster(cluster) => {
                match RON.to_string_pretty(&cluster, pretty_config) {
                    Ok(s) => Ok(s.into()),
                    Err(e) => Err(e.into()),
                }
            }
            RdsResource::DBSubnetGroup(group) => {
                match RON.to_string_pretty(&group, pretty_config) {
                    Ok(s) => Ok(s.into()),
                    Err(e) => Err(e.into()),
                }
            }
            RdsResource::DBParameterGroup(group) => {
                match RON.to_string_pretty(&group, pretty_config) {
                    Ok(s) => Ok(s.into()),
                    Err(e) => Err(e.into()),
                }
            }
        }
    }

    fn from_os_str(addr: &impl ResourceAddress, s: &OsStr) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = RdsResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s.as_bytes())?;
        match addr {
            RdsResourceAddress::DBInstance { region, id } => Ok(RdsResource::DBInstance(RON.from_str(s)?)),
            RdsResourceAddress::DBCluster { region, id } => Ok(RdsResource::DBCluster(RON.from_str(s)?)),
            RdsResourceAddress::DBSubnetGroup { region, name } => Ok(RdsResource::DBSubnetGroup(RON.from_str(s)?)),
            RdsResourceAddress::DBParameterGroup { region, name } => Ok(RdsResource::DBParameterGroup(RON.from_str(s)?)),
        }
    }
}
