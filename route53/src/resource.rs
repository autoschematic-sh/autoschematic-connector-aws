use std::{
    ffi::{OsStr, OsString},
    os::unix::ffi::OsStrExt,
};

use autoschematic_core::{
    connector::{Resource, ResourceAddress},
    util::PrettyConfig,
};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::RON;

use super::addr::Route53ResourceAddress;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct HostedZone {
    // id: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct HealthCheck {}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct RecordSet {
    pub ttl: Option<i64>,
    pub alias_target: Option<String>,
    pub resource_records: Option<Vec<String>>,
}

pub enum Route53Resource {
    HostedZone(HostedZone),
    RecordSet(RecordSet),
    HealthCheck(HealthCheck),
}

impl Resource for Route53Resource {
    fn to_os_string(&self) -> Result<OsString, anyhow::Error> {
        match self {
            Route53Resource::HostedZone(hosted_zone) => {
                match RON.to_string_pretty(&hosted_zone, PrettyConfig::default()) {
                    Ok(s) => Ok(s.into()),
                    Err(e) => Err(e.into()),
                }
            }
            Route53Resource::RecordSet(record_set) => {
                match RON.to_string_pretty(&record_set, PrettyConfig::default()) {
                    Ok(s) => Ok(s.into()),
                    Err(e) => Err(e.into()),
                }
            }
            Route53Resource::HealthCheck(health_check) => {
                match RON.to_string_pretty(&health_check, PrettyConfig::default()) {
                    Ok(s) => Ok(s.into()),
                    Err(e) => Err(e.into()),
                }
            }
        }
    }

    fn from_os_str(addr: &impl ResourceAddress, s: &OsStr) -> Result<Option<Self>, anyhow::Error>
    where
        Self: Sized,
    {
        let Some(addr) = Route53ResourceAddress::from_path(&addr.to_path_buf())? else {
            return Ok(None);
        };

        let s = str::from_utf8(s.as_bytes())?;
        match addr {
            Route53ResourceAddress::HostedZone(_name) => {
                return Ok(Some(Route53Resource::HostedZone(RON.from_str(s)?)));
            }
            Route53ResourceAddress::ResourceRecordSet(_, _, _) => {
                return Ok(Some(Route53Resource::RecordSet(RON.from_str(s)?)));
            }
            Route53ResourceAddress::HealthCheck(_) => {
                return Ok(Some(Route53Resource::HealthCheck(RON.from_str(s)?)));
            }
        }
    }
}
