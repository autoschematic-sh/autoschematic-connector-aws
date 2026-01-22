use autoschematic_core::{
    connector::{Resource, ResourceAddress},
    util::PrettyConfig,
};
use autoschematic_macros::FieldTypes;
use autoschematic_core::macros::FieldTypes;
use documented::{Documented, DocumentedFields};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::RON;

use super::addr::Route53ResourceAddress;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct HostedZone {
    // id: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct HealthCheck {}

/// Configuration for an alias record that routes traffic to an AWS resource.
#[derive(Debug, Serialize, Deserialize, PartialEq, Documented, DocumentedFields, FieldTypes)]
pub struct AliasTarget {
    /// The hosted zone ID of the target resource (e.g., CloudFront, ELB, S3).
    pub hosted_zone_id: String,
    /// The DNS name of the target resource.
    pub dns_name: String,
    /// Whether to check the health of the target resource before routing traffic.
    pub evaluate_target_health: bool,
}


/// A Route53 DNS record set.
#[derive(Debug, Serialize, Deserialize, PartialEq, Documented, DocumentedFields, FieldTypes)]
pub struct RecordSet {
    /// Time to live in seconds. Not used for alias records.
    pub ttl: Option<i64>,
    /// Alias target configuration. Mutually exclusive with resource_records.
    pub alias_target: Option<AliasTarget>,
    /// List of record values (e.g., IP addresses for A records). Mutually exclusive with alias_target.
    pub resource_records: Option<Vec<String>>,
}

pub enum Route53Resource {
    HostedZone(HostedZone),
    RecordSet(RecordSet),
    HealthCheck(HealthCheck),
}

impl Resource for Route53Resource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        match self {
            Route53Resource::HostedZone(hosted_zone) => match RON.to_string_pretty(&hosted_zone, PrettyConfig::default()) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            Route53Resource::RecordSet(record_set) => match RON.to_string_pretty(&record_set, PrettyConfig::default()) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            Route53Resource::HealthCheck(health_check) => match RON.to_string_pretty(&health_check, PrettyConfig::default()) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = Route53ResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;
        match addr {
            Route53ResourceAddress::HostedZone(_name) => Ok(Route53Resource::HostedZone(RON.from_str(s)?)),
            Route53ResourceAddress::ResourceRecordSet(_, _, _) => Ok(Route53Resource::RecordSet(RON.from_str(s)?)),
            Route53ResourceAddress::HealthCheck(_) => Ok(Route53Resource::HealthCheck(RON.from_str(s)?)),
        }
    }
}
