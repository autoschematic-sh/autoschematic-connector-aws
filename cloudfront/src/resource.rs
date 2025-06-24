use autoschematic_core::connector::{Resource, ResourceAddress};
use autoschematic_core::util::{PrettyConfig, RON};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::addr::CloudFrontResourceAddress;

type Tags = HashMap<String, String>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Distribution {
    pub domain_name: String,
    pub enabled: bool,
    pub default_root_object: Option<String>,
    pub origins: Vec<Origin>,
    pub default_cache_behavior: CacheBehavior,
    pub cache_behaviors: Vec<CacheBehavior>,
    pub comment: Option<String>,
    pub price_class: Option<String>,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Origin {
    pub id: String,
    pub domain_name: String,
    pub origin_path: Option<String>,
    pub custom_origin_config: Option<CustomOriginConfig>,
    pub s3_origin_config: Option<S3OriginConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CustomOriginConfig {
    pub http_port: i32,
    pub https_port: i32,
    pub origin_protocol_policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct S3OriginConfig {
    pub origin_access_identity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CacheBehavior {
    pub path_pattern: Option<String>,
    pub target_origin_id: String,
    pub viewer_protocol_policy: String,
    pub allowed_methods: Vec<String>,
    pub cached_methods: Vec<String>,
    pub compress: bool,
    pub ttl_settings: TtlSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TtlSettings {
    pub default_ttl: Option<i64>,
    pub max_ttl:     Option<i64>,
    pub min_ttl:     i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OriginAccessControl {
    pub name: String,
    pub description: Option<String>,
    pub origin_access_control_origin_type: String,
    pub signing_behavior: String,
    pub signing_protocol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CachePolicy {
    pub name: String,
    pub comment: Option<String>,
    pub default_ttl: Option<i64>,
    pub max_ttl: Option<i64>,
    pub min_ttl: i64,
    pub parameters_in_cache_key_and_forwarded_to_origin: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OriginRequestPolicy {
    pub name: String,
    pub comment: Option<String>,
    pub cookies_config: Option<HashMap<String, serde_json::Value>>,
    pub headers_config: Option<HashMap<String, serde_json::Value>>,
    pub query_strings_config: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ResponseHeadersPolicy {
    pub name: String,
    pub comment: Option<String>,
    pub cors_config: Option<HashMap<String, serde_json::Value>>,
    pub custom_headers_config: Option<HashMap<String, serde_json::Value>>,
    pub security_headers_config: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RealtimeLogConfig {
    pub name: String,
    pub end_points: Vec<EndPoint>,
    pub fields: Vec<String>,
    pub sampling_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EndPoint {
    pub stream_type: String,
    pub kinesis_stream_config: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Function {
    pub name: String,
    pub function_code: String,
    pub runtime: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KeyGroup {
    pub name:    String,
    pub comment: Option<String>,
    pub items:   Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PublicKey {
    pub name: String,
    pub comment: Option<String>,
    pub encoded_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FieldLevelEncryptionConfig {
    pub comment: Option<String>,
    pub caller_reference: String,
    pub content_type_profile_config: Option<HashMap<String, serde_json::Value>>,
    pub query_arg_profile_config: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FieldLevelEncryptionProfile {
    pub name: String,
    pub comment: Option<String>,
    pub caller_reference: String,
    pub encryption_entities: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StreamingDistribution {
    pub domain_name: String,
    pub enabled: bool,
    pub comment: Option<String>,
    pub s3_origin: S3Origin,
    pub trusted_signers: Option<TrustedSigners>,
    pub price_class: Option<String>,
    pub tags: Tags,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct S3Origin {
    pub domain_name: String,
    pub origin_access_identity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TrustedSigners {
    pub enabled:  bool,
    pub quantity: i32,
    pub items:    Vec<String>,
}

pub enum CloudFrontResource {
    Distribution(Distribution),
    OriginAccessControl(OriginAccessControl),
    CachePolicy(CachePolicy),
    OriginRequestPolicy(OriginRequestPolicy),
    ResponseHeadersPolicy(ResponseHeadersPolicy),
    RealtimeLogConfig(RealtimeLogConfig),
    Function(Function),
    KeyGroup(KeyGroup),
    PublicKey(PublicKey),
    FieldLevelEncryptionConfig(FieldLevelEncryptionConfig),
    FieldLevelEncryptionProfile(FieldLevelEncryptionProfile),
    StreamingDistribution(StreamingDistribution),
}

impl Resource for CloudFrontResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        match self {
            CloudFrontResource::Distribution(dist) => match RON.to_string_pretty(&dist, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::OriginAccessControl(oac) => match RON.to_string_pretty(&oac, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::CachePolicy(policy) => match RON.to_string_pretty(&policy, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::OriginRequestPolicy(policy) => match RON.to_string_pretty(&policy, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::ResponseHeadersPolicy(policy) => match RON.to_string_pretty(&policy, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::RealtimeLogConfig(config) => match RON.to_string_pretty(&config, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::Function(function) => match RON.to_string_pretty(&function, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::KeyGroup(key_group) => match RON.to_string_pretty(&key_group, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::PublicKey(public_key) => match RON.to_string_pretty(&public_key, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::FieldLevelEncryptionConfig(config) => match RON.to_string_pretty(&config, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::FieldLevelEncryptionProfile(profile) => match RON.to_string_pretty(&profile, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudFrontResource::StreamingDistribution(dist) => match RON.to_string_pretty(&dist, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = CloudFrontResourceAddress::from_path(&addr.to_path_buf())?;

        let s = std::str::from_utf8(s)?;
        match addr {
            CloudFrontResourceAddress::Distribution { .. } => Ok(CloudFrontResource::Distribution(RON.from_str(s)?)),
            CloudFrontResourceAddress::OriginAccessControl { .. } => {
                Ok(CloudFrontResource::OriginAccessControl(RON.from_str(s)?))
            }
            CloudFrontResourceAddress::CachePolicy { .. } => Ok(CloudFrontResource::CachePolicy(RON.from_str(s)?)),
            CloudFrontResourceAddress::OriginRequestPolicy { .. } => {
                Ok(CloudFrontResource::OriginRequestPolicy(RON.from_str(s)?))
            }
            CloudFrontResourceAddress::ResponseHeadersPolicy { .. } => {
                Ok(CloudFrontResource::ResponseHeadersPolicy(RON.from_str(s)?))
            }
            CloudFrontResourceAddress::RealtimeLogConfig { .. } => Ok(CloudFrontResource::RealtimeLogConfig(RON.from_str(s)?)),
            CloudFrontResourceAddress::Function { .. } => Ok(CloudFrontResource::Function(RON.from_str(s)?)),
            CloudFrontResourceAddress::KeyGroup { .. } => Ok(CloudFrontResource::KeyGroup(RON.from_str(s)?)),
            CloudFrontResourceAddress::PublicKey { .. } => Ok(CloudFrontResource::PublicKey(RON.from_str(s)?)),
            CloudFrontResourceAddress::FieldLevelEncryptionConfig { .. } => {
                Ok(CloudFrontResource::FieldLevelEncryptionConfig(RON.from_str(s)?))
            }
            CloudFrontResourceAddress::FieldLevelEncryptionProfile { .. } => {
                Ok(CloudFrontResource::FieldLevelEncryptionProfile(RON.from_str(s)?))
            }
            CloudFrontResourceAddress::StreamingDistribution { .. } => {
                Ok(CloudFrontResource::StreamingDistribution(RON.from_str(s)?))
            }
        }
    }
}
