use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum CloudFrontResourceAddress {
    Distribution { distribution_id: String },
    OriginAccessControl { oac_id: String },
    CachePolicy { policy_id: String },
    OriginRequestPolicy { policy_id: String },
    ResponseHeadersPolicy { policy_id: String },
    RealtimeLogConfig { name: String },
    Function { name: String },
    KeyGroup { key_group_id: String },
    PublicKey { public_key_id: String },
    FieldLevelEncryptionConfig { config_id: String },
    FieldLevelEncryptionProfile { profile_id: String },
    StreamingDistribution { distribution_id: String },
}

impl ResourceAddress for CloudFrontResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            CloudFrontResourceAddress::Distribution { distribution_id } => {
                PathBuf::from(format!("aws/cloudfront/distributions/{}.ron", distribution_id))
            }
            CloudFrontResourceAddress::OriginAccessControl { oac_id } => {
                PathBuf::from(format!("aws/cloudfront/origin_access_controls/{}.ron", oac_id))
            }
            CloudFrontResourceAddress::CachePolicy { policy_id } => {
                PathBuf::from(format!("aws/cloudfront/cache_policies/{}.ron", policy_id))
            }
            CloudFrontResourceAddress::OriginRequestPolicy { policy_id } => {
                PathBuf::from(format!("aws/cloudfront/origin_request_policies/{}.ron", policy_id))
            }
            CloudFrontResourceAddress::ResponseHeadersPolicy { policy_id } => {
                PathBuf::from(format!("aws/cloudfront/response_headers_policies/{}.ron", policy_id))
            }
            CloudFrontResourceAddress::RealtimeLogConfig { name } => {
                PathBuf::from(format!("aws/cloudfront/realtime_log_configs/{}.ron", name))
            }
            CloudFrontResourceAddress::Function { name } => PathBuf::from(format!("aws/cloudfront/functions/{}.ron", name)),
            CloudFrontResourceAddress::KeyGroup { key_group_id } => {
                PathBuf::from(format!("aws/cloudfront/key_groups/{}.ron", key_group_id))
            }
            CloudFrontResourceAddress::PublicKey { public_key_id } => {
                PathBuf::from(format!("aws/cloudfront/public_keys/{}.ron", public_key_id))
            }
            CloudFrontResourceAddress::FieldLevelEncryptionConfig { config_id } => {
                PathBuf::from(format!("aws/cloudfront/field_level_encryption_configs/{}.ron", config_id))
            }
            CloudFrontResourceAddress::FieldLevelEncryptionProfile { profile_id } => {
                PathBuf::from(format!("aws/cloudfront/field_level_encryption_profiles/{}.ron", profile_id))
            }
            CloudFrontResourceAddress::StreamingDistribution { distribution_id } => {
                PathBuf::from(format!("aws/cloudfront/streaming_distributions/{}.ron", distribution_id))
            }
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "cloudfront", "distributions", distribution_id] if distribution_id.ends_with(".ron") => {
                let distribution_id = distribution_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::Distribution { distribution_id })
            }
            ["aws", "cloudfront", "origin_access_controls", oac_id] if oac_id.ends_with(".ron") => {
                let oac_id = oac_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::OriginAccessControl { oac_id })
            }
            ["aws", "cloudfront", "cache_policies", policy_id] if policy_id.ends_with(".ron") => {
                let policy_id = policy_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::CachePolicy { policy_id })
            }
            ["aws", "cloudfront", "origin_request_policies", policy_id] if policy_id.ends_with(".ron") => {
                let policy_id = policy_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::OriginRequestPolicy { policy_id })
            }
            ["aws", "cloudfront", "response_headers_policies", policy_id] if policy_id.ends_with(".ron") => {
                let policy_id = policy_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::ResponseHeadersPolicy { policy_id })
            }
            ["aws", "cloudfront", "realtime_log_configs", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::RealtimeLogConfig { name })
            }
            ["aws", "cloudfront", "functions", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::Function { name })
            }
            ["aws", "cloudfront", "key_groups", key_group_id] if key_group_id.ends_with(".ron") => {
                let key_group_id = key_group_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::KeyGroup { key_group_id })
            }
            ["aws", "cloudfront", "public_keys", public_key_id] if public_key_id.ends_with(".ron") => {
                let public_key_id = public_key_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::PublicKey { public_key_id })
            }
            ["aws", "cloudfront", "field_level_encryption_configs", config_id] if config_id.ends_with(".ron") => {
                let config_id = config_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::FieldLevelEncryptionConfig { config_id })
            }
            ["aws", "cloudfront", "field_level_encryption_profiles", profile_id] if profile_id.ends_with(".ron") => {
                let profile_id = profile_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::FieldLevelEncryptionProfile { profile_id })
            }
            ["aws", "cloudfront", "streaming_distributions", distribution_id] if distribution_id.ends_with(".ron") => {
                let distribution_id = distribution_id.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudFrontResourceAddress::StreamingDistribution { distribution_id })
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
