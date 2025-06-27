use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::tags::Tags;

use super::resource::{
    CacheBehavior, CachePolicy, Distribution, EndPoint, FieldLevelEncryptionConfig, FieldLevelEncryptionProfile, Function,
    KeyGroup, Origin, OriginAccessControl, OriginRequestPolicy, PublicKey, RealtimeLogConfig, ResponseHeadersPolicy,
    StreamingDistribution, TtlSettings,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum CloudFrontConnectorOp {
    // Distribution operations
    CreateDistribution(Distribution),
    UpdateDistribution {
        default_root_object: Option<String>,
        comment: Option<String>,
        price_class: Option<String>,
    },
    UpdateDistributionOrigins {
        origins: Vec<Origin>,
    },
    UpdateDistributionAliases {
        aliases: Option<Vec<String>>,
    },
    UpdateDistributionDefaultCacheBehavior {
        default_cache_behavior: CacheBehavior,
    },
    UpdateDistributionCacheBehaviors {
        cache_behaviors: Vec<CacheBehavior>,
    },
    EnableDistribution,
    DisableDistribution,
    CreateInvalidation {
        paths: Vec<String>,
        caller_reference: String,
    },
    DeleteDistribution,

    // Origin Access Control operations
    CreateOriginAccessControl(OriginAccessControl),
    UpdateOriginAccessControl {
        name: Option<String>,
        description: Option<String>,
        origin_access_control_origin_type: Option<String>,
        signing_behavior: Option<String>,
        signing_protocol: Option<String>,
    },
    DeleteOriginAccessControl,

    // Cache Policy operations
    CreateCachePolicy(CachePolicy),
    UpdateCachePolicy {
        name: Option<String>,
        comment: Option<String>,
        default_ttl: Option<i64>,
        max_ttl: Option<i64>,
        min_ttl: Option<i64>,
        parameters_in_cache_key_and_forwarded_to_origin: Option<HashMap<String, serde_json::Value>>,
    },
    DeleteCachePolicy,

    // Origin Request Policy operations
    CreateOriginRequestPolicy(OriginRequestPolicy),
    UpdateOriginRequestPolicy {
        name: Option<String>,
        comment: Option<String>,
        cookies_config: Option<HashMap<String, serde_json::Value>>,
        headers_config: Option<HashMap<String, serde_json::Value>>,
        query_strings_config: Option<HashMap<String, serde_json::Value>>,
    },
    DeleteOriginRequestPolicy,

    // Response Headers Policy operations
    CreateResponseHeadersPolicy(ResponseHeadersPolicy),
    UpdateResponseHeadersPolicy {
        name: Option<String>,
        comment: Option<String>,
        cors_config: Option<HashMap<String, serde_json::Value>>,
        custom_headers_config: Option<HashMap<String, serde_json::Value>>,
        security_headers_config: Option<HashMap<String, serde_json::Value>>,
    },
    DeleteResponseHeadersPolicy,

    // Realtime Log Config operations
    CreateRealtimeLogConfig(RealtimeLogConfig),
    UpdateRealtimeLogConfig {
        name: Option<String>,
        end_points: Option<Vec<EndPoint>>,
        fields: Option<Vec<String>>,
        sampling_rate: Option<f64>,
    },
    DeleteRealtimeLogConfig,

    // Function operations
    CreateFunction(Function),
    UpdateFunction {
        name: Option<String>,
        function_code: Option<String>,
        runtime: Option<String>,
    },
    PublishFunction {
        if_match: String,
    },
    // TestFunction {
    //     name: String,
    //     if_match: String,
    //     stage: String, // DEVELOPMENT or LIVE
    //     event_object: ron::Value,
    // },
    DeleteFunction,

    // Key Group operations
    CreateKeyGroup(KeyGroup),
    UpdateKeyGroup {
        name:    Option<String>,
        comment: Option<String>,
        items:   Option<Vec<String>>,
    },
    DeleteKeyGroup,

    // Public Key operations
    CreatePublicKey(PublicKey),
    UpdatePublicKey {
        name: Option<String>,
        comment: Option<String>,
        encoded_key: Option<String>,
    },
    DeletePublicKey,

    // Field Level Encryption Config operations
    CreateFieldLevelEncryptionConfig(FieldLevelEncryptionConfig),
    UpdateFieldLevelEncryptionConfig {
        comment: Option<String>,
        content_type_profile_config: Option<HashMap<String, serde_json::Value>>,
        query_arg_profile_config: Option<HashMap<String, serde_json::Value>>,
    },
    DeleteFieldLevelEncryptionConfig,

    // Field Level Encryption Profile operations
    CreateFieldLevelEncryptionProfile(FieldLevelEncryptionProfile),
    UpdateFieldLevelEncryptionProfile {
        name: Option<String>,
        comment: Option<String>,
        encryption_entities: Option<HashMap<String, serde_json::Value>>,
    },
    DeleteFieldLevelEncryptionProfile,

    // Streaming Distribution operations
    CreateStreamingDistribution(StreamingDistribution),
    UpdateStreamingDistribution {
        enabled:     Option<bool>,
        comment:     Option<String>,
        price_class: Option<String>,
    },
    DeleteStreamingDistribution,

    UpdateTags{ old_tags: Tags, new_tags: Tags }
}

impl ConnectorOp for CloudFrontConnectorOp {
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
