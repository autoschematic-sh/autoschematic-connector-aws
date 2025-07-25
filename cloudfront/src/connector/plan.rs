use std::path::Path;

use autoschematic_core::{
    connector::{ConnectorOp, PlanResponseElement, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values, optional_string_from_utf8},
};

use crate::{
    addr::CloudFrontResourceAddress,
    op::CloudFrontConnectorOp,
    resource::{
        CachePolicy, Distribution, FieldLevelEncryptionConfig, FieldLevelEncryptionProfile, Function, KeyGroup,
        OriginAccessControl, OriginRequestPolicy, PublicKey, RealtimeLogConfig, ResponseHeadersPolicy, StreamingDistribution,
    }, 
};

use super::CloudFrontConnector;

impl CloudFrontConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;
        let addr = CloudFrontResourceAddress::from_path(addr)?;

        match addr {
            CloudFrontResourceAddress::Distribution { distribution_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_distribution)) => {
                        let new_distribution: Distribution = RON.from_str(&new_distribution)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateDistribution(new_distribution),
                            format!("Create new CloudFront distribution {}", distribution_id)
                        )])
                    }
                    (Some(_old_distribution), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteDistribution,
                        format!("DELETE CloudFront distribution {}", distribution_id)
                    )]),
                    (Some(old_distribution), Some(new_distribution)) => {
                        let old_distribution: Distribution = RON.from_str(&old_distribution)?;
                        let new_distribution: Distribution = RON.from_str(&new_distribution)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_distribution.tags != new_distribution.tags {
                            let diff = diff_ron_values(&old_distribution.tags, &new_distribution.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateTags {
                                    old_tags: old_distribution.tags.clone(),
                                    new_tags: new_distribution.tags.clone()
                                },
                                format!("Modify tags for CloudFront distribution `{}`\n{}", distribution_id, diff)
                            ));
                        }

                        // Check for basic distribution property changes
                        let mut message = String::new();
                        let mut distribution_changed = false;
                        if old_distribution.default_root_object != new_distribution.default_root_object {
                            distribution_changed = true;
                            message.push_str(&format!(" default_root_object={:?}", new_distribution.default_root_object));
                        }
                        if old_distribution.comment != new_distribution.comment {
                            distribution_changed = true;
                            message.push_str(&format!(" comment={:?}", new_distribution.comment));
                        }
                        if old_distribution.price_class != new_distribution.price_class {
                            distribution_changed = true;
                            message.push_str(&format!(" price_class={:?}", new_distribution.price_class));
                        }

                        if distribution_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateDistribution {
                                    default_root_object: new_distribution.default_root_object.clone(),
                                    comment: new_distribution.comment.clone(),
                                    price_class: new_distribution.price_class.clone(),
                                },
                                format!("Update CloudFront distribution `{}`: {}", distribution_id, message)
                            ));
                        }

                        if old_distribution.aliases != new_distribution.aliases {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateDistributionAliases {
                                    aliases: new_distribution.aliases.clone(),
                                },
                                format!("Update aliases for CloudFront distribution `{}`", distribution_id)
                            ));
                        }

                        // Check for origins changes
                        if old_distribution.origins != new_distribution.origins {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateDistributionOrigins {
                                    origins: new_distribution.origins.clone(),
                                },
                                format!("Update origins for CloudFront distribution `{}`", distribution_id)
                            ));
                        }

                        // Check for default cache behavior changes
                        if old_distribution.default_cache_behavior != new_distribution.default_cache_behavior {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateDistributionDefaultCacheBehavior {
                                    default_cache_behavior: new_distribution.default_cache_behavior.clone(),
                                },
                                format!(
                                    "Update default cache behavior for CloudFront distribution `{}`",
                                    distribution_id
                                )
                            ));
                        }

                        // Check for cache behaviors changes
                        if old_distribution.cache_behaviors != new_distribution.cache_behaviors {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateDistributionCacheBehaviors {
                                    cache_behaviors: new_distribution.cache_behaviors.clone(),
                                },
                                format!("Update cache behaviors for CloudFront distribution `{}`", distribution_id)
                            ));
                        }

                        // Handle enable/disable operations
                        if old_distribution.enabled && !new_distribution.enabled {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::DisableDistribution,
                                format!("Disable CloudFront distribution `{}`", distribution_id)
                            ));
                        } else if !old_distribution.enabled && new_distribution.enabled {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::EnableDistribution,
                                format!("Enable CloudFront distribution `{}`", distribution_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::OriginAccessControl { oac_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_oac)) => {
                        let new_oac: OriginAccessControl = RON.from_str(&new_oac)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateOriginAccessControl(new_oac),
                            format!("Create new CloudFront origin access control {}", oac_id)
                        )])
                    }
                    (Some(_old_oac), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteOriginAccessControl,
                        format!("DELETE CloudFront origin access control {}", oac_id)
                    )]),
                    (Some(old_oac), Some(new_oac)) => {
                        let old_oac: OriginAccessControl = RON.from_str(&old_oac)?;
                        let new_oac: OriginAccessControl = RON.from_str(&new_oac)?;
                        let mut ops = Vec::new();

                        // Check for origin access control property changes
                        let mut oac_changed = false;
                        if old_oac.name != new_oac.name {
                            oac_changed = true;
                        }
                        if old_oac.description != new_oac.description {
                            oac_changed = true;
                        }
                        if old_oac.origin_access_control_origin_type != new_oac.origin_access_control_origin_type {
                            oac_changed = true;
                        }
                        if old_oac.signing_behavior != new_oac.signing_behavior {
                            oac_changed = true;
                        }
                        if old_oac.signing_protocol != new_oac.signing_protocol {
                            oac_changed = true;
                        }

                        if oac_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateOriginAccessControl {
                                    name: Some(new_oac.name.clone()),
                                    description: new_oac.description.clone(),
                                    origin_access_control_origin_type: Some(new_oac.origin_access_control_origin_type.clone()),
                                    signing_behavior: Some(new_oac.signing_behavior.clone()),
                                    signing_protocol: Some(new_oac.signing_protocol.clone()),
                                },
                                format!("Update CloudFront origin access control `{}`", oac_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::CachePolicy { policy_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_policy)) => {
                        let new_policy: CachePolicy = RON.from_str(&new_policy)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateCachePolicy(new_policy),
                            format!("Create new CloudFront cache policy {}", policy_id)
                        )])
                    }
                    (Some(_old_policy), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteCachePolicy,
                        format!("DELETE CloudFront cache policy {}", policy_id)
                    )]),
                    (Some(old_policy), Some(new_policy)) => {
                        let old_policy: CachePolicy = RON.from_str(&old_policy)?;
                        let new_policy: CachePolicy = RON.from_str(&new_policy)?;
                        let mut ops = Vec::new();

                        // Check for cache policy property changes
                        let mut policy_changed = false;
                        if old_policy.name != new_policy.name {
                            policy_changed = true;
                        }
                        if old_policy.comment != new_policy.comment {
                            policy_changed = true;
                        }
                        if old_policy.default_ttl != new_policy.default_ttl {
                            policy_changed = true;
                        }
                        if old_policy.max_ttl != new_policy.max_ttl {
                            policy_changed = true;
                        }
                        if old_policy.min_ttl != new_policy.min_ttl {
                            policy_changed = true;
                        }
                        if old_policy.parameters_in_cache_key_and_forwarded_to_origin
                            != new_policy.parameters_in_cache_key_and_forwarded_to_origin
                        {
                            policy_changed = true;
                        }

                        if policy_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateCachePolicy {
                                    name: Some(new_policy.name.clone()),
                                    comment: new_policy.comment.clone(),
                                    default_ttl: new_policy.default_ttl,
                                    max_ttl: new_policy.max_ttl,
                                    min_ttl: new_policy.min_ttl,
                                    parameters_in_cache_key_and_forwarded_to_origin: new_policy
                                        .parameters_in_cache_key_and_forwarded_to_origin
                                        .clone(),
                                },
                                format!("Update CloudFront cache policy `{}`", policy_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::OriginRequestPolicy { policy_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_policy)) => {
                        let new_policy: OriginRequestPolicy = RON.from_str(&new_policy)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateOriginRequestPolicy(new_policy),
                            format!("Create new CloudFront origin request policy {}", policy_id)
                        )])
                    }
                    (Some(_old_policy), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteOriginRequestPolicy,
                        format!("DELETE CloudFront origin request policy {}", policy_id)
                    )]),
                    (Some(old_policy), Some(new_policy)) => {
                        let old_policy: OriginRequestPolicy = RON.from_str(&old_policy)?;
                        let new_policy: OriginRequestPolicy = RON.from_str(&new_policy)?;
                        let mut ops = Vec::new();

                        // Check for origin request policy property changes
                        let mut policy_changed = false;
                        if old_policy.name != new_policy.name {
                            policy_changed = true;
                        }
                        if old_policy.comment != new_policy.comment {
                            policy_changed = true;
                        }
                        if old_policy.cookies_config != new_policy.cookies_config {
                            policy_changed = true;
                        }
                        if old_policy.headers_config != new_policy.headers_config {
                            policy_changed = true;
                        }
                        if old_policy.query_strings_config != new_policy.query_strings_config {
                            policy_changed = true;
                        }

                        if policy_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateOriginRequestPolicy {
                                    name: Some(new_policy.name.clone()),
                                    comment: new_policy.comment.clone(),
                                    cookies_config: new_policy.cookies_config.clone(),
                                    headers_config: new_policy.headers_config.clone(),
                                    query_strings_config: new_policy.query_strings_config.clone(),
                                },
                                format!("Update CloudFront origin request policy `{}`", policy_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::ResponseHeadersPolicy { policy_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_policy)) => {
                        let new_policy: ResponseHeadersPolicy = RON.from_str(&new_policy)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateResponseHeadersPolicy(new_policy),
                            format!("Create new CloudFront response headers policy {}", policy_id)
                        )])
                    }
                    (Some(_old_policy), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteResponseHeadersPolicy,
                        format!("DELETE CloudFront response headers policy {}", policy_id)
                    )]),
                    (Some(old_policy), Some(new_policy)) => {
                        let old_policy: ResponseHeadersPolicy = RON.from_str(&old_policy)?;
                        let new_policy: ResponseHeadersPolicy = RON.from_str(&new_policy)?;
                        let mut ops = Vec::new();

                        // Check for response headers policy property changes
                        let mut policy_changed = false;
                        if old_policy.name != new_policy.name {
                            policy_changed = true;
                        }
                        if old_policy.comment != new_policy.comment {
                            policy_changed = true;
                        }
                        if old_policy.cors_config != new_policy.cors_config {
                            policy_changed = true;
                        }
                        if old_policy.custom_headers_config != new_policy.custom_headers_config {
                            policy_changed = true;
                        }
                        if old_policy.security_headers_config != new_policy.security_headers_config {
                            policy_changed = true;
                        }

                        if policy_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateResponseHeadersPolicy {
                                    name: Some(new_policy.name.clone()),
                                    comment: new_policy.comment.clone(),
                                    cors_config: new_policy.cors_config.clone(),
                                    custom_headers_config: new_policy.custom_headers_config.clone(),
                                    security_headers_config: new_policy.security_headers_config.clone(),
                                },
                                format!("Update CloudFront response headers policy `{}`", policy_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::RealtimeLogConfig { name } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_config)) => {
                        let new_config: RealtimeLogConfig = RON.from_str(&new_config)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateRealtimeLogConfig(new_config),
                            format!("Create new CloudFront realtime log config {}", name)
                        )])
                    }
                    (Some(_old_config), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteRealtimeLogConfig,
                        format!("DELETE CloudFront realtime log config {}", name)
                    )]),
                    (Some(old_config), Some(new_config)) => {
                        let old_config: RealtimeLogConfig = RON.from_str(&old_config)?;
                        let new_config: RealtimeLogConfig = RON.from_str(&new_config)?;
                        let mut ops = Vec::new();

                        // Check for realtime log config property changes
                        let mut config_changed = false;
                        if old_config.name != new_config.name {
                            config_changed = true;
                        }
                        if old_config.end_points != new_config.end_points {
                            config_changed = true;
                        }
                        if old_config.fields != new_config.fields {
                            config_changed = true;
                        }
                        if old_config.sampling_rate != new_config.sampling_rate {
                            config_changed = true;
                        }

                        if config_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateRealtimeLogConfig {
                                    name: Some(new_config.name.clone()),
                                    end_points: Some(new_config.end_points.clone()),
                                    fields: Some(new_config.fields.clone()),
                                    sampling_rate: Some(new_config.sampling_rate),
                                },
                                format!("Update CloudFront realtime log config `{}`", name)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::Function { name } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_function)) => {
                        let new_function: Function = RON.from_str(&new_function)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateFunction(new_function),
                            format!("Create new CloudFront function {}", name)
                        )])
                    }
                    (Some(_old_function), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteFunction,
                        format!("DELETE CloudFront function {}", name)
                    )]),
                    (Some(old_function), Some(new_function)) => {
                        let old_function: Function = RON.from_str(&old_function)?;
                        let new_function: Function = RON.from_str(&new_function)?;
                        let mut ops = Vec::new();

                        // Check for function property changes
                        let mut function_changed = false;
                        if old_function.name != new_function.name {
                            function_changed = true;
                        }
                        if old_function.function_code != new_function.function_code {
                            function_changed = true;
                        }
                        if old_function.runtime != new_function.runtime {
                            function_changed = true;
                        }

                        if function_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateFunction {
                                    name: Some(new_function.name.clone()),
                                    function_code: Some(new_function.function_code.clone()),
                                    runtime: Some(new_function.runtime.clone()),
                                },
                                format!("Update CloudFront function `{}`", name)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::KeyGroup { key_group_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_key_group)) => {
                        let new_key_group: KeyGroup = RON.from_str(&new_key_group)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateKeyGroup(new_key_group),
                            format!("Create new CloudFront key group {}", key_group_id)
                        )])
                    }
                    (Some(_old_key_group), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteKeyGroup,
                        format!("DELETE CloudFront key group {}", key_group_id)
                    )]),
                    (Some(old_key_group), Some(new_key_group)) => {
                        let old_key_group: KeyGroup = RON.from_str(&old_key_group)?;
                        let new_key_group: KeyGroup = RON.from_str(&new_key_group)?;
                        let mut ops = Vec::new();

                        // Check for key group property changes
                        let mut key_group_changed = false;
                        if old_key_group.name != new_key_group.name {
                            key_group_changed = true;
                        }
                        if old_key_group.comment != new_key_group.comment {
                            key_group_changed = true;
                        }
                        if old_key_group.items != new_key_group.items {
                            key_group_changed = true;
                        }

                        if key_group_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateKeyGroup {
                                    name:    Some(new_key_group.name.clone()),
                                    comment: new_key_group.comment.clone(),
                                    items:   Some(new_key_group.items.clone()),
                                },
                                format!("Update CloudFront key group `{}`", key_group_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::PublicKey { public_key_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_public_key)) => {
                        let new_public_key: PublicKey = RON.from_str(&new_public_key)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreatePublicKey(new_public_key),
                            format!("Create new CloudFront public key {}", public_key_id)
                        )])
                    }
                    (Some(_old_public_key), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeletePublicKey,
                        format!("DELETE CloudFront public key {}", public_key_id)
                    )]),
                    (Some(old_public_key), Some(new_public_key)) => {
                        let old_public_key: PublicKey = RON.from_str(&old_public_key)?;
                        let new_public_key: PublicKey = RON.from_str(&new_public_key)?;
                        let mut ops = Vec::new();

                        // Check for public key property changes
                        let mut public_key_changed = false;
                        if old_public_key.name != new_public_key.name {
                            public_key_changed = true;
                        }
                        if old_public_key.comment != new_public_key.comment {
                            public_key_changed = true;
                        }
                        if old_public_key.encoded_key != new_public_key.encoded_key {
                            public_key_changed = true;
                        }

                        if public_key_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdatePublicKey {
                                    name: Some(new_public_key.name.clone()),
                                    comment: new_public_key.comment.clone(),
                                    encoded_key: Some(new_public_key.encoded_key.clone()),
                                },
                                format!("Update CloudFront public key `{}`", public_key_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::FieldLevelEncryptionConfig { config_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_config)) => {
                        let new_config: FieldLevelEncryptionConfig = RON.from_str(&new_config)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateFieldLevelEncryptionConfig(new_config),
                            format!("Create new CloudFront field level encryption config {}", config_id)
                        )])
                    }
                    (Some(_old_config), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteFieldLevelEncryptionConfig,
                        format!("DELETE CloudFront field level encryption config {}", config_id)
                    )]),
                    (Some(old_config), Some(new_config)) => {
                        let old_config: FieldLevelEncryptionConfig = RON.from_str(&old_config)?;
                        let new_config: FieldLevelEncryptionConfig = RON.from_str(&new_config)?;
                        let mut ops = Vec::new();

                        // Check for field level encryption config property changes
                        let mut config_changed = false;
                        if old_config.comment != new_config.comment {
                            config_changed = true;
                        }
                        if old_config.content_type_profile_config != new_config.content_type_profile_config {
                            config_changed = true;
                        }
                        if old_config.query_arg_profile_config != new_config.query_arg_profile_config {
                            config_changed = true;
                        }

                        if config_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateFieldLevelEncryptionConfig {
                                    comment: new_config.comment.clone(),
                                    content_type_profile_config: new_config.content_type_profile_config.clone(),
                                    query_arg_profile_config: new_config.query_arg_profile_config.clone(),
                                },
                                format!("Update CloudFront field level encryption config `{}`", config_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::FieldLevelEncryptionProfile { profile_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_profile)) => {
                        let new_profile: FieldLevelEncryptionProfile = RON.from_str(&new_profile)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateFieldLevelEncryptionProfile(new_profile),
                            format!("Create new CloudFront field level encryption profile {}", profile_id)
                        )])
                    }
                    (Some(_old_profile), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteFieldLevelEncryptionProfile,
                        format!("DELETE CloudFront field level encryption profile {}", profile_id)
                    )]),
                    (Some(old_profile), Some(new_profile)) => {
                        let old_profile: FieldLevelEncryptionProfile = RON.from_str(&old_profile)?;
                        let new_profile: FieldLevelEncryptionProfile = RON.from_str(&new_profile)?;
                        let mut ops = Vec::new();

                        // Check for field level encryption profile property changes
                        let mut profile_changed = false;
                        if old_profile.name != new_profile.name {
                            profile_changed = true;
                        }
                        if old_profile.comment != new_profile.comment {
                            profile_changed = true;
                        }
                        if old_profile.encryption_entities != new_profile.encryption_entities {
                            profile_changed = true;
                        }

                        if profile_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateFieldLevelEncryptionProfile {
                                    name: Some(new_profile.name.clone()),
                                    comment: new_profile.comment.clone(),
                                    encryption_entities: Some(new_profile.encryption_entities.clone()),
                                },
                                format!("Update CloudFront field level encryption profile `{}`", profile_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudFrontResourceAddress::StreamingDistribution { distribution_id } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_streaming_dist)) => {
                        let new_streaming_dist: StreamingDistribution = RON.from_str(&new_streaming_dist)?;
                        Ok(vec![connector_op!(
                            CloudFrontConnectorOp::CreateStreamingDistribution(new_streaming_dist),
                            format!("Create new CloudFront streaming distribution {}", distribution_id)
                        )])
                    }
                    (Some(_old_streaming_dist), None) => Ok(vec![connector_op!(
                        CloudFrontConnectorOp::DeleteStreamingDistribution,
                        format!("DELETE CloudFront streaming distribution {}", distribution_id)
                    )]),
                    (Some(old_streaming_dist), Some(new_streaming_dist)) => {
                        let old_streaming_dist: StreamingDistribution = RON.from_str(&old_streaming_dist)?;
                        let new_streaming_dist: StreamingDistribution = RON.from_str(&new_streaming_dist)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_streaming_dist.tags != new_streaming_dist.tags {
                            let diff = diff_ron_values(&old_streaming_dist.tags, &new_streaming_dist.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateTags{
                                    old_tags: old_streaming_dist.tags.clone(),
                                    new_tags: new_streaming_dist.tags.clone()
                                },
                                format!(
                                    "Modify tags for CloudFront streaming distribution `{}`\n{}",
                                    distribution_id, diff
                                )
                            ));
                        }

                        // Check for streaming distribution property changes
                        let mut streaming_dist_changed = false;
                        if old_streaming_dist.enabled != new_streaming_dist.enabled {
                            streaming_dist_changed = true;
                        }
                        if old_streaming_dist.comment != new_streaming_dist.comment {
                            streaming_dist_changed = true;
                        }
                        if old_streaming_dist.price_class != new_streaming_dist.price_class {
                            streaming_dist_changed = true;
                        }

                        if streaming_dist_changed {
                            ops.push(connector_op!(
                                CloudFrontConnectorOp::UpdateStreamingDistribution {
                                    enabled:     Some(new_streaming_dist.enabled),
                                    comment:     new_streaming_dist.comment.clone(),
                                    price_class: new_streaming_dist.price_class.clone(),
                                },
                                format!("Update CloudFront streaming distribution `{}`", distribution_id)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
        }
    }
}
