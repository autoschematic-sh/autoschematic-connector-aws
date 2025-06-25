use std::{collections::HashMap, path::Path};

use anyhow::{Context, bail};
use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};
use autoschematic_core::get_resource_output;
use aws_sdk_cloudfront::operation::get_key_group::GetKeyGroupError;

use crate::{addr::CloudFrontResourceAddress, resource::*};

use super::CloudFrontConnector;

impl CloudFrontConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let client = self.get_or_init_client("us-east-1").await?;

        let addr = CloudFrontResourceAddress::from_path(addr)?;

        match &addr {
            CloudFrontResourceAddress::Distribution { distribution_id } => {
                let result = client.get_distribution().id(distribution_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(distribution) = output.distribution else {
                            return Ok(None);
                        };

                        let Some(config) = distribution.distribution_config else {
                            return Ok(None);
                        };

                        // Very simplified conversion for now
                        let origins = config
                            .origins
                            .map(|o| {
                                o.items
                                    .into_iter()
                                    .map(|origin| Origin {
                                        id: origin.id,
                                        domain_name: origin.domain_name,
                                        origin_path: origin.origin_path,
                                        custom_origin_config: origin.custom_origin_config.map(|c| CustomOriginConfig {
                                            http_port: c.http_port,
                                            https_port: c.https_port,
                                            origin_protocol_policy: c.origin_protocol_policy.as_str().to_string(),
                                        }),
                                        s3_origin_config: origin.s3_origin_config.map(|c| S3OriginConfig { origin_access_identity: c.origin_access_identity })
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        let default_cache_behavior = config
                            .default_cache_behavior
                            .map(|dcb| CacheBehavior {
                                path_pattern: None,
                                target_origin_id: dcb.target_origin_id,
                                viewer_protocol_policy: dcb.viewer_protocol_policy.as_str().to_string(),
                                allowed_methods: dcb
                                    .allowed_methods
                                    .clone()
                                    .map(|am| am.items.iter().map(|m| m.as_str().to_string()).collect())
                                    .unwrap_or_default(),
                                cached_methods: dcb
                                    .allowed_methods
                                    .and_then(|am| am.cached_methods)
                                    .map(|cm| cm.items.iter().map(|m| m.as_str().to_string()).collect())
                                    .unwrap_or_default(),
                                compress: dcb.compress.unwrap_or(false),
                                ttl_settings: TtlSettings {
                                    default_ttl: dcb.default_ttl,
                                    max_ttl:     dcb.max_ttl,
                                    min_ttl:     dcb.min_ttl.unwrap_or(0),
                                },
                            })
                            .unwrap_or_else(|| CacheBehavior {
                                path_pattern: None,
                                target_origin_id: String::new(),
                                viewer_protocol_policy: String::new(),
                                allowed_methods: vec![],
                                cached_methods: vec![],
                                compress: false,
                                ttl_settings: TtlSettings {
                                    default_ttl: None,
                                    max_ttl:     None,
                                    min_ttl:     0,
                                },
                            });

                        let cache_behaviors = config
                            .cache_behaviors
                            .map(|cb| {
                                cb.items
                                    .unwrap_or_default()
                                    .into_iter()
                                    .map(|behavior| CacheBehavior {
                                        path_pattern: Some(behavior.path_pattern),
                                        target_origin_id: behavior.target_origin_id,
                                        viewer_protocol_policy: behavior.viewer_protocol_policy.as_str().to_string(),
                                        allowed_methods: behavior
                                            .allowed_methods
                                            .clone()
                                            .map(|am| am.items.iter().map(|m| m.as_str().to_string()).collect())
                                            .unwrap_or_default(),
                                        cached_methods: behavior
                                            .allowed_methods
                                            .and_then(|am| am.cached_methods)
                                            .map(|cm| cm.items.iter().map(|m| m.as_str().to_string()).collect())
                                            .unwrap_or_default(),
                                        compress: behavior.compress.unwrap_or(false),
                                        ttl_settings: TtlSettings {
                                            default_ttl: behavior.default_ttl,
                                            max_ttl:     behavior.max_ttl,
                                            min_ttl:     behavior.min_ttl.unwrap_or(0),
                                        },
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        let tags = self.get_tags_for_resource(&addr, client).await?;

                        let dist = Distribution {
                            domain_name: distribution.domain_name,
                            enabled: config.enabled,
                            default_root_object: config.default_root_object,
                            origins,
                            default_cache_behavior,
                            cache_behaviors,
                            comment: Some(config.comment),
                            price_class: config.price_class.map(|pc| pc.as_str().to_string()),
                            tags,
                        };

                        get_resource_output!(
                            CloudFrontResource::Distribution(dist),
                            [(String::from("distribution_id"), distribution_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_distribution() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::OriginAccessControl { oac_id } => {
                let result = client.get_origin_access_control().id(oac_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(oac) = output.origin_access_control else {
                            return Ok(None);
                        };

                        let Some(config) = oac.origin_access_control_config else {
                            return Ok(None);
                        };

                        let origin_access_control = OriginAccessControl {
                            name: config.name,
                            description: config.description,
                            origin_access_control_origin_type: config.origin_access_control_origin_type.as_str().to_string(),
                            signing_behavior: config.signing_behavior.as_str().to_string(),
                            signing_protocol: config.signing_protocol.as_str().to_string(),
                        };

                        get_resource_output!(
                            CloudFrontResource::OriginAccessControl(origin_access_control),
                            [(String::from("oac_id"), oac_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_origin_access_control() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::CachePolicy { policy_id } => {
                let result = client.get_cache_policy().id(policy_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(policy) = output.cache_policy else {
                            return Ok(None);
                        };

                        let Some(config) = policy.cache_policy_config else {
                            return Ok(None);
                        };

                        let cache_policy = CachePolicy {
                            name: config.name,
                            comment: config.comment,
                            default_ttl: config.default_ttl,
                            max_ttl: config.max_ttl,
                            min_ttl: config.min_ttl,
                            parameters_in_cache_key_and_forwarded_to_origin: None, // Simplified for now
                        };

                        get_resource_output!(
                            CloudFrontResource::CachePolicy(cache_policy),
                            [(String::from("policy_id"), policy_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_cache_policy() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::Function { name } => {
                let result = client.get_function().name(name).send().await;

                match result {
                    Ok(output) => {
                        let function_code =
                            String::from_utf8(output.function_code.unwrap_or_default().into_inner()).unwrap_or_default();

                        let function = Function {
                            name: name.clone(),
                            function_code,
                            runtime: "cloudfront-js-1.0".to_string(), // Default runtime
                        };

                        get_resource_output!(
                            CloudFrontResource::Function(function),
                            [(String::from("function_name"), name.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_function_exists() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::OriginRequestPolicy { policy_id } => {
                let result = client.get_origin_request_policy().id(policy_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(policy) = output.origin_request_policy else {
                            return Ok(None);
                        };

                        let Some(config) = policy.origin_request_policy_config else {
                            return Ok(None);
                        };

                        let origin_request_policy = OriginRequestPolicy {
                            name: config.name,
                            comment: config.comment,
                            cookies_config: None,       // Simplified for now
                            headers_config: None,       // Simplified for now
                            query_strings_config: None, // Simplified for now
                        };

                        get_resource_output!(
                            CloudFrontResource::OriginRequestPolicy(origin_request_policy),
                            [(String::from("policy_id"), policy_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_origin_request_policy() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::ResponseHeadersPolicy { policy_id } => {
                let result = client.get_response_headers_policy().id(policy_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(policy) = output.response_headers_policy else {
                            return Ok(None);
                        };

                        let Some(config) = policy.response_headers_policy_config else {
                            return Ok(None);
                        };

                        let response_headers_policy = ResponseHeadersPolicy {
                            name: config.name,
                            comment: config.comment,
                            cors_config: None,             // Simplified for now
                            custom_headers_config: None,   // Simplified for now
                            security_headers_config: None, // Simplified for now
                        };

                        get_resource_output!(
                            CloudFrontResource::ResponseHeadersPolicy(response_headers_policy),
                            [(String::from("policy_id"), policy_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_response_headers_policy() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::RealtimeLogConfig { name } => {
                let result = client.get_realtime_log_config().name(name).send().await;

                match result {
                    Ok(output) => {
                        let Some(config) = output.realtime_log_config else {
                            return Ok(None);
                        };

                        let realtime_log_config = RealtimeLogConfig {
                            name: config.name,
                            end_points: vec![], // Simplified for now
                            fields: config.fields,
                            sampling_rate: config.sampling_rate as f64,
                        };

                        get_resource_output!(
                            CloudFrontResource::RealtimeLogConfig(realtime_log_config),
                            [(String::from("name"), name.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_realtime_log_config() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::KeyGroup { key_group_id } => {
                let result = client.get_key_group().id(key_group_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(key_group) = output.key_group else {
                            return Ok(None);
                        };

                        let Some(config) = key_group.key_group_config else {
                            return Ok(None);
                        };

                        let key_group = KeyGroup {
                            name:    config.name,
                            comment: config.comment,
                            items:   config.items,
                        };

                        get_resource_output!(
                            CloudFrontResource::KeyGroup(key_group),
                            [(String::from("key_group_id"), key_group_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(GetKeyGroupError::NoSuchResource(_)) = e.as_service_error() {
                            return Ok(None);
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::PublicKey { public_key_id } => {
                let result = client.get_public_key().id(public_key_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(public_key) = output.public_key else {
                            return Ok(None);
                        };

                        let Some(config) = public_key.public_key_config else {
                            return Ok(None);
                        };

                        let public_key = PublicKey {
                            name: config.name,
                            comment: config.comment,
                            encoded_key: config.encoded_key,
                        };

                        get_resource_output!(
                            CloudFrontResource::PublicKey(public_key),
                            [(String::from("public_key_id"), public_key_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_public_key() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::FieldLevelEncryptionConfig { config_id } => {
                let result = client.get_field_level_encryption_config().id(config_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(config) = output.field_level_encryption_config else {
                            return Ok(None);
                        };

                        let field_level_encryption_config = FieldLevelEncryptionConfig {
                            comment: config.comment,
                            caller_reference: config.caller_reference,
                            content_type_profile_config: None, // Simplified for now
                            query_arg_profile_config: None,    // Simplified for now
                        };

                        get_resource_output!(
                            CloudFrontResource::FieldLevelEncryptionConfig(field_level_encryption_config),
                            [(String::from("config_id"), config_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_field_level_encryption_config() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::FieldLevelEncryptionProfile { profile_id } => {
                let result = client.get_field_level_encryption_profile().id(profile_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(profile) = output.field_level_encryption_profile else {
                            return Ok(None);
                        };

                        let Some(config) = profile.field_level_encryption_profile_config else {
                            return Ok(None);
                        };

                        let field_level_encryption_profile = FieldLevelEncryptionProfile {
                            name: config.name,
                            comment: config.comment,
                            caller_reference: config.caller_reference,
                            encryption_entities: HashMap::new(), // Simplified for now
                        };

                        get_resource_output!(
                            CloudFrontResource::FieldLevelEncryptionProfile(field_level_encryption_profile),
                            [(String::from("profile_id"), profile_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_field_level_encryption_profile() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }

            CloudFrontResourceAddress::StreamingDistribution { distribution_id } => {
                let result = client.get_streaming_distribution().id(distribution_id).send().await;

                match result {
                    Ok(output) => {
                        let Some(distribution) = output.streaming_distribution else {
                            return Ok(None);
                        };

                        let Some(config) = distribution.streaming_distribution_config else {
                            return Ok(None);
                        };

                        let Some(s3_origin) = config.s3_origin else {
                            bail!("Streaming Distribution has no S3 Origin");
                        };

                        let streaming_distribution = StreamingDistribution {
                            domain_name: config.caller_reference,
                            enabled: config.enabled,
                            comment: Some(config.comment),
                            s3_origin: S3Origin {
                                domain_name: s3_origin.domain_name,
                                origin_access_identity: s3_origin.origin_access_identity,
                            },
                            trusted_signers: config.trusted_signers.map(|trusted_signers| TrustedSigners {
                                enabled:  trusted_signers.enabled,
                                quantity: trusted_signers.quantity,
                                items:    trusted_signers.items.unwrap_or_default(),
                            }),
                            price_class: config.price_class.map(|pc| pc.as_str().to_string()),
                            tags: Default::default(), // Simplified for now
                        };

                        get_resource_output!(
                            CloudFrontResource::StreamingDistribution(streaming_distribution),
                            [(String::from("distribution_id"), distribution_id.into())]
                        )
                    }
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_no_such_streaming_distribution() {
                                return Ok(None);
                            }
                        }
                        Err(e.into())
                    }
                }
            }
        }
    }
}
