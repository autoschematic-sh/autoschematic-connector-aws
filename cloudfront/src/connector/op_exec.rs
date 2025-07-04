use std::{collections::HashMap, path::Path, str::FromStr};

use anyhow::{Context, bail};
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    error_util::invalid_op,
    op_exec_output,
};
use aws_sdk_cloudfront::types::{builders::AliasesBuilder, Aliases, ParametersInCacheKeyAndForwardedToOrigin, PriceClass, Tag, TagKeys, Tags};

use crate::{addr::CloudFrontResourceAddress, op::CloudFrontConnectorOp, tags::tag_diff, util::get_distribution_config};

use super::CloudFrontConnector;

impl CloudFrontConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;
        let op = CloudFrontConnectorOp::from_str(op)?;
        let account_id = self.account_id.lock().await.clone();

        // CloudFront is a global service, but we'll use us-east-1 as the default region
        let client = self.get_or_init_client().await?;

        if let CloudFrontConnectorOp::UpdateTags { old_tags, new_tags } = op {
            let (untag, newtag) = tag_diff(&old_tags, &new_tags)?;

            let arn = self.get_resource_arn(&addr).await?;
            if !untag.is_empty() {
                client
                    .untag_resource()
                    .resource(&arn)
                    .tag_keys(TagKeys::builder().set_items(Some(untag)).build())
                    .send()
                    .await?;
            }
            if !newtag.is_empty() {
                client
                    .tag_resource()
                    .resource(&arn)
                    .set_tags(Some(Tags::builder().set_items(Some(newtag)).build()))
                    .send()
                    .await?;
            }
            return op_exec_output!(format!("Updated tags for resource `{}`", arn));
        }

        match &addr {
            CloudFrontResourceAddress::Distribution { distribution_id } => {
                match op {
                    CloudFrontConnectorOp::CreateDistribution(distribution) => {
                        let mut distribution_config = aws_sdk_cloudfront::types::DistributionConfig::builder()
                            .caller_reference(&format!("autoschematic-{}", uuid::Uuid::new_v4()))
                            .enabled(distribution.enabled);

                        eprintln!("Create Distribution");

                        if let Some(comment) = &distribution.comment {
                            distribution_config = distribution_config.comment(comment);
                        }

                        if let Some(default_root_object) = &distribution.default_root_object {
                            distribution_config = distribution_config.default_root_object(default_root_object);
                        }

                        if let Some(price_class) = &distribution.price_class {
                            distribution_config = distribution_config
                                .price_class(aws_sdk_cloudfront::types::PriceClass::from(price_class.as_str()));
                        }

                        // Add origins
                        let mut origins_builder = aws_sdk_cloudfront::types::Origins::builder();
                        for origin in &distribution.origins {
                            let mut origin_builder = aws_sdk_cloudfront::types::Origin::builder()
                                .id(&origin.id)
                                .domain_name(&origin.domain_name);

                            if let Some(origin_path) = &origin.origin_path {
                                origin_builder = origin_builder.origin_path(origin_path);
                            }

                            if let Some(custom_config) = &origin.custom_origin_config {
                                let custom_origin_config = aws_sdk_cloudfront::types::CustomOriginConfig::builder()
                                    .http_port(custom_config.http_port)
                                    .https_port(custom_config.https_port)
                                    .origin_protocol_policy(aws_sdk_cloudfront::types::OriginProtocolPolicy::from(
                                        custom_config.origin_protocol_policy.as_str(),
                                    ))
                                    .build()
                                    .map_err(|e| anyhow::anyhow!("Failed to build custom origin config: {}", e))?;
                                origin_builder = origin_builder.custom_origin_config(custom_origin_config);
                            }

                            if let Some(s3_config) = &origin.s3_origin_config {
                                let s3_origin_config = aws_sdk_cloudfront::types::S3OriginConfig::builder();
                                let s3_origin_config =
                                    s3_origin_config.origin_access_identity(s3_config.origin_access_identity.clone());
                                origin_builder = origin_builder.s3_origin_config(s3_origin_config.build());
                            }

                            origins_builder = origins_builder.items(
                                origin_builder
                                    .build()
                                    .map_err(|e| anyhow::anyhow!("Failed to build origin: {}", e))?,
                            );
                        }
                        let origins = origins_builder
                            .quantity(distribution.origins.len() as i32)
                            .build()
                            .map_err(|e| anyhow::anyhow!("Failed to build origins: {}", e))?;

                        // Default cache behavior
                        let default_cache_behavior = aws_sdk_cloudfront::types::DefaultCacheBehavior::builder()
                            .target_origin_id(&distribution.default_cache_behavior.target_origin_id)
                            .viewer_protocol_policy(aws_sdk_cloudfront::types::ViewerProtocolPolicy::from(
                                distribution.default_cache_behavior.viewer_protocol_policy.as_str(),
                            ))
                            .cache_policy_id(distribution.default_cache_behavior.id)
                            .compress(distribution.default_cache_behavior.compress)
                            .set_min_ttl(distribution.default_cache_behavior.ttl_settings.min_ttl)
                            .build()
                            .map_err(|e| anyhow::anyhow!("Failed to build default cache behavior: {}", e))?;

                        distribution_config = distribution_config
                            .origins(origins)
                            .default_cache_behavior(default_cache_behavior);

                        eprintln!("Creating Distribution...");
                        let response = client
                            .create_distribution()
                            .distribution_config(
                                distribution_config
                                    .build()
                                    .map_err(|e| anyhow::anyhow!("Failed to build distribution config: {}", e))?,
                            )
                            .send()
                            .await?;
                        eprintln!("Created Distribution");

                        let distribution_result = response.distribution().context("No distribution in response")?;
                        let distribution_id = distribution_result.id();
                        let arn = distribution_result.arn();

                        op_exec_output!(
                            Some([
                                ("distribution_id", Some(distribution_id.to_string())),
                                ("distribution_arn", Some(arn.to_string())),
                                ("domain_name", Some(distribution_result.domain_name().to_string()))
                            ]),
                            format!("Created CloudFront distribution `{}`", distribution_id)
                        )
                    }

                    CloudFrontConnectorOp::DeleteDistribution => {
                        // First get the current ETag
                        let get_response = client.get_distribution().id(distribution_id).send().await?;

                        let etag = get_response.e_tag().context("No ETag in response")?;

                        client.delete_distribution().id(distribution_id).if_match(etag).send().await?;

                        op_exec_output!(format!("Deleted CloudFront distribution `{}`", distribution_id))
                    }

                    CloudFrontConnectorOp::EnableDistribution => {
                        let get_response = client.get_distribution_config().id(distribution_id).send().await?;

                        let config = get_response.distribution_config().context("No distribution config")?.clone();
                        let etag = get_response.e_tag().context("No ETag in response")?;

                        let updated_config = aws_sdk_cloudfront::types::DistributionConfig::builder()
                            .set_aliases(config.aliases().cloned())
                            .caller_reference(config.caller_reference().to_string())
                            .comment(config.comment().to_string())
                            .set_default_cache_behavior(config.default_cache_behavior().cloned())
                            .set_origins(config.origins().cloned())
                            .enabled(true)
                            .build()
                            .map_err(|e| anyhow::anyhow!("Failed to build updated distribution config: {}", e))?;

                        client
                            .update_distribution()
                            .id(distribution_id)
                            .distribution_config(updated_config)
                            .if_match(etag)
                            .send()
                            .await?;

                        op_exec_output!(format!("Enabled CloudFront distribution `{}`", distribution_id))
                    }

                    CloudFrontConnectorOp::DisableDistribution => {
                        let get_response = client.get_distribution_config().id(distribution_id).send().await?;

                        let config = get_response.distribution_config().context("No distribution config")?.clone();
                        let etag = get_response.e_tag().context("No ETag in response")?;

                        let updated_config = aws_sdk_cloudfront::types::DistributionConfig::builder()
                            .set_aliases(config.aliases().cloned())
                            .caller_reference(config.caller_reference().to_string())
                            .comment(config.comment().to_string())
                            .set_default_cache_behavior(config.default_cache_behavior().cloned())
                            .set_origins(config.origins().cloned())
                            .enabled(false)
                            .build()
                            .map_err(|e| anyhow::anyhow!("Failed to build updated distribution config: {}", e))?;

                        client
                            .update_distribution()
                            .id(distribution_id)
                            .distribution_config(updated_config)
                            .if_match(etag)
                            .send()
                            .await?;

                        op_exec_output!(format!("Disabled CloudFront distribution `{}`", distribution_id))
                    }

                    CloudFrontConnectorOp::CreateInvalidation { paths, caller_reference } => {
                        let invalidation_batch = aws_sdk_cloudfront::types::InvalidationBatch::builder()
                            .paths(
                                aws_sdk_cloudfront::types::Paths::builder()
                                    .quantity(paths.len() as i32)
                                    .set_items(Some(paths.clone()))
                                    .build()
                                    .map_err(|e| anyhow::anyhow!("Failed to build paths: {}", e))?,
                            )
                            .caller_reference(caller_reference)
                            .build()
                            .map_err(|e| anyhow::anyhow!("Failed to build invalidation batch: {}", e))?;

                        let response = client
                            .create_invalidation()
                            .distribution_id(distribution_id)
                            .invalidation_batch(invalidation_batch)
                            .send()
                            .await?;

                        let invalidation_id = response.invalidation().context("No invalidation in response")?.id();

                        op_exec_output!(
                            Some([("invalidation_id", Some(invalidation_id.to_string()))]),
                            format!(
                                "Created invalidation `{}` for distribution `{}`",
                                invalidation_id, distribution_id
                            )
                        )
                    }

                    CloudFrontConnectorOp::UpdateDistribution {
                        default_root_object,
                        comment,
                        price_class,
                    } => {
                        let (etag, mut config) = get_distribution_config(distribution_id, &client).await?;

                        if let Some(comment) = comment {
                            config.comment = comment.clone();
                        }

                        config.default_root_object = default_root_object;

                        if let Some(price_class) = price_class {
                            config.price_class = Some(PriceClass::from_str(&price_class)?);
                        }

                        client
                            .update_distribution()
                            .id(distribution_id)
                            .distribution_config(config)
                            .if_match(etag)
                            .send()
                            .await?;

                        op_exec_output!(format!(
                            "Updated distribution for CloudFront distribution `{}`",
                            distribution_id
                        ))
                    }

                    CloudFrontConnectorOp::UpdateDistributionAliases { aliases } => {
                        let (etag, mut config) = get_distribution_config(distribution_id, &client).await?;

                        if let Some(aliases) = aliases {
                            let aliases_len = Some(aliases.len() as i32);
                            config.aliases = Some(
                                Aliases::builder()
                                    .set_items(Some(aliases))
                                    .set_quantity(aliases_len)
                                    .build()?,
                            );
                        } else {
                            config.aliases = None
                        }

                        client
                            .update_distribution()
                            .id(distribution_id)
                            .distribution_config(config)
                            .if_match(etag)
                            .send()
                            .await?;

                        op_exec_output!(format!("Updated aliases for CloudFront distribution `{}`", distribution_id))
                    }

                    CloudFrontConnectorOp::UpdateDistributionOrigins { origins } => {
                        let (etag, mut config) = get_distribution_config(distribution_id, &client).await?;

                        // Build new origins
                        let mut origins_builder = aws_sdk_cloudfront::types::Origins::builder();
                        for origin in origins {
                            let mut origin_builder = aws_sdk_cloudfront::types::Origin::builder()
                                .id(&origin.id)
                                .domain_name(&origin.domain_name);

                            if let Some(origin_path) = &origin.origin_path {
                                origin_builder = origin_builder.origin_path(origin_path);
                            }

                            if let Some(custom_config) = &origin.custom_origin_config {
                                let custom_origin_config = aws_sdk_cloudfront::types::CustomOriginConfig::builder()
                                    .http_port(custom_config.http_port)
                                    .https_port(custom_config.https_port)
                                    .origin_protocol_policy(aws_sdk_cloudfront::types::OriginProtocolPolicy::from(
                                        custom_config.origin_protocol_policy.as_str(),
                                    ))
                                    .build()
                                    .map_err(|e| anyhow::anyhow!("Failed to build custom origin config: {}", e))?;
                                origin_builder = origin_builder.custom_origin_config(custom_origin_config);
                            }

                            if let Some(s3_config) = &origin.s3_origin_config {
                                let s3_origin_config = aws_sdk_cloudfront::types::S3OriginConfig::builder()
                                    .origin_access_identity(&s3_config.origin_access_identity)
                                    .build();
                                origin_builder = origin_builder.s3_origin_config(s3_origin_config);
                            }

                            origins_builder = origins_builder.items(
                                origin_builder
                                    .build()
                                    .map_err(|e| anyhow::anyhow!("Failed to build origin: {}", e))?,
                            );
                        }

                        config.origins = Some(origins_builder.build()?);

                        client
                            .update_distribution()
                            .id(distribution_id)
                            .distribution_config(config)
                            .if_match(etag)
                            .send()
                            .await?;

                        op_exec_output!(format!("Updated origins for CloudFront distribution `{}`", distribution_id))
                    }

                    CloudFrontConnectorOp::UpdateDistributionDefaultCacheBehavior { default_cache_behavior } => {
                        let (etag, config) = get_distribution_config(distribution_id, &client).await?;

                        // Build new default cache behavior
                        let mut default_cache_behavior_builder = aws_sdk_cloudfront::types::DefaultCacheBehavior::builder()
                            .target_origin_id(&default_cache_behavior.target_origin_id)
                            .viewer_protocol_policy(aws_sdk_cloudfront::types::ViewerProtocolPolicy::from(
                                default_cache_behavior.viewer_protocol_policy.as_str(),
                            ))
                            .compress(default_cache_behavior.compress);

                        default_cache_behavior_builder =
                            default_cache_behavior_builder.cache_policy_id(default_cache_behavior.id);

                        if let Some(min_ttl) = default_cache_behavior.ttl_settings.min_ttl {
                            default_cache_behavior_builder = default_cache_behavior_builder.min_ttl(min_ttl);
                        }

                        let new_default_cache_behavior = default_cache_behavior_builder
                            .build()
                            .map_err(|e| anyhow::anyhow!("Failed to build default cache behavior: {}", e))?;

                        let updated_config = aws_sdk_cloudfront::types::DistributionConfig::builder()
                            .set_aliases(config.aliases.clone())
                            .caller_reference(&config.caller_reference)
                            .comment(&config.comment)
                            .default_cache_behavior(new_default_cache_behavior)
                            .set_origins(config.origins.clone())
                            .set_cache_behaviors(config.cache_behaviors.clone())
                            .enabled(config.enabled)
                            .set_price_class(config.price_class.clone())
                            .build()
                            .map_err(|e| anyhow::anyhow!("Failed to build distribution config: {}", e))?;

                        client
                            .update_distribution()
                            .id(distribution_id)
                            .distribution_config(updated_config)
                            .if_match(etag)
                            .send()
                            .await?;

                        op_exec_output!(format!(
                            "Updated default cache behavior for CloudFront distribution `{}`",
                            distribution_id
                        ))
                    }

                    CloudFrontConnectorOp::UpdateDistributionCacheBehaviors { cache_behaviors } => {
                        let (etag, config) = get_distribution_config(distribution_id, &client).await?;

                        // Build new cache behaviors
                        let mut cache_behaviors_builder = aws_sdk_cloudfront::types::CacheBehaviors::builder();
                        for cache_behavior in &cache_behaviors {
                            let mut cache_behavior_builder = aws_sdk_cloudfront::types::CacheBehavior::builder()
                                .set_path_pattern(cache_behavior.path_pattern.clone())
                                .target_origin_id(&cache_behavior.target_origin_id)
                                .viewer_protocol_policy(aws_sdk_cloudfront::types::ViewerProtocolPolicy::from(
                                    cache_behavior.viewer_protocol_policy.as_str(),
                                ))
                                .compress(cache_behavior.compress);

                            cache_behavior_builder = cache_behavior_builder.cache_policy_id(cache_behavior.id.clone());

                            if let Some(min_ttl) = cache_behavior.ttl_settings.min_ttl {
                                cache_behavior_builder = cache_behavior_builder.min_ttl(min_ttl);
                            }

                            cache_behaviors_builder = cache_behaviors_builder.items(
                                cache_behavior_builder
                                    .build()
                                    .map_err(|e| anyhow::anyhow!("Failed to build cache behavior: {}", e))?,
                            );
                        }

                        let new_cache_behaviors = cache_behaviors_builder.build()?;

                        let updated_config = aws_sdk_cloudfront::types::DistributionConfig::builder()
                            .set_aliases(config.aliases.clone())
                            .caller_reference(&config.caller_reference)
                            .comment(&config.comment)
                            .set_default_cache_behavior(config.default_cache_behavior.clone())
                            .set_origins(config.origins.clone())
                            .cache_behaviors(new_cache_behaviors)
                            .enabled(config.enabled)
                            .set_price_class(config.price_class.clone())
                            .build()
                            .map_err(|e| anyhow::anyhow!("Failed to build distribution config: {}", e))?;

                        client
                            .update_distribution()
                            .id(distribution_id)
                            .distribution_config(updated_config)
                            .if_match(etag)
                            .send()
                            .await?;

                        op_exec_output!(format!(
                            "Updated cache behaviors for CloudFront distribution `{}`",
                            distribution_id
                        ))
                    }

                    _ => Err(invalid_op(&addr, &op)),
                }
            }

            CloudFrontResourceAddress::OriginAccessControl { oac_id } => match op {
                CloudFrontConnectorOp::CreateOriginAccessControl(oac) => {
                    let oac_config = aws_sdk_cloudfront::types::OriginAccessControlConfig::builder()
                        .name(&oac.name)
                        .origin_access_control_origin_type(aws_sdk_cloudfront::types::OriginAccessControlOriginTypes::from(
                            oac.origin_access_control_origin_type.as_str(),
                        ))
                        .signing_behavior(aws_sdk_cloudfront::types::OriginAccessControlSigningBehaviors::from(
                            oac.signing_behavior.as_str(),
                        ))
                        .signing_protocol(aws_sdk_cloudfront::types::OriginAccessControlSigningProtocols::from(
                            oac.signing_protocol.as_str(),
                        ));

                    let oac_config = if let Some(description) = &oac.description {
                        oac_config.description(description)
                    } else {
                        oac_config
                    };

                    let response = client
                        .create_origin_access_control()
                        .origin_access_control_config(
                            oac_config
                                .build()
                                .map_err(|e| anyhow::anyhow!("Failed to build origin access control config: {}", e))?,
                        )
                        .send()
                        .await?;

                    let oac_result = response
                        .origin_access_control()
                        .context("No origin access control in response")?;
                    let oac_id = oac_result.id();

                    op_exec_output!(
                        Some([("origin_access_control_id", Some(oac_id.to_string()))]),
                        format!("Created CloudFront origin access control `{}`", oac_id)
                    )
                }

                CloudFrontConnectorOp::UpdateOriginAccessControl {
                    name,
                    description,
                    origin_access_control_origin_type,
                    signing_behavior,
                    signing_protocol,
                } => {
                    let get_response = client.get_origin_access_control().id(oac_id).send().await?;
                    let current_oac = get_response
                        .origin_access_control()
                        .context("No origin access control in response")?;
                    let etag = get_response.e_tag().context("No ETag in response")?;

                    let Some(mut current_config) = current_oac.origin_access_control_config.clone() else {
                        bail!("UpdateOriginAccessControl: origin_access_control_config is None ");
                    };

                    // Update fields based on operation parameters
                    if let Some(name) = name {
                        current_config.name = name;
                    }
                    if let Some(description) = description {
                        current_config.description = Some(description);
                    }
                    if let Some(origin_type) = origin_access_control_origin_type {
                        current_config.origin_access_control_origin_type =
                            aws_sdk_cloudfront::types::OriginAccessControlOriginTypes::from(origin_type.as_str());
                    }
                    if let Some(behavior) = signing_behavior {
                        current_config.signing_behavior =
                            aws_sdk_cloudfront::types::OriginAccessControlSigningBehaviors::from(behavior.as_str());
                    }
                    if let Some(protocol) = signing_protocol {
                        current_config.signing_protocol =
                            aws_sdk_cloudfront::types::OriginAccessControlSigningProtocols::from(protocol.as_str());
                    }

                    client
                        .update_origin_access_control()
                        .id(oac_id)
                        .origin_access_control_config(current_config)
                        .if_match(etag)
                        .send()
                        .await?;

                    op_exec_output!(format!("Updated CloudFront origin access control `{}`", oac_id))
                }

                CloudFrontConnectorOp::DeleteOriginAccessControl => {
                    let get_response = client.get_origin_access_control().id(oac_id).send().await?;

                    let etag = get_response.e_tag().context("No ETag in response")?;

                    client.delete_origin_access_control().id(oac_id).if_match(etag).send().await?;

                    op_exec_output!(format!("Deleted CloudFront origin access control `{}`", oac_id))
                }

                _ => Err(invalid_op(&addr, &op)),
            },

            CloudFrontResourceAddress::CachePolicy { policy_id } => match op {
                CloudFrontConnectorOp::CreateCachePolicy(policy) => {
                    let cache_policy_config = aws_sdk_cloudfront::types::CachePolicyConfig::builder().name(&policy.name);

                    let cache_policy_config = if let Some(comment) = &policy.comment {
                        cache_policy_config.comment(comment)
                    } else {
                        cache_policy_config
                    };

                    let cache_policy_config = if let Some(default_ttl) = policy.default_ttl {
                        cache_policy_config.default_ttl(default_ttl)
                    } else {
                        cache_policy_config
                    };

                    let cache_policy_config = if let Some(min_ttl) = policy.min_ttl {
                        cache_policy_config.min_ttl(min_ttl)
                    } else {
                        cache_policy_config
                    };

                    let cache_policy_config = if let Some(max_ttl) = policy.max_ttl {
                        cache_policy_config.max_ttl(max_ttl)
                    } else {
                        cache_policy_config
                    };

                    let response = client
                        .create_cache_policy()
                        .cache_policy_config(cache_policy_config.build()?)
                        .send()
                        .await?;

                    let cache_policy_result = response.cache_policy().context("No cache policy in response")?;
                    let policy_id = cache_policy_result.id();

                    op_exec_output!(
                        Some([("cache_policy_id", Some(policy_id.to_string()))]),
                        format!("Created CloudFront cache policy `{}`", policy_id)
                    )
                }

                CloudFrontConnectorOp::UpdateCachePolicy {
                    name,
                    comment,
                    default_ttl,
                    max_ttl,
                    min_ttl,
                    parameters_in_cache_key_and_forwarded_to_origin,
                } => {
                    let get_response = client.get_cache_policy().id(policy_id).send().await?;
                    let current_policy = get_response.cache_policy().context("No cache policy in response")?;
                    let etag = get_response.e_tag().context("No ETag in response")?;
                    let current_config = current_policy.cache_policy_config();

                    let Some(mut current_config) = current_policy.cache_policy_config.clone() else {
                        bail!("UpdateCachePolicy: cache_policy_config is None");
                    };

                    if let Some(name) = name {
                        current_config.name = name;
                    }

                    if let Some(comment) = comment {
                        current_config.comment = Some(comment);
                    }

                    if let Some(default_ttl) = default_ttl {
                        current_config.default_ttl = Some(default_ttl);
                    }

                    if let Some(min_ttl) = min_ttl {
                        current_config.min_ttl = min_ttl;
                    }

                    if let Some(max_ttl) = max_ttl {
                        current_config.max_ttl = Some(max_ttl);
                    }

                    if let Some(parameters_in_cache_key_and_forwarded_to_origin) =
                        parameters_in_cache_key_and_forwarded_to_origin
                    {
                        // TODO this needs to be modelled in the resource/
                        todo!();
                        // let params = ParametersInCacheKeyAndForwardedToOrigin::builder();
                        // current_config.parameters_in_cache_key_and_forwarded_to_origin =
                        //     Some(parameters_in_cache_key_and_forwarded_to_origin);
                    }

                    client
                        .update_cache_policy()
                        .id(policy_id)
                        .cache_policy_config(current_config)
                        .if_match(etag)
                        .send()
                        .await?;

                    op_exec_output!(format!("Updated CloudFront cache policy `{}`", policy_id))
                }

                CloudFrontConnectorOp::DeleteCachePolicy => {
                    let get_response = client.get_cache_policy().id(policy_id).send().await?;

                    let etag = get_response.e_tag().context("No ETag in response")?;

                    client.delete_cache_policy().id(policy_id).if_match(etag).send().await?;

                    op_exec_output!(format!("Deleted CloudFront cache policy `{}`", policy_id))
                }

                _ => Err(invalid_op(&addr, &op)),
            },

            CloudFrontResourceAddress::Function { name } => match op {
                CloudFrontConnectorOp::CreateFunction(function) => {
                    let function_code = function.function_code.as_bytes();

                    let response = client
                        .create_function()
                        .name(&function.name)
                        .function_config(
                            aws_sdk_cloudfront::types::FunctionConfig::builder()
                                .comment("")
                                .runtime(aws_sdk_cloudfront::types::FunctionRuntime::from(function.runtime.as_str()))
                                .build()
                                .map_err(|e| anyhow::anyhow!("Failed to build function config: {}", e))?,
                        )
                        .function_code(aws_smithy_types::Blob::new(function_code))
                        .send()
                        .await?;

                    let function_summary = response.function_summary().context("No function summary in response")?;
                    let function_arn = function_summary
                        .function_metadata()
                        .context("No function metadata")?
                        .function_arn();

                    op_exec_output!(
                        Some([("function_arn", Some(function_arn.to_string()))]),
                        format!("Created CloudFront function `{}`", function.name)
                    )
                }

                CloudFrontConnectorOp::UpdateFunction {
                    name: new_name,
                    function_code,
                    runtime,
                } => {
                    let get_response = client.describe_function().name(name).send().await?;
                    let etag = get_response.e_tag().context("No ETag in response")?;

                    if let Some(function_code) = function_code {
                        client
                            .update_function()
                            .name(name)
                            .function_code(aws_smithy_types::Blob::new(function_code.as_bytes()))
                            .if_match(etag)
                            .send()
                            .await?;
                    }

                    op_exec_output!(format!("Updated CloudFront function `{}`", name))
                }

                CloudFrontConnectorOp::DeleteFunction => {
                    let get_response = client.describe_function().name(name).send().await?;

                    let etag = get_response.e_tag().context("No ETag in response")?;

                    client.delete_function().name(name).if_match(etag).send().await?;

                    op_exec_output!(format!("Deleted CloudFront function `{}`", name))
                }

                CloudFrontConnectorOp::PublishFunction { if_match } => {
                    client.publish_function().name(name).if_match(if_match).send().await?;

                    op_exec_output!(format!("Published CloudFront function `{}`", name))
                }

                _ => Err(invalid_op(&addr, &op)),
            },

            CloudFrontResourceAddress::KeyGroup { key_group_id } => match op {
                CloudFrontConnectorOp::CreateKeyGroup(key_group) => {
                    let key_group_config = aws_sdk_cloudfront::types::KeyGroupConfig::builder()
                        .name(&key_group.name)
                        .set_items(Some(key_group.items.clone()));

                    let key_group_config = if let Some(comment) = &key_group.comment {
                        key_group_config.comment(comment)
                    } else {
                        key_group_config
                    };

                    let response = client
                        .create_key_group()
                        .key_group_config(
                            key_group_config
                                .build()
                                .map_err(|e| anyhow::anyhow!("Failed to build key group config: {}", e))?,
                        )
                        .send()
                        .await?;

                    let key_group_result = response.key_group().context("No key group in response")?;
                    let key_group_id = key_group_result.id();

                    op_exec_output!(
                        Some([("key_group_id", Some(key_group_id.to_string()))]),
                        format!("Created CloudFront key group `{}`", key_group_id)
                    )
                }

                CloudFrontConnectorOp::UpdateKeyGroup { name, comment, items } => {
                    let get_response = client.get_key_group().id(key_group_id).send().await?;
                    let current_key_group = get_response.key_group().context("No key group in response")?;
                    let etag = get_response.e_tag().context("No ETag in response")?;

                    let Some(mut key_group_config) = current_key_group.key_group_config.clone() else {
                        bail!("UpdateKeyGroup: key_group_config is None");
                    };

                    if let Some(comment) = comment {
                        key_group_config.comment = Some(comment);
                    }

                    if let Some(items) = items {
                        key_group_config.items = items.clone();
                    }

                    client
                        .update_key_group()
                        .id(key_group_id)
                        .key_group_config(key_group_config)
                        .if_match(etag)
                        .send()
                        .await?;

                    op_exec_output!(format!("Updated CloudFront key group `{}`", key_group_id))
                }

                CloudFrontConnectorOp::DeleteKeyGroup => {
                    let get_response = client.get_key_group().id(key_group_id).send().await?;

                    let etag = get_response.e_tag().context("No ETag in response")?;

                    client.delete_key_group().id(key_group_id).if_match(etag).send().await?;

                    op_exec_output!(format!("Deleted CloudFront key group `{}`", key_group_id))
                }

                _ => Err(invalid_op(&addr, &op)),
            },

            CloudFrontResourceAddress::PublicKey { public_key_id } => match op {
                CloudFrontConnectorOp::CreatePublicKey(public_key) => {
                    let public_key_config = aws_sdk_cloudfront::types::PublicKeyConfig::builder()
                        .name(&public_key.name)
                        .encoded_key(&public_key.encoded_key)
                        .caller_reference(&format!("autoschematic-{}", uuid::Uuid::new_v4()));

                    let public_key_config = if let Some(comment) = &public_key.comment {
                        public_key_config.comment(comment)
                    } else {
                        public_key_config
                    };

                    let response = client
                        .create_public_key()
                        .public_key_config(
                            public_key_config
                                .build()
                                .map_err(|e| anyhow::anyhow!("Failed to build public key config: {}", e))?,
                        )
                        .send()
                        .await?;

                    let public_key_result = response.public_key().context("No public key in response")?;
                    let public_key_id = public_key_result.id();

                    op_exec_output!(
                        Some([("public_key_id", Some(public_key_id.to_string()))]),
                        format!("Created CloudFront public key `{}`", public_key_id)
                    )
                }

                CloudFrontConnectorOp::UpdatePublicKey {
                    name,
                    comment,
                    encoded_key,
                } => {
                    let get_response = client.get_public_key().id(public_key_id).send().await?;
                    let current_public_key = get_response.public_key().context("No public key in response")?;
                    let etag = get_response.e_tag().context("No ETag in response")?;

                    let Some(mut public_key_config) = current_public_key.public_key_config.clone() else {
                        bail!("UpdatePublicKey: public_key_config is None");
                    };

                    if let Some(name) = name {
                        public_key_config.name = name;
                    };

                    if let Some(comment) = comment {
                        public_key_config.comment = Some(comment)
                    }

                    client
                        .update_public_key()
                        .id(public_key_id)
                        .public_key_config(public_key_config)
                        .if_match(etag)
                        .send()
                        .await?;

                    op_exec_output!(format!("Updated CloudFront public key `{}`", public_key_id))
                }

                CloudFrontConnectorOp::DeletePublicKey => {
                    let get_response = client.get_public_key().id(public_key_id).send().await?;

                    let etag = get_response.e_tag().context("No ETag in response")?;

                    client.delete_public_key().id(public_key_id).if_match(etag).send().await?;

                    op_exec_output!(format!("Deleted CloudFront public key `{}`", public_key_id))
                }

                _ => Err(invalid_op(&addr, &op)),
            },

            CloudFrontResourceAddress::StreamingDistribution { distribution_id } => match op {
                CloudFrontConnectorOp::CreateStreamingDistribution(streaming_dist) => {
                    let s3_origin = aws_sdk_cloudfront::types::S3Origin::builder()
                        .domain_name(&streaming_dist.s3_origin.domain_name)
                        .origin_access_identity(&streaming_dist.s3_origin.origin_access_identity)
                        .build()
                        .map_err(|e| anyhow::anyhow!("Failed to build S3 origin: {}", e))?;

                    let mut streaming_config = aws_sdk_cloudfront::types::StreamingDistributionConfig::builder()
                        .caller_reference(&format!("autoschematic-{}", uuid::Uuid::new_v4()))
                        .s3_origin(s3_origin)
                        .enabled(streaming_dist.enabled);

                    if let Some(comment) = &streaming_dist.comment {
                        streaming_config = streaming_config.comment(comment);
                    }

                    if let Some(price_class) = &streaming_dist.price_class {
                        streaming_config =
                            streaming_config.price_class(aws_sdk_cloudfront::types::PriceClass::from(price_class.as_str()));
                    }

                    let response = client
                        .create_streaming_distribution()
                        .streaming_distribution_config(
                            streaming_config
                                .build()
                                .map_err(|e| anyhow::anyhow!("Failed to build streaming distribution config: {}", e))?,
                        )
                        .send()
                        .await?;

                    let streaming_dist_result = response
                        .streaming_distribution()
                        .context("No streaming distribution in response")?;
                    let distribution_id = streaming_dist_result.id();
                    let arn = streaming_dist_result.arn();

                    op_exec_output!(
                        Some([
                            ("streaming_distribution_id", Some(distribution_id.to_string())),
                            ("streaming_distribution_arn", Some(arn.to_string()))
                        ]),
                        format!("Created CloudFront streaming distribution `{}`", distribution_id)
                    )
                }

                CloudFrontConnectorOp::UpdateStreamingDistribution {
                    enabled,
                    comment,
                    price_class,
                } => {
                    let get_response = client.get_streaming_distribution_config().id(distribution_id).send().await?;
                    let current_config = get_response
                        .streaming_distribution_config()
                        .context("No streaming distribution config")?
                        .clone();
                    let etag = get_response.e_tag().context("No ETag in response")?;

                    let mut streaming_config = aws_sdk_cloudfront::types::StreamingDistributionConfig::builder()
                        .caller_reference(current_config.caller_reference().to_string())
                        .set_s3_origin(current_config.s3_origin().cloned())
                        .enabled(enabled.unwrap_or(current_config.enabled()));

                    if let Some(comment) = comment {
                        streaming_config = streaming_config.comment(comment);
                    } else {
                        streaming_config = streaming_config.comment(current_config.comment.clone());
                    }

                    if let Some(price_class) = price_class {
                        streaming_config =
                            streaming_config.price_class(aws_sdk_cloudfront::types::PriceClass::from(price_class.as_str()));
                    } else if let Some(current_price_class) = current_config.price_class() {
                        streaming_config = streaming_config.price_class(current_price_class.clone());
                    }

                    client
                        .update_streaming_distribution()
                        .id(distribution_id)
                        .streaming_distribution_config(streaming_config.build()?)
                        .if_match(etag)
                        .send()
                        .await?;

                    op_exec_output!(format!("Updated CloudFront streaming distribution `{}`", distribution_id))
                }

                CloudFrontConnectorOp::DeleteStreamingDistribution => {
                    let get_response = client.get_streaming_distribution().id(distribution_id).send().await?;

                    let etag = get_response.e_tag().context("No ETag in response")?;

                    client
                        .delete_streaming_distribution()
                        .id(distribution_id)
                        .if_match(etag)
                        .send()
                        .await?;

                    op_exec_output!(format!("Deleted CloudFront streaming distribution `{}`", distribution_id))
                }

                _ => Err(invalid_op(&addr, &op)),
            },

            // For resource types that don't have implemented operations yet
            _ => Err(invalid_op(&addr, &op)),
        }
    }
}
