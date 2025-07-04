use std::path::{Path, PathBuf};

use anyhow::bail;
use autoschematic_core::connector::ResourceAddress;

use crate::addr::CloudFrontResourceAddress;

use super::CloudFrontConnector;

impl CloudFrontConnector {
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();

        let client = self.get_or_init_client().await?;

        let mut next_marker: Option<String> = None;
        loop {
            let distributions = client.list_distributions().set_marker(next_marker).send().await?;
            let Some(distribution_list) = distributions.distribution_list() else {
                break;
            };

            if let Some(items) = &distribution_list.items {
                for dist in items {
                    results.push(
                        CloudFrontResourceAddress::Distribution {
                            distribution_id: dist.id.clone(),
                        }
                        .to_path_buf(),
                    );
                }
            }

            next_marker = distribution_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Origin Access Controls
        let mut next_marker: Option<String> = None;
        loop {
            let oacs = client.list_origin_access_controls().set_marker(next_marker).send().await?;
            let Some(oac_list) = oacs.origin_access_control_list() else {
                break;
            };

            if let Some(items) = &oac_list.items {
                for oac in items {
                    results.push(CloudFrontResourceAddress::OriginAccessControl { oac_id: oac.id.clone() }.to_path_buf());
                }
            }

            next_marker = oac_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Cache Policies
        let mut next_marker: Option<String> = None;
        loop {
            let policies = client.list_cache_policies().set_marker(next_marker).send().await?;
            let Some(cache_policy_list) = policies.cache_policy_list() else {
                break;
            };

            if let Some(items) = &cache_policy_list.items {
                for policy in items {
                    if let Some(cache_policy) = &policy.cache_policy {
                        results.push(
                            CloudFrontResourceAddress::CachePolicy {
                                policy_id: cache_policy.id.clone(),
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }

            next_marker = cache_policy_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Origin Request Policies
        let mut next_marker: Option<String> = None;
        loop {
            let policies = client.list_origin_request_policies().set_marker(next_marker).send().await?;
            let Some(origin_request_policy_list) = policies.origin_request_policy_list() else {
                break;
            };

            if let Some(items) = &origin_request_policy_list.items {
                for policy in items {
                    if let Some(origin_request_policy) = &policy.origin_request_policy {
                        results.push(
                            CloudFrontResourceAddress::OriginRequestPolicy {
                                policy_id: origin_request_policy.id.clone(),
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }

            next_marker = origin_request_policy_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Response Headers Policies
        let mut next_marker: Option<String> = None;
        loop {
            let policies = client.list_response_headers_policies().set_marker(next_marker).send().await?;
            let Some(response_headers_policy_list) = policies.response_headers_policy_list() else {
                break;
            };

            if let Some(items) = &response_headers_policy_list.items {
                for policy in items {
                    if let Some(response_headers_policy) = &policy.response_headers_policy {
                        results.push(
                            CloudFrontResourceAddress::ResponseHeadersPolicy {
                                policy_id: response_headers_policy.id.clone(),
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }

            next_marker = response_headers_policy_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Realtime Log Configs
        let mut next_marker: Option<String> = None;
        loop {
            let configs = client.list_realtime_log_configs().set_marker(next_marker).send().await?;
            let Some(realtime_log_configs) = configs.realtime_log_configs() else {
                break;
            };

            if let Some(items) = &realtime_log_configs.items {
                for config in items {
                    results.push(
                        CloudFrontResourceAddress::RealtimeLogConfig {
                            name: config.name.clone(),
                        }
                        .to_path_buf(),
                    );
                }
            }

            next_marker = realtime_log_configs.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Functions
        let mut next_marker: Option<String> = None;
        loop {
            let functions = client.list_functions().set_marker(next_marker).send().await?;
            let Some(function_list) = functions.function_list() else {
                break;
            };

            if let Some(items) = &function_list.items {
                for function in items {
                    results.push(
                        CloudFrontResourceAddress::Function {
                            name: function.name.clone(),
                        }
                        .to_path_buf(),
                    );
                }
            }

            next_marker = function_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Key Groups
        let mut next_marker: Option<String> = None;
        loop {
            let key_groups = client.list_key_groups().set_marker(next_marker).send().await?;
            let Some(key_group_list) = key_groups.key_group_list() else {
                break;
            };

            if let Some(items) = &key_group_list.items {
                for key_group in items {
                    if let Some(key_group) = &key_group.key_group {
                        results.push(
                            CloudFrontResourceAddress::KeyGroup {
                                key_group_id: key_group.id.clone(),
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }

            next_marker = key_group_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Public Keys
        let mut next_marker: Option<String> = None;
        loop {
            let public_keys = client.list_public_keys().set_marker(next_marker).send().await?;
            let Some(public_key_list) = public_keys.public_key_list() else {
                break;
            };

            if let Some(items) = &public_key_list.items {
                for public_key in items {
                    results.push(
                        CloudFrontResourceAddress::PublicKey {
                            public_key_id: public_key.id.clone(),
                        }
                        .to_path_buf(),
                    );
                }
            }

            next_marker = public_key_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Field Level Encryption Configs
        let mut next_marker: Option<String> = None;
        loop {
            let configs = client
                .list_field_level_encryption_configs()
                .set_marker(next_marker)
                .send()
                .await?;
            let Some(field_level_encryption_list) = configs.field_level_encryption_list() else {
                break;
            };

            if let Some(items) = &field_level_encryption_list.items {
                for config in items {
                    results.push(
                        CloudFrontResourceAddress::FieldLevelEncryptionConfig {
                            config_id: config.id.clone(),
                        }
                        .to_path_buf(),
                    );
                }
            }

            next_marker = field_level_encryption_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Field Level Encryption Profiles
        let mut next_marker: Option<String> = None;
        loop {
            let profiles = client
                .list_field_level_encryption_profiles()
                .set_marker(next_marker)
                .send()
                .await?;
            let Some(field_level_encryption_profile_list) = profiles.field_level_encryption_profile_list() else {
                break;
            };

            if let Some(items) = &field_level_encryption_profile_list.items {
                for profile in items {
                    results.push(
                        CloudFrontResourceAddress::FieldLevelEncryptionProfile {
                            profile_id: profile.id.clone(),
                        }
                        .to_path_buf(),
                    );
                }
            }

            next_marker = field_level_encryption_profile_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        // List Streaming Distributions
        let mut next_marker: Option<String> = None;
        loop {
            let distributions = client.list_streaming_distributions().set_marker(next_marker).send().await?;
            let Some(streaming_distribution_list) = distributions.streaming_distribution_list() else {
                break;
            };

            if let Some(items) = &streaming_distribution_list.items {
                for dist in items {
                    results.push(
                        CloudFrontResourceAddress::StreamingDistribution {
                            distribution_id: dist.id.clone(),
                        }
                        .to_path_buf(),
                    );
                }
            }

            next_marker = streaming_distribution_list.next_marker.clone();
            if next_marker.is_none() {
                break;
            }
        }

        Ok(results)
    }
}
