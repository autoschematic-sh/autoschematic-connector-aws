use anyhow::Context;
use std::{collections::HashMap, path::Path};

use autoschematic_core::{
    connector::{GetResourceOutput, Resource, ResourceAddress},
    get_resource_output,
    util::RON,
};

use crate::{
    addr::EcrResourceAddress,
    resource::{
        EcrResource, EncryptionConfiguration, ImageScanningConfiguration, LifecyclePolicy, PullThroughCacheRule,
        RegistryPolicy, Repository, RepositoryPolicy,
    },
    tags::Tags,
};

use super::EcrConnector;

impl EcrConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = EcrResourceAddress::from_path(addr)?;

        match addr {
            EcrResourceAddress::Repository { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                // Get repository details
                let describe_repos_resp = client.describe_repositories().repository_names(&name).send().await;

                match describe_repos_resp {
                    Ok(resp) => {
                        if let Some(repositories) = resp.repositories {
                            if let Some(repo) = repositories.first() {
                                // Get tags
                                let tags = if let Some(registry_id) = &repo.registry_id {
                                    let tags_resp = client
                                        .list_tags_for_resource()
                                        .resource_arn(format!("arn:aws:ecr:{}:{}:repository/{}", region, registry_id, name))
                                        .send()
                                        .await;

                                    match tags_resp {
                                        Ok(tags_data) => tags_data.tags.into(),
                                        Err(_) => Tags::default(),
                                    }
                                } else {
                                    Tags::default()
                                };

                                // Build repository resource
                                let repository = Repository {
                                    encryption_configuration: repo.encryption_configuration.as_ref().map(|encrypt_config| {
                                        EncryptionConfiguration {
                                            encryption_type: encrypt_config.encryption_type.as_str().to_string(),
                                            kms_key: encrypt_config.kms_key.clone(),
                                        }
                                    }),
                                    image_tag_mutability: repo.image_tag_mutability.as_ref().map(|m| m.to_string()),
                                    image_scanning_configuration: repo.image_scanning_configuration.as_ref().map(
                                        |scan_config| ImageScanningConfiguration {
                                            scan_on_push: scan_config.scan_on_push,
                                        },
                                    ),
                                    tags,
                                };

                                return get_resource_output!(
                                    EcrResource::Repository(repository),
                                    [(String::from("repository_url"), repo.repository_uri.clone()),]
                                );
                            }
                        }
                        Ok(None)
                    }
                    Err(_) => Ok(None), // Repository not found or other error
                }
            }
            EcrResourceAddress::RepositoryPolicy { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                // Get repository policy
                let policy_resp = client.get_repository_policy().repository_name(&name).send().await;

                match policy_resp {
                    Ok(policy_data) => {
                        if let Some(policy_text) = policy_data.policy_text {
                            // Parse policy JSON into RON value
                            let val: serde_json::Value = serde_json::from_str(&policy_text)?;
                            let rval: ron::Value = RON.from_str(&RON.to_string(&val)?)?;

                            let repo_policy = RepositoryPolicy { policy_document: rval };

                            return Ok(Some(GetResourceOutput {
                                resource_definition: EcrResource::RepositoryPolicy(repo_policy).to_bytes()?,
                                outputs: None,
                            }));
                        }
                        Ok(None)
                    }
                    Err(e) => {
                        tracing::error!("{:?}", e);
                        Ok(None) // Policy not found or other error
                    }
                }
            }
            EcrResourceAddress::LifecyclePolicy { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                // Get lifecycle policy
                let lifecycle_policy_resp = client.get_lifecycle_policy().repository_name(&name).send().await;

                match lifecycle_policy_resp {
                    Ok(policy_data) => {
                        if let Some(policy_text) = policy_data.lifecycle_policy_text {
                            // Parse policy JSON into RON value
                            let val: serde_json::Value = serde_json::from_str(&policy_text)?;
                            let rval: ron::Value = RON.from_str(&RON.to_string(&val)?)?;

                            let lifecycle_policy = LifecyclePolicy {
                                lifecycle_policy_text: rval,
                            };

                            return Ok(Some(GetResourceOutput {
                                resource_definition: EcrResource::LifecyclePolicy(lifecycle_policy).to_bytes()?,
                                outputs: None,
                            }));
                        }
                        Ok(None)
                    }
                    Err(e) => {
                        tracing::error!("{:?}", e);
                        Ok(None) // Policy not found or other error
                    }
                }
            }
            EcrResourceAddress::RegistryPolicy { region } => {
                let client = self.get_or_init_client(&region).await?;

                // Get registry policy
                let registry_policy_resp = client.get_registry_policy().send().await;

                match registry_policy_resp {
                    Ok(policy_data) => {
                        if let Some(policy_text) = policy_data.policy_text {
                            // Parse policy JSON into RON value
                            let val: serde_json::Value = serde_json::from_str(&policy_text)?;
                            let rval: ron::Value = RON.from_str(&RON.to_string(&val)?)?;

                            let registry_policy = RegistryPolicy { policy_document: rval };

                            return Ok(Some(GetResourceOutput {
                                resource_definition: EcrResource::RegistryPolicy(registry_policy).to_bytes()?,
                                outputs: None,
                            }));
                        }
                        Ok(None)
                    }
                    Err(e) => {
                        tracing::error!("{:?}", e);
                        Ok(None) // Policy not found or other error
                    }
                }
            }
            EcrResourceAddress::PullThroughCacheRule { region, prefix } => {
                let client = self.get_or_init_client(&region).await?;

                // Get pull through cache rule
                let rule_resp = client
                    .describe_pull_through_cache_rules()
                    .ecr_repository_prefixes(&prefix)
                    .send()
                    .await;

                match rule_resp {
                    Ok(rule_data) => {
                        if let Some(rules) = rule_data.pull_through_cache_rules {
                            if let Some(rule) = rules.first() {
                                if let (Some(repo_prefix), Some(registry_url)) =
                                    (&rule.ecr_repository_prefix, &rule.upstream_registry_url)
                                {
                                    if repo_prefix == &prefix {
                                        let pull_through_rule = PullThroughCacheRule {
                                            upstream_registry_url: registry_url.clone(),
                                            credential_arn: rule.credential_arn.clone(),
                                        };

                                        return Ok(Some(GetResourceOutput {
                                            resource_definition: EcrResource::PullThroughCacheRule(pull_through_rule)
                                                .to_bytes()?,
                                            outputs: None,
                                        }));
                                    }
                                }
                            }
                        }
                        Ok(None)
                    }
                    Err(e) => {
                        tracing::error!("{:?}", e);
                        Ok(None) // Rule not found or other error
                    }
                }
            }
        }
    }
}
