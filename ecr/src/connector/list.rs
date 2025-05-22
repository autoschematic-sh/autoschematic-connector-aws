use std::path::{Path, PathBuf};

use autoschematic_core::connector::ResourceAddress;

use crate::addr::EcrResourceAddress;

use super::EcrConnector;

impl EcrConnector {
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();

        for region_name in &self.config.enabled_regions {
            let client = self.get_or_init_client(region_name).await?;

            // List repositories in the region
            let repositories_resp = client.describe_repositories().send().await?;
            if let Some(repositories) = repositories_resp.repositories {
                for repo in repositories {
                    if let Some(repo_name) = repo.repository_name {
                        // Add repository
                        results.push(
                            EcrResourceAddress::Repository {
                                region: region_name.clone(),
                                name: repo_name.clone(),
                            }
                            .to_path_buf(),
                        );

                        // Check if repository policy exists before adding it
                        let policy_resp = client.get_repository_policy().repository_name(&repo_name).send().await;

                        if let Ok(policy_resp) = policy_resp {
                            if let Some(_policy_text) = policy_resp.policy_text {
                                results.push(
                                    EcrResourceAddress::RepositoryPolicy {
                                        region: region_name.clone(),
                                        name: repo_name.clone(),
                                    }
                                    .to_path_buf(),
                                );
                            }
                        }

                        // Check if lifecycle policy exists before adding it
                        let lifecycle_policy_resp = client.get_lifecycle_policy().repository_name(&repo_name).send().await;

                        if let Ok(lifecycle_policy_resp) = lifecycle_policy_resp {
                            if let Some(_lifecycle_policy_text) = lifecycle_policy_resp.lifecycle_policy_text {
                                results.push(
                                    EcrResourceAddress::LifecyclePolicy {
                                        region: region_name.clone(),
                                        name: repo_name,
                                    }
                                    .to_path_buf(),
                                );
                            }
                        }
                    }
                }
            }

            // Check if registry policy exists before adding it
            let registry_policy_resp = client.get_registry_policy().send().await;
            if registry_policy_resp.is_ok() && registry_policy_resp.unwrap().policy_text.is_some() {
                results.push(
                    EcrResourceAddress::RegistryPolicy {
                        region: region_name.clone(),
                    }
                    .to_path_buf(),
                );
            }

            // List and add pull through cache rules
            let pull_through_cache_rules_resp = client.describe_pull_through_cache_rules().send().await;
            if let Ok(rules_resp) = pull_through_cache_rules_resp {
                if let Some(rules) = rules_resp.pull_through_cache_rules {
                    for rule in rules {
                        if let Some(prefix) = rule.ecr_repository_prefix {
                            results.push(
                                EcrResourceAddress::PullThroughCacheRule {
                                    region: region_name.clone(),
                                    prefix,
                                }
                                .to_path_buf(),
                            );
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}
