use std::path::Path;

use autoschematic_core::{
    connector::{PlanResponseElement, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values, optional_string_from_utf8},
};

use autoschematic_core::connector::ConnectorOp;

use crate::resource::{LifecyclePolicy, PullThroughCacheRule, RegistryPolicy, Repository, RepositoryPolicy};

use super::{EcrConnector, EcrConnectorOp, EcrResourceAddress};

impl EcrConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        let addr = EcrResourceAddress::from_path(addr)?;

        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;

        match &addr {
            EcrResourceAddress::Repository { region, name } => {
                match (current, desired) {
                    (None, None) => Ok(Vec::new()),
                    (None, Some(new_repo)) => {
                        let new_repo: Repository = RON.from_str(&new_repo)?;
                        Ok(vec![connector_op!(
                            EcrConnectorOp::CreateRepository(new_repo),
                            format!("Create new ECR repository {} in region {}", name, region)
                        )])
                    }
                    (Some(_old_repo), None) => Ok(vec![connector_op!(
                        EcrConnectorOp::DeleteRepository { force: true },
                        format!("DELETE ECR repository {} in region {}", name, region)
                    )]),
                    (Some(old_repo), Some(new_repo)) => {
                        let old_repo: Repository = RON.from_str(&old_repo)?;
                        let new_repo: Repository = RON.from_str(&new_repo)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_repo.tags != new_repo.tags {
                            let diff = diff_ron_values(&old_repo.tags, &new_repo.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                EcrConnectorOp::UpdateRepositoryTags(old_repo.tags, new_repo.tags),
                                format!("Modify tags for ECR repository `{}`\n{}", name, diff)
                            ));
                        }

                        // Check for image tag mutability changes
                        if old_repo.image_tag_mutability != new_repo.image_tag_mutability
                            && let Some(mutability) = &new_repo.image_tag_mutability {
                                ops.push(connector_op!(
                                    EcrConnectorOp::UpdateImageTagMutability {
                                        image_tag_mutability: mutability.clone(),
                                    },
                                    format!("Update image tag mutability to {} for ECR repository `{}`", mutability, name)
                                ));
                            }

                        // Check for image scanning configuration changes
                        if old_repo.image_scanning_configuration != new_repo.image_scanning_configuration
                            && let Some(scanning_config) = &new_repo.image_scanning_configuration {
                                ops.push(connector_op!(
                                    EcrConnectorOp::UpdateImageScanningConfiguration {
                                        scan_on_push: scanning_config.scan_on_push,
                                    },
                                    format!(
                                        "Update image scanning configuration (scan_on_push: {}) for ECR repository `{}`",
                                        scanning_config.scan_on_push, name
                                    )
                                ));
                            }

                        // Check for encryption configuration changes
                        if old_repo.encryption_configuration != new_repo.encryption_configuration {
                            ops.push(connector_op!(
                                EcrConnectorOp::UpdateEncryptionConfiguration {
                                    encryption_configuration: new_repo.encryption_configuration,
                                },
                                format!("Update encryption configuration for ECR repository `{}`", name)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
            EcrResourceAddress::RepositoryPolicy { region, name } => match (current, desired) {
                (None, None) => Ok(Vec::new()),
                (None, Some(new_policy)) => {
                    let new_policy: RepositoryPolicy = RON.from_str(&new_policy)?;
                    Ok(vec![connector_op!(
                        EcrConnectorOp::SetRepositoryPolicy {
                            policy_document: new_policy.policy_document,
                        },
                        format!("Create repository policy for ECR repository {} in region {}", name, region)
                    )])
                }
                (Some(_old_policy), None) => Ok(vec![connector_op!(
                    EcrConnectorOp::DeleteRepositoryPolicy,
                    format!("DELETE repository policy for ECR repository {} in region {}", name, region)
                )]),
                (Some(old_policy), Some(new_policy)) => {
                    let old_policy: RepositoryPolicy = RON.from_str(&old_policy)?;
                    let new_policy: RepositoryPolicy = RON.from_str(&new_policy)?;

                    if old_policy.policy_document != new_policy.policy_document {
                        let diff =
                            diff_ron_values(&old_policy.policy_document, &new_policy.policy_document).unwrap_or_default();
                        Ok(vec![connector_op!(
                            EcrConnectorOp::SetRepositoryPolicy {
                                policy_document: new_policy.policy_document,
                            },
                            format!("Update repository policy for ECR repository `{}`\n{}", name, diff)
                        )])
                    } else {
                        Ok(Vec::new())
                    }
                }
            },
            EcrResourceAddress::LifecyclePolicy { region, name } => match (current, desired) {
                (None, None) => Ok(Vec::new()),
                (None, Some(new_policy)) => {
                    let new_policy: LifecyclePolicy = RON.from_str(&new_policy)?;
                    Ok(vec![connector_op!(
                        EcrConnectorOp::SetLifecyclePolicy {
                            lifecycle_policy_text: new_policy.lifecycle_policy_text,
                        },
                        format!("Create lifecycle policy for ECR repository {} in region {}", name, region)
                    )])
                }
                (Some(_old_policy), None) => Ok(vec![connector_op!(
                    EcrConnectorOp::DeleteLifecyclePolicy,
                    format!("DELETE lifecycle policy for ECR repository {} in region {}", name, region)
                )]),
                (Some(old_policy), Some(new_policy)) => {
                    let old_policy: LifecyclePolicy = RON.from_str(&old_policy)?;
                    let new_policy: LifecyclePolicy = RON.from_str(&new_policy)?;

                    if old_policy.lifecycle_policy_text != new_policy.lifecycle_policy_text {
                        let diff = diff_ron_values(&old_policy.lifecycle_policy_text, &new_policy.lifecycle_policy_text)
                            .unwrap_or_default();
                        Ok(vec![connector_op!(
                            EcrConnectorOp::SetLifecyclePolicy {
                                lifecycle_policy_text: new_policy.lifecycle_policy_text,
                            },
                            format!("Update lifecycle policy for ECR repository `{}`\n{}", name, diff)
                        )])
                    } else {
                        Ok(Vec::new())
                    }
                }
            },
            EcrResourceAddress::RegistryPolicy { region } => match (current, desired) {
                (None, None) => Ok(Vec::new()),
                (None, Some(new_policy)) => {
                    let new_policy: RegistryPolicy = RON.from_str(&new_policy)?;
                    Ok(vec![connector_op!(
                        EcrConnectorOp::SetRegistryPolicy {
                            policy_document: new_policy.policy_document,
                        },
                        format!("Create registry policy in region {}", region)
                    )])
                }
                (Some(_old_policy), None) => Ok(vec![connector_op!(
                    EcrConnectorOp::DeleteRegistryPolicy,
                    format!("DELETE registry policy in region {}", region)
                )]),
                (Some(old_policy), Some(new_policy)) => {
                    let old_policy: RegistryPolicy = RON.from_str(&old_policy)?;
                    let new_policy: RegistryPolicy = RON.from_str(&new_policy)?;

                    if old_policy.policy_document != new_policy.policy_document {
                        let diff =
                            diff_ron_values(&old_policy.policy_document, &new_policy.policy_document).unwrap_or_default();
                        Ok(vec![connector_op!(
                            EcrConnectorOp::SetRegistryPolicy {
                                policy_document: new_policy.policy_document,
                            },
                            format!("Update registry policy in region `{}`\n{}", region, diff)
                        )])
                    } else {
                        Ok(Vec::new())
                    }
                }
            },
            EcrResourceAddress::PullThroughCacheRule { region, prefix } => {
                match (current, desired) {
                    (None, None) => Ok(Vec::new()),
                    (None, Some(new_rule)) => {
                        let new_rule: PullThroughCacheRule = RON.from_str(&new_rule)?;
                        Ok(vec![connector_op!(
                            EcrConnectorOp::CreatePullThroughCacheRule {
                                upstream_registry_url: new_rule.upstream_registry_url,
                                credential_arn: new_rule.credential_arn,
                            },
                            format!("Create pull through cache rule for prefix {} in region {}", prefix, region)
                        )])
                    }
                    (Some(_old_rule), None) => Ok(vec![connector_op!(
                        EcrConnectorOp::DeletePullThroughCacheRule {},
                        format!("DELETE pull through cache rule for prefix {} in region {}", prefix, region)
                    )]),
                    (Some(old_rule), Some(new_rule)) => {
                        let old_rule: PullThroughCacheRule = RON.from_str(&old_rule)?;
                        let new_rule: PullThroughCacheRule = RON.from_str(&new_rule)?;

                        // For pull through cache rules, we can't update them directly,
                        // we need to delete and recreate if there are changes
                        if old_rule != new_rule {
                            Ok(vec![
                                connector_op!(
                                    EcrConnectorOp::DeletePullThroughCacheRule {},
                                    format!("DELETE existing pull through cache rule for prefix {}", prefix)
                                ),
                                connector_op!(
                                    EcrConnectorOp::CreatePullThroughCacheRule {
                                        upstream_registry_url: new_rule.upstream_registry_url,
                                        credential_arn: new_rule.credential_arn,
                                    },
                                    format!("CREATE updated pull through cache rule for prefix {}", prefix)
                                ),
                            ])
                        } else {
                            Ok(Vec::new())
                        }
                    }
                }
            }
        }
    }
}
