use anyhow::{bail, Context};
use std::collections::HashMap;

use super::{
    resource::{EncryptionConfiguration, Repository},
    tags::Tags,
};
use autoschematic_core::connector::{OpExecOutput, Resource};

/// Creates a repository using the provided configuration
pub async fn create_repository(
    client: &aws_sdk_ecr::Client,
    repo: &Repository,
) -> Result<OpExecOutput, anyhow::Error> {
    // Create the repository
    let mut create_repo = client.create_repository();

    if let Some(image_tag_mutability) = &repo.image_tag_mutability {
        create_repo = create_repo.image_tag_mutability(image_tag_mutability.as_str().into());
    }

    if let Some(encryption) = &repo.encryption_configuration {
        let mut encryption_config = aws_sdk_ecr::types::EncryptionConfiguration::builder()
            .encryption_type(encryption.encryption_type.as_str().into());

        if let Some(kms_key) = &encryption.kms_key {
            encryption_config = encryption_config.kms_key(kms_key);
        }

        create_repo = create_repo.encryption_configuration(encryption_config.build()?);
    }

    if let Some(scan_config) = &repo.image_scanning_configuration {
        let scan_configuration = aws_sdk_ecr::types::ImageScanningConfiguration::builder()
            .scan_on_push(scan_config.scan_on_push)
            .build();

        create_repo = create_repo.image_scanning_configuration(scan_configuration);
    }

    // Add tags if present
    for tag in repo.tags.to_vec()? {
        create_repo = create_repo.tags(tag);
    }

    let create_resp = create_repo.send().await?;

    let Some(repository) = create_resp.repository else {
        bail!("Failed to create repository: response did not contain repository details");
    };

    let Some(repository_name) = repository.repository_name else {
        bail!("Failed to create repository: response did not contain repository name");
    };

    let Some(repository_uri) = repository.repository_uri else {
        bail!("Failed to create repository: response did not contain repository URI");
    };

    let mut outputs = HashMap::new();
    outputs.insert(
        String::from("repository_name"),
        Some(repository_name.clone()),
    );
    outputs.insert(String::from("repository_uri"), Some(repository_uri.clone()));

    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!("Created ECR repository {}", repository_name)),
    })
}

/// Updates repository tags
pub async fn update_repository_tags(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
    // Get repository ARN
    let describe_resp = client
        .describe_repositories()
        .repository_names(repository_name)
        .send()
        .await?;

    let Some(repositories) = describe_resp.repositories else {
        bail!("Repository not found: {}", repository_name);
    };

    let Some(repository) = repositories.first() else {
        bail!("Repository not found: {}", repository_name);
    };

    let Some(repository_arn) = &repository.repository_arn else {
        bail!("Repository has no ARN: {}", repository_name);
    };

    // Calculate tag differences
    let (delete_keys, tags_to_add) = super::tags::tag_diff(old_tags, new_tags)?;

    // Remove tags if needed
    if !delete_keys.is_empty() {
        client
            .untag_resource()
            .resource_arn(repository_arn)
            .set_tag_keys(Some(delete_keys))
            .send()
            .await?;
    }

    // Add tags if needed
    if !tags_to_add.is_empty() {
        client
            .tag_resource()
            .resource_arn(repository_arn)
            .set_tags(Some(tags_to_add))
            .send()
            .await?;
    }

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Updated tags for ECR repository {}",
            repository_name
        )),
    })
}

/// Updates image tag mutability setting
pub async fn update_image_tag_mutability(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    image_tag_mutability: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .put_image_tag_mutability()
        .repository_name(repository_name)
        .image_tag_mutability(image_tag_mutability.into())
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Updated image tag mutability to {} for ECR repository {}",
            image_tag_mutability, repository_name
        )),
    })
}

/// Updates image scanning configuration
pub async fn update_image_scanning_configuration(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    scan_on_push: bool,
) -> Result<OpExecOutput, anyhow::Error> {
    let scanning_configuration = aws_sdk_ecr::types::ImageScanningConfiguration::builder()
        .scan_on_push(scan_on_push)
        .build();

    client
        .put_image_scanning_configuration()
        .repository_name(repository_name)
        .image_scanning_configuration(scanning_configuration)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Updated image scanning configuration (scan_on_push: {}) for ECR repository {}",
            scan_on_push, repository_name
        )),
    })
}

/// Updates repository encryption configuration
/// ??? Is this even possible???
pub async fn update_encryption_configuration(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    encryption_configuration: Option<EncryptionConfiguration>,
) -> Result<OpExecOutput, anyhow::Error> {
    let encryption_config = if let Some(encryption) = encryption_configuration {
        let mut builder = aws_sdk_ecr::types::EncryptionConfiguration::builder()
            .encryption_type(encryption.encryption_type.as_str().into());

        if let Some(kms_key) = &encryption.kms_key {
            builder = builder.kms_key(kms_key);
        }

        Some(builder.build())
    } else {
        None
    };

    // let mut request = client
    //     .put_image_tag_mutability()
    //     .repository_name(repository_name);

    // Need to refresh repository to get current image tag mutability since we need to include it in the request
    let describe_resp = client
        .describe_repositories()
        .repository_names(repository_name)
        .send()
        .await?;

    let Some(repositories) = describe_resp.repositories else {
        bail!("Repository not found: {}", repository_name);
    };

    let Some(repository) = repositories.first() else {
        bail!("Repository not found: {}", repository_name);
    };

    let image_tag_mutability = repository
        .image_tag_mutability
        .as_ref()
        .map(|m| m.as_str())
        .unwrap_or("MUTABLE");

    client
        .put_image_tag_mutability()
        .repository_name(repository_name)
        .image_tag_mutability(image_tag_mutability.into())
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Updated encryption configuration for ECR repository {}",
            repository_name
        )),
    })
}

/// Deletes a repository
pub async fn delete_repository(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    force: bool,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_repository()
        .repository_name(repository_name)
        .force(force)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted ECR repository {}", repository_name)),
    })
}

/// Sets a repository policy
pub async fn set_repository_policy(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    policy_document: &ron::Value,
) -> Result<OpExecOutput, anyhow::Error> {
    // Convert RON policy to JSON
    let policy_json = serde_json::to_string(policy_document)
        .context("Failed to serialize repository policy as JSON")?;

    client
        .set_repository_policy()
        .repository_name(repository_name)
        .policy_text(policy_json)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Set repository policy for ECR repository {}",
            repository_name
        )),
    })
}

/// Deletes a repository policy
pub async fn delete_repository_policy(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_repository_policy()
        .repository_name(repository_name)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Deleted repository policy for ECR repository {}",
            repository_name
        )),
    })
}

/// Sets a lifecycle policy
pub async fn set_lifecycle_policy(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    lifecycle_policy_text: &ron::Value,
) -> Result<OpExecOutput, anyhow::Error> {
    // Convert RON policy to JSON
    let policy_json = serde_json::to_string(lifecycle_policy_text)
        .context("Failed to serialize lifecycle policy as JSON")?;

    client
        .put_lifecycle_policy()
        .repository_name(repository_name)
        .lifecycle_policy_text(policy_json)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Set lifecycle policy for ECR repository {}",
            repository_name
        )),
    })
}

/// Deletes a lifecycle policy
pub async fn delete_lifecycle_policy(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_lifecycle_policy()
        .repository_name(repository_name)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Deleted lifecycle policy for ECR repository {}",
            repository_name
        )),
    })
}

/// Sets a registry policy
pub async fn set_registry_policy(
    client: &aws_sdk_ecr::Client,
    policy_document: &ron::Value,
) -> Result<OpExecOutput, anyhow::Error> {
    // Convert RON policy to JSON
    let policy_json = serde_json::to_string(policy_document)
        .context("Failed to serialize registry policy as JSON")?;

    client
        .put_registry_policy()
        .policy_text(policy_json)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Set ECR registry policy")),
    })
}

/// Deletes a registry policy
pub async fn delete_registry_policy(
    client: &aws_sdk_ecr::Client,
) -> Result<OpExecOutput, anyhow::Error> {
    client.delete_registry_policy().send().await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted ECR registry policy")),
    })
}

/// Tags an image
pub async fn tag_image(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    source_image_digest: &str,
    image_tag: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .put_image()
        .repository_name(repository_name)
        .image_tag(image_tag)
        .image_manifest(source_image_digest.to_string())
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Tagged image with digest {} as {} in ECR repository {}",
            source_image_digest, image_tag, repository_name
        )),
    })
}

/// Untags an image
pub async fn untag_image(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    image_tag: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .batch_delete_image()
        .repository_name(repository_name)
        .image_ids(
            aws_sdk_ecr::types::ImageIdentifier::builder()
                .image_tag(image_tag)
                .build(),
        )
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Untagged image {} in ECR repository {}",
            image_tag, repository_name
        )),
    })
}

/// Batch deletes images
pub async fn batch_delete_images(
    client: &aws_sdk_ecr::Client,
    repository_name: &str,
    image_ids: &[super::op::ImageId],
) -> Result<OpExecOutput, anyhow::Error> {
    let mut aws_image_ids = Vec::new();

    for image_id in image_ids {
        let mut builder = aws_sdk_ecr::types::ImageIdentifier::builder();

        if let Some(tag) = &image_id.image_tag {
            builder = builder.image_tag(tag);
        }

        if let Some(digest) = &image_id.image_digest {
            builder = builder.image_digest(digest);
        }

        aws_image_ids.push(builder.build());
    }

    client
        .batch_delete_image()
        .repository_name(repository_name)
        .set_image_ids(Some(aws_image_ids))
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Deleted {} images from ECR repository {}",
            image_ids.len(),
            repository_name
        )),
    })
}

/// Creates a pull through cache rule
pub async fn create_pull_through_cache_rule(
    client: &aws_sdk_ecr::Client,
    ecr_repository_prefix: &str,
    upstream_registry_url: &str,
    credential_arn: Option<String>,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut rule_builder = client
        .create_pull_through_cache_rule()
        .ecr_repository_prefix(ecr_repository_prefix)
        .upstream_registry_url(upstream_registry_url);

    if let Some(cred_arn) = credential_arn {
        rule_builder = rule_builder.credential_arn(cred_arn);
    }
    rule_builder.send().await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Created pull through cache rule for prefix {} from {}",
            ecr_repository_prefix, upstream_registry_url
        )),
    })
}

/// Deletes a pull through cache rule
pub async fn delete_pull_through_cache_rule(
    client: &aws_sdk_ecr::Client,
    ecr_repository_prefix: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    client
        .delete_pull_through_cache_rule()
        .ecr_repository_prefix(ecr_repository_prefix)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!(
            "Deleted pull through cache rule for prefix {}",
            ecr_repository_prefix
        )),
    })
}

/// Sets replication configuration
pub async fn set_replication_configuration(
    client: &aws_sdk_ecr::Client,
    rules: &[super::op::ReplicationRule],
) -> Result<OpExecOutput, anyhow::Error> {
    let mut replication_rules = Vec::new();

    for rule in rules {
        let mut rule_builder = aws_sdk_ecr::types::ReplicationRule::builder();

        // Add destinations
        let mut destinations = Vec::new();
        for registry_id in &rule.destinations {
            destinations.push(
                aws_sdk_ecr::types::ReplicationDestination::builder()
                    .region(registry_id)
                    .registry_id(registry_id)
                    .build()?,
            );
        }
        rule_builder = rule_builder.set_destinations(Some(destinations));

        // Add repository filters if present
        if let Some(filters) = &rule.repository_filters {
            let mut aws_filters = Vec::new();
            for filter in filters {
                aws_filters.push(
                    aws_sdk_ecr::types::RepositoryFilter::builder()
                        .filter_type(filter.filter_type.as_str().into())
                        .filter(filter.filter_value.clone())
                        .build()?,
                );
            }
            rule_builder = rule_builder.set_repository_filters(Some(aws_filters));
        }

        replication_rules.push(rule_builder.build()?);
    }

    let replication_config = aws_sdk_ecr::types::ReplicationConfiguration::builder()
        .set_rules(Some(replication_rules))
        .build()?;

    client
        .put_replication_configuration()
        .replication_configuration(replication_config)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Set ECR replication configuration")),
    })
}

/// Deletes replication configuration
pub async fn delete_replication_configuration(
    client: &aws_sdk_ecr::Client,
) -> Result<OpExecOutput, anyhow::Error> {
    // To delete all replication rules, set an empty rules array
    let replication_config = aws_sdk_ecr::types::ReplicationConfiguration::builder()
        .set_rules(Some(Vec::new()))
        .build()?;

    client
        .put_replication_configuration()
        .replication_configuration(replication_config)
        .send()
        .await?;

    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted ECR replication configuration")),
    })
}
