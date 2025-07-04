use std::collections::HashMap;

use super::{
    resource::{Secret },
    tags::Tags, 
    op::RotationRules,
};
use autoschematic_core::{connector::OpExecOutput, util::RON};

/// Creates a Secret using the provided configuration
pub async fn create_secret(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
    secret: &Secret,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut request = client.create_secret().name(secret_id);
    
    if let Some(description) = &secret.description {
        request = request.description(description);
    }
    
    if let Some(kms_key_id) = &secret.kms_key_id {
        request = request.kms_key_id(kms_key_id);
    }
    
    if let Some(secret_string) = &secret.secret_ref {
        request = request.secret_string(secret_string);
    }
    
    // Add tags if provided
    if secret.tags.len() > 0 {
        let aws_tags = secret.tags.to_vec()?;
        request = request.set_tags(Some(aws_tags));
    }
    
    let result = request.send().await?;
    
    let mut outputs = HashMap::new();
    outputs.insert(String::from("secret_id"), Some(secret_id.to_string()));
    
    Ok(OpExecOutput {
        outputs: Some(outputs),
        friendly_message: Some(format!("Created secret: {}", result.name().unwrap_or("unknown"))),
    })
}

/// Updates a secret's description
pub async fn update_secret_description(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
    description: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    let result = client
        .update_secret()
        .secret_id(secret_id)
        .description(description)
        .send()
        .await?;
        
    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated secret description: {}", result.name().unwrap_or("unknown"))),
    })
}

/// Updates a secret's value
pub async fn update_secret_value(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
    secret_string: &str,
    client_request_token: Option<&str>,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut request = client
        .put_secret_value()
        .secret_id(secret_id)
        .secret_string(secret_string);
        
    if let Some(token) = client_request_token {
        request = request.client_request_token(token);
    }
    
    let result = request.send().await?;
    
    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated secret value: {}", result.name().unwrap_or("unknown"))),
    })
}

/// Updates a secret's tags
pub async fn update_secret_tags(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecOutput, anyhow::Error> {
    // Calculate tag differences
    let (untag_keys, new_tagset) = super::tags::tag_diff(old_tags, new_tags)?;
    
    // Remove tags
    if !untag_keys.is_empty() {
        client
            .untag_resource()
            .secret_id(secret_id)
            .set_tag_keys(Some(untag_keys))
            .send()
            .await?;
    }
    
    // Add new tags
    if !new_tagset.is_empty() {
        client
            .tag_resource()
            .secret_id(secret_id)
            .set_tags(Some(new_tagset))
            .send()
            .await?;
    }
    
    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated tags for secret: {secret_id}")),
    })
}

/// Updates a secret's KMS key ID
pub async fn update_secret_kms_key_id(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
    kms_key_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    let result = client
        .update_secret()
        .secret_id(secret_id)
        .kms_key_id(kms_key_id)
        .send()
        .await?;
        
    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Updated KMS key for secret: {}", result.name().unwrap_or("unknown"))),
    })
}

/// Deletes a secret
pub async fn delete_secret(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
    recovery_window_in_days: Option<i64>,
    force_delete_without_recovery: Option<bool>,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut request = client.delete_secret().secret_id(secret_id);
    
    if let Some(window) = recovery_window_in_days {
        request = request.recovery_window_in_days(window);
    }
    
    if let Some(force) = force_delete_without_recovery
        && force {
            request = request.force_delete_without_recovery(true);
        }
    
    let result = request.send().await?;
    
    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted secret: {}", result.name().unwrap_or("unknown"))),
    })
}

/// Restores a previously deleted secret
pub async fn restore_secret(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    let result = client
        .restore_secret()
        .secret_id(secret_id)
        .send()
        .await?;
        
    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Restored secret: {}", result.name().unwrap_or("unknown"))),
    })
}

/// Configures rotation for a secret
pub async fn rotate_secret(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
    rotation_lambda_arn: &str,
    rotation_rules: &RotationRules,
) -> Result<OpExecOutput, anyhow::Error> {
    let mut request = client
        .rotate_secret()
        .secret_id(secret_id)
        .rotation_lambda_arn(rotation_lambda_arn);
    
    // Create rotation rules
    let mut rotation_rules_builder = aws_sdk_secretsmanager::types::RotationRulesType::builder();
    
    if let Some(days) = rotation_rules.automatically_after_days {
        rotation_rules_builder = rotation_rules_builder.automatically_after_days(days);
    }
    
    if let Some(duration) = &rotation_rules.duration {
        rotation_rules_builder = rotation_rules_builder.duration(duration);
    }
    
    if let Some(expression) = &rotation_rules.schedule_expression {
        rotation_rules_builder = rotation_rules_builder.schedule_expression(expression);
    }
    
    request = request.rotation_rules(rotation_rules_builder.build());
    
    let result = request.send().await?;
    
    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Configured rotation for secret: {}", result.name().unwrap_or("unknown"))),
    })
}

/// Sets a resource policy for a secret
pub async fn set_secret_policy(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
    policy_document: &ron::Value,
    block_public_policy: Option<bool>,
) -> Result<OpExecOutput, anyhow::Error> {
    let policy_text = RON.to_string(policy_document)?;
    
    let mut request = client
        .put_resource_policy()
        .secret_id(secret_id)
        .resource_policy(policy_text);
        
    if let Some(block) = block_public_policy {
        request = request.block_public_policy(block);
    }
    
    let result = request.send().await?;
    
    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Set policy for secret: {} (ARN: {})", 
            secret_id, 
            result.arn().unwrap_or("unknown"))),
    })
}

/// Deletes a resource policy from a secret
pub async fn delete_secret_policy(
    client: &aws_sdk_secretsmanager::Client,
    secret_id: &str,
) -> Result<OpExecOutput, anyhow::Error> {
    let result = client
        .delete_resource_policy()
        .secret_id(secret_id)
        .send()
        .await?;
        
    Ok(OpExecOutput {
        outputs: None,
        friendly_message: Some(format!("Deleted policy for secret: {} (ARN: {})", 
            secret_id, 
            result.arn().unwrap_or("unknown"))),
    })
}
