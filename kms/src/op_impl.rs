use anyhow::{bail, Context};
use aws_sdk_kms::types::{Tag, KeySpec, KeyUsageType};
use std::collections::HashMap;

use super::{
    resource::{KmsKey, KmsKeyPolicy, KmsAlias},
    tags::Tags,
};
use autoschematic_core::connector::OpExecResponse;

/// Creates a KMS key using the provided configuration
pub async fn create_key(
    client: &aws_sdk_kms::Client,
    key: &KmsKey,
) -> Result<OpExecResponse, anyhow::Error> {
    let mut create_key_req = client.create_key().description(&key.description);

    // Set key usage
    if !key.key_usage.is_empty() {
        let key_usage = match key.key_usage.as_str() {
            "ENCRYPT_DECRYPT" => KeyUsageType::EncryptDecrypt,
            "SIGN_VERIFY" => KeyUsageType::SignVerify,
            "GENERATE_VERIFY_MAC" => KeyUsageType::GenerateVerifyMac,
            _ => KeyUsageType::EncryptDecrypt, // Default
        };
        create_key_req = create_key_req.key_usage(key_usage);
    }

    // Set key spec
    if !key.customer_master_key_spec.is_empty() {
        match key.customer_master_key_spec.as_str() {
            "SYMMETRIC_DEFAULT" => {
                create_key_req = create_key_req.key_spec(KeySpec::SymmetricDefault);
            },
            "RSA_2048" => {
                create_key_req = create_key_req.key_spec(KeySpec::Rsa2048);
            },
            "RSA_3072" => {
                create_key_req = create_key_req.key_spec(KeySpec::Rsa3072);
            },
            "RSA_4096" => {
                create_key_req = create_key_req.key_spec(KeySpec::Rsa4096);
            },
            "ECC_NIST_P256" => {
                create_key_req = create_key_req.key_spec(KeySpec::EccNistP256);
            },
            "ECC_NIST_P384" => {
                create_key_req = create_key_req.key_spec(KeySpec::EccNistP384);
            },
            "ECC_NIST_P521" => {
                create_key_req = create_key_req.key_spec(KeySpec::EccNistP521);
            },
            "ECC_SECG_P256K1" => {
                create_key_req = create_key_req.key_spec(KeySpec::EccSecgP256K1);
            },
            "HMAC_224" => {
                create_key_req = create_key_req.key_spec(KeySpec::Hmac224);
            },
            "HMAC_256" => {
                create_key_req = create_key_req.key_spec(KeySpec::Hmac256);
            },
            "HMAC_384" => {
                create_key_req = create_key_req.key_spec(KeySpec::Hmac384);
            },
            "HMAC_512" => {
                create_key_req = create_key_req.key_spec(KeySpec::Hmac512);
            },
            "SM2" => {
                create_key_req = create_key_req.key_spec(KeySpec::Sm2);
            },
            _ => {
                create_key_req = create_key_req.key_spec(KeySpec::SymmetricDefault);
            }
        }
    }

    // Set multi-region
    if key.multi_region {
        create_key_req = create_key_req.multi_region(true);
    }

    // Set tags
    let aws_tags: Option<Vec<Tag>> = key.tags.clone().into();
    if let Some(tags) = aws_tags {
        create_key_req = create_key_req.set_tags(Some(tags));
    }

    // Create the key
    let create_key_resp = create_key_req.send().await?;

    let Some(key_metadata) = create_key_resp.key_metadata else {
        bail!("Failed to create KMS key: response did not contain key metadata");
    };

    let key_id = key_metadata.key_id else {
        bail!("Failed to create KMS key: response did not contain key ID");
    };

    // Enable/disable key as specified
    if !key.enabled {
        client.disable_key().key_id(&key_id).send().await?;
    }

    let mut outputs = HashMap::new();
    outputs.insert(String::from("key_id"), Some(key_id.clone()));

    Ok(OpExecResponse {
        outputs: Some(outputs),
        friendly_message: Some(format!("Created KMS key {key_id}")),
    })
}

/// Updates a KMS key description
pub async fn update_key_description(
    client: &aws_sdk_kms::Client,
    key_id: &str,
    description: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    client
        .update_key_description()
        .key_id(key_id)
        .description(description)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated description for KMS key {key_id}")),
    })
}

/// Updates KMS key tags
pub async fn update_key_tags(
    client: &aws_sdk_kms::Client,
    key_id: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> Result<OpExecResponse, anyhow::Error> {
    let (remove_keys, add_tags) = super::tags::kms_tag_diff(old_tags, new_tags)?;

    // Remove tags if needed
    if !remove_keys.is_empty() {
        client
            .untag_resource()
            .key_id(key_id)
            .set_tag_keys(Some(remove_keys))
            .send()
            .await?;
    }

    // Add/update tags if needed
    if !add_tags.is_empty() {
        client
            .tag_resource()
            .key_id(key_id)
            .set_tags(Some(add_tags))
            .send()
            .await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated tags for KMS key {key_id}")),
    })
}

/// Enables a KMS key
pub async fn enable_key(
    client: &aws_sdk_kms::Client,
    key_id: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    client.enable_key().key_id(key_id).send().await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Enabled KMS key {key_id}")),
    })
}

/// Disables a KMS key
pub async fn disable_key(
    client: &aws_sdk_kms::Client,
    key_id: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    client.disable_key().key_id(key_id).send().await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Disabled KMS key {key_id}")),
    })
}

/// Schedules a KMS key for deletion
pub async fn delete_key(
    client: &aws_sdk_kms::Client,
    key_id: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    // KMS requires a waiting period (minimum 7 days, maximum 30 days)
    client
        .schedule_key_deletion()
        .key_id(key_id)
        .pending_window_in_days(7) // Minimum waiting period
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Scheduled KMS key {key_id} for deletion (7-day waiting period)")),
    })
}

/// Updates a KMS key policy
pub async fn update_key_policy(
    client: &aws_sdk_kms::Client,
    key_id: &str,
    policy: &KmsKeyPolicy,
) -> Result<OpExecResponse, anyhow::Error> {
    // Convert the RON policy to JSON
    let policy_json = serde_json::to_string(&policy.policy_document)
        .context("Failed to serialize policy document as JSON")?;

    client
        .put_key_policy()
        .key_id(key_id)
        .policy_name("default") // KMS only supports the "default" policy name
        .policy(policy_json)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated policy for KMS key {key_id}")),
    })
}

/// Creates a KMS alias
pub async fn create_alias(
    client: &aws_sdk_kms::Client,
    alias_name: &str,
    alias: &KmsAlias,
) -> Result<OpExecResponse, anyhow::Error> {
    // Ensure the alias name has the required prefix
    let full_alias_name = if alias_name.starts_with("alias/") {
        alias_name.to_string()
    } else {
        format!("alias/{alias_name}")
    };

    client
        .create_alias()
        .alias_name(full_alias_name)
        .target_key_id(&alias.target_key_id)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Created KMS alias {} pointing to key {}", alias_name, alias.target_key_id)),
    })
}

/// Updates a KMS alias to point to a different key
pub async fn update_alias(
    client: &aws_sdk_kms::Client,
    alias_name: &str,
    target_key_id: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    // Ensure the alias name has the required prefix
    let full_alias_name = if alias_name.starts_with("alias/") {
        alias_name.to_string()
    } else {
        format!("alias/{alias_name}")
    };

    client
        .update_alias()
        .alias_name(full_alias_name)
        .target_key_id(target_key_id)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated KMS alias {alias_name} to point to key {target_key_id}")),
    })
}

/// Deletes a KMS alias
pub async fn delete_alias(
    client: &aws_sdk_kms::Client,
    alias_name: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    // Ensure the alias name has the required prefix
    let full_alias_name = if alias_name.starts_with("alias/") {
        alias_name.to_string()
    } else {
        format!("alias/{alias_name}")
    };

    client
        .delete_alias()
        .alias_name(full_alias_name)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Deleted KMS alias {alias_name}")),
    })
}

/// Enables automatic key rotation for a KMS key
pub async fn enable_key_rotation(
    client: &aws_sdk_kms::Client,
    key_id: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    client
        .enable_key_rotation()
        .key_id(key_id)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Enabled automatic key rotation for KMS key {key_id}")),
    })
}

/// Disables automatic key rotation for a KMS key
pub async fn disable_key_rotation(
    client: &aws_sdk_kms::Client,
    key_id: &str,
) -> Result<OpExecResponse, anyhow::Error> {
    client
        .disable_key_rotation()
        .key_id(key_id)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Disabled automatic key rotation for KMS key {key_id}")),
    })
}
