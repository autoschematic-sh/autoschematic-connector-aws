use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};

use super::{
    resource::Secret,
    tags::Tags,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum SecretsManagerConnectorOp {
    // Secret operations
    CreateSecret(Secret),
    UpdateSecretDescription {
        description: String,
    },
    UpdateSecretValue {
        secret_ref: String,
        client_request_token: Option<String>,
    },
    UpdateSecretTags(Tags, Tags),
    UpdateSecretKmsKeyId {
        kms_key_id: String,
    },
    DeleteSecret {
        recovery_window_in_days: Option<i64>,
        force_delete_without_recovery: Option<bool>,
    },
    RestoreSecret,
    RotateSecret {
        rotation_lambda_arn: String,
        rotation_rules: RotationRules,
    },

    // Secret Policy operations
    SetSecretPolicy {
        policy_document: ron::Value,
        block_public_policy: Option<bool>,
    },
    DeleteSecretPolicy,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RotationRules {
    pub automatically_after_days: Option<i64>,
    pub duration: Option<String>,
    pub schedule_expression: Option<String>,
}

impl ConnectorOp for SecretsManagerConnectorOp {
    fn to_string(&self) -> Result<String, anyhow::Error> {
        Ok(RON.to_string(self)?)
    }

    fn from_str(s: &str) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(RON.from_str(s)?)
    }
}
