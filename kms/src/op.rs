use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};

use super::{
    resource::{KmsAlias, KmsKey, KmsKeyPolicy},
    tags::Tags,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum KmsConnectorOp {
    // Key operations
    CreateKey(KmsKey),
    UpdateKeyDescription(String, String),
    UpdateKeyTags(Tags, Tags),
    EnableKey,
    DisableKey,
    DeleteKey,

    // Key policy operations
    UpdateKeyPolicy(KmsKeyPolicy, KmsKeyPolicy),

    // Alias operations
    CreateAlias(KmsAlias),
    UpdateAlias(String), // New target key ID
    DeleteAlias,

    // Key rotation operations
    EnableKeyRotation,
    DisableKeyRotation,
}

impl ConnectorOp for KmsConnectorOp {
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
