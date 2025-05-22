use autoschematic_core::connector::ConnectorOp;
use serde::{Deserialize, Serialize};

use autoschematic_core::util::RON;

use super::{
    resource::{EncryptionConfiguration, Repository},
    tags::Tags,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum EcrConnectorOp {
    // Repository operations
    CreateRepository(Repository),
    UpdateRepositoryTags(Tags, Tags),
    UpdateImageTagMutability {
        image_tag_mutability: String, // MUTABLE or IMMUTABLE
    },
    UpdateImageScanningConfiguration {
        scan_on_push: bool,
    },
    UpdateEncryptionConfiguration {
        encryption_configuration: Option<EncryptionConfiguration>,
    },
    DeleteRepository {
        force: bool, // When true, deletes the repository even if it contains images
    },

    // Repository Policy operations
    SetRepositoryPolicy {
        policy_document: ron::Value,
    },
    DeleteRepositoryPolicy,

    // Lifecycle Policy operations
    SetLifecyclePolicy {
        lifecycle_policy_text: ron::Value,
    },
    DeleteLifecyclePolicy,

    // Registry Policy operations
    SetRegistryPolicy {
        policy_document: ron::Value,
    },
    DeleteRegistryPolicy,

    // Image operations
    TagImage {
        source_image_digest: String,
        image_tag: String,
    },
    UntagImage {
        image_tag: String,
    },
    BatchDeleteImages {
        image_ids: Vec<ImageId>,
    },

    // Pull Through Cache operations
    CreatePullThroughCacheRule {
        upstream_registry_url: String,
        credential_arn: Option<String>,
    },
    DeletePullThroughCacheRule {
    },

    // Replication Configuration operations
    SetReplicationConfiguration {
        rules: Vec<ReplicationRule>,
    },
    DeleteReplicationConfiguration,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImageId {
    pub image_tag: Option<String>,
    pub image_digest: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationRule {
    pub destinations: Vec<String>, // List of destination registry IDs
    pub repository_filters: Option<Vec<RepositoryFilter>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RepositoryFilter {
    pub filter_type: String, // PREFIX_MATCH
    pub filter_value: String,
}

impl ConnectorOp for EcrConnectorOp {
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
