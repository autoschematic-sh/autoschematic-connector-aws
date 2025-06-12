use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};

use super::{
    resource::{AccessPoint, FileSystem, FileSystemProtection, LifecyclePolicy, MountTarget},
    tags::Tags,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum EfsConnectorOp {
    // FileSystem operations
    CreateFileSystem(FileSystem),
    UpdateFileSystemThroughput {
        throughput_mode: String,
        provisioned_throughput_in_mibps: Option<f64>,
    },
    UpdateFileSystemLifecyclePolicies {
        lifecycle_policies: Vec<LifecyclePolicy>,
    },
    UpdateFileSystemProtection {
        file_system_protection: FileSystemProtection,
    },
    UpdateFileSystemTags(Tags, Tags),
    DeleteFileSystem {
        bypass_protection: bool,
    },

    // MountTarget operations
    CreateMountTarget(MountTarget),
    UpdateMountTargetSecurityGroups {
        security_groups: Vec<String>,
    },
    DeleteMountTarget,

    // AccessPoint operations
    CreateAccessPoint(AccessPoint),
    UpdateAccessPointTags(Tags, Tags),
    DeleteAccessPoint,
}

impl ConnectorOp for EfsConnectorOp {
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
