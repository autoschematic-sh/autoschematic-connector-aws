use autoschematic_core::{
    connector::{Resource, ResourceAddress},
    util::RON,
};
use serde::{Deserialize, Serialize};

use super::{addr::EfsResourceAddress, tags::Tags};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct FileSystem {
    pub throughput_mode: String,                      // "bursting", "provisioned", or "elastic"
    pub provisioned_throughput_in_mibps: Option<f64>, // Only required if throughput_mode is "provisioned"
    pub performance_mode: String,                     // "generalPurpose" or "maxIO"
    pub encrypted: bool,
    pub kms_key_id: Option<String>,
    pub availability_zone_name: Option<String>, // For One Zone file systems
    pub lifecycle_policies: Vec<LifecyclePolicy>,
    pub file_system_protection: Option<FileSystemProtection>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LifecyclePolicy {
    pub transition_to_ia: Option<String>, // "AFTER_7_DAYS", "AFTER_14_DAYS", "AFTER_30_DAYS", "AFTER_60_DAYS", "AFTER_90_DAYS"
    pub transition_to_primary_storage_class: Option<String>, // "AFTER_1_ACCESS"
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct FileSystemProtection {
    pub replication_overwrite_protection: Option<String>, // "ENABLED" or "DISABLED"
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct MountTarget {
    pub subnet_id: String,
    pub security_groups: Vec<String>,
    pub ip_address: Option<String>, // If not provided, AWS assigns automatically
    pub file_system_id: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct AccessPoint {
    pub file_system_id: String,
    pub posix_user: Option<PosixUser>,
    pub root_directory: Option<RootDirectory>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct PosixUser {
    pub uid: i64,                         // User ID
    pub gid: i64,                         // Group ID
    pub secondary_gids: Option<Vec<i64>>, // Secondary group IDs
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct RootDirectory {
    pub path: Option<String>, // Path to the directory, defaults to "/"
    pub creation_info: Option<CreationInfo>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct CreationInfo {
    pub owner_uid:   i64,    // POSIX user ID to own the directory
    pub owner_gid:   i64,    // POSIX group ID to own the directory
    pub permissions: String, // POSIX permissions in octal string, e.g. "0755"
}

pub enum EfsResource {
    FileSystem(FileSystem),
    MountTarget(MountTarget),
    AccessPoint(AccessPoint),
}

// Implement the Resource trait
impl Resource for EfsResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default().struct_names(true);

        match self {
            EfsResource::FileSystem(fs) => match RON.to_string_pretty(&fs, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            EfsResource::MountTarget(mt) => match RON.to_string_pretty(&mt, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            EfsResource::AccessPoint(ap) => match RON.to_string_pretty(&ap, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr_option = EfsResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;

        match addr_option {
            EfsResourceAddress::FileSystem(_region, _fs_id) => Ok(EfsResource::FileSystem(RON.from_str(s)?)),
            EfsResourceAddress::MountTarget(_region, _fs_id, _mt_id) => Ok(EfsResource::MountTarget(RON.from_str(s)?)),
            EfsResourceAddress::AccessPoint(_region, _fs_id, _ap_id) => Ok(EfsResource::AccessPoint(RON.from_str(s)?)),
        }
    }
}
