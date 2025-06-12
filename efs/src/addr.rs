use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};
use std::path::{Path, PathBuf};

type Region = String;
type FileSystemId = String;
type MountTargetId = String;
type AccessPointId = String;

#[derive(Debug, Clone)]
pub enum EfsResourceAddress {
    FileSystem(Region, FileSystemId),
    MountTarget(Region, FileSystemId, MountTargetId),
    AccessPoint(Region, FileSystemId, AccessPointId),
}

impl ResourceAddress for EfsResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            EfsResourceAddress::FileSystem(region, fs_id) => {
                PathBuf::from(format!("aws/efs/{}/file_systems/{}.ron", region, fs_id))
            }
            EfsResourceAddress::MountTarget(region, fs_id, mt_id) => PathBuf::from(format!(
                "aws/efs/{}/file_systems/{}/mount_targets/{}.ron",
                region, fs_id, mt_id
            )),
            EfsResourceAddress::AccessPoint(region, fs_id, ap_id) => PathBuf::from(format!(
                "aws/efs/{}/file_systems/{}/access_points/{}.ron",
                region, fs_id, ap_id
            )),
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path
            .components()
            .map(|s| s.as_os_str().to_str().unwrap())
            .collect();

        match &path_components[..] {
            ["aws", "efs", region, "file_systems", fs_id] if fs_id.ends_with(".ron") => {
                let fs_id = fs_id.strip_suffix(".ron").unwrap().to_string();
                Ok(EfsResourceAddress::FileSystem(region.to_string(), fs_id))
            }
            ["aws", "efs", region, "file_systems", fs_id, "mount_targets", mt_id] if mt_id.ends_with(".ron") => {
                let mt_id = mt_id.strip_suffix(".ron").unwrap().to_string();
                Ok(EfsResourceAddress::MountTarget(region.to_string(), fs_id.to_string(), mt_id))
            }
            ["aws", "efs", region, "file_systems", fs_id, "access_points", ap_id] if ap_id.ends_with(".ron") => {
                let ap_id = ap_id.strip_suffix(".ron").unwrap().to_string();
                Ok(EfsResourceAddress::AccessPoint(region.to_string(), fs_id.to_string(), ap_id))
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
