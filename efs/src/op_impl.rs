use anyhow::bail;
use aws_sdk_efs::types::{LifecyclePolicy as SdkLifecyclePolicy, Tag};
use std::collections::HashMap;

use super::{
    resource::{AccessPoint, FileSystem, FileSystemProtection, LifecyclePolicy, MountTarget},
    tags::{tag_diff, Tags},
};
use autoschematic_core::connector::OpExecResponse;

// FileSystem operations
pub async fn create_file_system(
    client: &aws_sdk_efs::Client,
    file_system: &FileSystem,
    file_system_id: &str,
) -> anyhow::Result<OpExecResponse> {
    // Build create request
    let mut request = client
        .create_file_system()
        .performance_mode(file_system.performance_mode.as_str().into())
        .encrypted(file_system.encrypted)
        .throughput_mode(file_system.throughput_mode.as_str().into());

    // Add optional parameters
    if let Some(throughput) = file_system.provisioned_throughput_in_mibps {
        request = request.provisioned_throughput_in_mibps(throughput);
    }

    if let Some(kms_key_id) = &file_system.kms_key_id {
        request = request.kms_key_id(kms_key_id);
    }

    if let Some(az) = &file_system.availability_zone_name {
        request = request.availability_zone_name(az);
    }

    // Add tags
    let aws_tags: Vec<Tag> = file_system.tags.to_vec()?;
    if !aws_tags.is_empty() {
        request = request.set_tags(Some(aws_tags));
    }

    // Send the request
    let response = request.send().await?;

    let mut outputs = HashMap::new();
    let fs_id = response.file_system_id();
    outputs.insert(String::from("file_system_id"), Some(fs_id.to_string()));

    // Apply lifecycle policies if any are specified
    if !file_system.lifecycle_policies.is_empty() {
        let lifecycle_policies: Vec<SdkLifecyclePolicy> = file_system
            .lifecycle_policies
            .iter()
            .map(|policy| {
                let mut builder = SdkLifecyclePolicy::builder();

                if let Some(to_ia) = &policy.transition_to_ia {
                    builder = builder.transition_to_ia(to_ia.as_str().into());
                }

                if let Some(to_primary) = &policy.transition_to_primary_storage_class {
                    builder =
                        builder.transition_to_primary_storage_class(to_primary.as_str().into());
                }

                builder.build()
            })
            .collect();

        client
            .put_lifecycle_configuration()
            .file_system_id(response.file_system_id())
            .set_lifecycle_policies(Some(lifecycle_policies))
            .send()
            .await?;
    }

    // Apply file system protection if specified
    if let Some(protection) = &file_system.file_system_protection
        && let Some(replication_protection) = &protection.replication_overwrite_protection {
            client
                .update_file_system_protection()
                .file_system_id(response.file_system_id())
                .replication_overwrite_protection(replication_protection.as_str().into())
                .send()
                .await?;
        }

    Ok(OpExecResponse {
        outputs: Some(outputs),
        friendly_message: Some(format!(
            "Created EFS file system with ID: {}",
            response.file_system_id()
        )),
    })
}

pub async fn update_file_system_throughput(
    client: &aws_sdk_efs::Client,
    file_system_id: &str,
    throughput_mode: &str,
    provisioned_throughput_in_mibps: Option<f64>,
) -> anyhow::Result<OpExecResponse> {
    let mut request = client
        .update_file_system()
        .file_system_id(file_system_id)
        .throughput_mode(throughput_mode.into());

    // Add provisioned throughput if mode is provisioned
    if throughput_mode == "provisioned" {
        if let Some(throughput) = provisioned_throughput_in_mibps {
            request = request.provisioned_throughput_in_mibps(throughput);
        } else {
            bail!("Provisioned throughput value is required when throughput mode is 'provisioned'");
        }
    }

    let response = request.send().await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated throughput mode to '{throughput_mode}' for file system {file_system_id}"
        )),
    })
}

pub async fn update_file_system_lifecycle_policies(
    client: &aws_sdk_efs::Client,
    file_system_id: &str,
    lifecycle_policies: Vec<LifecyclePolicy>,
) -> anyhow::Result<OpExecResponse> {
    let lifecycle_policies: Vec<SdkLifecyclePolicy> = lifecycle_policies
        .iter()
        .map(|policy| {
            let mut builder = SdkLifecyclePolicy::builder();

            if let Some(to_ia) = &policy.transition_to_ia {
                builder = builder.transition_to_ia(to_ia.as_str().into());
            }

            if let Some(to_primary) = &policy.transition_to_primary_storage_class {
                builder = builder.transition_to_primary_storage_class(to_primary.as_str().into());
            }

            builder.build()
        })
        .collect();

    client
        .put_lifecycle_configuration()
        .file_system_id(file_system_id)
        .set_lifecycle_policies(Some(lifecycle_policies))
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated lifecycle policies for file system {file_system_id}"
        )),
    })
}

pub async fn update_file_system_protection(
    client: &aws_sdk_efs::Client,
    file_system_id: &str,
    file_system_protection: FileSystemProtection,
) -> anyhow::Result<OpExecResponse> {
    let mut request = client
        .update_file_system_protection()
        .file_system_id(file_system_id);

    if let Some(replication_protection) = file_system_protection.replication_overwrite_protection {
        request = request.replication_overwrite_protection(replication_protection.as_str().into());
    }

    request.send().await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated protection settings for file system {file_system_id}"
        )),
    })
}

pub async fn update_file_system_tags(
    client: &aws_sdk_efs::Client,
    file_system_id: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> anyhow::Result<OpExecResponse> {
    // Calculate tag differences
    let (remove_tags, add_tags) = tag_diff(old_tags, new_tags)?;

    // Remove tags if any need to be removed
    if !remove_tags.is_empty() {
        client
            .untag_resource()
            .resource_id(file_system_id)
            .set_tag_keys(Some(remove_tags))
            .send()
            .await?;
    }

    // Add new tags if any need to be added
    if !add_tags.is_empty() {
        client
            .tag_resource()
            .resource_id(file_system_id)
            .set_tags(Some(add_tags))
            .send()
            .await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated tags for file system {file_system_id}")),
    })
}

pub async fn delete_file_system(
    client: &aws_sdk_efs::Client,
    file_system_id: &str,
    bypass_protection: bool,
) -> anyhow::Result<OpExecResponse> {
    // First ensure all mount targets are deleted
    let mount_targets = client
        .describe_mount_targets()
        .file_system_id(file_system_id)
        .send()
        .await?;

    let mts = mount_targets.mount_targets();
    if !mts.is_empty() {
        if !bypass_protection {
            bail!("Cannot delete file system with existing mount targets. Set bypass_protection to true or delete mount targets first.");
        }

        // Delete all mount targets if bypass_protection is true
        for mt in mts {
            client
                .delete_mount_target()
                .mount_target_id(mt.mount_target_id())
                .send()
                .await?;
        }

        // Wait for mount targets to be fully deleted
        // This is a simplified approach, in production you might want to add retries and timeouts
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }

    client
        .delete_file_system()
        .file_system_id(file_system_id)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Deleted file system {file_system_id}")),
    })
}

// MountTarget operations
pub async fn create_mount_target(
    client: &aws_sdk_efs::Client,
    mount_target: &MountTarget,
    file_system_id: &str,
) -> anyhow::Result<OpExecResponse> {
    let mut request = client
        .create_mount_target()
        .file_system_id(file_system_id)
        .subnet_id(&mount_target.subnet_id)
        .set_security_groups(Some(mount_target.security_groups.clone()));

    // Add optional IP address if specified
    if let Some(ip_address) = &mount_target.ip_address {
        request = request.ip_address(ip_address);
    }

    let response = request.send().await?;

    let mut outputs = HashMap::new();
    let mt_id = response.mount_target_id();
    outputs.insert(String::from("mount_target_id"), Some(mt_id.to_string()));

    Ok(OpExecResponse {
        outputs: Some(outputs),
        friendly_message: Some(format!(
            "Created mount target with ID: {}",
            response.mount_target_id()
        )),
    })
}

pub async fn update_mount_target_security_groups(
    client: &aws_sdk_efs::Client,
    mount_target_id: &str,
    security_groups: Vec<String>,
) -> anyhow::Result<OpExecResponse> {
    client
        .modify_mount_target_security_groups()
        .mount_target_id(mount_target_id)
        .set_security_groups(Some(security_groups))
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!(
            "Updated security groups for mount target {mount_target_id}"
        )),
    })
}

pub async fn delete_mount_target(
    client: &aws_sdk_efs::Client,
    mount_target_id: &str,
) -> anyhow::Result<OpExecResponse> {
    client
        .delete_mount_target()
        .mount_target_id(mount_target_id)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Deleted mount target {mount_target_id}")),
    })
}

// AccessPoint operations
pub async fn create_access_point(
    client: &aws_sdk_efs::Client,
    access_point: &AccessPoint,
    file_system_id: &str,
) -> anyhow::Result<OpExecResponse> {
    let mut request = client.create_access_point().file_system_id(file_system_id);

    // Add POSIX user if specified
    if let Some(posix_user) = &access_point.posix_user {
        let mut posix_user_builder = aws_sdk_efs::types::PosixUser::builder()
            .uid(posix_user.uid)
            .gid(posix_user.gid);

        if let Some(secondary_gids) = &posix_user.secondary_gids {
            posix_user_builder =
                posix_user_builder.set_secondary_gids(Some(secondary_gids.clone()));
        }

        request = request.posix_user(posix_user_builder.build()?);
    }

    // Add root directory if specified
    if let Some(root_dir) = &access_point.root_directory {
        let mut root_dir_builder = aws_sdk_efs::types::RootDirectory::builder();

        if let Some(path) = &root_dir.path {
            root_dir_builder = root_dir_builder.path(path);
        }

        if let Some(creation_info) = &root_dir.creation_info {
            root_dir_builder = root_dir_builder.creation_info(
                aws_sdk_efs::types::CreationInfo::builder()
                    .owner_uid(creation_info.owner_uid)
                    .owner_gid(creation_info.owner_gid)
                    .permissions(&creation_info.permissions)
                    .build()?,
            );
        }

        request = request.root_directory(root_dir_builder.build());
    }

    // Add tags
    let aws_tags: Vec<Tag> = access_point.tags.to_vec()?;
    if !aws_tags.is_empty() {
        request = request.set_tags(Some(aws_tags));
    }

    let response = request.send().await?;

    let mut outputs = HashMap::new();
    if let Some(ap_id) = response.access_point_id() {
        outputs.insert(String::from("access_point_id"), Some(ap_id.to_string()));
    }

    Ok(OpExecResponse {
        outputs: Some(outputs),
        friendly_message: Some(format!(
            "Created access point with ID: {}",
            response.access_point_id().unwrap_or("unknown")
        )),
    })
}

pub async fn update_access_point_tags(
    client: &aws_sdk_efs::Client,
    access_point_id: &str,
    old_tags: &Tags,
    new_tags: &Tags,
) -> anyhow::Result<OpExecResponse> {
    // Calculate tag differences
    let (remove_tags, add_tags) = tag_diff(old_tags, new_tags)?;

    // Remove tags if any need to be removed
    if !remove_tags.is_empty() {
        client
            .untag_resource()
            .resource_id(access_point_id)
            .set_tag_keys(Some(remove_tags))
            .send()
            .await?;
    }

    // Add new tags if any need to be added
    if !add_tags.is_empty() {
        client
            .tag_resource()
            .resource_id(access_point_id)
            .set_tags(Some(add_tags))
            .send()
            .await?;
    }

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Updated tags for access point {access_point_id}")),
    })
}

pub async fn delete_access_point(
    client: &aws_sdk_efs::Client,
    access_point_id: &str,
) -> anyhow::Result<OpExecResponse> {
    client
        .delete_access_point()
        .access_point_id(access_point_id)
        .send()
        .await?;

    Ok(OpExecResponse {
        outputs: None,
        friendly_message: Some(format!("Deleted access point {access_point_id}")),
    })
}
