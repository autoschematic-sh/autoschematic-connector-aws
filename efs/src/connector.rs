pub use addr::EfsResourceAddress;
pub use op::EfsConnectorOp;
pub use resource::EfsResource;

pub use anyhow::Context;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::bail;
use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource,
        ResourceAddress, SkeletonOutput,
    },
    diag::DiagnosticOutput,
    util::{RON, diff_ron_values, optional_string_from_utf8, ron_check_eq, ron_check_syntax},
};
use autoschematic_core::{connector_op, get_resource_output, skeleton};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use config::EfsConnectorConfig;
use tokio::sync::Mutex;

use autoschematic_connector_aws_core::config::AwsServiceConfig;

use resource::{AccessPoint, FileSystem, MountTarget};
use tags::Tags;

use crate::{addr, config, op, op_impl, resource, tags};

#[derive(Default)]
pub struct EfsConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_efs::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<EfsConnectorConfig>,
    prefix: PathBuf,
}

impl EfsConnector {
    pub async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_efs::Client>> {
        let mut cache = self.client_cache.lock().await;

        if !cache.contains_key(region_s) {
            let region = RegionProviderChain::first_try(Region::new(region_s.to_owned()));

            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .timeout_config(
                    TimeoutConfig::builder()
                        .connect_timeout(Duration::from_secs(30))
                        .operation_timeout(Duration::from_secs(30))
                        .operation_attempt_timeout(Duration::from_secs(30))
                        .read_timeout(Duration::from_secs(30))
                        .build(),
                )
                .load()
                .await;
            let client = aws_sdk_efs::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }
}

#[async_trait]
impl Connector for EfsConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_addr) = EfsResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> anyhow::Result<Arc<dyn Connector>>
    where
        Self: Sized,
    {
        Ok(Arc::new(EfsConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let efs_config: EfsConnectorConfig = EfsConnectorConfig::try_load(&self.prefix).await?;

        let account_id = efs_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = efs_config;
        *self.account_id.lock().await = account_id;
        Ok(())
    }

    async fn list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();

        for region_name in &self.config.lock().await.enabled_regions {
            let client = self.get_or_init_client(region_name).await?;

            // List file systems
            let mut next_token: Option<String> = None;
            loop {
                let mut request = client.describe_file_systems();

                if let Some(token) = &next_token {
                    request = request.marker(token);
                }

                let response = request.send().await?;

                if let Some(file_systems) = &response.file_systems {
                    for fs in file_systems {
                        let fs_id = fs.file_system_id();
                        // Add file system to results
                        results.push(EfsResourceAddress::FileSystem(region_name.to_string(), fs_id.to_string()).to_path_buf());

                        // List mount targets for this file system
                        let mount_targets_resp = client.describe_mount_targets().file_system_id(fs_id).send().await?;

                        let mount_targets = mount_targets_resp.mount_targets();
                        for mt in mount_targets {
                            let mt_id = mt.mount_target_id();
                            results.push(
                                EfsResourceAddress::MountTarget(region_name.to_string(), fs_id.to_string(), mt_id.to_string())
                                    .to_path_buf(),
                            );
                        }

                        // List access points for this file system
                        let mut ap_next_token: Option<String> = None;
                        loop {
                            let mut ap_request = client.describe_access_points().file_system_id(fs_id);

                            if let Some(token) = &ap_next_token {
                                ap_request = ap_request.next_token(token);
                            }

                            let ap_response = ap_request.send().await?;

                            let access_points = ap_response.access_points();
                            for ap in access_points {
                                if let Some(ap_id) = ap.access_point_id() {
                                    results.push(
                                        EfsResourceAddress::AccessPoint(
                                            region_name.to_string(),
                                            fs_id.to_string(),
                                            ap_id.to_string(),
                                        )
                                        .to_path_buf(),
                                    );
                                }
                            }

                            ap_next_token = ap_response.next_token().map(String::from);
                            if ap_next_token.is_none() {
                                break;
                            }
                        }
                    }
                }

                next_token = response.marker().map(String::from);
                if next_token.is_none() {
                    break;
                }
            }
        }

        Ok(results)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr_option = EfsResourceAddress::from_path(addr)?;

        match addr_option {
            EfsResourceAddress::FileSystem(region, fs_id) => {
                let client = self.get_or_init_client(&region).await?;

                // Get file system details
                let response = client.describe_file_systems().file_system_id(fs_id.clone()).send().await?;

                let file_systems = response.file_systems();
                if !file_systems.is_empty() {
                    let fs = &file_systems[0];

                    // Get lifecycle policies
                    let lifecycle_resp = client
                        .describe_lifecycle_configuration()
                        .file_system_id(fs_id.clone())
                        .send()
                        .await?;

                    let lifecycle_policies = lifecycle_resp
                        .lifecycle_policies()
                        .iter()
                        .map(|lcp| resource::LifecyclePolicy {
                            transition_to_ia: lcp.transition_to_ia().map(|s| s.as_str().to_string()),
                            transition_to_primary_storage_class: lcp
                                .transition_to_primary_storage_class()
                                .map(|s| s.as_str().to_string()),
                        })
                        .collect();

                    // Build the file system resource
                    let file_system = FileSystem {
                        throughput_mode: fs.throughput_mode().map(|s| s.as_str().to_string()).unwrap_or_default(),
                        provisioned_throughput_in_mibps: fs.provisioned_throughput_in_mibps,
                        performance_mode: fs.performance_mode().to_string(),
                        encrypted: fs.encrypted.unwrap_or(false),
                        kms_key_id: fs.kms_key_id().map(String::from),
                        availability_zone_name: fs.availability_zone_name().map(String::from),
                        lifecycle_policies,
                        file_system_protection: fs.file_system_protection().map(|p| resource::FileSystemProtection {
                            replication_overwrite_protection: p
                                .replication_overwrite_protection()
                                .map(|s| s.as_str().to_string()),
                        }),
                        tags: Tags::from(fs.tags()),
                    };

                    return get_resource_output!(
                        EfsResource::FileSystem(file_system),
                        [(String::from("file_system_id"), fs_id.clone())]
                    );
                }

                Ok(None)
            }
            EfsResourceAddress::MountTarget(region, fs_id, mt_id) => {
                let client = self.get_or_init_client(&region).await?;

                // Get mount target details
                let response = client.describe_mount_targets().mount_target_id(mt_id.clone()).send().await?;

                let mount_targets = response.mount_targets();
                if !mount_targets.is_empty() {
                    let mt = &mount_targets[0];

                    // Get security groups
                    let sg_resp = client
                        .describe_mount_target_security_groups()
                        .mount_target_id(mt_id.clone())
                        .send()
                        .await?;

                    let security_groups = sg_resp.security_groups().to_vec();

                    // Build the mount target resource
                    let mount_target = MountTarget {
                        subnet_id: mt.subnet_id().to_string(),
                        security_groups,
                        ip_address: mt.ip_address().map(String::from),
                        file_system_id: mt.file_system_id().to_string(),
                    };

                    return get_resource_output!(
                        EfsResource::MountTarget(mount_target),
                        [
                            (String::from("file_system_id"), fs_id.clone()),
                            (String::from("mount_target_id"), mt_id.clone())
                        ]
                    );
                }

                Ok(None)
            }
            EfsResourceAddress::AccessPoint(region, fs_id, ap_id) => {
                let client = self.get_or_init_client(&region).await?;

                // Get access point details
                let response = client.describe_access_points().access_point_id(ap_id.clone()).send().await?;

                let access_points = response.access_points();
                if !access_points.is_empty() {
                    let ap = &access_points[0];

                    // Build the access point resource
                    let access_point = AccessPoint {
                        file_system_id: ap.file_system_id().unwrap_or_default().to_string(),
                        posix_user: ap.posix_user().map(|pu| resource::PosixUser {
                            uid: pu.uid(),
                            gid: pu.gid(),
                            secondary_gids: Some(pu.secondary_gids().to_vec()),
                        }),
                        root_directory: ap.root_directory().map(|rd| resource::RootDirectory {
                            path: rd.path().map(String::from),
                            creation_info: rd.creation_info().map(|ci| resource::CreationInfo {
                                owner_uid:   ci.owner_uid(),
                                owner_gid:   ci.owner_gid(),
                                permissions: ci.permissions().to_string(),
                            }),
                        }),
                        tags: Tags::from(ap.tags()),
                    };

                    return get_resource_output!(
                        EfsResource::AccessPoint(access_point),
                        [
                            (String::from("file_system_id"), fs_id.clone()),
                            (String::from("access_point_id"), ap_id.clone())
                        ]
                    );
                }

                Ok(None)
            }
        }
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr = EfsResourceAddress::from_path(addr)?;

        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;

        match addr {
            EfsResourceAddress::FileSystem(region, fs_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_fs_str)) => {
                        // Create a new file system
                        let new_fs: FileSystem = RON.from_str(&new_fs_str)?;
                        Ok(vec![connector_op!(
                            EfsConnectorOp::CreateFileSystem(new_fs),
                            format!("Create new EFS file system '{}'", fs_id)
                        )])
                    }
                    (Some(_), None) => {
                        // Delete existing file system
                        Ok(vec![connector_op!(
                            EfsConnectorOp::DeleteFileSystem {
                                bypass_protection: false,
                            },
                            format!("Delete EFS file system '{}'", fs_id)
                        )])
                    }
                    (Some(old_fs_str), Some(new_fs_str)) => {
                        // Compare old and new file system to determine what needs to be updated
                        let old_fs: FileSystem = RON.from_str(&old_fs_str)?;
                        let new_fs: FileSystem = RON.from_str(&new_fs_str)?;
                        let mut ops = Vec::new();

                        // Check for throughput mode changes
                        if old_fs.throughput_mode != new_fs.throughput_mode
                            || old_fs.provisioned_throughput_in_mibps != new_fs.provisioned_throughput_in_mibps
                        {
                            ops.push(connector_op!(
                                EfsConnectorOp::UpdateFileSystemThroughput {
                                    throughput_mode: new_fs.throughput_mode.clone(),
                                    provisioned_throughput_in_mibps: new_fs.provisioned_throughput_in_mibps,
                                },
                                format!("Update throughput mode for EFS file system '{}'", fs_id)
                            ));
                        }

                        // Check for lifecycle policy changes
                        if old_fs.lifecycle_policies != new_fs.lifecycle_policies {
                            ops.push(connector_op!(
                                EfsConnectorOp::UpdateFileSystemLifecyclePolicies {
                                    lifecycle_policies: new_fs.lifecycle_policies.clone(),
                                },
                                format!("Update lifecycle policies for EFS file system '{}'", fs_id)
                            ));
                        }

                        // Check for file system protection changes
                        if old_fs.file_system_protection != new_fs.file_system_protection
                            && let Some(protection) = new_fs.file_system_protection.clone() {
                                ops.push(connector_op!(
                                    EfsConnectorOp::UpdateFileSystemProtection {
                                        file_system_protection: protection,
                                    },
                                    format!("Update protection settings for EFS file system '{}'", fs_id)
                                ));
                            }

                        // Check for tag changes
                        if old_fs.tags != new_fs.tags {
                            let diff = diff_ron_values(&old_fs.tags, &new_fs.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                EfsConnectorOp::UpdateFileSystemTags(old_fs.tags, new_fs.tags,),
                                format!("Update tags for EFS file system '{}'\n{}", fs_id, diff)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
            EfsResourceAddress::MountTarget(region, fs_id, mt_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_mt_str)) => {
                        // Create a new mount target
                        let new_mt: MountTarget = RON.from_str(&new_mt_str)?;
                        Ok(vec![connector_op!(
                            EfsConnectorOp::CreateMountTarget(new_mt),
                            format!("Create new mount target for EFS file system '{}'", fs_id)
                        )])
                    }
                    (Some(_), None) => {
                        // Delete existing mount target
                        Ok(vec![connector_op!(
                            EfsConnectorOp::DeleteMountTarget,
                            format!("Delete mount target '{}' from EFS file system '{}'", mt_id, fs_id)
                        )])
                    }
                    (Some(old_mt_str), Some(new_mt_str)) => {
                        // Compare old and new mount target to determine what needs to be updated
                        let old_mt: MountTarget = RON.from_str(&old_mt_str)?;
                        let new_mt: MountTarget = RON.from_str(&new_mt_str)?;
                        let mut ops = Vec::new();

                        // Check for security group changes
                        if old_mt.security_groups != new_mt.security_groups {
                            ops.push(connector_op!(
                                EfsConnectorOp::UpdateMountTargetSecurityGroups {
                                    security_groups: new_mt.security_groups.clone(),
                                },
                                format!(
                                    "Update security groups for mount target '{}' on EFS file system '{}'",
                                    mt_id, fs_id
                                )
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
            EfsResourceAddress::AccessPoint(region, fs_id, ap_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_ap_str)) => {
                        // Create a new access point
                        let new_ap: AccessPoint = RON.from_str(&new_ap_str)?;
                        Ok(vec![connector_op!(
                            EfsConnectorOp::CreateAccessPoint(new_ap),
                            format!("Create new access point for EFS file system '{}'", fs_id)
                        )])
                    }
                    (Some(_), None) => {
                        // Delete existing access point
                        Ok(vec![connector_op!(
                            EfsConnectorOp::DeleteAccessPoint,
                            format!("Delete access point '{}' from EFS file system '{}'", ap_id, fs_id)
                        )])
                    }
                    (Some(old_ap_str), Some(new_ap_str)) => {
                        // Compare old and new access point to determine what needs to be updated
                        let old_ap: AccessPoint = RON.from_str(&old_ap_str)?;
                        let new_ap: AccessPoint = RON.from_str(&new_ap_str)?;
                        let mut ops = Vec::new();

                        // Check for tag changes (only tags can be modified on access points)
                        if old_ap.tags != new_ap.tags {
                            let diff = diff_ron_values(&old_ap.tags, &new_ap.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                EfsConnectorOp::UpdateAccessPointTags(old_ap.tags, new_ap.tags,),
                                format!(
                                    "Update tags for access point '{}' on EFS file system '{}'\n{}",
                                    ap_id, fs_id, diff
                                )
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
        }
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = EfsResourceAddress::from_path(addr)?;
        let op = EfsConnectorOp::from_str(op)?;

        match addr {
            EfsResourceAddress::FileSystem(region, fs_id) => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    EfsConnectorOp::CreateFileSystem(file_system) => {
                        op_impl::create_file_system(&client, &file_system, &fs_id).await
                    }
                    EfsConnectorOp::UpdateFileSystemThroughput {
                        throughput_mode,
                        provisioned_throughput_in_mibps,
                    } => {
                        op_impl::update_file_system_throughput(
                            &client,
                            &fs_id,
                            &throughput_mode,
                            provisioned_throughput_in_mibps,
                        )
                        .await
                    }
                    EfsConnectorOp::UpdateFileSystemLifecyclePolicies { lifecycle_policies } => {
                        op_impl::update_file_system_lifecycle_policies(&client, &fs_id, lifecycle_policies).await
                    }
                    EfsConnectorOp::UpdateFileSystemProtection { file_system_protection } => {
                        op_impl::update_file_system_protection(&client, &fs_id, file_system_protection).await
                    }
                    EfsConnectorOp::UpdateFileSystemTags(old_tags, new_tags) => {
                        op_impl::update_file_system_tags(&client, &fs_id, &old_tags, &new_tags).await
                    }
                    EfsConnectorOp::DeleteFileSystem { bypass_protection } => {
                        op_impl::delete_file_system(&client, &fs_id, bypass_protection).await
                    }
                    op => bail!("Invalid operation for FileSystem resource: {:?}", op),
                }
            }
            EfsResourceAddress::MountTarget(region, fs_id, mt_id) => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    EfsConnectorOp::CreateMountTarget(mount_target) => {
                        op_impl::create_mount_target(&client, &mount_target, &fs_id).await
                    }
                    EfsConnectorOp::UpdateMountTargetSecurityGroups { security_groups } => {
                        op_impl::update_mount_target_security_groups(&client, &mt_id, security_groups).await
                    }
                    EfsConnectorOp::DeleteMountTarget => op_impl::delete_mount_target(&client, &mt_id).await,
                    op => bail!("Invalid operation for MountTarget resource: {:?}", op),
                }
            }
            EfsResourceAddress::AccessPoint(region, fs_id, ap_id) => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    EfsConnectorOp::CreateAccessPoint(access_point) => {
                        op_impl::create_access_point(&client, &access_point, &fs_id).await
                    }
                    EfsConnectorOp::UpdateAccessPointTags(old_tags, new_tags) => {
                        op_impl::update_access_point_tags(&client, &ap_id, &old_tags, &new_tags).await
                    }
                    EfsConnectorOp::DeleteAccessPoint => op_impl::delete_access_point(&client, &ap_id).await,
                    op => bail!("Invalid operation for AccessPoint resource: {:?}", op),
                }
            }
        }
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        // Add skeleton for a file system (general purpose)
        res.push(skeleton!(
            EfsResourceAddress::FileSystem(String::from("[region]"), String::from("fs-12345678")),
            EfsResource::FileSystem(FileSystem {
                throughput_mode: String::from("bursting"),
                provisioned_throughput_in_mibps: None,
                performance_mode: String::from("generalPurpose"),
                encrypted: true,
                kms_key_id: None,
                availability_zone_name: None,
                lifecycle_policies: vec![resource::LifecyclePolicy {
                    transition_to_ia: Some(String::from("AFTER_30_DAYS")),
                    transition_to_primary_storage_class: Some(String::from("AFTER_1_ACCESS")),
                }],
                file_system_protection: Some(resource::FileSystemProtection {
                    replication_overwrite_protection: Some(String::from("ENABLED")),
                }),
                tags: Tags::default(),
            })
        ));

        // Add skeleton for a mount target
        res.push(skeleton!(
            EfsResourceAddress::MountTarget(
                String::from("[region]"),
                String::from("fs-12345678"),
                String::from("fsmt-12345678")
            ),
            EfsResource::MountTarget(MountTarget {
                subnet_id: String::from("subnet-12345678"),
                security_groups: vec![String::from("sg-12345678")],
                ip_address: Some(String::from("10.0.1.100")),
                file_system_id: String::from("fs-12345678"),
            })
        ));

        // Add skeleton for an access point
        res.push(skeleton!(
            EfsResourceAddress::AccessPoint(
                String::from("[region]"),
                String::from("fs-12345678"),
                String::from("fsap-12345678")
            ),
            EfsResource::AccessPoint(AccessPoint {
                file_system_id: String::from("fs-12345678"),
                posix_user: Some(resource::PosixUser {
                    uid: 1000,
                    gid: 1000,
                    secondary_gids: Some(vec![100, 101]),
                }),
                root_directory: Some(resource::RootDirectory {
                    path: Some(String::from("/app")),
                    creation_info: Some(resource::CreationInfo {
                        owner_uid:   1000,
                        owner_gid:   1000,
                        permissions: String::from("0755"),
                    }),
                }),
                tags: Tags::default(),
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = EfsResourceAddress::from_path(addr)?;

        match addr {
            EfsResourceAddress::AccessPoint { .. } => ron_check_eq::<resource::AccessPoint>(a, b),
            EfsResourceAddress::MountTarget { .. } => ron_check_eq::<resource::MountTarget>(a, b),
            EfsResourceAddress::FileSystem { .. } => ron_check_eq::<resource::FileSystem>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = EfsResourceAddress::from_path(addr)?;

        match addr {
            EfsResourceAddress::AccessPoint { .. } => ron_check_syntax::<resource::AccessPoint>(a),
            EfsResourceAddress::MountTarget { .. } => ron_check_syntax::<resource::MountTarget>(a),
            EfsResourceAddress::FileSystem { .. } => ron_check_syntax::<resource::FileSystem>(a),
        }
    }
}
