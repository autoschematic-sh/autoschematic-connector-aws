use autoschematic_core::{
    connector::{Resource, ResourceAddress},
    util::RON,
};
use serde::{Deserialize, Serialize};

use super::{addr::EcsResourceAddress, tags::Tags};

// Cluster resource definition
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Cluster {
    pub status: String,
    pub capacity_providers: Vec<String>,
    pub default_capacity_provider_strategy: Vec<CapacityProviderStrategyItem>,
    pub settings: Vec<ClusterSetting>,
    pub configuration: Option<ClusterConfiguration>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct CapacityProviderStrategyItem {
    pub capacity_provider: String,
    pub weight: Option<i32>,
    pub base: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ClusterSetting {
    pub name:  String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ClusterConfiguration {
    pub execute_command_configuration: Option<ExecuteCommandConfiguration>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ExecuteCommandConfiguration {
    pub kms_key_id: Option<String>,
    pub logging: Option<String>,
    pub log_configuration: Option<ExecuteCommandLogConfiguration>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ExecuteCommandLogConfiguration {
    pub cloud_watch_log_group_name: Option<String>,
    pub cloud_watch_encryption_enabled: Option<bool>,
    pub s3_bucket_name: Option<String>,
    pub s3_encryption_enabled: Option<bool>,
    pub s3_key_prefix: Option<String>,
}

// Service resource definition
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Service {
    pub task_definition: String,
    pub desired_count: i32,
    pub launch_type: Option<String>,
    pub capacity_provider_strategy: Vec<CapacityProviderStrategyItem>,
    pub platform_version: Option<String>,
    pub platform_family: Option<String>,
    pub deployment_configuration: Option<DeploymentConfiguration>,
    pub network_configuration: Option<NetworkConfiguration>,
    pub placement_constraints: Vec<PlacementConstraint>,
    pub placement_strategy: Vec<PlacementStrategy>,
    pub load_balancers: Vec<LoadBalancer>,
    pub service_registries: Vec<ServiceRegistry>,
    pub scheduling_strategy: Option<String>,
    pub enable_ecs_managed_tags: Option<bool>,
    pub propagate_tags: Option<String>,
    pub enable_execute_command: Option<bool>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct DeploymentConfiguration {
    pub deployment_circuit_breaker: Option<DeploymentCircuitBreaker>,
    pub maximum_percent: Option<i32>,
    pub minimum_healthy_percent: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct DeploymentCircuitBreaker {
    pub enable:   bool,
    pub rollback: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct NetworkConfiguration {
    pub awsvpc_configuration: Option<AwsVpcConfiguration>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct AwsVpcConfiguration {
    pub subnets: Vec<String>,
    pub security_groups: Vec<String>,
    pub assign_public_ip: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct PlacementConstraint {
    pub r#type:     String,
    pub expression: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct PlacementStrategy {
    pub r#type: String,
    pub field:  Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LoadBalancer {
    pub target_group_arn:   Option<String>,
    pub load_balancer_name: Option<String>,
    pub container_name:     Option<String>,
    pub container_port:     Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ServiceRegistry {
    pub registry_arn: Option<String>,
    pub port: Option<i32>,
    pub container_name: Option<String>,
    pub container_port: Option<i32>,
}

// TaskDefinition resource definition
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TaskDefinition {
    pub task_role_arn: Option<String>,
    pub execution_role_arn: Option<String>,
    pub network_mode: Option<String>,
    pub container_definitions: Vec<ContainerDefinition>,
    pub volumes: Vec<Volume>,
    pub placement_constraints: Vec<PlacementConstraint>,
    pub requires_compatibilities: Vec<String>,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub pid_mode: Option<String>,
    pub ipc_mode: Option<String>,
    pub proxy_configuration: Option<ProxyConfiguration>,
    pub runtime_platform: Option<RuntimePlatform>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ContainerDefinition {
    pub name: String,
    pub image: String,
    pub cpu: Option<i32>,
    pub memory: Option<i32>,
    pub memory_reservation: Option<i32>,
    pub links: Vec<String>,
    pub port_mappings: Vec<PortMapping>,
    pub essential: Option<bool>,
    pub entry_point: Vec<String>,
    pub command: Vec<String>,
    pub environment: Vec<KeyValuePair>,
    pub environment_files: Vec<EnvironmentFile>,
    pub mount_points: Vec<MountPoint>,
    pub volumes_from: Vec<VolumeFrom>,
    pub linux_parameters: Option<LinuxParameters>,
    pub secrets: Vec<Secret>,
    pub depends_on: Vec<ContainerDependency>,
    pub start_timeout: Option<i32>,
    pub stop_timeout: Option<i32>,
    pub hostname: Option<String>,
    pub user: Option<String>,
    pub working_directory: Option<String>,
    pub disable_networking: Option<bool>,
    pub privileged: Option<bool>,
    pub readonly_root_filesystem: Option<bool>,
    pub dns_servers: Vec<String>,
    pub dns_search_domains: Vec<String>,
    pub extra_hosts: Vec<HostEntry>,
    pub docker_security_options: Vec<String>,
    pub interactive: Option<bool>,
    pub pseudo_terminal: Option<bool>,
    pub docker_labels: std::collections::HashMap<String, String>,
    pub ulimits: Vec<Ulimit>,
    pub log_configuration: Option<LogConfiguration>,
    pub health_check: Option<HealthCheck>,
    pub system_controls: Vec<SystemControl>,
    pub resource_requirements: Vec<ResourceRequirement>,
    pub firelens_configuration: Option<FirelensConfiguration>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct PortMapping {
    pub container_port: Option<i32>,
    pub host_port: Option<i32>,
    pub protocol: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct KeyValuePair {
    pub name:  Option<String>,
    pub value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct EnvironmentFile {
    pub value:  String,
    pub r#type: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct MountPoint {
    pub source_volume:  Option<String>,
    pub container_path: Option<String>,
    pub read_only:      Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct VolumeFrom {
    pub source_container: Option<String>,
    pub read_only: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LinuxParameters {
    pub capabilities: Option<KernelCapabilities>,
    pub devices: Vec<Device>,
    pub init_process_enabled: Option<bool>,
    pub shared_memory_size: Option<i32>,
    pub tmpfs: Vec<Tmpfs>,
    pub max_swap: Option<i32>,
    pub swappiness: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct KernelCapabilities {
    pub add:  Vec<String>,
    pub drop: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Device {
    pub host_path:      String,
    pub container_path: Option<String>,
    pub permissions:    Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Tmpfs {
    pub container_path: String,
    pub size: i32,
    pub mount_options: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Secret {
    pub name: String,
    pub value_from: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ContainerDependency {
    pub container_name: String,
    pub condition:      String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct HostEntry {
    pub hostname:   String,
    pub ip_address: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Ulimit {
    pub name: String,
    pub soft_limit: i32,
    pub hard_limit: i32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LogConfiguration {
    pub log_driver: String,
    pub options: std::collections::HashMap<String, String>,
    pub secret_options: Vec<Secret>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct HealthCheck {
    pub command:      Vec<String>,
    pub interval:     Option<i32>,
    pub timeout:      Option<i32>,
    pub retries:      Option<i32>,
    pub start_period: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SystemControl {
    pub namespace: Option<String>,
    pub value:     Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ResourceRequirement {
    pub value:  String,
    pub r#type: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct FirelensConfiguration {
    pub r#type:  String,
    pub options: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Volume {
    pub name: String,
    pub host: Option<HostVolumeProperties>,
    pub docker_volume_configuration: Option<DockerVolumeConfiguration>,
    pub efs_volume_configuration: Option<EfsVolumeConfiguration>,
    pub fsx_windows_file_server_volume_configuration: Option<FsxWindowsFileServerVolumeConfiguration>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct HostVolumeProperties {
    pub source_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct DockerVolumeConfiguration {
    pub scope: Option<String>,
    pub autoprovision: Option<bool>,
    pub driver: Option<String>,
    pub driver_opts: std::collections::HashMap<String, String>,
    pub labels: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct EfsVolumeConfiguration {
    pub file_system_id: String,
    pub root_directory: Option<String>,
    pub transit_encryption: Option<String>,
    pub transit_encryption_port: Option<i32>,
    pub authorization_config: Option<EfsAuthorizationConfig>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct EfsAuthorizationConfig {
    pub iam: Option<String>,
    pub access_point_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct FsxWindowsFileServerVolumeConfiguration {
    pub file_system_id: String,
    pub root_directory: String,
    pub authorization_config: FsxWindowsFileServerAuthorizationConfig,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct FsxWindowsFileServerAuthorizationConfig {
    pub credentials_parameter: Option<String>,
    pub domain: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ProxyConfiguration {
    pub r#type: Option<String>,
    pub container_name: String,
    pub properties: Vec<KeyValuePair>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct RuntimePlatform {
    pub cpu_architecture: Option<String>,
    pub operating_system_family: Option<String>,
}

// Task resource definition
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Task {
    pub task_definition_arn: String,
    pub containers: Vec<Container>,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub last_status: String,
    pub desired_status: String,
    pub connectivity: Option<String>,
    pub connectivity_at: Option<String>,
    pub pull_started_at: Option<String>,
    pub pull_stopped_at: Option<String>,
    pub execution_stopped_at: Option<String>,
    pub launch_type: Option<String>,
    pub capacity_provider_name: Option<String>,
    pub platform_version: Option<String>,
    pub platform_family: Option<String>,
    pub attachments: Vec<Attachment>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Container {
    pub container_arn: Option<String>,
    pub task_arn: Option<String>,
    pub name: Option<String>,
    pub image: Option<String>,
    pub image_digest: Option<String>,
    pub runtime_id: Option<String>,
    pub last_status: Option<String>,
    pub exit_code: Option<i32>,
    pub reason: Option<String>,
    pub network_bindings: Vec<NetworkBinding>,
    pub network_interfaces: Vec<NetworkInterface>,
    pub health_status: Option<String>,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub memory_reservation: Option<String>,
    pub gpu_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct NetworkBinding {
    pub bind_ip: Option<String>,
    pub container_port: Option<i32>,
    pub host_port: Option<i32>,
    pub protocol: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct NetworkInterface {
    pub attachment_id: Option<String>,
    pub private_ipv4_address: Option<String>,
    pub ipv6_address: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Attachment {
    pub id:      String,
    pub r#type:  String,
    pub status:  String,
    pub details: Vec<KeyValuePair>,
}

// ContainerInstance resource definition
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ContainerInstance {
    pub ec2_instance_id: Option<String>,
    pub capacity_provider_name: Option<String>,
    pub version: i64,
    pub version_info: Option<VersionInfo>,
    pub remaining_resources: Vec<EcsContainerResource>,
    pub registered_resources: Vec<EcsContainerResource>,
    pub status: Option<String>,
    pub status_reason: Option<String>,
    pub agent_connected: bool,
    pub running_tasks_count: i32,
    pub pending_tasks_count: i32,
    pub agent_update_status: Option<String>,
    pub attributes: Vec<Attribute>,
    pub attachments: Vec<Attachment>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct VersionInfo {
    pub agent_version:  Option<String>,
    pub agent_hash:     Option<String>,
    pub docker_version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Attribute {
    pub name: String,
    pub value: Option<String>,
    pub target_type: Option<String>,
    pub target_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct EcsContainerResource {
    pub name: String,
    pub r#type: Option<String>,
    pub double_value: f64,
    pub long_value: i64,
    pub integer_value: i32,
    pub string_value: String,
}

// Enum for ECS resources
pub enum EcsResource {
    Cluster(Cluster),
    Service(Service),
    TaskDefinition(TaskDefinition),
    Task(Task),
    ContainerInstance(ContainerInstance),
}

// Implementation of Resource trait for EcsResource
impl Resource for EcsResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default().struct_names(true);
        match self {
            EcsResource::Cluster(cluster) => Ok(RON.to_string_pretty(&cluster, pretty_config)?.into()),
            EcsResource::Service(service) => Ok(RON.to_string_pretty(&service, pretty_config)?.into()),
            EcsResource::TaskDefinition(task_definition) => Ok(RON.to_string_pretty(&task_definition, pretty_config)?.into()),
            EcsResource::Task(task) => Ok(RON.to_string_pretty(&task, pretty_config)?.into()),
            EcsResource::ContainerInstance(container_instance) => {
                Ok(RON.to_string_pretty(&container_instance, pretty_config)?.into())
            }
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = EcsResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;

        match addr {
            EcsResourceAddress::Cluster(region, _name) => Ok(EcsResource::Cluster(RON.from_str(s)?)),
            EcsResourceAddress::Service(region, _cluster_name, _service_name) => Ok(EcsResource::Service(RON.from_str(s)?)),
            EcsResourceAddress::TaskDefinition(region, _task_def_id) => Ok(EcsResource::TaskDefinition(RON.from_str(s)?)),
            EcsResourceAddress::Task(region, _cluster_name, _task_id) => Ok(EcsResource::Task(RON.from_str(s)?)),
            EcsResourceAddress::ContainerInstance(region, _cluster_name, _container_instance_id) => {
                Ok(EcsResource::ContainerInstance(RON.from_str(s)?))
            }
        }
    }
}
