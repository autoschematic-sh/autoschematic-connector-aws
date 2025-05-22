use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};

use super::{
    resource::{Cluster, Service, TaskDefinition},
    tags::Tags,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum EcsConnectorOp {
    // Cluster operations
    CreateCluster(Cluster),
    UpdateClusterTags(Tags, Tags),
    UpdateClusterSettings {
        settings: Vec<(String, String)>,
    },
    UpdateClusterCapacityProviders {
        add_capacity_providers: Vec<String>,
        remove_capacity_providers: Vec<String>,
        default_strategy: Vec<(String, Option<i32>, Option<i32>)>, // (provider, weight, base)
    },
    DeleteCluster,

    // Service operations
    CreateService(Service),
    UpdateServiceTags(Tags, Tags),
    UpdateServiceDesiredCount(i32),
    UpdateServiceTaskDefinition(String),
    UpdateServiceDeploymentConfiguration {
        maximum_percent: Option<i32>,
        minimum_healthy_percent: Option<i32>,
        enable_circuit_breaker: Option<bool>,
        enable_rollback: Option<bool>,
    },
    EnableExecuteCommand(bool),
    DeleteService,

    // TaskDefinition operations
    RegisterTaskDefinition(TaskDefinition),
    UpdateTaskDefinitionTags(Tags, Tags),
    DeregisterTaskDefinition,

    // Task operations
    RunTask {
        cluster: String,
        task_definition: String,
        count: i32,
        launch_type: Option<String>,
        platform_version: Option<String>,
        network_configuration: Option<NetworkConfigurationRequest>,
        overrides: Option<TaskOverride>,
        tags: Tags,
    },
    StopTask {
        reason: Option<String>,
    },
    UpdateTaskTags(Tags, Tags),

    // ContainerInstance operations
    RegisterContainerInstance {
        cluster: String,
        instance_identity_document: String,
        attributes: Vec<(String, Option<String>)>,
        tags: Tags,
    },
    UpdateContainerInstanceAttributes {
        attributes: Vec<(String, Option<String>)>,
        remove_attributes: Vec<String>,
    },
    UpdateContainerInstanceTags(Tags, Tags),
    DeregisterContainerInstance {
        force: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkConfigurationRequest {
    pub subnets: Vec<String>,
    pub security_groups: Vec<String>,
    pub assign_public_ip: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskOverride {
    pub container_overrides: Vec<ContainerOverride>,
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub execution_role_arn: Option<String>,
    pub task_role_arn: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContainerOverride {
    pub name: String,
    pub command: Option<Vec<String>>,
    pub environment: Vec<(String, String)>,
    pub cpu: Option<i32>,
    pub memory: Option<i32>,
    pub memory_reservation: Option<i32>,
}

impl ConnectorOp for EcsConnectorOp {
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
