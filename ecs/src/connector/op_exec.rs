use std::{collections::HashMap, path::Path};

use anyhow::bail;
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    error::{AutoschematicError, AutoschematicErrorType},
    error_util::{invalid_addr, invalid_op},
    op_exec_output,
};

use crate::{addr::EcsResourceAddress, op::EcsConnectorOp, op_impl};

use super::EcsConnector;

impl EcsConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = EcsResourceAddress::from_path(addr)?;
        let op = EcsConnectorOp::from_str(op)?;

        match &addr {
            EcsResourceAddress::Cluster(region, cluster_name) => {
                match op {
                    EcsConnectorOp::CreateCluster(cluster) => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::create_cluster(&client, &cluster, &cluster_name).await
                    }
                    EcsConnectorOp::UpdateClusterTags(old_tags, new_tags) => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::update_cluster_tags(&client, &cluster_name, &old_tags, &new_tags).await
                    }
                    EcsConnectorOp::UpdateClusterSettings { settings } => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::update_cluster_settings(&client, &cluster_name, settings).await
                    }
                    EcsConnectorOp::UpdateClusterCapacityProviders {
                        add_capacity_providers,
                        remove_capacity_providers,
                        default_strategy,
                    } => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::update_cluster_capacity_providers(
                            &client,
                            &cluster_name,
                            add_capacity_providers,
                            remove_capacity_providers,
                            default_strategy,
                        )
                        .await
                    }
                    EcsConnectorOp::DeleteCluster => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::delete_cluster(&client, &cluster_name).await
                    }
                    _ => return Err(invalid_op(&addr, &op)),
                }
            }
            EcsResourceAddress::Service(region, cluster_name, service_name) => {
                match op {
                    EcsConnectorOp::CreateService(service) => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::create_service(&client, &cluster_name, &service, &service_name).await
                    }
                    EcsConnectorOp::UpdateServiceTags(old_tags, new_tags) => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::update_service_tags(&client, &cluster_name, &service_name, &old_tags, &new_tags).await
                    }
                    EcsConnectorOp::UpdateServiceDesiredCount(desired_count) => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::update_service_desired_count(&client, &cluster_name, &service_name, desired_count).await
                    }
                    EcsConnectorOp::UpdateServiceTaskDefinition(task_definition) => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::update_service_task_definition(&client, &cluster_name, &service_name, &task_definition).await
                    }
                    EcsConnectorOp::UpdateServiceDeploymentConfiguration {
                        maximum_percent,
                        minimum_healthy_percent,
                        enable_circuit_breaker,
                        enable_rollback,
                    } => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::update_service_deployment_configuration(
                            &client,
                            &cluster_name,
                            &service_name,
                            maximum_percent,
                            minimum_healthy_percent,
                            enable_circuit_breaker,
                            enable_rollback,
                        )
                        .await
                    }
                    EcsConnectorOp::EnableExecuteCommand(enable) => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::enable_execute_command(&client, &cluster_name, &service_name, enable).await
                    }
                    EcsConnectorOp::DeleteService => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::delete_service(&client, &cluster_name, &service_name).await
                    }
                    _ => return Err(invalid_op(&addr, &op)),
                }
            }
            EcsResourceAddress::TaskDefinition(region, family) => {
                match op {
                    EcsConnectorOp::RegisterTaskDefinition(task_definition) => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::register_task_definition(&client, &family, &task_definition).await
                    }
                    EcsConnectorOp::UpdateTaskDefinitionTags(old_tags, new_tags) => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::update_task_definition_tags(&client, &family, &old_tags, &new_tags).await
                    }
                    EcsConnectorOp::DeregisterTaskDefinition => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::deregister_task_definition(&client, &family).await
                    }
                    _ => return Err(invalid_op(&addr, &op)),
                }
            }
            EcsResourceAddress::Task(region, cluster_name, task_id) => {
                match op {
                    EcsConnectorOp::RunTask {
                        cluster: _, // We acknowledge the field but don't need it for the function call
                        task_definition,
                        count,
                        launch_type,
                        platform_version,
                        network_configuration,
                        overrides,
                        tags,
                    } => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::run_task(
                            &client,
                            &cluster_name, // Use the address cluster_name instead of the op param
                            &task_definition,
                            count,
                            launch_type,
                            platform_version,
                            network_configuration,
                            overrides,
                            &tags,
                        )
                        .await
                    }
                    EcsConnectorOp::StopTask { reason } => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::stop_task(&client, &cluster_name, &task_id, reason).await
                    }
                    EcsConnectorOp::UpdateTaskTags(old_tags, new_tags) => {
                        let client = self.get_or_init_client(&region).await?;
                        // We need the full ARN for task tags
                        let task_arn = format!("arn:aws:ecs:{}:{}:task/{}/{}", region, self.account_id, cluster_name, task_id);
                        op_impl::update_task_tags(&client, &task_arn, &old_tags, &new_tags).await
                    }
                    _ => return Err(invalid_op(&addr, &op)),
                }
            }
            EcsResourceAddress::ContainerInstance(region, cluster_name, container_instance_id) => {
                match op {
                    EcsConnectorOp::RegisterContainerInstance {
                        cluster: _, // We acknowledge the field but don't need it for the function call
                        instance_identity_document,
                        attributes,
                        tags,
                    } => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::register_container_instance(
                            &client,
                            &cluster_name,
                            &instance_identity_document,
                            attributes,
                            &tags,
                        )
                        .await
                    }
                    EcsConnectorOp::UpdateContainerInstanceAttributes {
                        attributes,
                        remove_attributes,
                    } => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::update_container_instance_attributes(
                            &client,
                            &cluster_name,
                            &container_instance_id,
                            attributes,
                            remove_attributes,
                        )
                        .await
                    }
                    EcsConnectorOp::UpdateContainerInstanceTags(old_tags, new_tags) => {
                        let client = self.get_or_init_client(&region).await?;
                        // We need the full ARN for container instance tags
                        let container_instance_arn = format!(
                            "arn:aws:ecs:{}:{}:container-instance/{}/{}",
                            region, self.account_id, cluster_name, container_instance_id
                        );
                        op_impl::update_container_instance_tags(&client, &container_instance_arn, &old_tags, &new_tags).await
                    }
                    EcsConnectorOp::DeregisterContainerInstance { force } => {
                        let client = self.get_or_init_client(&region).await?;
                        op_impl::deregister_container_instance(&client, &cluster_name, &container_instance_id, force).await
                    }
                    _ => return Err(invalid_op(&addr, &op)),
                }
            }
        }
    }
}
