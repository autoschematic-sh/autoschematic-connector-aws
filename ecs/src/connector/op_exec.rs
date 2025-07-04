use std::path::Path;

use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    error_util::invalid_op,
};

use crate::{addr::EcsResourceAddress, op::EcsConnectorOp, op_impl};

use super::EcsConnector;

impl EcsConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = EcsResourceAddress::from_path(addr)?;
        let op = EcsConnectorOp::from_str(op)?;
        let account_id = self.account_id.lock().await.clone();

        match &addr {
            EcsResourceAddress::Cluster(region, cluster_name) => match op {
                EcsConnectorOp::CreateCluster(cluster) => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::create_cluster(&client, &cluster, cluster_name).await
                }
                EcsConnectorOp::UpdateClusterTags(old_tags, new_tags) => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::update_cluster_tags(&client, cluster_name, &old_tags, &new_tags).await
                }
                EcsConnectorOp::UpdateClusterSettings { settings } => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::update_cluster_settings(&client, cluster_name, settings).await
                }
                EcsConnectorOp::UpdateClusterCapacityProviders {
                    add_capacity_providers,
                    remove_capacity_providers,
                    default_strategy,
                } => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::update_cluster_capacity_providers(
                        &client,
                        cluster_name,
                        add_capacity_providers,
                        remove_capacity_providers,
                        default_strategy,
                    )
                    .await
                }
                EcsConnectorOp::DeleteCluster => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::delete_cluster(&client, cluster_name).await
                }
                _ => Err(invalid_op(&addr, &op)),
            },
            EcsResourceAddress::Service(region, cluster_name, service_name) => match op {
                EcsConnectorOp::CreateService(service) => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::create_service(&client, cluster_name, &service, service_name).await
                }
                EcsConnectorOp::UpdateServiceTags(old_tags, new_tags) => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::update_service_tags(&client, cluster_name, service_name, &old_tags, &new_tags).await
                }
                EcsConnectorOp::UpdateServiceDesiredCount(desired_count) => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::update_service_desired_count(&client, cluster_name, service_name, desired_count).await
                }
                EcsConnectorOp::UpdateServiceTaskDefinition(task_definition) => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::update_service_task_definition(&client, cluster_name, service_name, &task_definition).await
                }
                EcsConnectorOp::UpdateServiceDeploymentConfiguration {
                    maximum_percent,
                    minimum_healthy_percent,
                    enable_circuit_breaker,
                    enable_rollback,
                } => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::update_service_deployment_configuration(
                        &client,
                        cluster_name,
                        service_name,
                        maximum_percent,
                        minimum_healthy_percent,
                        enable_circuit_breaker,
                        enable_rollback,
                    )
                    .await
                }
                EcsConnectorOp::UpdateServiceLoadBalancers {
                    old_load_balancers,
                    new_load_balancers,
                } => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::update_service_load_balancers(
                        &client,
                        cluster_name,
                        service_name,
                        old_load_balancers,
                        new_load_balancers,
                    )
                    .await
                }
                EcsConnectorOp::EnableExecuteCommand(enable) => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::enable_execute_command(&client, cluster_name, service_name, enable).await
                }
                EcsConnectorOp::DeleteService => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::delete_service(&client, cluster_name, service_name).await
                }
                _ => Err(invalid_op(&addr, &op)),
            },
            EcsResourceAddress::TaskDefinition(region, family) => match op {
                EcsConnectorOp::RegisterTaskDefinition(task_definition) => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::register_task_definition(&client, family, &task_definition).await
                }
                EcsConnectorOp::UpdateTaskDefinitionTags(old_tags, new_tags) => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::update_task_definition_tags(&client, family, &old_tags, &new_tags).await
                }
                EcsConnectorOp::DeregisterTaskDefinition => {
                    let client = self.get_or_init_client(region).await?;
                    op_impl::deregister_task_definition(&client, family).await
                }
                _ => Err(invalid_op(&addr, &op)),
            },
        }
    }
}
