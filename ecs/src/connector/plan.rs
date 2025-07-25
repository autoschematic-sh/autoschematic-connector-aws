use std::path::Path;

use autoschematic_core::{
    connector::{PlanResponseElement, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values, optional_string_from_utf8},
};

use autoschematic_core::connector::ConnectorOp;

use crate::{addr::EcsResourceAddress, op::EcsConnectorOp, resource};

use super::EcsConnector;

impl EcsConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        let addr = EcsResourceAddress::from_path(addr)?;

        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;

        match addr {
            EcsResourceAddress::Cluster(_region, cluster_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_cluster)) => {
                        let new_cluster: resource::Cluster = RON.from_str(&new_cluster)?;
                        Ok(vec![connector_op!(
                            EcsConnectorOp::CreateCluster(new_cluster),
                            format!("Create new ECS cluster {}", cluster_name)
                        )])
                    }
                    (Some(_old_cluster), None) => Ok(vec![connector_op!(
                        EcsConnectorOp::DeleteCluster,
                        format!("DELETE ECS cluster {}", cluster_name)
                    )]),
                    (Some(old_cluster), Some(new_cluster)) => {
                        let old_cluster: resource::Cluster = RON.from_str(&old_cluster)?;
                        let new_cluster: resource::Cluster = RON.from_str(&new_cluster)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_cluster.tags != new_cluster.tags {
                            let diff = diff_ron_values(&old_cluster.tags, &new_cluster.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                EcsConnectorOp::UpdateClusterTags(old_cluster.tags, new_cluster.tags),
                                format!("Modify tags for ECS cluster `{}`\n{}", cluster_name, diff)
                            ));
                        }
                        
                        // Check for settings changes
                        if old_cluster.settings != new_cluster.settings {
                            let mut new_settings = Vec::new();
                            for setting in &new_cluster.settings {
                                new_settings.push((setting.name.clone(), setting.value.clone()));
                            }

                            ops.push(connector_op!(
                                EcsConnectorOp::UpdateClusterSettings { settings: new_settings },
                                format!("Modify settings for ECS cluster `{}`", cluster_name)
                            ));
                        }

                        // Check for capacity provider changes
                        let old_providers = &old_cluster.capacity_providers;
                        let new_providers = &new_cluster.capacity_providers;

                        // Find providers to add and remove
                        let mut add_providers = Vec::new();
                        let mut remove_providers = Vec::new();

                        for provider in new_providers {
                            if !old_providers.contains(provider) {
                                add_providers.push(provider.clone());
                            }
                        }

                        for provider in old_providers {
                            if !new_providers.contains(provider) {
                                remove_providers.push(provider.clone());
                            }
                        }

                        // Check for default strategy changes
                        let old_strategy = &old_cluster.default_capacity_provider_strategy;
                        let new_strategy = &new_cluster.default_capacity_provider_strategy;

                        if !add_providers.is_empty() || !remove_providers.is_empty() || old_strategy != new_strategy {
                            // Create strategy entries in the format expected by the operation
                            let mut strategy_entries = Vec::new();
                            for s in new_strategy {
                                strategy_entries.push((s.capacity_provider.clone(), s.weight, s.base));
                            }

                            ops.push(connector_op!(
                                EcsConnectorOp::UpdateClusterCapacityProviders {
                                    add_capacity_providers: add_providers,
                                    remove_capacity_providers: remove_providers,
                                    default_strategy: strategy_entries,
                                },
                                format!("Modify capacity providers for ECS cluster `{}`", cluster_name)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
            EcsResourceAddress::Service(_region, cluster_name, service_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_service)) => {
                        let new_service: resource::Service = RON.from_str(&new_service)?;
                        Ok(vec![connector_op!(
                            EcsConnectorOp::CreateService(new_service),
                            format!("Create new ECS service {} in cluster {}", service_name, cluster_name)
                        )])
                    }
                    (Some(_old_service), None) => Ok(vec![connector_op!(
                        EcsConnectorOp::DeleteService,
                        format!("DELETE ECS service {} in cluster {}", service_name, cluster_name)
                    )]),
                    (Some(old_service), Some(new_service)) => {
                        let old_service: resource::Service = RON.from_str(&old_service)?;
                        let new_service: resource::Service = RON.from_str(&new_service)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_service.tags != new_service.tags {
                            let diff = diff_ron_values(&old_service.tags, &new_service.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                EcsConnectorOp::UpdateServiceTags(old_service.tags, new_service.tags),
                                format!(
                                    "Modify tags for ECS service `{}` in cluster `{}`\n{}",
                                    service_name, cluster_name, diff
                                )
                            ));
                        }

                        // Check for desired count changes
                        if old_service.desired_count != new_service.desired_count {
                            ops.push(connector_op!(
                                EcsConnectorOp::UpdateServiceDesiredCount(new_service.desired_count),
                                format!(
                                    "Update desired count to {} for ECS service `{}` in cluster `{}`",
                                    new_service.desired_count, service_name, cluster_name
                                )
                            ));
                        }

                        // Check for task definition changes
                        if old_service.task_definition != new_service.task_definition {
                            ops.push(connector_op!(
                                EcsConnectorOp::UpdateServiceTaskDefinition(new_service.task_definition),
                                format!(
                                    "Update task definition for ECS service `{}` in cluster `{}`",
                                    service_name, cluster_name
                                )
                            ));
                        }

                        // Check for deployment configuration changes
                        let old_deployment_config = old_service.deployment_configuration.as_ref();
                        let new_deployment_config = new_service.deployment_configuration.as_ref();


                        if old_deployment_config != new_deployment_config {
                            let circuit_breaker_enable = new_deployment_config
                                .and_then(|config| config.deployment_circuit_breaker.as_ref().map(|cb| cb.enable));
                            let circuit_breaker_rollback = new_deployment_config
                                .and_then(|config| config.deployment_circuit_breaker.as_ref().map(|cb| cb.rollback));

                            ops.push(connector_op!(
                                EcsConnectorOp::UpdateServiceDeploymentConfiguration {
                                    maximum_percent: new_deployment_config.and_then(|config| config.maximum_percent),
                                    minimum_healthy_percent: new_deployment_config
                                        .and_then(|config| config.minimum_healthy_percent),
                                    enable_circuit_breaker: circuit_breaker_enable,
                                    enable_rollback: circuit_breaker_rollback,
                                },
                                format!(
                                    "Update deployment configuration for ECS service `{}` in cluster `{}`",
                                    service_name, cluster_name
                                )
                            ));
                        }

                        // Check for execute command enablement changes
                        if old_service.enable_execute_command != new_service.enable_execute_command
                            && let Some(enable_execute_command) = new_service.enable_execute_command {
                                ops.push(connector_op!(
                                    EcsConnectorOp::EnableExecuteCommand(enable_execute_command),
                                    format!(
                                        "Set execute command to {} for ECS service `{}` in cluster `{}`",
                                        enable_execute_command, service_name, cluster_name
                                    )
                                ));
                            }
                        
                        // Check for load balancer changes
                        if old_service.load_balancers != new_service.load_balancers {
                            let diff = diff_ron_values(&old_service.load_balancers, &new_service.load_balancers).unwrap_or_default();
                            ops.push(connector_op!(
                                EcsConnectorOp::UpdateServiceLoadBalancers {
                                    old_load_balancers: old_service.load_balancers,
                                    new_load_balancers: new_service.load_balancers,
                                },
                                format!(
                                    "Update load balancers for ECS service `{}` in cluster `{}`\n{}",
                                    service_name, cluster_name, diff
                                )
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
            EcsResourceAddress::TaskDefinition(_region, task_def_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_task_def)) => {
                        let new_task_def: resource::TaskDefinition = RON.from_str(&new_task_def)?;
                        Ok(vec![connector_op!(
                            EcsConnectorOp::RegisterTaskDefinition(new_task_def),
                            format!("Register new ECS task definition {}", task_def_id)
                        )])
                    }
                    (Some(_old_task_def), None) => Ok(vec![connector_op!(
                        EcsConnectorOp::DeregisterTaskDefinition,
                        format!("Deregister ECS task definition {}", task_def_id)
                    )]),
                    (Some(old_task_def), Some(new_task_def)) => {
                        // Task definitions are immutable in ECS, so we can't update them
                        // Instead, we need to deregister the old one and register a new one
                        // For simplicity, we'll just check for tag changes here
                        let old_task_def: resource::TaskDefinition = RON.from_str(&old_task_def)?;
                        let new_task_def: resource::TaskDefinition = RON.from_str(&new_task_def)?;
                        let mut ops = Vec::new();

                        // Check for tag changes (though this would need a separate operation to update tags)
                        if let Ok(diff) = diff_ron_values(&old_task_def, &new_task_def)
                            && !diff.is_empty() {
                                ops.push(connector_op!(
                                    EcsConnectorOp::RegisterTaskDefinition(new_task_def),
                                    format!("Update ECS task definition {}\n{}", task_def_id, diff)
                                ));
                            }

                        Ok(ops)
                    }
                }
            }
        }
    }
}
