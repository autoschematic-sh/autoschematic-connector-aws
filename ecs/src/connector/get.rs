use std::{collections::HashMap, path::Path};

use autoschematic_core::{
    connector::{GetResourceOutput, Resource, ResourceAddress},
    get_resource_output,
};

use anyhow::Context;

use crate::{
    addr::EcsResourceAddress,
    resource::{self, EcsResource},
    tags,
};

use crate::util;

use super::EcsConnector;

impl EcsConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = EcsResourceAddress::from_path(addr)?;
        match addr {
            EcsResourceAddress::Cluster(region, cluster_name) => {
                let client = self.get_or_init_client(&region).await?;
                let cluster = util::get_cluster(&client, &cluster_name).await?;

                if let Some(cluster) = cluster {
                    // Convert AWS SDK cluster to our internal representation
                    let our_cluster = resource::Cluster {
                        status: cluster.status().map(|s| s.to_string()).unwrap_or_default(),
                        capacity_providers: cluster.capacity_providers().to_vec(),
                        default_capacity_provider_strategy: cluster
                            .default_capacity_provider_strategy()
                            .iter()
                            .map(|s| {
                                resource::CapacityProviderStrategyItem {
                                    capacity_provider: s.capacity_provider().to_string(),
                                    weight: Some(s.weight),
                                    base: Some(s.base),
                                }
                            })
                            .collect(),
                        settings: cluster
                            .settings()
                            .iter()
                            .map(|s| {
                                resource::ClusterSetting {
                                    name: s.name().map(|n| n.as_str().to_string()).unwrap_or_default(),
                                    value: s.value().unwrap_or_default().to_string(),
                                }
                            })
                            .collect(),
                        configuration: cluster.configuration().map(|c| {
                            resource::ClusterConfiguration {
                                execute_command_configuration: c.execute_command_configuration().map(|e| {
                                    resource::ExecuteCommandConfiguration {
                                        kms_key_id: e.kms_key_id().map(|k| k.to_string()),
                                        logging: e.logging().map(|l| l.as_str().to_string()),
                                        log_configuration: e.log_configuration().map(|lc| {
                                            resource::ExecuteCommandLogConfiguration {
                                                cloud_watch_log_group_name: lc
                                                    .cloud_watch_log_group_name()
                                                    .map(|c| c.to_string()),
                                                cloud_watch_encryption_enabled: Some(lc.cloud_watch_encryption_enabled),
                                                s3_bucket_name: lc.s3_bucket_name().map(|s| s.to_string()),
                                                s3_encryption_enabled: Some(lc.s3_encryption_enabled),
                                                s3_key_prefix: lc.s3_key_prefix().map(|s| s.to_string()),
                                            }
                                        }),
                                    }
                                }),
                            }
                        }),
                        tags: tags::Tags::from(cluster.tags()),
                    };

                    return get_resource_output!(
                        EcsResource::Cluster(our_cluster),
                        [(String::from("cluster_name"), cluster_name)]
                    );
                }

                Ok(None)
            }
            EcsResourceAddress::Service(region, cluster_name, service_name) => {
                let client = self.get_or_init_client(&region).await?;
                let service = util::get_service(&client, &cluster_name, &service_name).await?;

                if let Some(service) = service {
                    // Convert AWS SDK service to our internal representation
                    let our_service = resource::Service {
                        task_definition: service.task_definition().unwrap_or_default().to_string(),
                        desired_count: service.desired_count,
                        launch_type: service.launch_type().map(|lt| lt.as_str().to_string()),
                        capacity_provider_strategy: service
                            .capacity_provider_strategy()
                            .iter()
                            .map(|s| {
                                resource::CapacityProviderStrategyItem {
                                    capacity_provider: s.capacity_provider().to_string(),
                                    weight: Some(s.weight),
                                    base: Some(s.base),
                                }
                            })
                            .collect(),
                        platform_version: service.platform_version().map(|p| p.to_string()),
                        platform_family: service.platform_family().map(|p| p.to_string()),
                        deployment_configuration: service.deployment_configuration().map(|dc| {
                            resource::DeploymentConfiguration {
                                deployment_circuit_breaker: dc.deployment_circuit_breaker().map(|cb| {
                                    resource::DeploymentCircuitBreaker {
                                        enable: cb.enable,
                                        rollback: cb.rollback,
                                    }
                                }),
                                maximum_percent: dc.maximum_percent,
                                minimum_healthy_percent: dc.minimum_healthy_percent,
                            }
                        }),
                        network_configuration: service.network_configuration().map(|nc| {
                            resource::NetworkConfiguration {
                                awsvpc_configuration: nc.awsvpc_configuration().map(|vpc| {
                                    resource::AwsVpcConfiguration {
                                        subnets: vpc.subnets().to_vec(),
                                        security_groups: vpc.security_groups().to_vec(),
                                        assign_public_ip: vpc.assign_public_ip().map(|p| p.as_str().to_string()),
                                    }
                                }),
                            }
                        }),
                        placement_constraints: service
                            .placement_constraints()
                            .iter()
                            .map(|pc| {
                                resource::PlacementConstraint {
                                    r#type: pc.r#type().map(|t| t.as_str().to_string()).unwrap_or_default(),
                                    expression: pc.expression().map(|e| e.to_string()),
                                }
                            })
                            .collect(),
                        placement_strategy: service
                            .placement_strategy()
                            .iter()
                            .map(|ps| {
                                resource::PlacementStrategy {
                                    r#type: ps.r#type().map(|t| t.as_str().to_string()).unwrap_or_default(),
                                    field: ps.field().map(|f| f.to_string()),
                                }
                            })
                            .collect(),
                        load_balancers: service
                            .load_balancers()
                            .iter()
                            .map(|lb| {
                                resource::LoadBalancer {
                                    target_group_arn: lb.target_group_arn().map(|tg| tg.to_string()),
                                    load_balancer_name: lb.load_balancer_name().map(|ln| ln.to_string()),
                                    container_name: lb.container_name().map(|cn| cn.to_string()),
                                    container_port: lb.container_port,
                                }
                            })
                            .collect(),
                        service_registries: service
                            .service_registries()
                            .iter()
                            .map(|sr| {
                                resource::ServiceRegistry {
                                    registry_arn: sr.registry_arn().map(|ra| ra.to_string()),
                                    port: sr.port,
                                    container_name: sr.container_name().map(|cn| cn.to_string()),
                                    container_port: sr.container_port,
                                }
                            })
                            .collect(),
                        scheduling_strategy: service.scheduling_strategy().map(|ss| ss.as_str().to_string()),
                        enable_ecs_managed_tags: Some(service.enable_ecs_managed_tags),
                        propagate_tags: service.propagate_tags().map(|pt| pt.as_str().to_string()),
                        enable_execute_command: Some(service.enable_execute_command),
                        tags: tags::Tags::from(service.tags()),
                    };

                    return get_resource_output!(
                        EcsResource::Service(our_service),
                        [
                            (String::from("cluster_name"), cluster_name),
                            (String::from("service_name"), service_name)
                        ]
                    );
                }

                Ok(None)
            }
            EcsResourceAddress::TaskDefinition(region, family) => {
                let client = self.get_or_init_client(&region).await?;
                let task_def = util::get_task_definition(&client, &family).await;

                if let Ok(Some(task_def)) = task_def {
                    // Convert AWS SDK task definition to our internal representation
                    let our_task_def = resource::TaskDefinition {
                        task_role_arn: task_def.task_role_arn().map(|t| t.to_string()),
                        execution_role_arn: task_def.execution_role_arn().map(|e| e.to_string()),
                        network_mode: task_def.network_mode().map(|n| n.as_str().to_string()),
                        container_definitions: task_def
                            .container_definitions()
                            .iter()
                            .map(|cd| {
                                // Container definition conversion is complex, simplified here
                                resource::ContainerDefinition {
                                    name: cd.name().unwrap_or_default().to_string(),
                                    image: cd.image().unwrap_or_default().to_string(),
                                    cpu: Some(cd.cpu),
                                    memory: cd.memory,
                                    memory_reservation: cd.memory_reservation,
                                    links: cd.links().to_vec(),
                                    port_mappings: cd
                                        .port_mappings()
                                        .iter()
                                        .map(|pm| {
                                            resource::PortMapping {
                                                container_port: pm.container_port,
                                                host_port: pm.host_port,
                                                protocol: pm.protocol().map(|p| p.as_str().to_string()),
                                            }
                                        })
                                        .collect(),
                                    essential: cd.essential,
                                    entry_point: cd.entry_point().to_vec(),
                                    command: cd.command().to_vec(),
                                    environment: cd
                                        .environment()
                                        .iter()
                                        .map(|e| {
                                            resource::KeyValuePair {
                                                name: e.name().map(|n| n.to_string()),
                                                value: e.value().map(|v| v.to_string()),
                                            }
                                        })
                                        .collect(),
                                    environment_files: cd
                                        .environment_files()
                                        .iter()
                                        .map(|ef| {
                                            resource::EnvironmentFile {
                                                value: ef.value().to_string(),
                                                r#type: ef.r#type().to_string(),
                                            }
                                        })
                                        .collect(),
                                    mount_points: cd
                                        .mount_points()
                                        .iter()
                                        .map(|mp| {
                                            resource::MountPoint {
                                                source_volume: mp.source_volume().map(|sv| sv.to_string()),
                                                container_path: mp.container_path().map(|cp| cp.to_string()),
                                                read_only: mp.read_only,
                                            }
                                        })
                                        .collect(),
                                    volumes_from: cd
                                        .volumes_from()
                                        .iter()
                                        .map(|vf| {
                                            resource::VolumeFrom {
                                                source_container: vf.source_container().map(|sc| sc.to_string()),
                                                read_only: vf.read_only,
                                            }
                                        })
                                        .collect(),
                                    // Other fields omitted for brevity
                                    linux_parameters: cd.linux_parameters().map(|lp| {
                                        resource::LinuxParameters {
                                            capabilities: lp.capabilities().map(|c| {
                                                resource::KernelCapabilities {
                                                    add: c.add().to_vec(),
                                                    drop: c.drop().to_vec(),
                                                }
                                            }),
                                            devices: lp
                                                .devices()
                                                .iter()
                                                .map(|d| {
                                                    resource::Device {
                                                        host_path: d.host_path().to_string(),
                                                        container_path: d.container_path().map(|cp| cp.to_string()),
                                                        permissions: d
                                                            .permissions()
                                                            .iter()
                                                            .map(|p| p.as_str().to_string())
                                                            .collect(),
                                                    }
                                                })
                                                .collect(),
                                            init_process_enabled: lp.init_process_enabled,
                                            shared_memory_size: lp.shared_memory_size,
                                            tmpfs: lp
                                                .tmpfs()
                                                .iter()
                                                .map(|t| {
                                                    resource::Tmpfs {
                                                        container_path: t.container_path().to_string(),
                                                        size: t.size,
                                                        mount_options: t.mount_options().to_vec(),
                                                    }
                                                })
                                                .collect(),
                                            max_swap: lp.max_swap,
                                            swappiness: lp.swappiness,
                                        }
                                    }),
                                    secrets: cd
                                        .secrets()
                                        .iter()
                                        .map(|s| {
                                            resource::Secret {
                                                name: s.name().to_string(),
                                                value_from: s.value_from().to_string(),
                                            }
                                        })
                                        .collect(),
                                    depends_on: cd
                                        .depends_on()
                                        .iter()
                                        .map(|d| {
                                            resource::ContainerDependency {
                                                container_name: d.container_name().to_string(),
                                                condition: d.condition().to_string(),
                                            }
                                        })
                                        .collect(),
                                    start_timeout: cd.start_timeout,
                                    stop_timeout: cd.stop_timeout,
                                    hostname: cd.hostname().map(|h| h.to_string()),
                                    user: cd.user().map(|u| u.to_string()),
                                    working_directory: cd.working_directory().map(|w| w.to_string()),
                                    disable_networking: cd.disable_networking,
                                    privileged: cd.privileged,
                                    readonly_root_filesystem: cd.readonly_root_filesystem,
                                    dns_servers: cd.dns_servers().to_vec(),
                                    dns_search_domains: cd.dns_search_domains().to_vec(),
                                    extra_hosts: cd
                                        .extra_hosts()
                                        .iter()
                                        .map(|eh| {
                                            resource::HostEntry {
                                                hostname: eh.hostname().to_string(),
                                                ip_address: eh.ip_address().to_string(),
                                            }
                                        })
                                        .collect(),
                                    docker_security_options: cd.docker_security_options().to_vec(),
                                    interactive: cd.interactive,
                                    pseudo_terminal: cd.pseudo_terminal,
                                    docker_labels: cd.docker_labels().unwrap_or(&HashMap::default()).clone(),
                                    ulimits: cd
                                        .ulimits()
                                        .iter()
                                        .map(|u| {
                                            resource::Ulimit {
                                                name: u.name().to_string(),
                                                soft_limit: u.soft_limit,
                                                hard_limit: u.hard_limit,
                                            }
                                        })
                                        .collect(),
                                    log_configuration: cd.log_configuration().map(|lc| {
                                        resource::LogConfiguration {
                                            log_driver: lc.log_driver().to_string(),
                                            options: lc.options().unwrap_or(&HashMap::default()).clone(),
                                            secret_options: lc
                                                .secret_options()
                                                .iter()
                                                .map(|so| {
                                                    resource::Secret {
                                                        name: so.name().to_string(),
                                                        value_from: so.value_from().to_string(),
                                                    }
                                                })
                                                .collect(),
                                        }
                                    }),
                                    health_check: cd.health_check().map(|hc| {
                                        resource::HealthCheck {
                                            command: hc.command().to_vec(),
                                            interval: hc.interval,
                                            timeout: hc.timeout,
                                            retries: hc.retries,
                                            start_period: hc.start_period,
                                        }
                                    }),
                                    system_controls: cd
                                        .system_controls()
                                        .iter()
                                        .map(|sc| {
                                            resource::SystemControl {
                                                namespace: sc.namespace().map(|n| n.to_string()),
                                                value: sc.value().map(|v| v.to_string()),
                                            }
                                        })
                                        .collect(),
                                    resource_requirements: cd
                                        .resource_requirements()
                                        .iter()
                                        .map(|rr| {
                                            resource::ResourceRequirement {
                                                value: rr.value().to_string(),
                                                r#type: rr.r#type().to_string(),
                                            }
                                        })
                                        .collect(),
                                    firelens_configuration: cd.firelens_configuration().map(|fc| {
                                        resource::FirelensConfiguration {
                                            r#type: fc.r#type().to_string(),
                                            options: fc.options().unwrap_or(&HashMap::default()).clone(),
                                        }
                                    }),
                                }
                            })
                            .collect(),
                        volumes: task_def
                            .volumes()
                            .iter()
                            .map(|v| {
                                // Volume conversion simplified
                                resource::Volume {
                                    name: v.name().unwrap_or_default().to_string(),
                                    host: v.host().map(|h| {
                                        resource::HostVolumeProperties {
                                            source_path: h.source_path().map(|sp| sp.to_string()),
                                        }
                                    }),
                                    docker_volume_configuration: v.docker_volume_configuration().map(|dvc| {
                                        resource::DockerVolumeConfiguration {
                                            scope: dvc.scope().map(|s| s.as_str().to_string()),
                                            autoprovision: dvc.autoprovision,
                                            driver: dvc.driver().map(|d| d.to_string()),
                                            driver_opts: dvc.driver_opts().unwrap_or(&HashMap::default()).clone(),
                                            labels: dvc.labels().unwrap_or(&HashMap::default()).clone(),
                                        }
                                    }),
                                    efs_volume_configuration: v.efs_volume_configuration().map(|evc| {
                                        resource::EfsVolumeConfiguration {
                                            file_system_id: evc.file_system_id().to_string(),
                                            root_directory: evc.root_directory().map(|rd| rd.to_string()),
                                            transit_encryption: evc.transit_encryption().map(|te| te.as_str().to_string()),
                                            transit_encryption_port: evc.transit_encryption_port,
                                            authorization_config: evc.authorization_config().map(|ac| {
                                                resource::EfsAuthorizationConfig {
                                                    iam: ac.iam().map(|i| i.as_str().to_string()),
                                                    access_point_id: ac.access_point_id().map(|api| api.to_string()),
                                                }
                                            }),
                                        }
                                    }),
                                    fsx_windows_file_server_volume_configuration: v
                                        .fsx_windows_file_server_volume_configuration()
                                        .map(|fvc| {
                                            let auth_config = fvc.authorization_config();
                                            resource::FsxWindowsFileServerVolumeConfiguration {
                                                file_system_id: fvc.file_system_id().to_string(),
                                                root_directory: fvc.root_directory().to_string(),
                                                authorization_config: resource::FsxWindowsFileServerAuthorizationConfig {
                                                    credentials_parameter: auth_config
                                                        .map(|a| a.credentials_parameter().to_string()),
                                                    domain: auth_config.map(|a| a.domain().to_string()),
                                                },
                                            }
                                        }),
                                }
                            })
                            .collect(),
                        placement_constraints: task_def
                            .placement_constraints()
                            .iter()
                            .map(|pc| {
                                resource::PlacementConstraint {
                                    r#type: pc.r#type().map(|t| t.as_str().to_string()).unwrap_or_default(),
                                    expression: pc.expression().map(|e| e.to_string()),
                                }
                            })
                            .collect(),
                        requires_compatibilities: task_def
                            .requires_compatibilities()
                            .iter()
                            .map(|rc| rc.as_str().to_string())
                            .collect(),
                        cpu: task_def.cpu().map(|c| c.to_string()),
                        memory: task_def.memory().map(|m| m.to_string()),
                        pid_mode: task_def.pid_mode().map(|p| p.as_str().to_string()),
                        ipc_mode: task_def.ipc_mode().map(|i| i.as_str().to_string()),
                        proxy_configuration: None, // Simplified
                        runtime_platform: task_def.runtime_platform().map(|rp| {
                            resource::RuntimePlatform {
                                cpu_architecture: rp.cpu_architecture().map(|ca| ca.as_str().to_string()),
                                operating_system_family: rp.operating_system_family().map(|osf| osf.to_string()),
                            }
                        }),
                    };

                    return get_resource_output!(
                        EcsResource::TaskDefinition(our_task_def),
                        [(String::from("task_definition_id"), family)]
                    );
                }

                Ok(None)
            }
            EcsResourceAddress::Task(region, cluster_name, task_id) => {
                let client = self.get_or_init_client(&region).await?;
                let task = util::get_task(&client, &cluster_name, &task_id).await?;

                if let Some(task) = task {
                    // Convert AWS SDK task to our internal representation
                    let our_task = resource::Task {
                        task_definition_arn: task.task_definition_arn().unwrap_or_default().to_string(),
                        containers: task
                            .containers()
                            .iter()
                            .map(|c| {
                                // Container conversion simplified
                                resource::Container {
                                    container_arn: c.container_arn().map(|ca| ca.to_string()),
                                    task_arn: c.task_arn().map(|ta| ta.to_string()),
                                    name: c.name().map(|n| n.to_string()),
                                    image: c.image().map(|i| i.to_string()),
                                    image_digest: c.image_digest().map(|id| id.to_string()),
                                    runtime_id: c.runtime_id().map(|ri| ri.to_string()),
                                    last_status: c.last_status().map(|ls| ls.to_string()),
                                    exit_code: c.exit_code,
                                    reason: c.reason().map(|r| r.to_string()),
                                    network_bindings: c
                                        .network_bindings()
                                        .iter()
                                        .map(|nb| {
                                            resource::NetworkBinding {
                                                bind_ip: nb.bind_ip().map(|bi| bi.to_string()),
                                                container_port: nb.container_port,
                                                host_port: nb.host_port,
                                                protocol: nb.protocol().map(|p| p.as_str().to_string()),
                                            }
                                        })
                                        .collect(),
                                    network_interfaces: c
                                        .network_interfaces()
                                        .iter()
                                        .map(|ni| {
                                            resource::NetworkInterface {
                                                attachment_id: ni.attachment_id().map(|ai| ai.to_string()),
                                                private_ipv4_address: ni.private_ipv4_address().map(|pa| pa.to_string()),
                                                ipv6_address: ni.ipv6_address().map(|ia| ia.to_string()),
                                            }
                                        })
                                        .collect(),
                                    health_status: c.health_status().map(|hs| hs.as_str().to_string()),
                                    cpu: c.cpu().map(|c| c.to_string()),
                                    memory: c.memory().map(|m| m.to_string()),
                                    memory_reservation: c.memory_reservation().map(|mr| mr.to_string()),
                                    gpu_ids: c.gpu_ids().to_vec(),
                                }
                            })
                            .collect(),
                        cpu: task.cpu().map(|c| c.to_string()),
                        memory: task.memory().map(|m| m.to_string()),
                        last_status: task.last_status().map(|ls| ls.to_string()).unwrap_or_default(),
                        desired_status: task.desired_status().map(|ds| ds.to_string()).unwrap_or_default(),
                        connectivity: task.connectivity().map(|c| c.as_str().to_string()),
                        connectivity_at: task
                            .connectivity_at()
                            .map(|ca| ca.fmt(aws_smithy_types::date_time::Format::DateTime).unwrap_or_default()),
                        pull_started_at: task
                            .pull_started_at()
                            .map(|psa| psa.fmt(aws_smithy_types::date_time::Format::DateTime).unwrap_or_default()),
                        pull_stopped_at: task
                            .pull_stopped_at()
                            .map(|psa| psa.fmt(aws_smithy_types::date_time::Format::DateTime).unwrap_or_default()),
                        execution_stopped_at: task
                            .execution_stopped_at()
                            .map(|esa| esa.fmt(aws_smithy_types::date_time::Format::DateTime).unwrap_or_default()),
                        launch_type: task.launch_type().map(|lt| lt.as_str().to_string()),
                        capacity_provider_name: task.capacity_provider_name().map(|cpn| cpn.to_string()),
                        platform_version: task.platform_version().map(|pv| pv.to_string()),
                        platform_family: task.platform_family().map(|pf| pf.to_string()),
                        attachments: task
                            .attachments()
                            .iter()
                            .map(|a| {
                                resource::Attachment {
                                    id: a.id().unwrap_or_default().to_string(),
                                    r#type: a.r#type().unwrap_or_default().to_string(),
                                    status: a.status().unwrap_or_default().to_string(),
                                    details: a
                                        .details()
                                        .iter()
                                        .map(|d| {
                                            resource::KeyValuePair {
                                                name: d.name().map(|n| n.to_string()),
                                                value: d.value().map(|v| v.to_string()),
                                            }
                                        })
                                        .collect(),
                                }
                            })
                            .collect(),
                        tags: tags::Tags::from(task.tags()),
                    };

                    return get_resource_output!(
                        EcsResource::Task(our_task),
                        [
                            (String::from("cluster_name"), cluster_name),
                            (String::from("task_id"), task_id)
                        ]
                    );
                }

                Ok(None)
            }
            EcsResourceAddress::ContainerInstance(region, cluster_name, container_instance_id) => {
                let client = self.get_or_init_client(&region).await?;
                let container_instance = util::get_container_instance(&client, &cluster_name, &container_instance_id).await?;

                if let Some(container_instance) = container_instance {
                    // Convert AWS SDK container instance to our internal representation
                    let our_container_instance = resource::ContainerInstance {
                        ec2_instance_id: container_instance.ec2_instance_id().map(|eid| eid.to_string()),
                        capacity_provider_name: container_instance.capacity_provider_name().map(|cpn| cpn.to_string()),
                        version: Some(container_instance.version),
                        version_info: container_instance.version_info().map(|vi| {
                            resource::VersionInfo {
                                agent_version: vi.agent_version().map(|av| av.to_string()),
                                agent_hash: vi.agent_hash().map(|ah| ah.to_string()),
                                docker_version: vi.docker_version().map(|dv| dv.to_string()),
                            }
                        }),
                        remaining_resources: container_instance
                            .remaining_resources()
                            .iter()
                            .map(|rr| {
                                resource::EcsContainerResource {
                                    name: rr.name().unwrap_or_default().to_string(),
                                    r#type: rr.r#type().map(|t| t.to_string()),
                                    double_value: rr.double_value,
                                    long_value: rr.long_value,
                                    integer_value: rr.integer_value,
                                    string_value: rr.string_set_value().iter().map(|sv| sv.to_string()).collect(),
                                }
                            })
                            .collect(),
                        registered_resources: container_instance
                            .registered_resources()
                            .iter()
                            .map(|rr| {
                                resource::EcsContainerResource {
                                    name: rr.name().unwrap_or_default().to_string(),
                                    r#type: rr.r#type().map(|t| t.to_string()),
                                    double_value: rr.double_value,
                                    long_value: rr.long_value,
                                    integer_value: rr.integer_value,
                                    string_value: rr.string_set_value().iter().map(|sv| sv.to_string()).collect(),
                                }
                            })
                            .collect(),
                        status: container_instance.status().map(|s| s.to_string()).unwrap_or_default(),
                        status_reason: container_instance.status_reason().map(|sr| sr.to_string()),
                        agent_connected: container_instance.agent_connected,
                        running_tasks_count: container_instance.running_tasks_count,
                        pending_tasks_count: container_instance.pending_tasks_count,
                        agent_update_status: container_instance.agent_update_status().map(|aus| aus.as_str().to_string()),
                        attributes: container_instance
                            .attributes()
                            .iter()
                            .map(|a| {
                                resource::Attribute {
                                    name: a.name().to_string(),
                                    value: a.value().map(|v| v.to_string()),
                                    target_type: a.target_type().map(|tt| tt.as_str().to_string()),
                                    target_id: a.target_id().map(|ti| ti.to_string()),
                                }
                            })
                            .collect(),
                        attachments: container_instance
                            .attachments()
                            .iter()
                            .map(|a| {
                                resource::Attachment {
                                    id: a.id().unwrap_or_default().to_string(),
                                    r#type: a.r#type().unwrap_or_default().to_string(),
                                    status: a.status().unwrap_or_default().to_string(),
                                    details: a
                                        .details()
                                        .iter()
                                        .map(|d| {
                                            resource::KeyValuePair {
                                                name: d.name().map(|n| n.to_string()),
                                                value: d.value().map(|v| v.to_string()),
                                            }
                                        })
                                        .collect(),
                                }
                            })
                            .collect(),
                        tags: tags::Tags::from(container_instance.tags()),
                    };

                    return get_resource_output!(
                        EcsResource::ContainerInstance(our_container_instance),
                        [
                            (String::from("cluster_name"), cluster_name),
                            (String::from("container_instance_id"), container_instance_id)
                        ]
                    );
                }

                Ok(None)
            }
        }
    }
}
