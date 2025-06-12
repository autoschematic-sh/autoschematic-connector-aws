use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::config::EcsConnectorConfig;
use crate::resource::{Cluster, ContainerInstance, EcsResource, Service, Task, TaskDefinition};
use crate::{addr::EcsResourceAddress, resource, tags};
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_core::{connector::FilterOutput, skeleton};
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource, ResourceAddress, SkeletonOutput,
    },
    diag::DiagnosticOutput,
    util::{ron_check_eq, ron_check_syntax},
};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use tokio::sync::Mutex;

use autoschematic_connector_aws_core::config::{AwsConnectorConfig, AwsServiceConfig};

pub mod get;
pub mod list;
pub mod op_exec;
pub mod plan;

#[derive(Default)]
pub struct EcsConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_ecs::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<EcsConnectorConfig>,
    prefix: PathBuf,
}

impl EcsConnector {
    async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_ecs::Client>> {
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
            let client = aws_sdk_ecs::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }
}

#[async_trait]
impl Connector for EcsConnector {
    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> anyhow::Result<Box<dyn Connector>>
    where
        Self: Sized,
    {
        Ok(Box::new(EcsConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let ecs_config: EcsConnectorConfig = EcsConnectorConfig::try_load(&self.prefix).await?;

        let account_id = ecs_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = ecs_config;
        *self.account_id.lock().await = account_id;
        Ok(())
    }

    async fn filter(&self, addr: &Path) -> anyhow::Result<FilterOutput> {
        if let Ok(_addr) = EcsResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        // Cluster skeleton
        res.push(skeleton!(
            EcsResourceAddress::Cluster(String::from("[region]"), String::from("[cluster_name]")),
            EcsResource::Cluster(Cluster {
                status: String::from("ACTIVE"),
                capacity_providers: vec![String::from("FARGATE"), String::from("FARGATE_SPOT"),],
                default_capacity_provider_strategy: vec![
                    resource::CapacityProviderStrategyItem {
                        capacity_provider: String::from("FARGATE"),
                        weight: Some(1),
                        base: Some(1),
                    },
                    resource::CapacityProviderStrategyItem {
                        capacity_provider: String::from("FARGATE_SPOT"),
                        weight: Some(4),
                        base: Some(0),
                    },
                ],
                settings: vec![resource::ClusterSetting {
                    name:  String::from("containerInsights"),
                    value: String::from("enabled"),
                }],
                configuration: Some(resource::ClusterConfiguration {
                    execute_command_configuration: Some(resource::ExecuteCommandConfiguration {
                        kms_key_id: None,
                        logging: Some(String::from("DEFAULT")),
                        log_configuration: None,
                    }),
                }),
                tags: tags::Tags::default(),
            })
        ));

        // Service skeleton - Fargate service with load balancer
        res.push(skeleton!(
            EcsResourceAddress::Service(
                String::from("[region]"),
                String::from("[cluster_name]"),
                String::from("[service_name]")
            ),
            EcsResource::Service(Service {
                task_definition: String::from("[task_definition_family]:[revision]"),
                desired_count: 2,
                launch_type: Some(String::from("FARGATE")),
                capacity_provider_strategy: Vec::new(),
                platform_version: Some(String::from("LATEST")),
                platform_family: None,
                deployment_configuration: Some(resource::DeploymentConfiguration {
                    deployment_circuit_breaker: Some(resource::DeploymentCircuitBreaker {
                        enable:   true,
                        rollback: true,
                    }),
                    maximum_percent: Some(200),
                    minimum_healthy_percent: Some(100),
                }),
                network_configuration: Some(resource::NetworkConfiguration {
                    awsvpc_configuration: Some(resource::AwsVpcConfiguration {
                        subnets: vec![
                            String::from("subnet-0123456789abcdef0"),
                            String::from("subnet-0123456789abcdef1"),
                        ],
                        security_groups: vec![String::from("sg-0123456789abcdef0"),],
                        assign_public_ip: Some(String::from("ENABLED")),
                    }),
                }),
                placement_constraints: Vec::new(),
                placement_strategy: Vec::new(),
                load_balancers: vec![resource::LoadBalancer {
                    target_group_arn:   Some(String::from(
                        "arn:aws:elasticloadbalancing:[region]:[account_id]:targetgroup/[target-group-name]/[target-group-id]"
                    )),
                    load_balancer_name: None,
                    container_name:     Some(String::from("web")),
                    container_port:     Some(80),
                },],
                service_registries: Vec::new(),
                scheduling_strategy: Some(String::from("REPLICA")),
                enable_ecs_managed_tags: Some(true),
                propagate_tags: Some(String::from("SERVICE")),
                enable_execute_command: Some(true),
                tags: tags::Tags::default(),
            })
        ));

        // Task Definition skeleton - Web application with Nginx
        res.push(skeleton!(
            EcsResourceAddress::TaskDefinition(String::from("[region]"), String::from("[task_definition_family]:[revision]")),
            EcsResource::TaskDefinition(TaskDefinition {
                task_role_arn: Some(String::from("arn:aws:iam::[account_id]:role/[task_role_name]")),
                execution_role_arn: Some(String::from("arn:aws:iam::[account_id]:role/ecsTaskExecutionRole")),
                network_mode: Some(String::from("awsvpc")),
                container_definitions: vec![resource::ContainerDefinition {
                    name: String::from("web"),
                    image: String::from("nginx:latest"),
                    cpu: Some(256),
                    memory: Some(512),
                    memory_reservation: None,
                    links: Vec::new(),
                    port_mappings: vec![resource::PortMapping {
                        container_port: Some(80),
                        host_port: Some(80),
                        protocol: Some(String::from("tcp")),
                    },],
                    essential: Some(true),
                    entry_point: Vec::new(),
                    command: Vec::new(),
                    environment: vec![resource::KeyValuePair {
                        name:  Some(String::from("ENVIRONMENT")),
                        value: Some(String::from("production")),
                    },],
                    environment_files: Vec::new(),
                    mount_points: Vec::new(),
                    volumes_from: Vec::new(),
                    linux_parameters: None,
                    secrets: Vec::new(),
                    depends_on: Vec::new(),
                    start_timeout: None,
                    stop_timeout: None,
                    hostname: None,
                    user: None,
                    working_directory: None,
                    disable_networking: None,
                    privileged: Some(false),
                    readonly_root_filesystem: Some(false),
                    dns_servers: Vec::new(),
                    dns_search_domains: Vec::new(),
                    extra_hosts: Vec::new(),
                    docker_security_options: Vec::new(),
                    interactive: None,
                    pseudo_terminal: None,
                    docker_labels: std::collections::HashMap::new(),
                    ulimits: Vec::new(),
                    log_configuration: Some(resource::LogConfiguration {
                        log_driver: String::from("awslogs"),
                        options: {
                            let mut map = std::collections::HashMap::new();
                            map.insert(String::from("awslogs-group"), String::from("/ecs/webapp"));
                            map.insert(String::from("awslogs-region"), String::from("[region]"));
                            map.insert(String::from("awslogs-stream-prefix"), String::from("ecs"));
                            map
                        },
                        secret_options: Vec::new(),
                    }),
                    health_check: Some(resource::HealthCheck {
                        command:      vec![String::from("CMD-SHELL"), String::from("curl -f http://localhost/ || exit 1"),],
                        interval:     Some(30),
                        timeout:      Some(5),
                        retries:      Some(3),
                        start_period: Some(60),
                    }),
                    system_controls: Vec::new(),
                    resource_requirements: Vec::new(),
                    firelens_configuration: None,
                },],
                volumes: Vec::new(),
                placement_constraints: Vec::new(),
                requires_compatibilities: vec![String::from("FARGATE"),],
                cpu: Some(String::from("256")),
                memory: Some(String::from("512")),
                pid_mode: None,
                ipc_mode: None,
                proxy_configuration: None,
                runtime_platform: Some(resource::RuntimePlatform {
                    cpu_architecture: Some(String::from("X86_64")),
                    operating_system_family: Some(String::from("LINUX")),
                }),
            })
        ));

        // Task skeleton
        res.push(skeleton!(
            EcsResourceAddress::Task(
                String::from("[region]"),
                String::from("[cluster_name]"),
                String::from("[task_id]")
            ),
            EcsResource::Task(Task {
                task_definition_arn: String::from(
                    "arn:aws:ecs:[region]:[account_id]:task-definition/[task_definition_family]:[revision]"
                ),
                containers: vec![resource::Container {
                    container_arn: Some(String::from("arn:aws:ecs:[region]:[account_id]:container/[container_id]")),
                    task_arn: Some(String::from("arn:aws:ecs:[region]:[account_id]:task/[task_id]")),
                    name: Some(String::from("web")),
                    image: Some(String::from("nginx:latest")),
                    image_digest: Some(String::from(
                        "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
                    )),
                    runtime_id: Some(String::from("12345678901234567890123456789012-1234567890")),
                    last_status: Some(String::from("RUNNING")),
                    exit_code: None,
                    reason: None,
                    network_bindings: Vec::new(),
                    network_interfaces: vec![resource::NetworkInterface {
                        attachment_id: Some(String::from("attachment-1234567890abcdef0")),
                        private_ipv4_address: Some(String::from("10.0.1.100")),
                        ipv6_address: None,
                    },],
                    health_status: Some(String::from("HEALTHY")),
                    cpu: Some(String::from("256")),
                    memory: Some(String::from("512")),
                    memory_reservation: None,
                    gpu_ids: Vec::new(),
                },],
                cpu: Some(String::from("256")),
                memory: Some(String::from("512")),
                last_status: String::from("RUNNING"),
                desired_status: String::from("RUNNING"),
                connectivity: Some(String::from("CONNECTED")),
                connectivity_at: Some(String::from("2023-01-01T00:00:00Z")),
                pull_started_at: Some(String::from("2023-01-01T00:00:00Z")),
                pull_stopped_at: Some(String::from("2023-01-01T00:00:10Z")),
                execution_stopped_at: None,
                launch_type: Some(String::from("FARGATE")),
                capacity_provider_name: Some(String::from("FARGATE")),
                platform_version: Some(String::from("1.4.0")),
                platform_family: None,
                attachments: vec![resource::Attachment {
                    id:      String::from("attachment-1234567890abcdef0"),
                    r#type:  String::from("ElasticNetworkInterface"),
                    status:  String::from("ATTACHED"),
                    details: vec![
                        resource::KeyValuePair {
                            name:  Some(String::from("subnetId")),
                            value: Some(String::from("subnet-0123456789abcdef0")),
                        },
                        resource::KeyValuePair {
                            name:  Some(String::from("networkInterfaceId")),
                            value: Some(String::from("eni-0123456789abcdef0")),
                        },
                        resource::KeyValuePair {
                            name:  Some(String::from("privateIPv4Address")),
                            value: Some(String::from("10.0.1.100")),
                        },
                    ],
                },],
                tags: tags::Tags::default(),
            })
        ));

        // Container Instance skeleton
        res.push(skeleton!(
            EcsResourceAddress::ContainerInstance(
                String::from("[region]"),
                String::from("[cluster_name]"),
                String::from("[container_instance_id]")
            ),
            EcsResource::ContainerInstance(ContainerInstance {
                ec2_instance_id: Some(String::from("i-0123456789abcdef0")),
                capacity_provider_name: Some(String::from("capacity-provider-name")),
                version: Some(8),
                version_info: Some(resource::VersionInfo {
                    agent_version:  Some(String::from("1.57.1")),
                    agent_hash:     Some(String::from("12345678abc")),
                    docker_version: Some(String::from("20.10.13")),
                }),
                remaining_resources: vec![
                    resource::EcsContainerResource {
                        name: String::from("CPU"),
                        r#type: Some(String::from("INTEGER")),
                        double_value: 0.0,
                        long_value: 0,
                        integer_value: 2048,
                        string_value: String::new(),
                    },
                    resource::EcsContainerResource {
                        name: String::from("MEMORY"),
                        r#type: Some(String::from("INTEGER")),
                        double_value: 0.0,
                        long_value: 0,
                        integer_value: 3968,
                        string_value: String::new(),
                    },
                ],
                registered_resources: vec![
                    resource::EcsContainerResource {
                        name: String::from("CPU"),
                        r#type: Some(String::from("INTEGER")),
                        double_value: 0.0,
                        long_value: 0,
                        integer_value: 4096,
                        string_value: String::new(),
                    },
                    resource::EcsContainerResource {
                        name: String::from("MEMORY"),
                        r#type: Some(String::from("INTEGER")),
                        double_value: 0.0,
                        long_value: 0,
                        integer_value: 8192,
                        string_value: String::new(),
                    },
                ],
                status: String::from("ACTIVE"),
                status_reason: None,
                agent_connected: true,
                running_tasks_count: 2,
                pending_tasks_count: 0,
                agent_update_status: None,
                attributes: vec![
                    resource::Attribute {
                        name: String::from("ecs.availability-zone"),
                        value: Some(String::from("us-east-1a")),
                        target_type: None,
                        target_id: None,
                    },
                    resource::Attribute {
                        name: String::from("ecs.ami-id"),
                        value: Some(String::from("ami-0123456789abcdef0")),
                        target_type: None,
                        target_id: None,
                    },
                    resource::Attribute {
                        name: String::from("ecs.instance-type"),
                        value: Some(String::from("t3.xlarge")),
                        target_type: None,
                        target_id: None,
                    },
                ],
                attachments: Vec::new(),
                tags: tags::Tags::default(),
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = EcsResourceAddress::from_path(addr)?;
        match addr {
            EcsResourceAddress::Cluster(_, _) => ron_check_eq::<resource::Cluster>(a, b),
            EcsResourceAddress::Service(_, _, _) => ron_check_eq::<resource::Service>(a, b),
            EcsResourceAddress::TaskDefinition(_, _) => ron_check_eq::<resource::TaskDefinition>(a, b),
            EcsResourceAddress::Task(_, _, _) => ron_check_eq::<resource::Task>(a, b),
            EcsResourceAddress::ContainerInstance(_, _, _) => ron_check_eq::<resource::ContainerInstance>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = EcsResourceAddress::from_path(addr)?;

        match addr {
            EcsResourceAddress::Cluster(_, _) => ron_check_syntax::<resource::Cluster>(a),
            EcsResourceAddress::Service(_, _, _) => ron_check_syntax::<resource::Service>(a),
            EcsResourceAddress::TaskDefinition(_, _) => ron_check_syntax::<resource::TaskDefinition>(a),
            EcsResourceAddress::Task(_, _, _) => ron_check_syntax::<resource::Task>(a),
            EcsResourceAddress::ContainerInstance(_, _, _) => ron_check_syntax::<resource::ContainerInstance>(a),
        }
    }
}
