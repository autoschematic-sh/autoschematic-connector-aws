use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::config::EcsConnectorConfig;
use crate::resource::{Cluster, EcsResource, Service, TaskDefinition};
use crate::{addr::EcsResourceAddress, resource, tags};
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_core::{connector::FilterResponse, skeleton};
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, GetResourceResponse, OpExecResponse, PlanResponseElement, Resource, ResourceAddress, SkeletonResponse,
    },
    diag::DiagnosticResponse,
    util::{ron_check_eq, ron_check_syntax},
};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use tokio::sync::Mutex;

use autoschematic_connector_aws_core::config::AwsServiceConfig;

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
    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> anyhow::Result<Arc<dyn Connector>>
    where
        Self: Sized,
    {
        Ok(Arc::new(EcsConnector {
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

    async fn filter(&self, addr: &Path) -> anyhow::Result<FilterResponse> {
        if let Ok(_addr) = EcsResourceAddress::from_path(addr) {
            Ok(FilterResponse::Resource)
        } else {
            Ok(FilterResponse::None)
        }
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceResponse>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonResponse>, anyhow::Error> {
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


        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = EcsResourceAddress::from_path(addr)?;
        match addr {
            EcsResourceAddress::Cluster(_, _) => ron_check_eq::<resource::Cluster>(a, b),
            EcsResourceAddress::Service(_, _, _) => ron_check_eq::<resource::Service>(a, b),
            EcsResourceAddress::TaskDefinition(_, _) => ron_check_eq::<resource::TaskDefinition>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<Option<DiagnosticResponse>, anyhow::Error> {
        let addr = EcsResourceAddress::from_path(addr)?;

        match addr {
            EcsResourceAddress::Cluster(_, _) => ron_check_syntax::<resource::Cluster>(a),
            EcsResourceAddress::Service(_, _, _) => ron_check_syntax::<resource::Service>(a),
            EcsResourceAddress::TaskDefinition(_, _) => ron_check_syntax::<resource::TaskDefinition>(a),
        }
    }
}
