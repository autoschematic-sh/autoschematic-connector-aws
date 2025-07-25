pub use crate::addr::CloudWatchResourceAddress;
use crate::{
    config, resource,
    resource::{
        Alarm, AlarmAction, CloudWatchResource, Dashboard, Dimension, EventRule, EventTarget, LogGroup, LogStream, Metric,
        StatOptions,
    },
    tags::Tags,
};

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, FilterResponse, GetResourceResponse, OpExecResponse, PlanResponseElement, Resource, ResourceAddress,
        SkeletonResponse,
    },
    diag::DiagnosticResponse,
    skeleton,
    util::{ron_check_eq, ron_check_syntax},
};
use aws_config::{BehaviorVersion, Region};
use config::CloudWatchConnectorConfig;
use tokio::sync::Mutex;
mod get;
mod list;
mod op_exec;
mod plan;

#[derive(Default)]
pub struct CloudWatchConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_cloudwatch::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<CloudWatchConnectorConfig>,
    prefix: PathBuf,
}

impl CloudWatchConnector {
    pub async fn get_or_init_client(&self, region: &str) -> anyhow::Result<Arc<aws_sdk_cloudwatch::Client>> {
        let mut client_cache = self.client_cache.lock().await;

        if let Some(client) = client_cache.get(region) {
            return Ok(client.clone());
        }

        let config = self.config.lock().await;

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region.to_string()))
            .load()
            .await;

        let client = Arc::new(aws_sdk_cloudwatch::Client::new(&aws_config));
        client_cache.insert(region.to_string(), client.clone());

        Ok(client)
    }
}

#[async_trait]
impl Connector for CloudWatchConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterResponse, anyhow::Error> {
        if let Ok(_addr) = CloudWatchResourceAddress::from_path(addr) {
            Ok(FilterResponse::Resource)
        } else {
            Ok(FilterResponse::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(CloudWatchConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let secrets_config: CloudWatchConnectorConfig = CloudWatchConnectorConfig::try_load(&self.prefix).await?;

        let account_id = secrets_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = secrets_config;
        *self.account_id.lock().await = account_id;
        Ok(())
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

        let region = String::from("[region]");
        let alarm_name = String::from("[alarm_name]");

        // CloudWatch Alarm
        res.push(skeleton!(
            CloudWatchResourceAddress::Alarm(region.clone(), alarm_name),
            CloudWatchResource::Alarm(Alarm {
                name: String::from("[alarm_name]"),
                description: Some(String::from("[description]")),
                metric_name: Some(String::from("[metric_name]")),
                namespace: Some(String::from("[namespace]")),
                statistic: Some(String::from("Average")),
                extended_statistic: None,
                dimensions: Some(vec![Dimension {
                    name: String::from("[dimension_name]"),
                    value: String::from("[dimension_value]"),
                }]),
                period: Some(300),
                unit: Some(String::from("Count")),
                evaluation_periods: 2,
                datapoints_to_alarm: Some(2),
                threshold: Some(10.0),
                comparison_operator: String::from("GreaterThanThreshold"),
                treat_missing_data: Some(String::from("missing")),
                evaluate_low_sample_count_percentile: None,
                metrics: None,
                alarm_actions: Some(vec![AlarmAction {
                    arn: String::from("[sns_topic_arn]"),
                }]),
                ok_actions: None,
                insufficient_data_actions: None,
                tags: Tags::default(),
            })
        ));

        // CloudWatch Dashboard
        let dashboard_name = String::from("[dashboard_name]");
        res.push(skeleton!(
            CloudWatchResourceAddress::Dashboard(region.clone(), dashboard_name),
            CloudWatchResource::Dashboard(Dashboard {
                name: String::from("[dashboard_name]"),
                body: ron::Value::String(String::from("{\"widgets\":[]}")),
                tags: Tags::default(),
            })
        ));

        // CloudWatch Log Group
        let log_group_name = String::from("[log_group_name]");
        res.push(skeleton!(
            CloudWatchResourceAddress::LogGroup(region.clone(), log_group_name.clone()),
            CloudWatchResource::LogGroup(LogGroup {
                retention_policy: Some(crate::resource::RetentionPolicy { retention_in_days: 14 }),
                kms_key_id: None,
                metric_filters: None,
                tags: Tags::default(),
            })
        ));

        // CloudWatch Log Stream
        let log_stream_name = String::from("[log_stream_name]");
        res.push(skeleton!(
            CloudWatchResourceAddress::LogStream(region.clone(), log_group_name.clone(), log_stream_name),
            CloudWatchResource::LogStream(LogStream {
                name: String::from("[log_stream_name]"),
                log_group_name: String::from("[log_group_name]"),
            })
        ));

        // CloudWatch Metric
        let namespace = String::from("[namespace]");
        let metric_name = String::from("[metric_name]");
        res.push(skeleton!(
            CloudWatchResourceAddress::Metric(region.clone(), namespace.clone(), metric_name),
            CloudWatchResource::Metric(Metric {
                namespace: String::from("[namespace]"),
                name: String::from("[metric_name]"),
                dimensions: Some(vec![Dimension {
                    name: String::from("[dimension_name]"),
                    value: String::from("[dimension_value]"),
                }]),
                stat_options: Some(StatOptions {
                    stat: String::from("Average"),
                    unit: Some(String::from("Count")),
                    period: 300,
                }),
                tags: Tags::default(),
            })
        ));

        // CloudWatch Event Rule
        let event_rule_name = String::from("[event_rule_name]");
        res.push(skeleton!(
            CloudWatchResourceAddress::EventRule(region.clone(), event_rule_name),
            CloudWatchResource::EventRule(EventRule {
                name: String::from("[event_rule_name]"),
                description: Some(String::from("[description]")),
                schedule_expression: Some(String::from("rate(5 minutes)")),
                event_pattern: None,
                state: String::from("ENABLED"),
                targets: Some(vec![EventTarget {
                    id: String::from("[target_id]"),
                    arn: String::from("[target_arn]"),
                    role_arn: None,
                    input: None,
                    input_path: None,
                    input_transformer: None,
                }]),
                role_arn: None,
                tags: Tags::default(),
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = CloudWatchResourceAddress::from_path(addr)?;

        match addr {
            CloudWatchResourceAddress::Alarm(_, _) => ron_check_eq::<resource::Alarm>(a, b),
            CloudWatchResourceAddress::Dashboard(_, _) => ron_check_eq::<resource::Dashboard>(a, b),
            CloudWatchResourceAddress::LogGroup(_, _) => ron_check_eq::<resource::LogGroup>(a, b),
            CloudWatchResourceAddress::LogStream(_, _, _) => ron_check_eq::<resource::LogStream>(a, b),
            CloudWatchResourceAddress::Metric(_, _, _) => ron_check_eq::<resource::Metric>(a, b),
            CloudWatchResourceAddress::EventRule(_, _) => ron_check_eq::<resource::EventRule>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<Option<DiagnosticResponse>, anyhow::Error> {
        let addr = CloudWatchResourceAddress::from_path(addr)?;

        match addr {
            CloudWatchResourceAddress::Alarm(_, _) => ron_check_syntax::<resource::Alarm>(a),
            CloudWatchResourceAddress::Dashboard(_, _) => ron_check_syntax::<resource::Dashboard>(a),
            CloudWatchResourceAddress::LogGroup(_, _) => ron_check_syntax::<resource::LogGroup>(a),
            CloudWatchResourceAddress::LogStream(_, _, _) => ron_check_syntax::<resource::LogStream>(a),
            CloudWatchResourceAddress::Metric(_, _, _) => ron_check_syntax::<resource::Metric>(a),
            CloudWatchResourceAddress::EventRule(_, _) => ron_check_syntax::<resource::EventRule>(a),
        }
    }
}
