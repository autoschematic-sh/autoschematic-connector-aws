use autoschematic_core::{
    connector::{Resource, ResourceAddress},
    util::RON,
};
use serde::{Deserialize, Serialize};

use crate::tags::Tags;

use super::addr::CloudWatchResourceAddress;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Dimension {
    pub name:  String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct MetricDataQuery {
    pub id: String,
    pub metric_stat: Option<MetricStat>,
    pub expression: Option<String>,
    pub label: Option<String>,
    pub period: Option<i64>,
    pub return_data: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct MetricStat {
    pub metric: MetricIdentifier,
    pub period: i64,
    pub stat:   String,
    pub unit:   Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct MetricIdentifier {
    pub namespace: String,
    pub name: String,
    pub dimensions: Option<Vec<Dimension>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct AlarmAction {
    pub arn: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Alarm {
    pub name: String,
    pub description: Option<String>,
    pub metric_name: Option<String>,
    pub namespace: Option<String>,
    pub statistic: Option<String>,
    pub extended_statistic: Option<String>,
    pub dimensions: Option<Vec<Dimension>>,
    pub period: Option<i64>,
    pub unit: Option<String>,
    pub evaluation_periods: i64,
    pub datapoints_to_alarm: Option<i64>,
    pub threshold: Option<f64>,
    pub comparison_operator: String,
    pub treat_missing_data: Option<String>,
    pub evaluate_low_sample_count_percentile: Option<String>,
    pub metrics: Option<Vec<MetricDataQuery>>,
    pub alarm_actions: Option<Vec<AlarmAction>>,
    pub ok_actions: Option<Vec<AlarmAction>>,
    pub insufficient_data_actions: Option<Vec<AlarmAction>>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Dashboard {
    pub name: String,
    pub body: ron::Value, // JSON object that contains widget info
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct RetentionPolicy {
    pub retention_in_days: i32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LogGroup {
    pub name: String,
    pub retention_policy: Option<RetentionPolicy>,
    pub kms_key_id: Option<String>,
    pub metric_filters: Option<Vec<MetricFilter>>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct MetricFilter {
    pub filter_name: String,
    pub filter_pattern: String,
    pub metric_transformations: Vec<MetricTransformation>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct MetricTransformation {
    pub metric_name:      String,
    pub metric_namespace: String,
    pub metric_value:     String,
    pub default_value:    Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LogStream {
    pub name: String,
    pub log_group_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Metric {
    pub namespace: String,
    pub name: String,
    pub dimensions: Option<Vec<Dimension>>,
    pub stat_options: Option<StatOptions>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct StatOptions {
    pub stat:   String,
    pub unit:   Option<String>,
    pub period: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct EventTarget {
    pub id: String,
    pub arn: String,
    pub role_arn: Option<String>,
    pub input: Option<String>,
    pub input_path: Option<String>,
    pub input_transformer: Option<InputTransformer>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct InputTransformer {
    pub input_paths:      std::collections::HashMap<String, String>,
    pub input_cloudwatch: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct EventPattern {
    pub pattern: ron::Value, // JSON object representation of event pattern
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct EventRule {
    pub name: String,
    pub description: Option<String>,
    pub schedule_expression: Option<String>,
    pub event_pattern: Option<EventPattern>,
    pub state: String, // ENABLED or DISABLED
    pub targets: Option<Vec<EventTarget>>,
    pub role_arn: Option<String>,
    pub tags: Tags,
}

pub enum CloudWatchResource {
    Alarm(Alarm),
    Dashboard(Dashboard),
    LogGroup(LogGroup),
    LogStream(LogStream),
    Metric(Metric),
    EventRule(EventRule),
}

impl Resource for CloudWatchResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default();

        match self {
            CloudWatchResource::Alarm(alarm) => match RON.to_string_pretty(&alarm, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudWatchResource::Dashboard(dashboard) => match RON.to_string_pretty(&dashboard, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudWatchResource::LogGroup(log_group) => match RON.to_string_pretty(&log_group, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudWatchResource::LogStream(log_stream) => match RON.to_string_pretty(&log_stream, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudWatchResource::Metric(metric) => match RON.to_string_pretty(&metric, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            CloudWatchResource::EventRule(rule) => match RON.to_string_pretty(&rule, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = CloudWatchResourceAddress::from_path(&addr.to_path_buf())?;
        let s = str::from_utf8(s)?;

        match addr {
            CloudWatchResourceAddress::Alarm(_region, _name) => Ok(CloudWatchResource::Alarm(RON.from_str(s)?)),
            CloudWatchResourceAddress::Dashboard(_region, _name) => {
                Ok(CloudWatchResource::Dashboard(RON.from_str(s)?))
            }
            CloudWatchResourceAddress::LogGroup(_region, _name) => {
                Ok(CloudWatchResource::LogGroup(RON.from_str(s)?))
            }
            CloudWatchResourceAddress::LogStream(_region, _group_name, _stream_name) => {
                Ok(CloudWatchResource::LogStream(RON.from_str(s)?))
            }
            CloudWatchResourceAddress::Metric(_region, _namespace, _name) => {
                Ok(CloudWatchResource::Metric(RON.from_str(s)?))
            }
            CloudWatchResourceAddress::EventRule(_region, _name) => {
                Ok(CloudWatchResource::EventRule(RON.from_str(s)?))
            }
        }
    }
}
