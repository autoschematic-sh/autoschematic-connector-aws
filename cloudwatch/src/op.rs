use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};

use super::{
    resource::{
        Alarm, AlarmAction, Dashboard, Dimension, EventPattern, EventRule, EventTarget, LogGroup, LogStream, MetricDataQuery, MetricTransformation,
    },
    tags::Tags,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum CloudWatchConnectorOp {
    // Alarm operations
    CreateAlarm(Alarm),
    UpdateAlarm {
        threshold: Option<f64>,
        evaluation_periods: Option<i64>,
        datapoints_to_alarm: Option<i64>,
        comparison_operator: Option<String>,
        treat_missing_data: Option<String>,
        evaluate_low_sample_count_percentile: Option<String>,
        metrics: Option<Vec<MetricDataQuery>>,
        alarm_actions: Option<Vec<AlarmAction>>,
        ok_actions: Option<Vec<AlarmAction>>,
        insufficient_data_actions: Option<Vec<AlarmAction>>,
    },
    UpdateAlarmTags(Tags, Tags),
    DeleteAlarm,

    // Dashboard operations
    CreateDashboard(Dashboard),
    UpdateDashboardBody {
        body: ron::Value,
    },
    UpdateDashboardTags(Tags, Tags),
    DeleteDashboard,

    // Log Group operations
    CreateLogGroup(LogGroup),
    UpdateLogGroupRetention {
        retention_in_days: i32,
    },
    UpdateLogGroupKmsKey {
        kms_key_id: Option<String>,
    },
    UpdateLogGroupTags(Tags, Tags),
    DeleteLogGroup,

    // Log Stream operations
    CreateLogStream(LogStream),
    DeleteLogStream,
    PutLogEvents {
        log_group_name:  String,
        log_stream_name: String,
        log_events:      Vec<LogEvent>,
    },

    // Metric Filter operations
    PutMetricFilter {
        log_group_name: String,
        filter_name: String,
        filter_pattern: String,
        metric_transformations: Vec<MetricTransformation>,
    },
    DeleteMetricFilter {
        log_group_name: String,
        filter_name:    String,
    },

    // Metric operations
    PutMetricData {
        namespace:   String,
        metric_data: Vec<MetricData>,
    },
    UpdateMetricTags(Tags, Tags),

    // Event Rule operations
    CreateEventRule(EventRule),
    UpdateEventRule {
        description: Option<String>,
        schedule_expression: Option<String>,
        event_pattern: Option<EventPattern>,
        state: Option<String>,
        role_arn: Option<String>,
    },
    UpdateEventRuleTags(Tags, Tags),
    DeleteEventRule,

    // Event Target operations
    PutEventTargets {
        rule_name: String,
        targets:   Vec<EventTarget>,
    },
    RemoveEventTargets {
        rule_name:  String,
        target_ids: Vec<String>,
    },

    // Anomaly Detection operations
    CreateAnomalyDetector {
        namespace: String,
        metric_name: String,
        dimensions: Option<Vec<Dimension>>,
        stat: String,
        configuration: Option<ron::Value>, // AnomalyDetectorConfiguration as ron::Value
    },
    UpdateAnomalyDetector {
        namespace: String,
        metric_name: String,
        dimensions: Option<Vec<Dimension>>,
        stat: String,
        configuration: ron::Value,
    },
    DeleteAnomalyDetector {
        namespace: String,
        metric_name: String,
        dimensions: Option<Vec<Dimension>>,
        stat: String,
    },

    // Composite Alarm operations
    CreateCompositeAlarm {
        alarm_name: String,
        alarm_rule: String, // ALARM(alarm1) AND ALARM(alarm2) OR ...
        actions_enabled: bool,
        alarm_actions: Option<Vec<String>>,
        ok_actions: Option<Vec<String>>,
        insufficient_data_actions: Option<Vec<String>>,
        tags: Tags,
    },
    UpdateCompositeAlarm {
        alarm_name: String,
        alarm_rule: Option<String>,
        actions_enabled: Option<bool>,
        alarm_actions: Option<Vec<String>>,
        ok_actions: Option<Vec<String>>,
        insufficient_data_actions: Option<Vec<String>>,
    },
    UpdateCompositeAlarmTags {
        alarm_name:     String,
        tags_to_add:    Tags,
        tags_to_remove: Tags,
    },
    DeleteCompositeAlarm {
        alarm_name: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogEvent {
    pub timestamp: i64,
    pub message:   String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetricData {
    pub metric_name: String,
    pub dimensions: Option<Vec<Dimension>>,
    pub timestamp: Option<i64>,
    pub value: Option<f64>,
    pub statistic_values: Option<StatisticSet>,
    pub values: Option<Vec<f64>>,
    pub counts: Option<Vec<f64>>,
    pub unit: Option<String>,
    pub storage_resolution: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatisticSet {
    pub sample_count: f64,
    pub sum: f64,
    pub minimum: f64,
    pub maximum: f64,
}

impl ConnectorOp for CloudWatchConnectorOp {
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
