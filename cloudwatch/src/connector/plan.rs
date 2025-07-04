use std::path::Path;

use autoschematic_core::{
    connector::{ConnectorOp, OpPlanOutput, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values, optional_string_from_utf8},
};

use crate::{
    addr::CloudWatchResourceAddress,
    op::CloudWatchConnectorOp,
    resource::{Alarm, Dashboard, EventRule, LogGroup, LogStream, Metric},
};

use super::CloudWatchConnector;

impl CloudWatchConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;
        let addr = CloudWatchResourceAddress::from_path(addr)?;

        match addr {
            CloudWatchResourceAddress::Alarm(region, alarm_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_alarm)) => {
                        let new_alarm: Alarm = RON.from_str(&new_alarm)?;
                        Ok(vec![connector_op!(
                            CloudWatchConnectorOp::CreateAlarm(new_alarm),
                            format!("Create new CloudWatch alarm {}", alarm_name)
                        )])
                    }
                    (Some(_old_alarm), None) => Ok(vec![connector_op!(
                        CloudWatchConnectorOp::DeleteAlarm,
                        format!("DELETE CloudWatch alarm {}", alarm_name)
                    )]),
                    (Some(old_alarm), Some(new_alarm)) => {
                        let old_alarm: Alarm = RON.from_str(&old_alarm)?;
                        let new_alarm: Alarm = RON.from_str(&new_alarm)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_alarm.tags != new_alarm.tags {
                            let diff = diff_ron_values(&old_alarm.tags, &new_alarm.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                CloudWatchConnectorOp::UpdateAlarmTags(old_alarm.tags.clone(), new_alarm.tags.clone()),
                                format!("Modify tags for CloudWatch alarm `{}`\n{}", alarm_name, diff)
                            ));
                        }

                        // Check for alarm property changes
                        let mut alarm_changed = false;
                        if old_alarm.threshold != new_alarm.threshold {
                            alarm_changed = true;
                        }
                        if old_alarm.evaluation_periods != new_alarm.evaluation_periods {
                            alarm_changed = true;
                        }
                        if old_alarm.comparison_operator != new_alarm.comparison_operator {
                            alarm_changed = true;
                        }
                        if old_alarm.treat_missing_data != new_alarm.treat_missing_data {
                            alarm_changed = true;
                        }
                        if old_alarm.alarm_actions != new_alarm.alarm_actions {
                            alarm_changed = true;
                        }

                        if alarm_changed {
                            ops.push(connector_op!(
                                CloudWatchConnectorOp::UpdateAlarm {
                                    threshold: new_alarm.threshold,
                                    evaluation_periods: Some(new_alarm.evaluation_periods),
                                    datapoints_to_alarm: new_alarm.datapoints_to_alarm,
                                    comparison_operator: Some(new_alarm.comparison_operator.clone()),
                                    treat_missing_data: new_alarm.treat_missing_data.clone(),
                                    evaluate_low_sample_count_percentile: new_alarm
                                        .evaluate_low_sample_count_percentile
                                        .clone(),
                                    metrics: new_alarm.metrics.clone(),
                                    alarm_actions: new_alarm.alarm_actions.clone(),
                                    ok_actions: new_alarm.ok_actions.clone(),
                                    insufficient_data_actions: new_alarm.insufficient_data_actions.clone(),
                                },
                                format!("Update CloudWatch alarm `{}`", alarm_name)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudWatchResourceAddress::Dashboard(region, dashboard_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_dashboard)) => {
                        let new_dashboard: Dashboard = RON.from_str(&new_dashboard)?;
                        Ok(vec![connector_op!(
                            CloudWatchConnectorOp::CreateDashboard(new_dashboard),
                            format!("Create new CloudWatch dashboard {}", dashboard_name)
                        )])
                    }
                    (Some(_old_dashboard), None) => Ok(vec![connector_op!(
                        CloudWatchConnectorOp::DeleteDashboard,
                        format!("DELETE CloudWatch dashboard {}", dashboard_name)
                    )]),
                    (Some(old_dashboard), Some(new_dashboard)) => {
                        let old_dashboard: Dashboard = RON.from_str(&old_dashboard)?;
                        let new_dashboard: Dashboard = RON.from_str(&new_dashboard)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_dashboard.tags != new_dashboard.tags {
                            let diff = diff_ron_values(&old_dashboard.tags, &new_dashboard.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                CloudWatchConnectorOp::UpdateDashboardTags(
                                    old_dashboard.tags.clone(),
                                    new_dashboard.tags.clone()
                                ),
                                format!("Modify tags for CloudWatch dashboard `{}`\n{}", dashboard_name, diff)
                            ));
                        }

                        // Check for dashboard body changes
                        if old_dashboard.body != new_dashboard.body {
                            ops.push(connector_op!(
                                CloudWatchConnectorOp::UpdateDashboardBody {
                                    body: new_dashboard.body.clone(),
                                },
                                format!("Update CloudWatch dashboard body `{}`", dashboard_name)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudWatchResourceAddress::LogGroup(region, log_group_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_log_group)) => {
                        let new_log_group: LogGroup = RON.from_str(&new_log_group)?;
                        Ok(vec![connector_op!(
                            CloudWatchConnectorOp::CreateLogGroup(new_log_group),
                            format!("Create new CloudWatch log group {}", log_group_name)
                        )])
                    }
                    (Some(_old_log_group), None) => Ok(vec![connector_op!(
                        CloudWatchConnectorOp::DeleteLogGroup,
                        format!("DELETE CloudWatch log group {}", log_group_name)
                    )]),
                    (Some(old_log_group), Some(new_log_group)) => {
                        let old_log_group: LogGroup = RON.from_str(&old_log_group)?;
                        let new_log_group: LogGroup = RON.from_str(&new_log_group)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_log_group.tags != new_log_group.tags {
                            let diff = diff_ron_values(&old_log_group.tags, &new_log_group.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                CloudWatchConnectorOp::UpdateLogGroupTags(
                                    old_log_group.tags.clone(),
                                    new_log_group.tags.clone()
                                ),
                                format!("Modify tags for CloudWatch log group `{}`\n{}", log_group_name, diff)
                            ));
                        }

                        // Check for retention policy changes
                        if old_log_group.retention_policy != new_log_group.retention_policy
                            && let Some(retention_policy) = &new_log_group.retention_policy {
                                ops.push(connector_op!(
                                    CloudWatchConnectorOp::UpdateLogGroupRetention {
                                        retention_in_days: retention_policy.retention_in_days,
                                    },
                                    format!("Update retention policy for CloudWatch log group `{}`", log_group_name)
                                ));
                            }

                        // Check for KMS key changes
                        if old_log_group.kms_key_id != new_log_group.kms_key_id {
                            ops.push(connector_op!(
                                CloudWatchConnectorOp::UpdateLogGroupKmsKey {
                                    kms_key_id: new_log_group.kms_key_id.clone(),
                                },
                                format!("Update KMS key for CloudWatch log group `{}`", log_group_name)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudWatchResourceAddress::LogStream(region, log_group_name, log_stream_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_log_stream)) => {
                        let new_log_stream: LogStream = RON.from_str(&new_log_stream)?;
                        Ok(vec![connector_op!(
                            CloudWatchConnectorOp::CreateLogStream(new_log_stream),
                            format!("Create new CloudWatch log stream {}", log_stream_name)
                        )])
                    }
                    (Some(_old_log_stream), None) => Ok(vec![connector_op!(
                        CloudWatchConnectorOp::DeleteLogStream,
                        format!("DELETE CloudWatch log stream {}", log_stream_name)
                    )]),
                    (Some(_old_log_stream), Some(_new_log_stream)) => {
                        // Log streams generally don't have updatable properties
                        Ok(vec![])
                    }
                }
            }

            CloudWatchResourceAddress::Metric(region, namespace, metric_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(_new_metric)) => {
                        // Metrics are created implicitly when data is put
                        Ok(vec![])
                    }
                    (Some(_old_metric), None) => {
                        // Metrics cannot be explicitly deleted
                        Ok(vec![])
                    }
                    (Some(old_metric), Some(new_metric)) => {
                        let old_metric: Metric = RON.from_str(&old_metric)?;
                        let new_metric: Metric = RON.from_str(&new_metric)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_metric.tags != new_metric.tags {
                            let diff = diff_ron_values(&old_metric.tags, &new_metric.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                CloudWatchConnectorOp::UpdateMetricTags(old_metric.tags.clone(), new_metric.tags.clone()),
                                format!("Modify tags for CloudWatch metric `{}`\n{}", metric_name, diff)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }

            CloudWatchResourceAddress::EventRule(region, rule_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_rule)) => {
                        let new_rule: EventRule = RON.from_str(&new_rule)?;
                        Ok(vec![connector_op!(
                            CloudWatchConnectorOp::CreateEventRule(new_rule),
                            format!("Create new EventBridge rule {}", rule_name)
                        )])
                    }
                    (Some(_old_rule), None) => Ok(vec![connector_op!(
                        CloudWatchConnectorOp::DeleteEventRule,
                        format!("DELETE EventBridge rule {}", rule_name)
                    )]),
                    (Some(old_rule), Some(new_rule)) => {
                        let old_rule: EventRule = RON.from_str(&old_rule)?;
                        let new_rule: EventRule = RON.from_str(&new_rule)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_rule.tags != new_rule.tags {
                            let diff = diff_ron_values(&old_rule.tags, &new_rule.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                CloudWatchConnectorOp::UpdateEventRuleTags(old_rule.tags.clone(), new_rule.tags.clone()),
                                format!("Modify tags for EventBridge rule `{}`\n{}", rule_name, diff)
                            ));
                        }

                        // Check for rule property changes
                        let mut rule_changed = false;
                        if old_rule.description != new_rule.description {
                            rule_changed = true;
                        }
                        if old_rule.schedule_expression != new_rule.schedule_expression {
                            rule_changed = true;
                        }
                        if old_rule.event_pattern != new_rule.event_pattern {
                            rule_changed = true;
                        }
                        if old_rule.state != new_rule.state {
                            rule_changed = true;
                        }
                        if old_rule.role_arn != new_rule.role_arn {
                            rule_changed = true;
                        }

                        if rule_changed {
                            ops.push(connector_op!(
                                CloudWatchConnectorOp::UpdateEventRule {
                                    description: new_rule.description.clone(),
                                    schedule_expression: new_rule.schedule_expression.clone(),
                                    event_pattern: new_rule.event_pattern.clone(),
                                    state: Some(new_rule.state.clone()),
                                    role_arn: new_rule.role_arn.clone(),
                                },
                                format!("Update EventBridge rule `{}`", rule_name)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
        }
    }
}
