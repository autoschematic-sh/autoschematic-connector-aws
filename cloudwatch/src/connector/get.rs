use std::{collections::HashMap, path::Path};

use crate::addr::CloudWatchResourceAddress;
use crate::resource::*;
use crate::tags::Tags;
use anyhow::Context;
use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};
use autoschematic_core::get_resource_output;

use super::CloudWatchConnector;

impl CloudWatchConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = CloudWatchResourceAddress::from_path(addr)?;

        match addr {
            CloudWatchResourceAddress::Alarm(region, alarm_name) => {
                let client = self.get_or_init_client(&region).await?;

                let alarm_response = client.describe_alarms().alarm_names(&alarm_name).send().await?;

                if let Some(metric_alarms) = alarm_response.metric_alarms {
                    if let Some(aws_alarm) = metric_alarms.into_iter().next() {
                        let dimensions = aws_alarm.dimensions.map(|dims| {
                            dims.into_iter()
                                .filter_map(|d| {
                                    if let (Some(name), Some(value)) = (d.name, d.value) {
                                        Some(Dimension { name, value })
                                    } else {
                                        None
                                    }
                                })
                                .collect()
                        });

                        let alarm = Alarm {
                            name: aws_alarm.alarm_name.unwrap_or_default(),
                            description: aws_alarm.alarm_description,
                            metric_name: aws_alarm.metric_name,
                            namespace: aws_alarm.namespace,
                            statistic: aws_alarm.statistic.map(|s| s.as_str().to_string()),
                            extended_statistic: aws_alarm.extended_statistic,
                            dimensions,
                            period: aws_alarm.period.map(|p| p as i64),
                            unit: aws_alarm.unit.map(|u| u.as_str().to_string()),
                            evaluation_periods: aws_alarm.evaluation_periods.unwrap_or(1) as i64,
                            datapoints_to_alarm: aws_alarm.datapoints_to_alarm.map(|d| d as i64),
                            threshold: aws_alarm.threshold,
                            comparison_operator: aws_alarm
                                .comparison_operator
                                .map(|c| c.as_str().to_string())
                                .unwrap_or_default(),
                            treat_missing_data: aws_alarm.treat_missing_data,
                            evaluate_low_sample_count_percentile: aws_alarm.evaluate_low_sample_count_percentile,
                            metrics: None, // TODO: Handle composite alarms
                            alarm_actions: aws_alarm
                                .alarm_actions
                                .map(|actions| actions.into_iter().map(|arn| AlarmAction { arn }).collect()),
                            ok_actions: aws_alarm
                                .ok_actions
                                .map(|actions| actions.into_iter().map(|arn| AlarmAction { arn }).collect()),
                            insufficient_data_actions: aws_alarm
                                .insufficient_data_actions
                                .map(|actions| actions.into_iter().map(|arn| AlarmAction { arn }).collect()),
                            tags: Tags::default(), // TODO: Fetch tags
                        };

                        get_resource_output!(
                            CloudWatchResource::Alarm(alarm),
                            [(String::from("alarm_name"), Some(alarm_name))]
                        )
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }

            CloudWatchResourceAddress::Dashboard(region, dashboard_name) => {
                let client = self.get_or_init_client(&region).await?;

                let dashboard_response = client.get_dashboard().dashboard_name(&dashboard_name).send().await;

                match dashboard_response {
                    Ok(response) => {
                        if let Some(dashboard_body) = response.dashboard_body {
                            let body: ron::Value = serde_json::from_str(&dashboard_body)
                                .map_err(|e| anyhow::anyhow!("Failed to parse dashboard body: {}", e))?;

                            let dashboard = Dashboard {
                                name: dashboard_name.clone(),
                                body,
                                tags: Tags::default(), // TODO: Fetch tags
                            };

                            get_resource_output!(
                                CloudWatchResource::Dashboard(dashboard),
                                [(String::from("dashboard_name"), Some(dashboard_name))]
                            )
                        } else {
                            Ok(None)
                        }
                    }
                    Err(_) => Ok(None),
                }
            }

            CloudWatchResourceAddress::LogGroup(region, log_group_name) => {
                let logs_client = aws_sdk_cloudwatchlogs::Client::new(
                    &aws_config::defaults(aws_config::BehaviorVersion::latest())
                        .region(aws_config::Region::new(region.clone()))
                        .load()
                        .await,
                );

                let log_group_response = logs_client
                    .describe_log_groups()
                    .log_group_name_prefix(&log_group_name)
                    .send()
                    .await?;

                if let Some(log_groups) = log_group_response.log_groups {
                    if let Some(aws_log_group) = log_groups
                        .into_iter()
                        .find(|lg| lg.log_group_name.as_ref() == Some(&log_group_name))
                    {
                        let retention_policy = aws_log_group
                            .retention_in_days
                            .map(|days| RetentionPolicy { retention_in_days: days });

                        let log_group = LogGroup {
                            name: log_group_name.clone(),
                            retention_policy,
                            kms_key_id: aws_log_group.kms_key_id,
                            metric_filters: None,  // TODO: Fetch metric filters
                            tags: Tags::default(), // TODO: Fetch tags
                        };

                        get_resource_output!(
                            CloudWatchResource::LogGroup(log_group),
                            [(String::from("log_group_name"), Some(log_group_name))]
                        )
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }

            CloudWatchResourceAddress::LogStream(region, log_group_name, log_stream_name) => {
                let logs_client = aws_sdk_cloudwatchlogs::Client::new(
                    &aws_config::defaults(aws_config::BehaviorVersion::latest())
                        .region(aws_config::Region::new(region.clone()))
                        .load()
                        .await,
                );

                let log_stream_response = logs_client
                    .describe_log_streams()
                    .log_group_name(&log_group_name)
                    .log_stream_name_prefix(&log_stream_name)
                    .send()
                    .await?;

                if let Some(log_streams) = log_stream_response.log_streams {
                    if let Some(_aws_log_stream) = log_streams
                        .into_iter()
                        .find(|ls| ls.log_stream_name.as_ref() == Some(&log_stream_name))
                    {
                        let log_stream = LogStream {
                            name: log_stream_name.clone(),
                            log_group_name: log_group_name.clone(),
                        };

                        get_resource_output!(
                            CloudWatchResource::LogStream(log_stream),
                            [(String::from("log_stream_name"), Some(log_stream_name))]
                        )
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }

            CloudWatchResourceAddress::Metric(region, namespace, metric_name) => {
                let client = self.get_or_init_client(&region).await?;

                let metrics_response = client
                    .list_metrics()
                    .namespace(&namespace)
                    .metric_name(&metric_name)
                    .send()
                    .await?;

                if let Some(metrics) = metrics_response.metrics {
                    if let Some(aws_metric) = metrics.into_iter().next() {
                        let dimensions = aws_metric.dimensions.map(|dims| {
                            dims.into_iter()
                                .filter_map(|d| {
                                    if let (Some(name), Some(value)) = (d.name, d.value) {
                                        Some(Dimension { name, value })
                                    } else {
                                        None
                                    }
                                })
                                .collect()
                        });

                        let metric = Metric {
                            namespace: namespace.clone(),
                            name: metric_name.clone(),
                            dimensions,
                            stat_options: None,
                            tags: Tags::default(),
                        };

                        get_resource_output!(
                            CloudWatchResource::Metric(metric),
                            [(String::from("metric_name"), Some(metric_name))]
                        )
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }

            CloudWatchResourceAddress::EventRule(region, rule_name) => {
                let events_client = aws_sdk_eventbridge::Client::new(
                    &aws_config::defaults(aws_config::BehaviorVersion::latest())
                        .region(aws_config::Region::new(region.clone()))
                        .load()
                        .await,
                );

                let rule_response = events_client.describe_rule().name(&rule_name).send().await;

                match rule_response {
                    Ok(response) => {
                        let event_pattern = response.event_pattern.map(|pattern| {
                            let pattern_value: ron::Value =
                                serde_json::from_str(&pattern).unwrap_or_else(|_| ron::Value::String(pattern));
                            EventPattern { pattern: pattern_value }
                        });

                        let rule = EventRule {
                            name: rule_name.clone(),
                            description: response.description,
                            schedule_expression: response.schedule_expression,
                            event_pattern,
                            state: response
                                .state
                                .map(|s| s.as_str().to_string())
                                .unwrap_or_else(|| "ENABLED".to_string()),
                            targets: None, // TODO: Fetch targets
                            role_arn: response.role_arn,
                            tags: Tags::default(), // TODO: Fetch tags
                        };

                        get_resource_output!(
                            CloudWatchResource::EventRule(rule),
                            [(String::from("rule_name"), Some(rule_name))]
                        )
                    }
                    Err(_) => Ok(None),
                }
            }
        }
    }
}
