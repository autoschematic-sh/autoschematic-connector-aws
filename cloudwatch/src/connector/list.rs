use std::path::{Path, PathBuf};

use autoschematic_core::connector::ResourceAddress;

use crate::addr::CloudWatchResourceAddress;

use super::CloudWatchConnector;

impl CloudWatchConnector {
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();
        let config = self.config.lock().await;

        for region_name in &config.enabled_regions {
            let client = self.get_or_init_client(region_name).await?;

            // List CloudWatch Alarms
            let mut alarms = client.describe_alarms().into_paginator().send();
            while let Some(alarms_page) = alarms.next().await {
                if let Some(metric_alarms) = alarms_page?.metric_alarms {
                    for alarm in metric_alarms {
                        if let Some(alarm_name) = alarm.alarm_name {
                            results.push(CloudWatchResourceAddress::Alarm(region_name.clone(), alarm_name).to_path_buf());
                        }
                    }
                }
            }

            // List CloudWatch Dashboards
            let mut dashboards = client.list_dashboards().into_paginator().send();
            while let Some(dashboards_page) = dashboards.next().await {
                if let Some(dashboard_entries) = dashboards_page?.dashboard_entries {
                    for dashboard in dashboard_entries {
                        if let Some(dashboard_name) = dashboard.dashboard_name {
                            results
                                .push(CloudWatchResourceAddress::Dashboard(region_name.clone(), dashboard_name).to_path_buf());
                        }
                    }
                }
            }

            // List CloudWatch Log Groups
            let logs_client = aws_sdk_cloudwatchlogs::Client::new(
                &aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(aws_config::Region::new(region_name.clone()))
                    .load()
                    .await,
            );

            let mut log_groups = logs_client.describe_log_groups().into_paginator().send();
            while let Some(log_groups_page) = log_groups.next().await {
                if let Some(log_groups_list) = log_groups_page?.log_groups {
                    for log_group in log_groups_list {
                        if let Some(log_group_name) = log_group.log_group_name {
                            results.push(
                                CloudWatchResourceAddress::LogGroup(region_name.clone(), log_group_name.clone()).to_path_buf(),
                            );

                            // List Log Streams for each Log Group
                            let mut log_streams = logs_client
                                .describe_log_streams()
                                .log_group_name(&log_group_name)
                                .into_paginator()
                                .send();

                            while let Some(log_streams_page) = log_streams.next().await {
                                if let Some(log_streams_list) = log_streams_page?.log_streams {
                                    for log_stream in log_streams_list {
                                        if let Some(log_stream_name) = log_stream.log_stream_name {
                                            results.push(
                                                CloudWatchResourceAddress::LogStream(
                                                    region_name.clone(),
                                                    log_group_name.clone(),
                                                    log_stream_name,
                                                )
                                                .to_path_buf(),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // List CloudWatch Metrics
            let mut metrics = client.list_metrics().into_paginator().send();
            while let Some(metrics_page) = metrics.next().await {
                if let Some(metrics_list) = metrics_page?.metrics {
                    for metric in metrics_list {
                        if let (Some(namespace), Some(metric_name)) = (metric.namespace, metric.metric_name) {
                            results.push(
                                CloudWatchResourceAddress::Metric(region_name.clone(), namespace, metric_name).to_path_buf(),
                            );
                        }
                    }
                }
            }

            // List CloudWatch Events Rules (EventBridge)
            let events_client = aws_sdk_eventbridge::Client::new(
                &aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(aws_config::Region::new(region_name.clone()))
                    .load()
                    .await,
            );

            let mut next_token: Option<String> = None;
            loop {
                let mut request = events_client.list_rules();
                if let Some(token) = &next_token {
                    request = request.next_token(token);
                }

                let rules_response = request.send().await?;

                if let Some(rules_list) = rules_response.rules {
                    for rule in rules_list {
                        if let Some(rule_name) = rule.name {
                            results.push(CloudWatchResourceAddress::EventRule(region_name.clone(), rule_name).to_path_buf());
                        }
                    }
                }

                next_token = rules_response.next_token;
                if next_token.is_none() {
                    break;
                }
            }
        }

        Ok(results)
    }
}
