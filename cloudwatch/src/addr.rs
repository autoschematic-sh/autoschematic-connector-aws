use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

type Region = String;
type AlarmName = String;
type DashboardName = String;
type LogGroupName = String;
type LogStreamName = String;
type MetricName = String;
type Namespace = String;
type EventRuleName = String;

#[derive(Debug, Clone)]
pub enum CloudWatchResourceAddress {
    Alarm(Region, AlarmName),
    Dashboard(Region, DashboardName),
    LogGroup(Region, LogGroupName),
    LogStream(Region, LogGroupName, LogStreamName),
    Metric(Region, Namespace, MetricName),
    EventRule(Region, EventRuleName),
}

impl ResourceAddress for CloudWatchResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            CloudWatchResourceAddress::Alarm(region, name) => {
                PathBuf::from(format!("aws/cloudwatch/{region}/alarms/{name}.ron"))
            }
            CloudWatchResourceAddress::Dashboard(region, name) => {
                PathBuf::from(format!("aws/cloudwatch/{region}/dashboards/{name}.ron"))
            }
            CloudWatchResourceAddress::LogGroup(region, name) => {
                PathBuf::from(format!("aws/cloudwatch/{region}/log_groups/{name}.ron"))
            }
            CloudWatchResourceAddress::LogStream(region, group_name, stream_name) => PathBuf::from(format!(
                "aws/cloudwatch/{region}/log_groups/{group_name}/streams/{stream_name}.ron"
            )),
            CloudWatchResourceAddress::Metric(region, namespace, name) => {
                PathBuf::from(format!("aws/cloudwatch/{region}/metrics/{namespace}/{name}.ron"))
            }
            CloudWatchResourceAddress::EventRule(region, name) => {
                PathBuf::from(format!("aws/cloudwatch/{region}/event_rules/{name}.ron"))
            }
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "cloudwatch", region, "alarms", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudWatchResourceAddress::Alarm(region.to_string(), name))
            }
            ["aws", "cloudwatch", region, "dashboards", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudWatchResourceAddress::Dashboard(region.to_string(), name))
            }
            ["aws", "cloudwatch", region, "log_groups", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudWatchResourceAddress::LogGroup(region.to_string(), name))
            }
            ["aws", "cloudwatch", region, "log_groups", group_name, "streams", stream_name]
                if stream_name.ends_with(".ron") =>
            {
                let stream_name = stream_name.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudWatchResourceAddress::LogStream(
                    region.to_string(),
                    group_name.to_string(),
                    stream_name,
                ))
            }
            ["aws", "cloudwatch", region, "metrics", namespace, name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudWatchResourceAddress::Metric(
                    region.to_string(),
                    namespace.to_string(),
                    name,
                ))
            }
            ["aws", "cloudwatch", region, "event_rules", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(CloudWatchResourceAddress::EventRule(region.to_string(), name))
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
