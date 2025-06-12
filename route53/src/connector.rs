use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use addr::Route53ResourceAddress;
use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource, ResourceAddress,
        SkeletonOutput,
    },
    diag::DiagnosticOutput,
    skeleton,
    util::{optional_string_from_utf8, ron_check_eq, ron_check_syntax},
};
use resource::{HealthCheck, HostedZone, RecordSet, Route53Resource};

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use aws_sdk_route53::config::Region;
use tokio::sync::Mutex;

pub mod get;
pub mod list;
pub mod op_exec;
pub mod plan;

use crate::{addr, resource};

#[derive(Default)]
pub struct Route53Connector {
    prefix: PathBuf,
    client: Mutex<Option<aws_sdk_route53::Client>>,
}

#[async_trait]
impl Connector for Route53Connector {
    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_addr) = Route53ResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(Route53Connector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let region = RegionProviderChain::first_try(Region::new("global".to_owned()));

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

        *self.client.lock().await = Some(aws_sdk_route53::Client::new(&config));

        Ok(())
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
        self.do_plan(addr, optional_string_from_utf8(current)?, optional_string_from_utf8(desired)?)
            .await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }
    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        tracing::error!("route53::get_skeletons");
        // res.push(skeleton!(Route53ResourceAddress::HealthCheck(String::from("[name]")), Route53Resource::HealthCheck(HealthCheck {})));
        res.push(skeleton!(
            Route53ResourceAddress::HostedZone(String::from("[domain_name]")),
            Route53Resource::HostedZone(HostedZone {})
        ));
        tracing::error!("route53::get_skeletons");

        res.push(skeleton!(
            Route53ResourceAddress::ResourceRecordSet(
                String::from("[domain_name]"),
                String::from("[record_name]"),
                String::from("[type]")
            ),
            Route53Resource::RecordSet(RecordSet {
                ttl: Some(600),
                alias_target: None,
                resource_records: Some(vec!["record text goes here".into()]),
            })
        ));
        tracing::error!("route53::get_skeletons");

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = Route53ResourceAddress::from_path(addr)?;

        match addr {
            Route53ResourceAddress::HostedZone(_) => ron_check_eq::<HostedZone>(a, b),
            Route53ResourceAddress::ResourceRecordSet(_, _, _) => ron_check_eq::<RecordSet>(a, b),
            Route53ResourceAddress::HealthCheck(_) => ron_check_eq::<HealthCheck>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = Route53ResourceAddress::from_path(addr)?;

        match addr {
            Route53ResourceAddress::HostedZone(_) => ron_check_syntax::<HostedZone>(a),
            Route53ResourceAddress::ResourceRecordSet(_, _, _) => ron_check_syntax::<RecordSet>(a),
            Route53ResourceAddress::HealthCheck(_) => ron_check_syntax::<HealthCheck>(a),
        }
    }
}
