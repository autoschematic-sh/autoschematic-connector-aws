pub use crate::addr::CloudWatchResourceAddress;
use crate::{config, resource};

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
use autoschematic_core::{
    connector::{Connector, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, ResourceAddress},
    diag::DiagnosticOutput,
    util::{ron_check_eq, ron_check_syntax},
};
use config::CloudWatchConnectorConfig;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct CloudWatchConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_cloudwatch::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<CloudWatchConnectorConfig>,
    prefix: PathBuf,
}

#[async_trait]
impl Connector for CloudWatchConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_addr) = CloudWatchResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(CloudWatchConnector {
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

    async fn list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        todo!()
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        todo!()
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        todo!()
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        todo!()
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

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
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
