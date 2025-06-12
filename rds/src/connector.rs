use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use autoschematic_connector_aws_core::config::{AwsConnectorConfig, AwsServiceConfig};
use autoschematic_core::{
    connector::{Connector, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, ResourceAddress},
    diag::DiagnosticOutput,
    util::{ron_check_eq, ron_check_syntax},
};

use anyhow::bail;
use aws_config::{BehaviorVersion, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use tokio::sync::Mutex;

use crate::{
    addr::RdsResourceAddress,
    config::RdsConnectorConfig,
    resource::{RdsDBCluster, RdsDBInstance, RdsDBParameterGroup, RdsDBSubnetGroup},
};

mod get;
mod list;
mod op_exec;
mod plan;

#[derive(Default)]
pub struct RdsConnector {
    pub prefix: PathBuf,
    pub client_cache: Mutex<HashMap<String, Arc<aws_sdk_rds::Client>>>,
    pub account_id: Mutex<String>,
    pub config: Mutex<RdsConnectorConfig>,
}

#[async_trait]
impl Connector for RdsConnector {
    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        let mut conn = Self::default();

        conn.prefix = prefix.into();

        Ok(Box::new(conn))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let secrets_config: RdsConnectorConfig = RdsConnectorConfig::try_load(&self.prefix).await?;

        let account_id = secrets_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = secrets_config;
        *self.account_id.lock().await = account_id;
        Ok(())
    }

    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if addr == PathBuf::from("aws/rds/config.ron") {
            Ok(FilterOutput::Config)
        } else if let Ok(_addr) = RdsResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
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
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> Result<bool, anyhow::Error> {
        let addr = RdsResourceAddress::from_path(addr)?;

        match addr {
            RdsResourceAddress::DBInstance { region, id } => ron_check_eq::<RdsDBInstance>(a, b),
            RdsResourceAddress::DBCluster { region, id } => ron_check_eq::<RdsDBCluster>(a, b),
            RdsResourceAddress::DBSubnetGroup { region, name } => ron_check_eq::<RdsDBSubnetGroup>(a, b),
            RdsResourceAddress::DBParameterGroup { region, name } => ron_check_eq::<RdsDBParameterGroup>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = RdsResourceAddress::from_path(addr)?;

        match addr {
            RdsResourceAddress::DBInstance { region, id } => ron_check_syntax::<RdsDBInstance>(a),
            RdsResourceAddress::DBCluster { region, id } => ron_check_syntax::<RdsDBCluster>(a),
            RdsResourceAddress::DBSubnetGroup { region, name } => ron_check_syntax::<RdsDBSubnetGroup>(a),
            RdsResourceAddress::DBParameterGroup { region, name } => ron_check_syntax::<RdsDBParameterGroup>(a),
        }
    }
}
