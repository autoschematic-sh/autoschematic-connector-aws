use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsConnectorConfig;
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
    pub account_id: Mutex<Option<String>>,
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
        let config_file = AwsConnectorConfig::try_load(&self.prefix)?;

        let region_provider = RegionProviderChain::default_provider();

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
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

        let sts_region = RegionProviderChain::first_try(aws_sdk_sts::config::Region::new("us-east-1".to_owned()));
        let sts_config = aws_config::defaults(BehaviorVersion::latest())
            .region(sts_region)
            .load()
            .await;

        let sts_client = aws_sdk_sts::Client::new(&sts_config);

        let caller_identity = sts_client.get_caller_identity().send().await;
        match caller_identity {
            Ok(caller_identity) => {
                let Some(account_id) = caller_identity.account else {
                    bail!("Failed to get current account ID!");
                };

                if let Some(config_file) = config_file {
                    if config_file.account_id != account_id {
                        bail!(
                            "Credentials do not match configured account id: creds = {}, aws/config.ron = {}",
                            account_id,
                            config_file.account_id
                        );
                    }
                }

                let rds_config: RdsConnectorConfig = RdsConnectorConfig::try_load(&self.prefix)?.unwrap_or_default();

                *self.client_cache.lock().await = HashMap::new();
                *self.account_id.lock().await = Some(account_id);
                *self.config.lock().await = rds_config;
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to call sts:GetCallerIdentity: {}", e);
                Err(e.into())
            }
        }
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
        current: Option<OsString>,
        desired: Option<OsString>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn eq(&self, addr: &Path, a: &OsStr, b: &OsStr) -> Result<bool, anyhow::Error> {
        let addr = RdsResourceAddress::from_path(addr)?;

        match addr {
            RdsResourceAddress::DBInstance { region, id } => ron_check_eq::<RdsDBInstance>(a, b),
            RdsResourceAddress::DBCluster { region, id } => ron_check_eq::<RdsDBCluster>(a, b),
            RdsResourceAddress::DBSubnetGroup { region, name } => ron_check_eq::<RdsDBSubnetGroup>(a, b),
            RdsResourceAddress::DBParameterGroup { region, name } => ron_check_eq::<RdsDBParameterGroup>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &OsStr) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = RdsResourceAddress::from_path(addr)?;

        match addr {
            RdsResourceAddress::DBInstance { region, id } => ron_check_syntax::<RdsDBInstance>(a),
            RdsResourceAddress::DBCluster { region, id } => ron_check_syntax::<RdsDBCluster>(a),
            RdsResourceAddress::DBSubnetGroup { region, name } => ron_check_syntax::<RdsDBSubnetGroup>(a),
            RdsResourceAddress::DBParameterGroup { region, name } => ron_check_syntax::<RdsDBParameterGroup>(a),
        }
    }
}
