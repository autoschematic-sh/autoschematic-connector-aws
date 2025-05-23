use std::{ffi::OsString, path::Path};
use std::collections::HashMap;

use crate::addr::RdsResourceAddress;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, OpPlanOutput, ResourceAddress,
    },
    connector_op,
    util::{diff_ron_values, optional_string_from_utf8, RON},
};
use crate::resource::{RdsDBCluster,  RdsDBInstance, RdsDBParameterGroup, RdsDBSubnetGroup};

use super::RdsConnector;

impl RdsConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<OsString>,
        desired: Option<OsString>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;
        
        let addr = RdsResourceAddress::from_path(addr)?;
        match addr {
            RdsResourceAddress::DBInstance { region, id } => todo!(),
            RdsResourceAddress::DBCluster { region, id } => todo!(),
            RdsResourceAddress::DBSubnetGroup { region, name } => todo!(),
            RdsResourceAddress::DBParameterGroup { region, name } => todo!(),
        }
    }
}