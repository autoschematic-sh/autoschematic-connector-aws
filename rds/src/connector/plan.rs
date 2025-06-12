use std::path::Path;

use crate::addr::RdsResourceAddress;
use autoschematic_core::{
    connector::{OpPlanOutput, ResourceAddress},
    util::optional_string_from_utf8,
};

use super::RdsConnector;

impl RdsConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
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
