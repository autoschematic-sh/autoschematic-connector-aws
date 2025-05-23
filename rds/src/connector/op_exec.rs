use std::{collections::HashMap, path::Path};

use anyhow::bail;
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    error::{AutoschematicError, AutoschematicErrorType},
    op_exec_output,
};
use super::RdsConnector;

impl RdsConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        todo!();
    }
}
