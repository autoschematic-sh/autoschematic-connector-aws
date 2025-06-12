use std::path::Path;

use autoschematic_core::connector::OpExecOutput;
use super::RdsConnector;

impl RdsConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        todo!();
    }
}
