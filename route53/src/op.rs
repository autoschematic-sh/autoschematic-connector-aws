use autoschematic_core::connector::ConnectorOp;
use serde::{Deserialize, Serialize};

use autoschematic_core::util::RON;

use super::resource::{HostedZone, RecordSet};



#[derive(Debug, Serialize, Deserialize)]
pub enum Route53ConnectorOp {
    CreateHostedZone(HostedZone),
    ModifyHostedZone(HostedZone, HostedZone),
    DeleteHostedZone,
    CreateResourceRecordSet(RecordSet),
    DeleteResourceRecordSet(RecordSet),
}

impl ConnectorOp for Route53ConnectorOp {
    fn to_string(&self) -> Result<String, anyhow::Error> {
        Ok(RON.to_string(self)?)
    }

    fn from_str(s: &str) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(RON.from_str(s)?)
    }
}