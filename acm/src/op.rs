use autoschematic_core::connector::ConnectorOp;
use autoschematic_core::util::RON;
use serde::{Deserialize, Serialize};

use crate::{resource::AcmCertificate, tags::Tags};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AcmConnectorOp {
    RequestCertificate(AcmCertificate),
    DeleteCertificate,
    AddTags(Tags),
    RemoveTags(Vec<String>),
    UpdateTags(Tags, Tags),
}

impl ConnectorOp for AcmConnectorOp {
    fn to_string(&self) -> Result<String, anyhow::Error> {
        Ok(RON.to_string(self)?)
    }

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        Ok(RON.from_str(s)?)
    }
}
