

use autoschematic_core::connector::{
    ConnectorOp,
    Resource,
};
use serde::{Deserialize, Serialize};


use autoschematic_core::util::RON;

use super::tags::Tags;
use super::resource::{IamPolicy, IamRole, IamUser};

#[derive(Debug, Serialize, Deserialize)]
pub enum IamConnectorOp {
    CreateUser(IamUser),
    UpdateUserTags(Tags, Tags),
    AttachUserPolicy(String),
    DetachUserPolicy(String),
    DeleteUser,
    CreateRole(IamRole),
    AttachRolePolicy(String),
    DetachRolePolicy(String),
    UpdateAssumeRolePolicy(Option<ron::Value>, Option<ron::Value>),
    UpdateRoleTags(Tags, Tags),
    DeleteRole,
    CreatePolicy(IamPolicy),
    UpdatePolicyDocument(ron::Value, ron::Value),
    UpdatePolicyTags(Tags, Tags),
    DeletePolicy,
}

impl ConnectorOp for IamConnectorOp {
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