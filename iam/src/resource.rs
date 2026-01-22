use std::collections::HashSet;

use autoschematic_core::connector::{Resource, ResourceAddress};
use autoschematic_core::macros::FieldTypes;
use autoschematic_macros::FieldTypes;
use documented::{Documented, DocumentedFields};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::{PrettyConfig, RON};

use super::addr::IamResourceAddress;
use super::tags::Tags;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Documented, DocumentedFields, FieldTypes)]
#[serde(deny_unknown_fields)]
/// An IAM user is an identity that you create in AWS. The user represents the person or application that uses it to interact with AWS.
pub struct IamUser {
    /// The set of IAM policies attached to the user, by ARN.
    pub attached_policies: HashSet<String>,
    /// A set of key-value pairs to apply to the user.
    pub tags: Tags,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Documented, DocumentedFields, FieldTypes)]
#[serde(deny_unknown_fields)]
/// An IAM role is an IAM identity that you can create in your account that has specific permissions. A role is intended to be assumable by anyone who needs it.
pub struct IamRole {
    /// The set of IAM policies attached to the role, by ARN.
    pub attached_policies: HashSet<String>,
    /// The AssumeRolePolicyDocument defines who is allowed to assume the role. For more information, see [https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_permissions-to-switch.html]
    pub assume_role_policy_document: Option<ron::Value>,
    /// A set of key-value pairs to apply to the role.
    pub tags: Tags,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Documented, DocumentedFields, FieldTypes)]
#[serde(deny_unknown_fields)]
/// An IAM policy is an entity that, when attached to an identity or resource, defines their permissions.
pub struct IamPolicy {
    /// The JSON policy document that defines the permissions for the policy.
    pub policy_document: ron::Value,
    /// A set of key-value pairs to apply to the policy.
    pub tags: Tags,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Documented, DocumentedFields, FieldTypes)]
#[serde(deny_unknown_fields)]
/// An IAM group is a collection of IAM users. Groups let you specify permissions for multiple users, which can make it easier to manage the permissions for those users.
pub struct IamGroup {
    /// The set of IAM policies attached to the group, by ARN.
    pub attached_policies: HashSet<String>,
    /// The set of users in the group.
    pub users: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum IamResource {
    User(IamUser),
    Role(IamRole),
    Group(IamGroup),
    Policy(IamPolicy),
}

impl Resource for IamResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        match self {
            IamResource::User(user) => match RON.to_string_pretty(&user, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            IamResource::Role(role) => match RON.to_string_pretty(&role, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            IamResource::Group(group) => match RON.to_string_pretty(&group, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            IamResource::Policy(policy) => match RON.to_string_pretty(&policy, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = IamResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;
        match addr {
            IamResourceAddress::User { .. } => Ok(IamResource::User(RON.from_str(s)?)),
            IamResourceAddress::Role { .. } => Ok(IamResource::Role(RON.from_str(s)?)),
            IamResourceAddress::Group { .. } => Ok(IamResource::Group(RON.from_str(s)?)),
            IamResourceAddress::Policy { .. } => Ok(IamResource::Policy(RON.from_str(s)?)),
        }
    }
}
