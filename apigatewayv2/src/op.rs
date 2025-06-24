use std::collections::HashMap;

use autoschematic_core::connector::ConnectorOp;
use serde::{Deserialize, Serialize};

use autoschematic_core::util::RON;

use super::resource::{Api, Authorizer, Integration, Route, Stage};

type Tags = HashMap<String, String>;

#[derive(Debug, Serialize, Deserialize)]
pub enum ApiGatewayV2ConnectorOp {
    CreateApi(Api),
    UpdateApi(Api, Api),
    UpdateApiTags(Tags, Tags),
    DeleteApi,

    CreateRoute(Route),
    UpdateRoute(Route, Route),
    DeleteRoute,

    CreateIntegration(Integration),
    UpdateIntegration(Integration, Integration),
    DeleteIntegration,

    CreateStage(Stage),
    UpdateStage(Stage, Stage),
    UpdateStageTags(Tags, Tags),
    DeleteStage,

    CreateAuthorizer(Authorizer),
    UpdateAuthorizer(Authorizer, Authorizer),
    DeleteAuthorizer,
}

impl ConnectorOp for ApiGatewayV2ConnectorOp {
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
