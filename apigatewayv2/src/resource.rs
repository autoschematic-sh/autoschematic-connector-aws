use std::collections::HashMap;

use autoschematic_core::connector::{Resource, ResourceAddress};
use serde::{Deserialize, Serialize};

use super::addr::ApiGatewayV2ResourceAddress;
use autoschematic_core::util::{PrettyConfig, RON};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Api {
    pub name: String,
    pub protocol_type: String,
    pub api_endpoint: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Route {
    pub route_key: String,
    pub target:    Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Integration {
    pub integration_type: String,
    pub integration_uri:  String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Stage {
    pub stage_name: String,
    pub auto_deploy: bool,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Authorizer {
    pub authorizer_type: String,
    pub authorizer_uri:  String,
    pub identity_source: Vec<String>,
}

pub enum ApiGatewayV2Resource {
    Api(Api),
    Route(Route),
    Integration(Integration),
    Stage(Stage),
    Authorizer(Authorizer),
}

impl Resource for ApiGatewayV2Resource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        match self {
            ApiGatewayV2Resource::Api(api) => Ok(RON.to_string_pretty(&api, pretty_config)?.into()),
            ApiGatewayV2Resource::Route(route) => Ok(RON.to_string_pretty(&route, pretty_config)?.into()),
            ApiGatewayV2Resource::Integration(integration) => Ok(RON.to_string_pretty(&integration, pretty_config)?.into()),
            ApiGatewayV2Resource::Stage(stage) => Ok(RON.to_string_pretty(&stage, pretty_config)?.into()),
            ApiGatewayV2Resource::Authorizer(authorizer) => Ok(RON.to_string_pretty(&authorizer, pretty_config)?.into()),
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = ApiGatewayV2ResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;
        match addr {
            ApiGatewayV2ResourceAddress::Api { .. } => Ok(ApiGatewayV2Resource::Api(RON.from_str(s)?)),
            ApiGatewayV2ResourceAddress::Route { .. } => Ok(ApiGatewayV2Resource::Route(RON.from_str(s)?)),
            ApiGatewayV2ResourceAddress::Integration { .. } => Ok(ApiGatewayV2Resource::Integration(RON.from_str(s)?)),
            ApiGatewayV2ResourceAddress::Stage { .. } => Ok(ApiGatewayV2Resource::Stage(RON.from_str(s)?)),
            ApiGatewayV2ResourceAddress::Authorizer { .. } => Ok(ApiGatewayV2Resource::Authorizer(RON.from_str(s)?)),
        }
    }
}
