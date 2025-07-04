use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

type Region = String;
type ApiId = String;

#[derive(Debug, Clone)]
pub enum ApiGatewayV2ResourceAddress {
    Api {
        region: Region,
        api_id: ApiId,
    },
    Route {
        region:   Region,
        api_id:   ApiId,
        route_id: String,
    },
    Integration {
        region: Region,
        api_id: ApiId,
        integration_id: String,
    },
    Stage {
        region:     Region,
        api_id:     ApiId,
        stage_name: String,
    },
    Authorizer {
        region: Region,
        api_id: ApiId,
        authorizer_id: String,
    },
}

impl ResourceAddress for ApiGatewayV2ResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match self {
            ApiGatewayV2ResourceAddress::Api { region, api_id } => {
                PathBuf::from(format!("aws/apigatewayv2/{region}/apis/{api_id}.ron"))
            }
            ApiGatewayV2ResourceAddress::Route {
                region,
                api_id,
                route_id,
            } => PathBuf::from(format!("aws/apigatewayv2/{region}/apis/{api_id}/routes/{route_id}.ron")),
            ApiGatewayV2ResourceAddress::Integration {
                region,
                api_id,
                integration_id,
            } => PathBuf::from(format!(
                "aws/apigatewayv2/{region}/apis/{api_id}/integrations/{integration_id}.ron"
            )),
            ApiGatewayV2ResourceAddress::Stage {
                region,
                api_id,
                stage_name,
            } => PathBuf::from(format!(
                "aws/apigatewayv2/{region}/apis/{api_id}/stages/{stage_name}.ron"
            )),
            ApiGatewayV2ResourceAddress::Authorizer {
                region,
                api_id,
                authorizer_id,
            } => PathBuf::from(format!(
                "aws/apigatewayv2/{region}/apis/{api_id}/authorizers/{authorizer_id}.ron"
            )),
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "apigatewayv2", region, "apis", api_id] if api_id.ends_with(".ron") => {
                let api_id = api_id.strip_suffix(".ron").unwrap().to_string();
                Ok(ApiGatewayV2ResourceAddress::Api {
                    region: region.to_string(),
                    api_id,
                })
            }
            ["aws", "apigatewayv2", region, "apis", api_id, "routes", route_id] if route_id.ends_with(".ron") => {
                let route_id = route_id.strip_suffix(".ron").unwrap().to_string();
                Ok(ApiGatewayV2ResourceAddress::Route {
                    region: region.to_string(),
                    api_id: api_id.to_string(),
                    route_id,
                })
            }
            ["aws", "apigatewayv2", region, "apis", api_id, "integrations", integration_id]
                if integration_id.ends_with(".ron") =>
            {
                let integration_id = integration_id.strip_suffix(".ron").unwrap().to_string();
                Ok(ApiGatewayV2ResourceAddress::Integration {
                    region: region.to_string(),
                    api_id: api_id.to_string(),
                    integration_id,
                })
            }
            ["aws", "apigatewayv2", region, "apis", api_id, "stages", stage_name] if stage_name.ends_with(".ron") => {
                let stage_name = stage_name.strip_suffix(".ron").unwrap().to_string();
                Ok(ApiGatewayV2ResourceAddress::Stage {
                    region: region.to_string(),
                    api_id: api_id.to_string(),
                    stage_name,
                })
            }
            ["aws", "apigatewayv2", region, "apis", api_id, "authorizers", authorizer_id]
                if authorizer_id.ends_with(".ron") =>
            {
                let authorizer_id = authorizer_id.strip_suffix(".ron").unwrap().to_string();
                Ok(ApiGatewayV2ResourceAddress::Authorizer {
                    region: region.to_string(),
                    api_id: api_id.to_string(),
                    authorizer_id,
                })
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
