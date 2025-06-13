use std::{path::PathBuf, sync::Arc, time::Duration};

use crate::{addr::ApiGatewayV2ResourceAddress, connector::ApiGatewayV2Connector};
use anyhow::bail;
use autoschematic_core::connector::ResourceAddress;
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};

impl ApiGatewayV2Connector {
    pub async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_apigatewayv2::Client>> {
        let mut cache = self.client_cache.lock().await;

        if !cache.contains_key(region_s) {
            let region = RegionProviderChain::first_try(Region::new(region_s.to_owned()));

            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .timeout_config(
                    TimeoutConfig::builder()
                        .connect_timeout(Duration::from_secs(30))
                        .operation_timeout(Duration::from_secs(30))
                        .operation_attempt_timeout(Duration::from_secs(30))
                        .read_timeout(Duration::from_secs(30))
                        .build(),
                )
                .load()
                .await;
            let client = aws_sdk_apigatewayv2::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }

    pub async fn list_apis(&self, region: &str) -> Result<Vec<PathBuf>, anyhow::Error> {
        let client = self.get_or_init_client(region).await?;
        let mut next_token: Option<String> = None;
        let mut paths = Vec::new();

        loop {
            let resp = client.get_apis().set_next_token(next_token).send().await?;

            for api in resp.items() {
                if let Some(api_id) = api.api_id() {
                    let path = ApiGatewayV2ResourceAddress::Api {
                        region: region.to_string(),
                        api_id: api_id.to_string(),
                    }
                    .to_path_buf();
                    paths.push(path);
                }
            }

            next_token = resp.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }

        Ok(paths)
    }

    pub async fn list_routes(&self, region: &str, api_id: &str) -> Result<Vec<PathBuf>, anyhow::Error> {
        let client = self.get_or_init_client(region).await?;
        let mut next_token: Option<String> = None;
        let mut paths = Vec::new();

        loop {
            let resp = client.get_routes().api_id(api_id).set_next_token(next_token).send().await?;

            for route in resp.items() {
                if let Some(route_id) = route.route_id() {
                    let path = ApiGatewayV2ResourceAddress::Route {
                        region:   region.to_string(),
                        api_id:   api_id.to_string(),
                        route_id: route_id.to_string(),
                    }
                    .to_path_buf();
                    paths.push(path);
                }
            }

            next_token = resp.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }

        Ok(paths)
    }

    pub async fn list_integrations(&self, region: &str, api_id: &str) -> Result<Vec<PathBuf>, anyhow::Error> {
        let client = self.get_or_init_client(region).await?;
        let mut next_token: Option<String> = None;
        let mut paths = Vec::new();

        loop {
            let resp = client
                .get_integrations()
                .api_id(api_id)
                .set_next_token(next_token)
                .send()
                .await?;

            for integration in resp.items() {
                if let Some(integration_id) = integration.integration_id() {
                    let path = ApiGatewayV2ResourceAddress::Integration {
                        region: region.to_string(),
                        api_id: api_id.to_string(),
                        integration_id: integration_id.to_string(),
                    }
                    .to_path_buf();
                    paths.push(path);
                }
            }

            next_token = resp.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }

        Ok(paths)
    }

    pub async fn list_stages(&self, region: &str, api_id: &str) -> Result<Vec<PathBuf>, anyhow::Error> {
        let client = self.get_or_init_client(region).await?;
        let mut next_token: Option<String> = None;
        let mut paths = Vec::new();

        loop {
            let resp = client.get_stages().api_id(api_id).set_next_token(next_token).send().await?;

            for stage in resp.items() {
                if let Some(stage_name) = stage.stage_name() {
                    let path = ApiGatewayV2ResourceAddress::Stage {
                        region:     region.to_string(),
                        api_id:     api_id.to_string(),
                        stage_name: stage_name.to_string(),
                    }
                    .to_path_buf();
                    paths.push(path);
                }
            }

            next_token = resp.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }

        Ok(paths)
    }

    pub async fn list_authorizers(&self, region: &str, api_id: &str) -> Result<Vec<PathBuf>, anyhow::Error> {
        let client = self.get_or_init_client(region).await?;
        let mut next_token: Option<String> = None;
        let mut paths = Vec::new();

        loop {
            let resp = client
                .get_authorizers()
                .api_id(api_id)
                .set_next_token(next_token)
                .send()
                .await?;

            for authorizer in resp.items() {
                if let Some(authorizer_id) = authorizer.authorizer_id() {
                    let path = ApiGatewayV2ResourceAddress::Authorizer {
                        region: region.to_string(),
                        api_id: api_id.to_string(),
                        authorizer_id: authorizer_id.to_string(),
                    }
                    .to_path_buf();
                    paths.push(path);
                }
            }

            next_token = resp.next_token().map(|s| s.to_string());
            if next_token.is_none() {
                break;
            }
        }

        Ok(paths)
    }
}
