use crate::addr::CloudFrontResourceAddress;
use crate::op::CloudFrontConnectorOp;
use crate::resource::{
    CacheBehavior, CachePolicy, CloudFrontResource, Distribution, Function, KeyGroup, OriginAccessControl, PublicKey,
    StreamingDistribution, TtlSettings,
};

mod get;
mod list;
mod op_exec;
mod plan;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::config::CloudFrontConnectorConfig;
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
use autoschematic_core::connector::VirtToPhyOutput;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource,
        ResourceAddress, SkeletonOutput,
    },
    diag::DiagnosticOutput,
    skeleton,
};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct CloudFrontConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_cloudfront::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<CloudFrontConnectorConfig>,
    prefix: PathBuf,
}

impl CloudFrontConnector {
    pub async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_cloudfront::Client>> {
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
            let client = aws_sdk_cloudfront::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }

    pub async fn get_resource_arn(&self, addr: &CloudFrontResourceAddress) -> anyhow::Result<String> {
        match addr {
            CloudFrontResourceAddress::Distribution { distribution_id } => Ok(format!(
                "arn:aws:cloudfront::{}:distribution/{}",
                self.account_id.lock().await,
                distribution_id
            )),
            CloudFrontResourceAddress::OriginAccessControl { oac_id } => Ok(format!(
                "arn:aws:cloudfront::{}:originaccesscontrol/{}",
                self.account_id.lock().await,
                oac_id
            )),
            CloudFrontResourceAddress::CachePolicy { policy_id } => Ok(format!(
                "arn:aws:cloudfront::{}:policy/{}",
                self.account_id.lock().await,
                policy_id
            )),
            CloudFrontResourceAddress::OriginRequestPolicy { policy_id } => Ok(format!(
                "arn:aws:cloudfront::{}:originrequestpolicy/{}",
                self.account_id.lock().await,
                policy_id
            )),
            CloudFrontResourceAddress::ResponseHeadersPolicy { policy_id } => Ok(format!(
                "arn:aws:cloudfront::{}:responseheaderspolicy/{}",
                self.account_id.lock().await,
                policy_id
            )),
            CloudFrontResourceAddress::RealtimeLogConfig { name } => Ok(format!(
                "arn:aws:cloudfront::{}:realtimelogconfig/{}",
                self.account_id.lock().await,
                name
            )),
            CloudFrontResourceAddress::Function { name } => Ok(format!(
                "arn:aws:cloudfront::{}:function/{}",
                self.account_id.lock().await,
                name
            )),
            CloudFrontResourceAddress::KeyGroup { key_group_id } => Ok(format!(
                "arn:aws:cloudfront::{}:keygroup/{}",
                self.account_id.lock().await,
                key_group_id
            )),
            CloudFrontResourceAddress::PublicKey { public_key_id } => Ok(format!(
                "arn:aws:cloudfront::{}:publickey/{}",
                self.account_id.lock().await,
                public_key_id
            )),
            CloudFrontResourceAddress::FieldLevelEncryptionConfig { config_id } => Ok(format!(
                "arn:aws:cloudfront::{}:fieldlevelencryptionconfig/{}",
                self.account_id.lock().await,
                config_id
            )),
            CloudFrontResourceAddress::FieldLevelEncryptionProfile { profile_id } => Ok(format!(
                "arn:aws:cloudfront::{}:fieldlevelencryptionprofile/{}",
                self.account_id.lock().await,
                profile_id
            )),
            CloudFrontResourceAddress::StreamingDistribution { distribution_id } => Ok(format!(
                "arn:aws:cloudfront::{}:streamingdistribution/{}",
                self.account_id.lock().await,
                distribution_id
            )),
        }
    }

    pub async fn get_tags_for_resource(
        &self,
        addr: &CloudFrontResourceAddress,
        client: Arc<aws_sdk_cloudfront::Client>,
    ) -> anyhow::Result<HashMap<String, String>> {
        let tag_list = client
            .list_tags_for_resource()
            .resource(self.get_resource_arn(addr).await?)
            .send()
            .await?;

        let mut tags = HashMap::new();
        if let Some(tag_list) = tag_list.tags.and_then(|t| t.items) {
            for tag in tag_list {
                if let Some(value) = tag.value {
                    tags.insert(tag.key, value);
                }
            }
        }
        Ok(tags)
    }
}

#[async_trait]
impl Connector for CloudFrontConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_) = CloudFrontResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(CloudFrontConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let config: CloudFrontConnectorConfig = CloudFrontConnectorConfig::try_load(&self.prefix).await?;

        let account_id = config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = config;
        *self.account_id.lock().await = account_id;
        Ok(())
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    // async fn addr_virt_to_phy(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
    //     let Some(addr) = CloudFrontResourceAddress::from_path(addr)? else {
    //         return Ok(None);
    //     };

    //     let Some(outputs) = get_outputs(&self.prefix, &addr)? else {
    //         return Ok(None);
    //     };

    //     match addr {
    //         CloudFrontResourceAddress::Secret(region, secret_name) => {
    //             let secret_name = get_output_or_bail(&outputs, "secret_name")?;
    //             Ok(Some(
    //                 CloudFrontResourceAddress::Secret(region, secret_name).to_path_buf(),
    //             ))
    //         }
    //         CloudFrontResourceAddress::SecretPolicy(region, secret_name) => {
    //             let Some(secret_outputs) = get_outputs(
    //                 &self.prefix,
    //                 &CloudFrontResourceAddress::Secret(region.clone(), secret_name),
    //             )?
    //             else {
    //                 return Ok(None);
    //             };

    //             let secret_name = get_output_or_bail(&secret_outputs, "secret_name")?;
    //             Ok(Some(
    //                 CloudFrontResourceAddress::Secret(region, secret_name).to_path_buf(),
    //             ))
    //         }
    //         _ => Ok(Some(addr.to_path_buf())),
    //     }
    // }

    // async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
    //     let Some(addr) = CloudFrontResourceAddress::from_path(addr)? else {
    //         return Ok(None);
    //     };

    //     match &addr {
    //         CloudFrontResourceAddress::Secret(_, _) => {
    //             if let Some(secret_addr) = output_phy_to_virt(&self.prefix, &addr)? {
    //                 return Ok(Some(secret_addr.to_path_buf()));
    //             }
    //         }
    //         CloudFrontResourceAddress::SecretPolicy(_, _) => {
    //             if let Some(secret_addr) = output_phy_to_virt(&self.prefix, &addr)? {
    //                 return Ok(Some(secret_addr.to_path_buf()));
    //             }
    //         }
    //         _ => {
    //             return Ok(Some(addr.to_path_buf()));
    //         }
    //     }
    //     Ok(Some(addr.to_path_buf()))
    // }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        // CloudFront Distribution
        let distribution_id = String::from("[distribution_id]");
        res.push(skeleton!(
            CloudFrontResourceAddress::Distribution { distribution_id },
            CloudFrontResource::Distribution(Distribution {
                domain_name: String::from("[domain_name]"),
                enabled: true,
                default_root_object: Some(String::from("index.html")),
                origins: vec![],
                default_cache_behavior: CacheBehavior {
                    path_pattern: None,
                    target_origin_id: String::from("[origin_id]"),
                    viewer_protocol_policy: String::from("redirect-to-https"),
                    allowed_methods: vec![String::from("GET"), String::from("HEAD")],
                    cached_methods: vec![String::from("GET"), String::from("HEAD")],
                    compress: true,
                    ttl_settings: TtlSettings {
                        default_ttl: Some(86400),
                        max_ttl:     Some(31536000),
                        min_ttl:     0,
                    },
                },
                cache_behaviors: vec![],
                comment: Some(String::from("[comment]")),
                price_class: Some(String::from("PriceClass_All")),
                tags: std::collections::HashMap::new(),
            })
        ));

        // Origin Access Control
        let oac_id = String::from("[oac_id]");
        res.push(skeleton!(
            CloudFrontResourceAddress::OriginAccessControl { oac_id },
            CloudFrontResource::OriginAccessControl(OriginAccessControl {
                name: String::from("[oac_name]"),
                description: Some(String::from("[description]")),
                origin_access_control_origin_type: String::from("s3"),
                signing_behavior: String::from("always"),
                signing_protocol: String::from("sigv4"),
            })
        ));

        // Cache Policy
        let policy_id = String::from("[cache_policy_id]");
        res.push(skeleton!(
            CloudFrontResourceAddress::CachePolicy { policy_id },
            CloudFrontResource::CachePolicy(CachePolicy {
                name: String::from("[cache_policy_name]"),
                comment: Some(String::from("[comment]")),
                default_ttl: Some(86400),
                max_ttl: Some(31536000),
                min_ttl: 0,
                parameters_in_cache_key_and_forwarded_to_origin: None,
            })
        ));

        // CloudFront Function
        let name = String::from("[function_name]");
        res.push(skeleton!(
            CloudFrontResourceAddress::Function { name },
            CloudFrontResource::Function(Function {
                name: String::from("[function_name]"),
                function_code: String::from("function handler(event) { return event.request; }"),
                runtime: String::from("cloudfront-js-1.0"),
            })
        ));

        // Key Group
        let key_group_id = String::from("[key_group_id]");
        res.push(skeleton!(
            CloudFrontResourceAddress::KeyGroup { key_group_id },
            CloudFrontResource::KeyGroup(KeyGroup {
                name:    String::from("[key_group_name]"),
                comment: Some(String::from("[comment]")),
                items:   vec![String::from("[public_key_id]")],
            })
        ));

        // Public Key
        let public_key_id = String::from("[public_key_id]");
        res.push(skeleton!(
            CloudFrontResourceAddress::PublicKey { public_key_id },
            CloudFrontResource::PublicKey(PublicKey {
                name: String::from("[public_key_name]"),
                comment: Some(String::from("[comment]")),
                encoded_key: String::from("[base64_encoded_public_key]"),
            })
        ));

        // Streaming Distribution
        let distribution_id = String::from("[streaming_distribution_id]");
        res.push(skeleton!(
            CloudFrontResourceAddress::StreamingDistribution { distribution_id },
            CloudFrontResource::StreamingDistribution(StreamingDistribution {
                domain_name: String::from("[domain_name]"),
                enabled: true,
                comment: Some(String::from("[comment]")),
                s3_origin: crate::resource::S3Origin {
                    domain_name: String::from("[s3_bucket_domain_name]"),
                    origin_access_identity: String::from("[origin_access_identity]"),
                },
                trusted_signers: None,
                price_class: Some(String::from("PriceClass_All")),
                tags: std::collections::HashMap::new(),
            })
        ));

        Ok(res)
    }

    async fn addr_virt_to_phy(&self, addr: &Path) -> anyhow::Result<VirtToPhyOutput> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;

        match &addr {
            CloudFrontResourceAddress::Distribution { .. } => {
                if let Some(distribution_id) = addr.get_output(&self.prefix, "distribution_id")? {
                    Ok(VirtToPhyOutput::Present(
                        CloudFrontResourceAddress::Distribution { distribution_id }.to_path_buf(),
                    ))
                } else {
                    Ok(VirtToPhyOutput::NotPresent)
                }
            }
            CloudFrontResourceAddress::OriginAccessControl { oac_id } => todo!(),
            CloudFrontResourceAddress::CachePolicy { policy_id } => todo!(),
            CloudFrontResourceAddress::OriginRequestPolicy { policy_id } => todo!(),
            CloudFrontResourceAddress::ResponseHeadersPolicy { policy_id } => todo!(),
            CloudFrontResourceAddress::RealtimeLogConfig { name } => todo!(),
            CloudFrontResourceAddress::Function { name } => todo!(),
            CloudFrontResourceAddress::KeyGroup { key_group_id } => todo!(),
            CloudFrontResourceAddress::PublicKey { public_key_id } => todo!(),
            CloudFrontResourceAddress::FieldLevelEncryptionConfig { config_id } => todo!(),
            CloudFrontResourceAddress::FieldLevelEncryptionProfile { profile_id } => todo!(),
            CloudFrontResourceAddress::StreamingDistribution { distribution_id } => todo!(),
        }
    }

    async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;

        match &addr {
            CloudFrontResourceAddress::Distribution { .. } => {
                if let Some(virt_addr) = addr.phy_to_virt(&self.prefix)? {
                    return Ok(Some(virt_addr.to_path_buf()));
                }
            }
            _ => todo!(),
        }
        Ok(None)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;

        match addr {
            CloudFrontResourceAddress::Distribution { .. } => Ok(a == b),
            CloudFrontResourceAddress::OriginAccessControl { .. } => Ok(a == b),
            CloudFrontResourceAddress::CachePolicy { .. } => Ok(a == b),
            CloudFrontResourceAddress::OriginRequestPolicy { .. } => Ok(a == b),
            CloudFrontResourceAddress::ResponseHeadersPolicy { .. } => Ok(a == b),
            CloudFrontResourceAddress::RealtimeLogConfig { .. } => Ok(a == b),
            CloudFrontResourceAddress::Function { .. } => Ok(a == b),
            CloudFrontResourceAddress::KeyGroup { .. } => Ok(a == b),
            CloudFrontResourceAddress::PublicKey { .. } => Ok(a == b),
            CloudFrontResourceAddress::FieldLevelEncryptionConfig { .. } => Ok(a == b),
            CloudFrontResourceAddress::FieldLevelEncryptionProfile { .. } => Ok(a == b),
            CloudFrontResourceAddress::StreamingDistribution { .. } => Ok(a == b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;
        match addr {
            CloudFrontResourceAddress::Distribution { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::OriginAccessControl { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::CachePolicy { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::OriginRequestPolicy { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::ResponseHeadersPolicy { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::RealtimeLogConfig { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::Function { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::KeyGroup { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::PublicKey { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::FieldLevelEncryptionConfig { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::FieldLevelEncryptionProfile { .. } => Ok(DiagnosticOutput::default()),
            CloudFrontResourceAddress::StreamingDistribution { .. } => Ok(DiagnosticOutput::default()),
        }
    }
}
