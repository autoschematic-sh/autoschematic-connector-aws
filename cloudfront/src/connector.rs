use crate::addr::CloudFrontResourceAddress;
use crate::op::CloudFrontConnectorOp;
use crate::resource::{self, CloudFrontResource};

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
use autoschematic_core::util::{ron_check_eq, ron_check_syntax};
use autoschematic_core::virt_to_phy;
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
    client:     Mutex<Option<Arc<aws_sdk_cloudfront::Client>>>,
    account_id: Mutex<String>,
    config:     Mutex<CloudFrontConnectorConfig>,
    prefix:     PathBuf,
}

impl CloudFrontConnector {
    pub async fn get_or_init_client(&self) -> anyhow::Result<Arc<aws_sdk_cloudfront::Client>> {
        // let mut client = self.client.lock().await;

        if let Some(client) = &*self.client.lock().await {
            return Ok(client.clone());
        }

        let region = RegionProviderChain::first_try(Region::new("us-east-1"));

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
        let new_client = Arc::new(aws_sdk_cloudfront::Client::new(&config));
        *self.client.lock().await = Some(new_client.clone());
        Ok(new_client)

        // Ok(*self.client.clone())
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

        // *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = config;
        *self.account_id.lock().await = account_id;
        // self.get_or_init_client();
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
            CloudFrontResource::Distribution(resource::Distribution {
                enabled: true,
                default_root_object: Some(String::from("index.html")),
                aliases: Some(vec!["example.com".into()]),
                origins: vec![],
                default_cache_behavior: resource::CacheBehavior {
                    id: String::from("default"),
                    path_pattern: None,
                    target_origin_id: String::from("[origin_id]"),
                    viewer_protocol_policy: String::from("redirect-to-https"),
                    allowed_methods: vec![String::from("GET"), String::from("HEAD")],
                    cached_methods: vec![String::from("GET"), String::from("HEAD")],
                    compress: true,
                    ttl_settings: resource::TtlSettings {
                        default_ttl: Some(86400),
                        max_ttl:     Some(31536000),
                        min_ttl:     None,
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
            CloudFrontResource::OriginAccessControl(resource::OriginAccessControl {
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
            CloudFrontResource::CachePolicy(resource::CachePolicy {
                name: String::from("[cache_policy_name]"),
                comment: Some(String::from("[comment]")),
                default_ttl: Some(86400),
                max_ttl: Some(31536000),
                min_ttl: None,
                parameters_in_cache_key_and_forwarded_to_origin: None,
            })
        ));

        // CloudFront Function
        let name = String::from("[function_name]");
        res.push(skeleton!(
            CloudFrontResourceAddress::Function { name },
            CloudFrontResource::Function(resource::Function {
                name: String::from("[function_name]"),
                function_code: String::from("function handler(event) { return event.request; }"),
                runtime: String::from("cloudfront-js-1.0"),
            })
        ));

        // Key Group
        let key_group_id = String::from("[key_group_id]");
        res.push(skeleton!(
            CloudFrontResourceAddress::KeyGroup { key_group_id },
            CloudFrontResource::KeyGroup(resource::KeyGroup {
                name:    String::from("[key_group_name]"),
                comment: Some(String::from("[comment]")),
                items:   vec![String::from("[public_key_id]")],
            })
        ));

        // Public Key
        let public_key_id = String::from("[public_key_id]");
        res.push(skeleton!(
            CloudFrontResourceAddress::PublicKey { public_key_id },
            CloudFrontResource::PublicKey(resource::PublicKey {
                name: String::from("[public_key_name]"),
                comment: Some(String::from("[comment]")),
                encoded_key: String::from("[base64_encoded_public_key]"),
            })
        ));

        // Streaming Distribution
        let distribution_id = String::from("[streaming_distribution_id]");
        res.push(skeleton!(
            CloudFrontResourceAddress::StreamingDistribution { distribution_id },
            CloudFrontResource::StreamingDistribution(resource::StreamingDistribution {
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

        virt_to_phy!(
            CloudFrontResourceAddress, addr, &self.prefix,
            trivial => [
                Distribution { distribution_id },
                OriginAccessControl { oac_id },
                CachePolicy { policy_id },
                OriginRequestPolicy { policy_id },
                ResponseHeadersPolicy { policy_id },
                KeyGroup { key_group_id },
                PublicKey { public_key_id },
                FieldLevelEncryptionConfig { config_id },
                FieldLevelEncryptionProfile { profile_id },
                StreamingDistribution { distribution_id }
            ],
            null => [
                RealtimeLogConfig { name },
                Function { name }
            ],
            todo => [
            ]
        )
        // match &addr {
        //     CloudFrontResourceAddress::Distribution { .. } => {
        //         if let Some(distribution_id) = addr.get_output(&self.prefix, "distribution_id")? {
        //             Ok(VirtToPhyOutput::Present(
        //                 CloudFrontResourceAddress::Distribution { distribution_id }.to_path_buf(),
        //             ))
        //         } else {
        //             Ok(VirtToPhyOutput::NotPresent)
        //         }
        //     }
        //     CloudFrontResourceAddress::OriginAccessControl { oac_id } => todo!(),
        //     CloudFrontResourceAddress::CachePolicy { policy_id } => todo!(),
        //     CloudFrontResourceAddress::OriginRequestPolicy { policy_id } => todo!(),
        //     CloudFrontResourceAddress::ResponseHeadersPolicy { policy_id } => todo!(),
        //     CloudFrontResourceAddress::RealtimeLogConfig { name } => {
        //         Ok(VirtToPhyOutput::Null(CloudFrontResourceAddress::RealtimeLogConfig { name }))
        //     }
        //     CloudFrontResourceAddress::Function { name } => {
        //         Ok(VirtToPhyOutput::Null(CloudFrontResourceAddress::Function { name }))
        //     }
        //     CloudFrontResourceAddress::KeyGroup { key_group_id } => todo!(),
        //     CloudFrontResourceAddress::PublicKey { public_key_id } => todo!(),
        //     CloudFrontResourceAddress::FieldLevelEncryptionConfig { config_id } => todo!(),
        //     CloudFrontResourceAddress::FieldLevelEncryptionProfile { profile_id } => todo!(),
        //     CloudFrontResourceAddress::StreamingDistribution { distribution_id } => todo!(),
        // }
    }

    async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;

        // match &addr {
        //     CloudFrontResourceAddress::Distribution { .. } => {
        if let Some(virt_addr) = addr.phy_to_virt(&self.prefix)? {
            return Ok(Some(virt_addr.to_path_buf()));
        }
        // }
        // _ => todo!(),
        // }
        Ok(None)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;

        match addr {
            CloudFrontResourceAddress::Distribution { .. } => ron_check_eq::<resource::Distribution>(a, b),
            CloudFrontResourceAddress::OriginAccessControl { .. } => ron_check_eq::<resource::OriginAccessControl>(a, b),
            CloudFrontResourceAddress::CachePolicy { .. } => ron_check_eq::<resource::CachePolicy>(a, b),
            CloudFrontResourceAddress::OriginRequestPolicy { .. } => ron_check_eq::<resource::OriginRequestPolicy>(a, b),
            CloudFrontResourceAddress::ResponseHeadersPolicy { .. } => ron_check_eq::<resource::ResponseHeadersPolicy>(a, b),
            CloudFrontResourceAddress::RealtimeLogConfig { .. } => ron_check_eq::<resource::RealtimeLogConfig>(a, b),
            CloudFrontResourceAddress::Function { .. } => ron_check_eq::<resource::Function>(a, b),
            CloudFrontResourceAddress::KeyGroup { .. } => ron_check_eq::<resource::KeyGroup>(a, b),
            CloudFrontResourceAddress::PublicKey { .. } => ron_check_eq::<resource::PublicKey>(a, b),
            CloudFrontResourceAddress::FieldLevelEncryptionConfig { .. } => {
                ron_check_eq::<resource::FieldLevelEncryptionConfig>(a, b)
            }
            CloudFrontResourceAddress::FieldLevelEncryptionProfile { .. } => {
                ron_check_eq::<resource::FieldLevelEncryptionProfile>(a, b)
            }
            CloudFrontResourceAddress::StreamingDistribution { .. } => ron_check_eq::<resource::StreamingDistribution>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = CloudFrontResourceAddress::from_path(addr)?;
        match addr {
            CloudFrontResourceAddress::Distribution { .. } => ron_check_syntax::<resource::Distribution>(a),
            CloudFrontResourceAddress::OriginAccessControl { .. } => ron_check_syntax::<resource::OriginAccessControl>(a),
            CloudFrontResourceAddress::CachePolicy { .. } => ron_check_syntax::<resource::CachePolicy>(a),
            CloudFrontResourceAddress::OriginRequestPolicy { .. } => ron_check_syntax::<resource::OriginRequestPolicy>(a),
            CloudFrontResourceAddress::ResponseHeadersPolicy { .. } => ron_check_syntax::<resource::ResponseHeadersPolicy>(a),
            CloudFrontResourceAddress::RealtimeLogConfig { .. } => ron_check_syntax::<resource::RealtimeLogConfig>(a),
            CloudFrontResourceAddress::Function { .. } => ron_check_syntax::<resource::Function>(a),
            CloudFrontResourceAddress::KeyGroup { .. } => ron_check_syntax::<resource::KeyGroup>(a),
            CloudFrontResourceAddress::PublicKey { .. } => ron_check_syntax::<resource::PublicKey>(a),
            CloudFrontResourceAddress::FieldLevelEncryptionConfig { .. } => {
                ron_check_syntax::<resource::FieldLevelEncryptionConfig>(a)
            }
            CloudFrontResourceAddress::FieldLevelEncryptionProfile { .. } => {
                ron_check_syntax::<resource::FieldLevelEncryptionProfile>(a)
            }
            CloudFrontResourceAddress::StreamingDistribution { .. } => ron_check_syntax::<resource::StreamingDistribution>(a),
        }
    }
}
