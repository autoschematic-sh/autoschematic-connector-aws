use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{addr::AcmResourceAddress, config::AcmConnectorConfig, resource::AcmCertificate};
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, DocIdent, FilterResponse, GetDocResponse, GetResourceResponse, OpExecResponse, PlanResponseElement,
        Resource, ResourceAddress, SkeletonResponse, VirtToPhyResponse,
    },
    diag::DiagnosticResponse,
    skeleton,
    util::{optional_string_from_utf8, ron_check_eq, ron_check_syntax},
};
use resource::{AcmResource, ValidationOption};

use tags::Tags;
use tokio::sync::{Mutex, RwLock};

use crate::{resource, tags};

mod get;
mod get_doc;
mod list;
mod op_exec;
mod plan;

pub mod client_cache;

#[derive(Default)]
pub struct AcmConnector {
    pub client_cache: Mutex<HashMap<String, Arc<aws_sdk_acm::Client>>>,
    pub config: RwLock<AcmConnectorConfig>,
    pub account_id: RwLock<Option<String>>,
    pub prefix: PathBuf,
}

#[async_trait]
impl Connector for AcmConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterResponse, anyhow::Error> {
        if let Ok(_addr) = AcmResourceAddress::from_path(addr) {
            Ok(FilterResponse::Resource)
        } else {
            Ok(FilterResponse::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(AcmConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let ecr_config: AcmConnectorConfig = AcmConnectorConfig::try_load(&self.prefix).await?;

        let account_id = ecr_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.write().await = ecr_config;
        *self.account_id.write().await = Some(account_id);
        Ok(())
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn subpaths(&self) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut res = Vec::new();

        for region in &self.config.read().await.enabled_regions {
            res.push(PathBuf::from(format!("aws/acm/{region}")));
        }

        Ok(res)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceResponse>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        self.do_plan(addr, optional_string_from_utf8(current)?, optional_string_from_utf8(desired)?)
            .await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn addr_virt_to_phy(&self, addr: &Path) -> anyhow::Result<VirtToPhyResponse> {
        let addr = AcmResourceAddress::from_path(addr)?;

        match &addr {
            AcmResourceAddress::Certificate { region, .. } => {
                let Some(certificate_id) = addr.get_output(&self.prefix, "certificate_id")? else {
                    return Ok(VirtToPhyResponse::NotPresent);
                };
                Ok(VirtToPhyResponse::Present(
                    AcmResourceAddress::Certificate {
                        region: region.into(),
                        certificate_id,
                    }
                    .to_path_buf(),
                ))
            }
        }
    }

    async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
        let addr = AcmResourceAddress::from_path(addr)?;

        match &addr {
            AcmResourceAddress::Certificate { .. } => {
                if let Some(cert_addr) = addr.phy_to_virt(&self.prefix)? {
                    return Ok(Some(cert_addr.to_path_buf()));
                }
            }
        }
        Ok(None)
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonResponse>, anyhow::Error> {
        let mut res = Vec::new();

        // ACM Certificate skeleton for DNS validation
        res.push(skeleton!(
            AcmResourceAddress::Certificate {
                region: String::from("us-east-1"),
                certificate_id: String::from("[certificate-id]"),
            },
            AcmResource::Certificate(AcmCertificate {
                domain_name: String::from("example.com"),
                subject_alternative_names: vec![String::from("*.example.com"), String::from("api.example.com")],
                validation_method: String::from("DNS"),
                validation_options: vec![
                    ValidationOption {
                        domain_name: String::from("example.com"),
                        validation_domain: None,
                    },
                    ValidationOption {
                        domain_name: String::from("*.example.com"),
                        validation_domain: Some(String::from("example.com")),
                    }
                ],
                certificate_transparency_logging_preference: Some(String::from("ENABLED")),
                tags: Tags::default(),
            })
        ));

        // ACM Certificate skeleton for email validation
        res.push(skeleton!(
            AcmResourceAddress::Certificate {
                region: String::from("us-east-1"),
                certificate_id: String::from("[certificate-id-email-validation]"),
            },
            AcmResource::Certificate(AcmCertificate {
                domain_name: String::from("mail.example.com"),
                subject_alternative_names: vec![],
                validation_method: String::from("EMAIL"),
                validation_options: vec![ValidationOption {
                    domain_name: String::from("mail.example.com"),
                    validation_domain: Some(String::from("example.com")),
                }],
                certificate_transparency_logging_preference: Some(String::from("DISABLED")),
                tags: Tags::default(),
            })
        ));

        Ok(res)
    }

    async fn get_docstring(&self, _addr: &Path, ident: DocIdent) -> anyhow::Result<Option<GetDocResponse>> {
        self.do_get_doc(ident).await
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = AcmResourceAddress::from_path(addr)?;

        match addr {
            AcmResourceAddress::Certificate { .. } => ron_check_eq::<AcmCertificate>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<Option<DiagnosticResponse>, anyhow::Error> {
        let addr = AcmResourceAddress::from_path(addr)?;

        match addr {
            AcmResourceAddress::Certificate { .. } => ron_check_syntax::<AcmCertificate>(a),
        }
    }
}
