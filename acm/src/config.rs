use std::path::Path;

use autoschematic_connector_aws_core::{
    config::verify_sts_account_id,
    config::{AwsConnectorConfig, AwsServiceConfig, TimeoutConfig},
    impl_aws_config,
};
use autoschematic_core::util::RON;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct AcmConnectorConfig {
    pub account_id:      Option<String>,
    pub endpoint_url:    Option<String>,
    pub timeout_config:  Option<TimeoutConfig>,
    pub sts_region:      String,
    pub enabled_regions: Vec<String>,
}

impl_aws_config!(AcmConnectorConfig, "aws/acm/config.ron");
