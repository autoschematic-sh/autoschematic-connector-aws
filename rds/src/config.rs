use std::path::Path;

use autoschematic_connector_aws_core::{
    config::{AwsConnectorConfig, AwsServiceConfig, TimeoutConfig, verify_sts_account_id},
    impl_aws_config,
};
use autoschematic_core::util::RON;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct RdsConnectorConfig {
    pub account_id:      Option<String>,
    pub endpoint_url:    Option<String>,
    pub timeout_config:  Option<TimeoutConfig>,
    pub sts_region:      String,
    pub enabled_regions: Vec<String>,
}

impl_aws_config!(RdsConnectorConfig, "aws/rds/config.ron");
