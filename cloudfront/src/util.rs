use anyhow::Context;
use aws_sdk_cloudfront::types::DistributionConfig;

pub async fn get_distribution_config(distribution_id: &str, client: &aws_sdk_cloudfront::Client) -> anyhow::Result<(String, DistributionConfig)> {
    let get_response = client.get_distribution_config().id(distribution_id).send().await?;

    let config = get_response.distribution_config().context("No distribution config")?.clone();
    let etag = get_response.e_tag().context("No ETag in response")?;
    Ok((etag.to_string(), config))
}
