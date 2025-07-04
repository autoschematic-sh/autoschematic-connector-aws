use std::sync::Arc;

use aws_sdk_s3::operation::list_buckets::ListBucketsOutput;

pub async fn list_buckets(
    client: Arc<aws_sdk_s3::Client>,
    region_name: &str,
    prefix: Option<String>,
) -> anyhow::Result<Vec<String>> {
    let mut res = Vec::new();
    let bucket_stream = if let Some(prefix) = prefix {
        client
            .list_buckets()
            .bucket_region(region_name)
            .prefix(prefix)
            .into_paginator()
            .send()
    } else {
        client
            .list_buckets()
            .bucket_region(region_name)
            .into_paginator()
            .send()
    };

    let bucket_output: Vec<Result<ListBucketsOutput, _>> = bucket_stream.collect().await;
    for bucket_result in bucket_output {
        if let Ok(bucket) = bucket_result
            && let Some(buckets) = bucket.buckets {
                for bucket in buckets {
                    if let Some(bucket_name) = bucket.name {
                        res.push(bucket_name);
                    }
                }
            }
    }
    Ok(res)
}
