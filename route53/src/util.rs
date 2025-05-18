
pub async fn list_hosted_zones(
    client: &aws_sdk_route53::Client,
) -> Result<Vec<(String, String)>, anyhow::Error> {
    let mut results = Vec::new();

    let mut list_result = client.list_hosted_zones().send().await?;

    for hz in list_result.hosted_zones {
        results.push((hz.id, hz.name))
    }

    loop {
        if list_result.is_truncated {
            list_result = client
                .list_hosted_zones()
                .set_marker(list_result.next_marker)
                .send()
                .await?;

            for hz in list_result.hosted_zones {
                results.push((hz.id, hz.name))
            }
        } else {
            break;
        }
    }

    Ok(results)
}

pub async fn list_resource_record_sets(
    client: &aws_sdk_route53::Client,
    hosted_zone_id: &str,
) -> Result<Vec<(String, String)>, anyhow::Error> {
    let mut results = Vec::new();

    let hosted_zone_id = String::from(hosted_zone_id);

    let mut list_result = client
        .list_resource_record_sets()
        .set_hosted_zone_id(Some(hosted_zone_id.clone()))
        .send()
        .await?;

    for record in list_result.resource_record_sets {
        results.push((record.name, record.r#type.to_string()))
    }

    loop {
        if list_result.is_truncated {

            list_result = client
                .list_resource_record_sets()
                .set_hosted_zone_id(Some(hosted_zone_id.clone()))
                .set_start_record_identifier(list_result.next_record_identifier)
                .send()
                .await?;


            for record in list_result.resource_record_sets {
                results.push((record.name, record.r#type.to_string()));
            }
        } else {
            break;
        }
    }

    Ok(results)
}
