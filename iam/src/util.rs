

pub async fn list_attached_user_policies(
    client: &aws_sdk_iam::Client,
    user_name: &String,
) -> Result<Vec<String>, anyhow::Error> {
    let mut results = Vec::new();

    let mut list_result = client
        .list_attached_user_policies()
        .user_name(user_name)
        .send()
        .await?;

    let Some(attached_policies) = list_result.attached_policies else {
        return Ok(Vec::new());
    };

    for policy in attached_policies {
        if let Some(policy_name) = policy.policy_name {
            results.push(policy_name);
        }
    }

    loop {
        if list_result.is_truncated {
            list_result = client
                .list_attached_user_policies()
                .user_name(user_name)
                .set_marker(list_result.marker)
                .send()
                .await?;

            let Some(attached_policies) = list_result.attached_policies else {
                break;
            };

            for policy in attached_policies {
                if let Some(policy_name) = policy.policy_name {
                    results.push(policy_name);
                }
            }
        } else {
            break;
        }
    }

    Ok(results)
}

pub async fn list_attached_role_policies(
    client: &aws_sdk_iam::Client,
    role_name: &String,
) -> Result<Vec<String>, anyhow::Error> {
    let mut results = Vec::new();

    let mut list_result = client
        .list_attached_role_policies()
        .role_name(role_name)
        .send()
        .await?;

    let Some(attached_policies) = list_result.attached_policies else {
        return Ok(Vec::new());
    };

    for policy in attached_policies {
        if let Some(policy_name) = policy.policy_name {
            results.push(policy_name);
        }
    }

    loop {
        if list_result.is_truncated {
            list_result = client
                .list_attached_role_policies()
                .role_name(role_name)
                .set_marker(list_result.marker)
                .send()
                .await?;

            let Some(attached_policies) = list_result.attached_policies else {
                break;
            };

            for policy in attached_policies {
                if let Some(policy_name) = policy.policy_name {
                    results.push(policy_name);
                }
            }
        } else {
            break;
        }
    }

    Ok(results)
}
