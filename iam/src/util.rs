use std::collections::HashSet;

pub async fn list_attached_user_policies(
    client: &aws_sdk_iam::Client,
    user_name: &String,
) -> Result<HashSet<String>, anyhow::Error> {
    let mut results = HashSet::new();

    let mut list_result = client.list_attached_user_policies().user_name(user_name).send().await?;

    let Some(attached_policies) = list_result.attached_policies else {
        return Ok(HashSet::new());
    };

    for policy in attached_policies {
        if let Some(policy_arn) = policy.policy_arn {
            results.insert(policy_arn);
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
                if let Some(policy_arn) = policy.policy_arn {
                    results.insert(policy_arn);
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
) -> Result<HashSet<String>, anyhow::Error> {
    let mut results = HashSet::new();

    let mut list_result = client.list_attached_role_policies().role_name(role_name).send().await?;

    let Some(attached_policies) = list_result.attached_policies else {
        return Ok(HashSet::new());
    };

    for policy in attached_policies {
        if let Some(policy_arn) = policy.policy_arn {
            results.insert(policy_arn);
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
                if let Some(policy_arn) = policy.policy_arn {
                    results.insert(policy_arn);
                }
            }
        } else {
            break;
        }
    }

    Ok(results)
}

pub async fn list_attached_group_policies(
    client: &aws_sdk_iam::Client,
    group_name: &String,
) -> Result<HashSet<String>, anyhow::Error> {
    let mut results = HashSet::new();

    let mut list_result = client.list_attached_group_policies().group_name(group_name).send().await?;

    let Some(attached_policies) = list_result.attached_policies else {
        return Ok(HashSet::new());
    };

    for policy in attached_policies {
        if let Some(policy_arn) = policy.policy_arn {
            results.insert(policy_arn);
        }
    }

    loop {
        if list_result.is_truncated {
            list_result = client
                .list_attached_group_policies()
                .group_name(group_name)
                .set_marker(list_result.marker)
                .send()
                .await?;

            let Some(attached_policies) = list_result.attached_policies else {
                break;
            };

            for policy in attached_policies {
                if let Some(policy_arn) = policy.policy_arn {
                    results.insert(policy_arn);
                }
            }
        } else {
            break;
        }
    }

    Ok(results)
}

pub fn policies_removed<'a>(current: &'a HashSet<String>, desired: &'a HashSet<String>) -> Vec<&'a String> {
    current.difference(desired).collect()
}

pub fn policies_added<'a>(current: &'a HashSet<String>, desired: &'a HashSet<String>) -> Vec<&'a String> {
    desired.difference(current).collect()
}

pub fn users_removed<'a>(current: &'a HashSet<String>, desired: &'a HashSet<String>) -> Vec<&'a String> {
    current.difference(desired).collect()
}

pub fn users_added<'a>(current: &'a HashSet<String>, desired: &'a HashSet<String>) -> Vec<&'a String> {
    desired.difference(current).collect()
}
