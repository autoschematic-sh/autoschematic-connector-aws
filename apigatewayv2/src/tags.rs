use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// Define Tags similar to S3 implementation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Tags(HashMap<String, String>);

impl From<Option<HashMap<String, String>>> for Tags {
    fn from(value: Option<HashMap<String, String>>) -> Self {
        match value {
            Some(tags) => Tags(tags),
            None => Tags(HashMap::new()),
        }
    }
}

// impl Into<Option<Vec<Tag>>> for Tags {
//     fn into(self) -> Option<Vec<Tag>> {
//         let mut out_vec = Vec::new();

//         for (k, v) in self.0 {
//             out_vec.push(Tag::builder().key(k).value(v).build());
//         }

//         Some(out_vec)
//     }
// }

impl Tags {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn to_vec(&self) -> anyhow::Result<Vec<(String, String)>> {
        let mut out_vec = Vec::new();

        for (k, v) in &self.0 {
            out_vec.push((k.clone(), v.clone()));
        }

        Ok(out_vec)
    }
}

// From a pair of hashmap determine the set of aws_ecs::Tag structs to pass to untag and set_tags respectively
pub fn tag_diff(
    old_tags: &HashMap<String, String>,
    new_tags: &HashMap<String, String>,
) -> anyhow::Result<(Vec<String>, HashMap<String, String>)> {
    let mut untag_keys = Vec::new();
    for k in old_tags.keys() {
        if !new_tags.contains_key(k) {
            untag_keys.push(k.to_string());
        }
    }

    let mut new_tagset = HashMap::<String, String>::new();
    for (key, new_value) in new_tags {
        if !old_tags.contains_key(key) {
            new_tagset.insert(key.clone(), new_value.clone());
        } else if let Some(old_value) = old_tags.get(key) {
            if old_value != new_value {
                new_tagset.insert(key.clone(), new_value.clone());
            }
        }
    }

    Ok((untag_keys, new_tagset))
}
