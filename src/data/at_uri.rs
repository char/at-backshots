use anyhow::Result;

pub fn parse_at_uri(uri: &str) -> Result<(&str, &str, &str)> {
    let rest = uri
        .strip_prefix("at://")
        .ok_or_else(|| anyhow::anyhow!("at uri: could not find repo"))?;
    let (repo, rest) = rest
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("at uri: could not find collection"))?;
    let (collection, rkey) = rest
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("at uri: could not find rkey"))?;

    let mut rkey = rkey;
    if let Some((q_before, _q_after)) = rkey.split_once('?') {
        rkey = q_before;
    }
    if let Some((f_before, _f_after)) = rkey.split_once('#') {
        rkey = f_before;
    }

    Ok((repo, collection, rkey))
}
