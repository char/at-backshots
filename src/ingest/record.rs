use std::collections::HashSet;

use anyhow::Result;
use ipld_core::ipld::Ipld;

#[inline(always)]
pub fn get_backlinks(record: &Ipld) -> Result<HashSet<(&str, &str)>> {
    let mut backlinks = HashSet::<(&str, &str)>::new();
    for child in record.iter() {
        // a StrongRef is an Ipld::Map with "cid" and "uri"
        let Ipld::Map(map) = child else {
            continue;
        };
        if let (Some(Ipld::String(cid)), Some(Ipld::String(uri))) = (map.get("cid"), map.get("uri"))
        {
            backlinks.insert((cid, uri));
        }
    }
    Ok(backlinks)
}
