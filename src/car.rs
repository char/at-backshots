use std::{
    collections::BTreeMap,
    io::{Read, Seek, SeekFrom},
    str::FromStr,
};

use anyhow::{Context, Result};
use ipld_core::{
    cid::{multihash::Multihash, Cid},
    ipld::Ipld,
};

#[derive(Debug)]
pub struct CarBlockInfo {
    pub cid: Cid,
    pub pos: usize,
    pub len: usize,
}

#[derive(Debug)]
pub struct CarFile {
    pub roots: Vec<Cid>,
    pub blocks: BTreeMap<Cid, CarBlockInfo>,
}

fn read_varint<R: Read>(reader: &mut R) -> Result<(usize, usize)> {
    // a u64 encoded as leb128 takes up 10 bytes

    let mut b = [0u8; 10];
    for i in 0..10 {
        reader.read_exact(&mut b[i..i + 1])?;
        if unsigned_varint::decode::is_last(b[i]) {
            let slice = &b[..=i];
            let (num, _) = unsigned_varint::decode::usize(slice)?;
            return Ok((num, i + 1));
        }
    }

    anyhow::bail!("overflow");
}

fn read_cid<R: Read>(reader: &mut R) -> Result<(Cid, usize)> {
    let mut cid_length = 0;
    let mut cid_header_buf = [0u8; 3];
    reader.read_exact(&mut cid_header_buf)?;
    cid_length += cid_header_buf.len();

    let [version, codec, hash_type] = cid_header_buf;
    assert_eq!(version, 1, "cid is not v1");
    assert!(codec == 0x55 || codec == 0x71, "cid is not raw / dcbor");

    let cid = match hash_type {
        0x12 => {
            let hash_size = {
                let mut b = [0u8; 1];
                reader.read_exact(&mut b)?;
                b[0]
            };
            cid_length += 1;
            assert_eq!(hash_size, 32, "sha2-256 should be 32 bytes long");

            let mut hash_buf = [0u8; 32];
            reader.read_exact(&mut hash_buf)?;
            cid_length += hash_buf.len();
            Cid::new_v1(codec as u64, Multihash::wrap(hash_type as u64, &hash_buf)?)
        }
        0x1e => {
            // read a variable length blake3 hash ^-^
            let (hash_size, n) = read_varint(reader)?;
            cid_length += n;

            let mut hash_buf = vec![0u8; hash_size];
            reader.read_exact(&mut hash_buf)?;
            cid_length += hash_buf.len();
            Cid::new_v1(codec as u64, Multihash::wrap(hash_type as u64, &hash_buf)?)
        }
        _ => anyhow::bail!("unsupported hash type"),
    };

    Ok((cid, cid_length))
}

pub fn read_car_v1<R: Read + Seek>(reader: &mut R) -> Result<CarFile> {
    reader.seek(SeekFrom::Start(0))?;

    // skip the header (we don't care rn)
    let (header_size, header_size_size /* dw */) = read_varint(reader)?;
    let mut pos = reader.seek(SeekFrom::Current(header_size.try_into()?))? as usize;

    // blocks
    let mut blocks = Vec::<CarBlockInfo>::new();

    loop {
        // if this first read fails, we have probably hit the end of the archve
        let Ok((block_size, n)) = read_varint(reader) else {
            break;
        };
        pos += n;
        let (cid, cid_length) = read_cid(reader)?;
        pos += cid_length;

        let len = block_size - cid_length;
        blocks.push(CarBlockInfo { cid, pos, len });

        let _ = reader.seek(SeekFrom::Current(len.try_into()?))?;
        pos += len;
    }

    let _ = reader.seek(SeekFrom::Start(header_size_size.try_into()?))?;
    let mut header_buf = vec![0u8; header_size];
    reader.read_exact(&mut header_buf)?;

    let Ipld::Map(header) = serde_ipld_dagcbor::from_slice::<Ipld>(&header_buf)? else {
        anyhow::bail!("header was not a map")
    };
    let Some(Ipld::Integer(1)) = header.get("version") else {
        anyhow::bail!("version was not 1")
    };
    let Some(Ipld::List(roots)) = header.get("roots") else {
        anyhow::bail!("roots was not a list")
    };
    let roots = roots
        .iter()
        .map(|r| match r {
            Ipld::String(cid) => Cid::from_str(cid).context("root was not a valid cid string"),
            Ipld::Link(cid) => Ok(*cid),
            _ => Err(anyhow::anyhow!("root was not a cid")),
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(CarFile {
        roots,
        blocks: blocks.into_iter().map(|b| (b.cid, b)).collect(),
    })
}

impl CarFile {
    pub fn read_block<R: Read + Seek>(&self, reader: &mut R, cid: &Cid) -> Result<Vec<u8>> {
        let block = self.blocks.get(cid).context("block doesn't exist in car")?;
        reader.seek(SeekFrom::Start(block.pos as u64))?;
        let mut buf = vec![0u8; block.len];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}
