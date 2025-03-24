use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    mem::size_of,
};

use anyhow::Result;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::data::{record::RecordId, Padding};

use super::pread_all;

#[derive(Debug, Clone, Copy, KnownLayout, Immutable, IntoBytes, FromBytes)]
pub struct RecordIndexHeader {
    pub num_entries: u64,
    _pad: Padding<24>,
}
pub const INDEX_HEADER_SIZE: usize = size_of::<RecordIndexHeader>();
const _: [(); 32] = [(); INDEX_HEADER_SIZE];

#[derive(Debug, Clone, Copy, KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub struct RecordIndexEntry {
    pub target: RecordId,
    pub count: u32,
    // byte index in links file divided by POS_ALIGN (we'll write padding)
    pub position: u32,
}
pub const INDEX_ENTRY_SIZE: usize = size_of::<RecordIndexEntry>();
// size assertion
const _: [(); 32] = [(); INDEX_ENTRY_SIZE];
const POS_ALIGN: u64 = 32;

// to store an array of `count` BacklinkEntry structures:
//   - order by rkey
//   - count × u64 rkey
//   - count × leb128 (u32 as u31) collection
//   - count × leb128 (u64) did
// if 32nd bit of collection is set, this record has been deleted and should be skipped.
// we store the rkeys uncompressed and contiguously so that we can binary search
// for a specific RecordId, so that deletion-marking in compacted stores can be fast

// TODO: we should do something better than passing a BTreeMap<RecordId, BTreeSet<RecordId>>
//       once we know what we want for converting live stores to compacted ones

pub fn write_compacted(
    index: &mut File,
    links: &mut File,
    backlinks: &BTreeMap<RecordId, BTreeSet<RecordId>>,
) -> Result<()> {
    index.seek(SeekFrom::End(0))?;
    links.seek(SeekFrom::End(0))?;

    let mut index_w = BufWriter::new(index);
    let mut links_w = BufWriter::new(links);

    for (target, sources) in backlinks {
        let mut links_pos = links_w.stream_position()?;
        if links_pos % POS_ALIGN != 0 {
            let zeroes = [0u8; POS_ALIGN as usize];
            let padding = POS_ALIGN - links_pos % POS_ALIGN;
            links_w.write_all(&zeroes[..padding as usize])?;
            links_pos += padding;
        }

        let index_entry = RecordIndexEntry {
            target: *target,
            count: sources.len().try_into().expect("too many!"),
            position: (links_pos / 32) as u32,
        };

        for source in sources.iter() {
            links_w.write_all(&source.rkey.to_le_bytes())?;
        }
        for source in sources.iter() {
            let mut collection_buf = unsigned_varint::encode::u32_buffer();
            let collection_slice =
                unsigned_varint::encode::u32(source.collection, &mut collection_buf);
            links_w.write_all(collection_slice)?;
        }
        for source in sources.iter() {
            let mut did_buf = unsigned_varint::encode::u64_buffer();
            let did_slice = unsigned_varint::encode::u64(source.did, &mut did_buf);
            links_w.write_all(did_slice)?;
        }

        index_entry.write_to_io(&mut index_w)?;
        links_w.flush()?;
        index_w.flush()?;
    }

    Ok(())
}

pub fn read_compacted(
    index: &mut File,
    links: &mut File,
    target: &RecordId,
    records: &mut BTreeSet<RecordId>,
) -> Result<()> {
    let header: RecordIndexHeader = {
        let mut header_buf = [0u8; INDEX_HEADER_SIZE];
        pread_all(&*index, &mut header_buf, 0)?;
        zerocopy::transmute!(header_buf)
    };

    // bsearch for the index entry
    let index_entry = {
        let mut start = 0;
        let mut end = header.num_entries as usize;
        loop {
            let i = start + (start + end) / 2;

            let entry: RecordIndexEntry = {
                let mut entry_buf = [0u8; INDEX_ENTRY_SIZE];
                pread_all(&*index, &mut entry_buf, i * INDEX_ENTRY_SIZE)?;
                zerocopy::transmute!(entry_buf)
            };

            match entry.target.cmp(target) {
                Ordering::Less => start = i + 1,
                Ordering::Greater => end = i,
                Ordering::Equal => break Some(entry),
            }

            if end >= start {
                break None;
            }
        }
    };
    let Some(index_entry) = index_entry else {
        return Ok(());
    };

    let mut rkeys_buf = vec![0u8; index_entry.count as usize * 8];
    links.seek(SeekFrom::Start(index_entry.position as u64 * POS_ALIGN))?;
    links.read_exact(&mut rkeys_buf)?;
    let (_start, rkeys, _end) = unsafe { rkeys_buf.as_mut_slice().align_to_mut::<u64>() };

    let mut collections = Vec::<u32>::with_capacity(index_entry.count as usize);
    for _ in 0..index_entry.count {
        collections.push(unsigned_varint::io::read_u32(&mut *links)?);
    }
    let mut dids = Vec::<u64>::with_capacity(index_entry.count as usize);
    for _ in 0..index_entry.count {
        dids.push(unsigned_varint::io::read_u64(&mut *links)?);
    }

    for ((rkey, collection), did) in rkeys
        .iter()
        .zip(collections.into_iter())
        .zip(dids.into_iter())
    {
        records.insert(RecordId::new(did, collection, *rkey));
    }

    Ok(())
}
