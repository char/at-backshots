// nix::fnctl::flock(..)
#![allow(deprecated)]

use std::{
    cmp::Ordering,
    collections::BTreeSet,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
    os::fd::AsRawFd,
    path::Path,
};

use anyhow::Result;
use nix::fcntl::{flock, FlockArg};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    data::{record::RecordId, Padding},
    storage::pwrite_all,
};

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

pub struct CompactedStorageWriter {
    index: File,        // create, append
    links: File,        // create, append
    index_random: File, // write

    last_target: Option<RecordId>,
}

impl CompactedStorageWriter {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let _ = std::fs::create_dir_all(&dir);

        let index = File::options()
            .create(true)
            .append(true)
            .open(dir.as_ref().join("index.dat"))?;
        let links = File::options()
            .create(true)
            .append(true)
            .open(dir.as_ref().join("links.dat"))?;
        let index_random = File::options()
            .read(true)
            .write(true)
            .truncate(false)
            .open(dir.as_ref().join("index.dat"))?;

        Ok(CompactedStorageWriter {
            index,
            links,
            index_random,

            last_target: None,
        })
    }

    pub fn log_backlinks(&mut self, target: &RecordId, sources: &[RecordId]) -> Result<()> {
        debug_assert!(sources.is_sorted(), "links must be logged in order!");
        if let Some(last_target) = self.last_target {
            debug_assert!(&last_target <= target, "links must be logged in order!");
        }
        self.last_target.replace(*target);

        let mut links_pos = self.links.stream_position()?;
        if links_pos % POS_ALIGN != 0 {
            let zeroes = [0u8; POS_ALIGN as usize];
            let padding = POS_ALIGN - links_pos % POS_ALIGN;
            self.links.write_all(&zeroes[..padding as usize])?;
            links_pos += padding;
        }

        let entry = RecordIndexEntry {
            target: *target,
            count: sources.len().try_into().expect("too many!"),
            position: (links_pos / 32) as u32,
        };

        for source in sources.iter() {
            self.links.write_all(&source.rkey.to_le_bytes())?;
        }
        for source in sources.iter() {
            let mut collection_buf = unsigned_varint::encode::u32_buffer();
            let collection_slice =
                unsigned_varint::encode::u32(source.collection, &mut collection_buf);
            self.links.write_all(collection_slice)?;
        }
        for source in sources.iter() {
            let mut did_buf = unsigned_varint::encode::u64_buffer();
            let did_slice = unsigned_varint::encode::u64(source.did, &mut did_buf);
            self.links.write_all(did_slice)?;
        }

        {
            flock(self.index_random.as_raw_fd(), FlockArg::LockExclusive)?;
            let mut header: RecordIndexHeader = {
                let mut header_buf = [0u8; INDEX_HEADER_SIZE];
                pread_all(&self.index_random, &mut header_buf, 0)?;
                zerocopy::transmute!(header_buf)
            };
            entry.write_to_io(&mut self.index)?;
            header.num_entries += 1;
            pwrite_all(&self.index_random, header.as_bytes(), 0)?;
            let _ = flock(self.index_random.as_raw_fd(), FlockArg::Unlock);
        }

        Ok(())
    }
}

pub struct CompactedStorageReader {
    index: File, // read
    links: File, // read
}

impl CompactedStorageReader {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let index = File::options()
            .read(true)
            .open(dir.as_ref().join("index.dat"))?;
        let links = File::options()
            .read(true)
            .open(dir.as_ref().join("links.dat"))?;
        Ok(Self { index, links })
    }

    // simple binary search
    pub fn find_index_entry(&self, target: &RecordId) -> Result<Option<RecordIndexEntry>> {
        let header: RecordIndexHeader = {
            let mut header_buf = [0u8; INDEX_HEADER_SIZE];
            pread_all(&self.index, &mut header_buf, 0)?;
            zerocopy::transmute!(header_buf)
        };

        let mut start = 0;
        let mut end = header.num_entries as usize;
        loop {
            let i = start + (start + end) / 2;

            let entry: RecordIndexEntry = {
                let mut entry_buf = [0u8; INDEX_ENTRY_SIZE];
                pread_all(&self.index, &mut entry_buf, i * INDEX_ENTRY_SIZE)?;
                zerocopy::transmute!(entry_buf)
            };

            match entry.target.cmp(target) {
                Ordering::Less => start = i + 1,
                Ordering::Greater => end = i,
                Ordering::Equal => return Ok(Some(entry)),
            }

            if end >= start {
                break;
            }
        }

        Ok(None)
    }

    pub fn read_backlinks(
        &mut self,
        target: &RecordId,
        records: &mut BTreeSet<RecordId>,
    ) -> Result<()> {
        let Some(entry) = self.find_index_entry(target)? else {
            return Ok(());
        };

        let mut rkeys_buf = vec![0u8; entry.count as usize * 8];
        self.links
            .seek(SeekFrom::Start(entry.position as u64 * POS_ALIGN))?;
        self.links.read_exact(&mut rkeys_buf)?;
        let (_start, rkeys, _end) = unsafe { rkeys_buf.as_mut_slice().align_to_mut::<u64>() };

        let mut collections = Vec::<u32>::with_capacity(entry.count as usize);
        for _ in 0..entry.count {
            collections.push(unsigned_varint::io::read_u32(&mut self.links)?);
        }
        let mut dids = Vec::<u64>::with_capacity(entry.count as usize);
        for _ in 0..entry.count {
            dids.push(unsigned_varint::io::read_u64(&mut self.links)?);
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
}
