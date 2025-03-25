// i need nix::fnctl::flock(..) because Flock wants an OwnedFd instead of one that stays open
#![allow(deprecated)]

use std::{
    collections::BTreeMap,
    fs::File,
    io::{Seek, SeekFrom, Write},
    mem::size_of,
    os::fd::AsRawFd,
    path::Path,
};

use anyhow::Result;
use nix::fcntl::{flock, FlockArg};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::data::{record::RecordId, Padding};

use super::{pread_all, pwrite_all};

#[derive(Debug, Clone, Copy, KnownLayout, IntoBytes, FromBytes)]
pub struct IndexHeader {
    pub num_records: u64,
    _pad: Padding<56>,
}
const INDEX_HEADER_SIZE: usize = size_of::<IndexHeader>();
// assert IndexHeader is 64 bytes
const _: [(); 64] = [(); INDEX_HEADER_SIZE];

#[derive(Debug, Clone, Copy, KnownLayout, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub struct RecordIndexEntry {
    pub target: RecordId,
    // absolute index in backlinks to BacklinkEntry (MAX_VALUE if null)
    pub head: u64,
    pub tail: u64,
}
const INDEX_ENTRY_SIZE: usize = size_of::<RecordIndexEntry>();
// assert IndexEntry is 40 bytes
const _: [(); 40] = [(); INDEX_ENTRY_SIZE];

#[derive(Debug, Clone, Copy, KnownLayout, IntoBytes, FromBytes, Immutable)]
#[repr(C, packed)]
pub struct IndexValue {
    pub head: u64,
    pub tail: u64,
    pub idx: u64,
}

#[derive(Debug, Clone, Copy, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub struct BacklinkEntry {
    pub source: RecordId,
    pub next: i32, // relative offset in backlinks to BacklinkEntry (0 if null)
    pub prev: i32,
}
const BACKLINK_ENTRY_SIZE: usize = size_of::<BacklinkEntry>();
// assert BacklinkEntry is 32 bytes
const _: [(); 32] = [(); BACKLINK_ENTRY_SIZE];

pub struct LiveStorageWriter {
    index_btree: BTreeMap<RecordId, IndexValue>,
    index_file: File,        // create, write, read
    index_file_append: File, // append
    links_file: File,        // create, write, read
}

impl LiveStorageWriter {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let _ = std::fs::create_dir_all(&dir);

        let base_options = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .clone();

        let mut index_file = base_options.clone().open(dir.as_ref().join("index.dat"))?;
        let index_file_append = base_options
            .clone()
            .write(false)
            .append(true)
            .open(dir.as_ref().join("index.dat"))?;
        {
            let mut buf = [0u8; INDEX_HEADER_SIZE];
            if pread_all(&index_file, &mut buf, 0).is_err() {
                let mut header = IndexHeader {
                    num_records: 0,
                    _pad: Default::default(),
                };
                pwrite_all(&mut index_file, header.as_mut_bytes(), 0)?;
            }
        };

        let index_btree = Self::load_btree(&mut index_file)?;

        let links_file = base_options.clone().open(dir.as_ref().join("links.dat"))?;

        Ok(Self {
            index_file,
            index_file_append,
            index_btree,
            links_file,
        })
    }

    fn load_btree(index_file: &mut File) -> Result<BTreeMap<RecordId, IndexValue>> {
        // TODO: this should probably not be all in-memory but we ball for now

        let mut map = BTreeMap::new();
        index_file.seek(SeekFrom::Start(INDEX_HEADER_SIZE as u64))?;
        let mut idx = 0;
        loop {
            let Ok(entry) = RecordIndexEntry::read_from_io(&mut *index_file) else {
                break;
            };
            map.insert(
                entry.target,
                IndexValue {
                    head: entry.head,
                    tail: entry.tail,
                    idx,
                },
            );
            idx += 1;
        }

        Ok(map)
    }

    fn find_in_index(&mut self, target: &RecordId) -> Result<IndexValue> {
        let value = self
            .index_btree
            .get(target)
            .ok_or(anyhow::anyhow!("not found"))?;
        Ok(*value)
    }

    fn update_index(&mut self, target: &RecordId, index_value: IndexValue) -> Result<()> {
        let index_entry_idx = usize::try_from(index_value.idx).unwrap();

        self.index_btree.insert(*target, index_value);
        pwrite_all(
            &mut self.index_file,
            RecordIndexEntry {
                target: *target,
                head: index_value.head,
                tail: index_value.tail,
            }
            .as_mut_bytes(),
            INDEX_HEADER_SIZE + index_entry_idx * INDEX_ENTRY_SIZE,
        )?;
        Ok(())
    }

    fn add_to_index(&mut self, target: &RecordId, index_value: IndexValue) -> Result<()> {
        self.index_btree.insert(*target, index_value);
        self.index_file_append.write_all(
            RecordIndexEntry {
                target: *target,
                head: index_value.head,
                tail: index_value.tail,
            }
            .as_mut_bytes(),
        )?;

        Ok(())
    }

    fn alloc_entry_slot(&mut self) -> Result<u64> {
        let index_raw_fd = self.index_file.as_raw_fd();
        flock(index_raw_fd, FlockArg::LockExclusive).expect("failed to acquire index.dat lock");

        let mut header: IndexHeader = {
            let mut buf = [0u8; INDEX_HEADER_SIZE];
            pread_all(&self.index_file, &mut buf, 0)?;
            zerocopy::transmute!(buf)
        };
        let cnt = header.num_records;
        header.num_records += 1;
        // allocate empty space at cnt
        let pos = usize::try_from(cnt).unwrap() * BACKLINK_ENTRY_SIZE;
        pwrite_all(&mut self.links_file, &[0u8; BACKLINK_ENTRY_SIZE], pos)?;
        pwrite_all(&mut self.index_file, header.as_mut_bytes(), 0)?;

        let _ = flock(index_raw_fd, FlockArg::Unlock);

        Ok(cnt)
    }

    pub fn log_backlink(&mut self, target: &RecordId, source: &RecordId) -> Result<()> {
        if let Ok(mut index_value) = self.find_in_index(target) {
            let mut tail_entry: Option<BacklinkEntry> = None;
            let tail_slot = index_value.tail;
            let tail_pos = usize::try_from(tail_slot).unwrap() * BACKLINK_ENTRY_SIZE;

            if index_value.tail != u64::MAX {
                let e: BacklinkEntry = {
                    let mut buf = [0u8; BACKLINK_ENTRY_SIZE];
                    pread_all(&self.links_file, &mut buf, tail_pos)?;
                    zerocopy::transmute!(buf)
                };
                if &e.source == source {
                    // we can cheaply avoid writing a duplicate entry here
                    return Ok(());
                }
                tail_entry.replace(e);
            }

            let mut new_entry = BacklinkEntry {
                source: *source,
                next: 0,
                prev: 0,
            };
            let slot = self.alloc_entry_slot()?;
            let pos = usize::try_from(slot).unwrap() * BACKLINK_ENTRY_SIZE;

            if index_value.head == u64::MAX {
                index_value.head = slot;
            }

            // set prev to the previous end of the chain
            if index_value.tail != u64::MAX {
                new_entry.prev = (tail_slot as i64 - slot as i64).try_into().unwrap();
            }
            pwrite_all(&mut self.links_file, new_entry.as_mut_bytes(), pos)?;

            // update the 'next' at the end of the chain if we need to
            if let Some(mut tail_entry) = tail_entry {
                tail_entry.next = (slot as i64 - tail_slot as i64).try_into().unwrap();
                pwrite_all(&mut self.links_file, tail_entry.as_mut_bytes(), tail_pos)?;
            }

            // update end of the chain
            index_value.tail = slot;
            self.update_index(target, index_value)?;
        } else {
            let mut new_entry = BacklinkEntry {
                source: *source,
                next: 0,
                prev: 0,
            };
            let slot = self.alloc_entry_slot()?;
            let pos = usize::try_from(slot).unwrap() * BACKLINK_ENTRY_SIZE;
            pwrite_all(&mut self.links_file, new_entry.as_mut_bytes(), pos)?;
            self.add_to_index(
                target,
                IndexValue {
                    head: slot,
                    tail: slot,
                    idx: self.index_btree.len() as u64,
                },
            )?;
        }

        Ok(())
    }

    pub fn read_backlinks(&mut self, target: &RecordId) -> Result<Vec<BacklinkEntry>> {
        let index_value = match self.find_in_index(target) {
            Ok(i) => i,
            Err(e) if e.to_string() == "not found" => return Ok(vec![]),
            Err(e) => return Err(e),
        };

        let mut links = Vec::new();
        let mut link_idx = index_value.head;
        loop {
            let link: BacklinkEntry = {
                let pos = usize::try_from(link_idx).unwrap() * BACKLINK_ENTRY_SIZE;
                let mut buf = [0u8; BACKLINK_ENTRY_SIZE];
                pread_all(&self.links_file, &mut buf, pos)?;
                zerocopy::transmute!(buf)
            };
            links.push(link);
            if link.next == 0 {
                break;
            }
            link_idx = link_idx.checked_add_signed(link.next as i64).unwrap();
        }

        Ok(links)
    }
}
