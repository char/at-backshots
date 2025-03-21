// i need nix::fnctl::flock(..) because Flock wants an OwnedFd instead of one that stays open
#![allow(deprecated)]

use std::{
    collections::BTreeMap,
    fs::File,
    io::{Seek, SeekFrom, Write},
    os::fd::{AsFd, AsRawFd},
    path::Path,
};

use anyhow::Result;
use nix::{
    fcntl::{flock, FlockArg},
    libc::off_t,
    sys::uio,
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::data::record::RecordId;

#[derive(Clone, Copy, KnownLayout, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub struct Padding<const LEN: usize>(pub [u8; LEN]);
impl<const LEN: usize> std::fmt::Debug for Padding<LEN> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Padding").finish()
    }
}
impl<const LEN: usize> From<[u8; LEN]> for Padding<LEN> {
    fn from(value: [u8; LEN]) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, KnownLayout, IntoBytes, FromBytes)]
pub struct IndexHeader {
    pub num_links: u64,
    _pad: Padding<56>,
}
const INDEX_HEADER_SIZE: usize = std::mem::size_of::<IndexHeader>();
// assert IndexHeader is 64 bytes
const _: [(); 64] = [(); INDEX_HEADER_SIZE];

#[derive(Debug, Clone, Copy, KnownLayout, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub struct IndexEntry {
    pub target: RecordId,
    // absolute index in backlinks to BacklinkEntry (MAX_VALUE if null)
    pub head: u64,
    pub tail: u64,
    // _pad: Padding<16>,
}
const INDEX_ENTRY_SIZE: usize = std::mem::size_of::<IndexEntry>();
// assert IndexEntry is 32 bytes
const _: [(); 32] = [(); INDEX_ENTRY_SIZE];

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
const BACKLINK_ENTRY_SIZE: usize = std::mem::size_of::<BacklinkEntry>();
// assert BacklinkEntry is 24 bytes
const _: [(); 24] = [(); BACKLINK_ENTRY_SIZE];

pub struct BacklinkStorage {
    index_btree: BTreeMap<RecordId, IndexValue>,
    index_file: File,        // create, write, read
    index_file_append: File, // append
    links_file: File,        // create, write, read
}

fn pread_all(fd: impl AsFd, buf: &mut [u8], offset: usize) -> Result<()> {
    let mut read = 0;
    while read < buf.len() {
        let res = uio::pread(fd.as_fd(), &mut buf[read..], (offset + read) as off_t)?;
        read += res;
        if res == 0 {
            anyhow::bail!("no data to read");
        }
    }
    Ok(())
}

fn pwrite_all(fd: impl AsFd, buf: &[u8], offset: usize) -> Result<()> {
    let mut written = 0;
    while written < buf.len() {
        written += uio::pwrite(fd.as_fd(), &buf[written..], (offset + written) as off_t)?;
    }
    Ok(())
}

impl BacklinkStorage {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let base_options = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .clone();

        let mut index_file = base_options
            .clone()
            .open(dir.as_ref().join("./index.dat"))?;
        let index_file_append = base_options
            .clone()
            .write(false)
            .append(true)
            .open(dir.as_ref().join("./index.dat"))?;
        {
            let mut buf = [0u8; INDEX_HEADER_SIZE];
            if pread_all(&index_file, &mut buf, 0).is_err() {
                let mut header = IndexHeader {
                    num_links: 0,
                    _pad: [0u8; 56].into(),
                };
                pwrite_all(&mut index_file, header.as_mut_bytes(), 0)?;
            }
        };

        let index_btree = Self::load_btree(&mut index_file)?;

        let links_file = base_options
            .clone()
            .open(dir.as_ref().join("./links.dat"))?;

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
            let Ok(entry) = IndexEntry::read_from_io(&mut *index_file) else {
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
            IndexEntry {
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
            IndexEntry {
                target: *target,
                head: index_value.head,
                tail: index_value.tail,
            }
            .as_mut_bytes(),
        )?;

        Ok(())
    }

    pub fn write_backlink(&mut self, target: &RecordId, source: &RecordId) -> Result<()> {
        let mut new_entry = BacklinkEntry {
            source: *source,
            next: 0,
            prev: 0,
        };
        let new_entry_idx = {
            let index_raw_fd = self.index_file.as_raw_fd();
            flock(index_raw_fd, FlockArg::LockExclusive).expect("failed to acquire index.dat lock");

            let mut header: IndexHeader = {
                let mut buf = [0u8; INDEX_HEADER_SIZE];
                pread_all(&self.index_file, &mut buf, 0)?;
                zerocopy::transmute!(buf)
            };
            let cnt = header.num_links;
            header.num_links += 1;
            // allocate empty space at cnt
            let pos = usize::try_from(cnt).unwrap() * BACKLINK_ENTRY_SIZE;
            pwrite_all(&mut self.links_file, &[0u8; BACKLINK_ENTRY_SIZE], pos)?;
            pwrite_all(&mut self.index_file, header.as_mut_bytes(), 0)?;

            let _ = flock(index_raw_fd, FlockArg::Unlock);

            cnt
        };
        let new_entry_pos = usize::try_from(new_entry_idx).unwrap() * BACKLINK_ENTRY_SIZE;

        if let Ok(mut index_value) = self.find_in_index(target) {
            let index_entry_idx = usize::try_from(index_value.idx).unwrap();

            if index_value.head == u64::MAX {
                index_value.head = new_entry_idx;
            }

            // set prev to the end of the chain
            if index_value.tail != u64::MAX {
                new_entry.prev = (index_value.tail as i64 - new_entry_idx as i64)
                    .try_into()
                    .unwrap();
            }
            pwrite_all(
                &mut self.links_file,
                new_entry.as_mut_bytes(),
                new_entry_pos,
            )?;

            // update the 'next' at the end of the chain if we need to
            if index_value.tail != u64::MAX {
                let tail_entry_idx: usize = index_value.tail.try_into().unwrap();
                let mut tail_entry: BacklinkEntry = {
                    let mut buf = [0u8; BACKLINK_ENTRY_SIZE];
                    pread_all(
                        &self.links_file,
                        &mut buf,
                        tail_entry_idx * BACKLINK_ENTRY_SIZE,
                    )?;
                    zerocopy::transmute!(buf)
                };
                tail_entry.next = (new_entry_idx as i64 - index_value.tail as i64)
                    .try_into()
                    .unwrap();
                pwrite_all(
                    &mut self.links_file,
                    tail_entry.as_mut_bytes(),
                    tail_entry_idx * BACKLINK_ENTRY_SIZE,
                )?;
            }

            // update the index entry on disk
            index_value.tail = new_entry_idx;
            self.update_index(target, index_value)?;
            pwrite_all(
                &mut self.index_file,
                IndexEntry {
                    target: *target,
                    head: index_value.head,
                    tail: index_value.tail,
                }
                .as_mut_bytes(),
                INDEX_HEADER_SIZE + index_entry_idx * INDEX_ENTRY_SIZE,
            )?;
        } else {
            pwrite_all(
                &mut self.links_file,
                new_entry.as_mut_bytes(),
                new_entry_pos,
            )?;
            self.add_to_index(
                target,
                IndexValue {
                    head: new_entry_idx,
                    tail: new_entry_idx,
                    idx: self.index_btree.len() as u64,
                },
            )?;
        }

        Ok(())
    }

    pub fn read_backlinks(&mut self, target: &RecordId) -> Result<Vec<BacklinkEntry>> {
        let index_value = self.find_in_index(target)?;

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
