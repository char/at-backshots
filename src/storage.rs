use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

use anyhow::Result;
use zerocopy::{BigEndian, FromBytes, IntoBytes, KnownLayout, I64, U64};

use crate::{data::record::RecordId, AppState};

#[derive(Debug, Clone, Copy, KnownLayout, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub struct IndexEntry {
    pub target: RecordId,
    // absolute offset in backlinks to BacklinkEntry (0 if null)
    pub first: U64<BigEndian>,
    // TODO: last ? then we don't have to walk the whole list on every append
}

#[derive(Debug, Clone, Copy, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub struct BacklinkEntry {
    pub source: RecordId,
    pub next: I64<BigEndian>, // relative offset in backlinks to BacklinkEntry (0 if null)
}
const BACKLINK_ENTRY_LEN: usize = std::mem::size_of::<BacklinkEntry>();

pub struct BacklinkStorage<S1: Read + Write + Seek = File, S2: Read + Write + Seek = File> {
    tail: u64,
    index: S1,
    backlinks: S2,
}

impl BacklinkStorage<File, File> {
    pub fn new(dir: impl AsRef<std::path::Path>) -> Result<Self> {
        let _ = std::fs::create_dir_all(dir.as_ref());

        let backlinks = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.as_ref().join("./backlinks.dat"))?;

        let index = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.as_ref().join("./index.dat"))?;

        let mut storage = BacklinkStorage {
            backlinks,
            index,
            tail: 0,
        };
        let _ = storage.read_tail();
        Ok(storage)
    }
}

impl<S1, S2> BacklinkStorage<S1, S2>
where
    S1: Read + Write + Seek,
    S2: Read + Write + Seek,
{
    pub fn find(&mut self, record: &RecordId) -> Result<u64> {
        // TODO: probably some B-tree bullshit instead of using the flat file
        // but this is super compact and it should be fine until we're reading like a gigabyte
        self.index.seek(SeekFrom::Start(8))?;
        loop {
            let index_entry = IndexEntry::read_from_io(&mut self.index)?;
            if &index_entry.target == record {
                break Ok(index_entry.first.get());
            }
        }
    }

    fn read_tail(&mut self) -> Result<()> {
        self.index.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; 8];
        self.index.read_exact(&mut buf)?;
        self.tail = u64::from_be_bytes(buf);
        Ok(())
    }

    pub fn store_backlink(&mut self, target: &RecordId, source: &RecordId) -> Result<()> {
        let mut entry = BacklinkEntry {
            source: *source,
            next: I64::ZERO,
        };

        let new_entry_pos = self.tail;
        self.backlinks.seek(SeekFrom::Start(new_entry_pos))?;
        self.backlinks.write_all(entry.as_mut_bytes())?;

        self.tail += std::mem::size_of::<BacklinkEntry>() as u64;
        self.index.seek(SeekFrom::Start(0))?;
        self.index.write_all(&self.tail.to_be_bytes())?;

        if let Ok(offset) = self.find(target) {
            self.backlinks.seek(SeekFrom::Start(offset))?;
            let _backlink_entry = loop {
                let curr_entry = BacklinkEntry::read_from_io(&mut self.backlinks)?;
                self.backlinks
                    .seek(SeekFrom::Current(-(BACKLINK_ENTRY_LEN as i64)))?;

                let next = curr_entry.next.get();
                if next == 0 {
                    break curr_entry;
                }
                self.backlinks.seek(SeekFrom::Current(next))?;
            };
            let tail_entry_pos = self.backlinks.stream_position()?;
            entry.next.set(new_entry_pos as i64 - tail_entry_pos as i64);
            self.backlinks.write_all(entry.as_mut_bytes())?;
        } else {
            self.index.seek(SeekFrom::End(0))?;
            self.index.write_all(
                IndexEntry {
                    target: *target,
                    first: U64::new(new_entry_pos),
                }
                .as_mut_bytes(),
            )?;
        }

        Ok(())
    }

    pub fn get_backlinks(&mut self, target: &RecordId) -> Result<Vec<RecordId>> {
        let mut backlinks = Vec::new();
        if let Ok(offset) = self.find(target) {
            self.backlinks.seek(SeekFrom::Start(offset))?;
            loop {
                let entry = BacklinkEntry::read_from_io(&mut self.backlinks)?;
                backlinks.push(entry.source);
                let next = entry.next.get();
                if next == 0 {
                    break;
                }
                self.backlinks
                    .seek(SeekFrom::Current(next - (BACKLINK_ENTRY_LEN as i64)))?;
            }
        }

        Ok(backlinks)
    }
}

pub fn test_storage() -> Result<()> {
    let app = AppState::new("http://127.0.0.1:2485".into())?;

    let mut storage = BacklinkStorage::new("./backlinks")?;

    let target = RecordId::from_at_uri(
        &app,
        "at://did:plc:7x6rtuenkuvxq3zsvffp2ide/app.bsky.feed.post/3lkpfgi6mck23",
    )?;
    storage.store_backlink(
        &target,
        &RecordId::from_at_uri(
            &app,
            "at://did:plc:7x6rtuenkuvxq3zsvffp2ide/app.bsky.feed.post/3lkphv6xqn22i",
        )?,
    )?;

    dbg!(storage.get_backlinks(&target)?);

    Ok(())
}
