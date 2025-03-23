use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::data::record::RecordId;

#[derive(Debug, Clone, Copy, KnownLayout, Immutable, IntoBytes, FromBytes)]
#[repr(C, packed)]
pub struct RecordIndexEntry {
    pub target: RecordId,
    pub flags: u32,
    pub count: u32,
    pub head: u32, // byte index in links file divided by 32 (we'll write padding)
}
pub const INDEX_ENTRY_SIZE: usize = std::mem::size_of::<RecordIndexEntry>();
// size assertion
const _: [(); 32] = [(); INDEX_ENTRY_SIZE];

// to store an array of `count` BacklinkEntry structures:
//   - order by rkey
//   - count × u64 rkey
//   - count × leb128 (u32 as u31) collection
//   - count × leb128 (u64) did
// if 32nd bit of collection is set, this record has been deleted and should be skipped.
// we store the rkeys uncompressed and contiguously so that we can binary search
// for a specific RecordId, so that deletion-marking in compacted stores can be fast
