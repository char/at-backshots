use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub mod at_uri;
pub mod cid;
pub mod did;
pub mod record;

#[derive(
    Clone, Copy, KnownLayout, IntoBytes, FromBytes, Immutable, PartialEq, Eq, PartialOrd, Ord,
)]
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
impl<const LEN: usize> Default for Padding<LEN> {
    fn default() -> Self {
        Self([0u8; LEN])
    }
}
