use std::{
    fmt::Display,
    io::{Cursor, Write},
    str::FromStr,
};

#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
pub struct CidV1<const HASH_SIZE: usize> {
    // [version = 1, codec, hash_type = 0x12, 0]
    // codec = 0x55 (raw) or 0x71 (dcbor)
    pub meta: u32,
    pub hash: [u8; HASH_SIZE],
}
pub type CidV1Sha256 = CidV1<32>;

pub fn cidv1_meta(version: u8, codec: u8, hash_type: u8) -> u32 {
    version as u32 | ((codec as u32) << 8) | ((hash_type as u32) << 16)
}

impl FromStr for CidV1Sha256 {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (_base, data) = multibase::decode(s)?;

        let [version, codec, hash_type] = data[..3] else {
            anyhow::bail!("not enough data")
        };
        if version != 1 {
            anyhow::bail!("version was incorrect (expected 1, got {})", version);
        }
        if hash_type != 0x12 {
            anyhow::bail!(
                "multihash hash type was incorrect (expected sha256 [0x12], got {:x})",
                hash_type
            );
        }

        let hash_size = data[3];
        if hash_size != 32 {
            anyhow::bail!(
                "multihash hash size was incorrect (expected 32, got {})",
                hash_size
            );
        }

        let mut hash: [u8; 32] = [0; 32];
        hash.copy_from_slice(&data[4..4 + 32]);

        Ok(CidV1Sha256 {
            meta: cidv1_meta(version, codec, hash_type),
            hash,
        })
    }
}

impl Display for CidV1Sha256 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut bytes = Vec::<u8>::with_capacity(self.hash.len() + 8);
        {
            let mut cursor = Cursor::new(&mut bytes);
            cursor
                .write_all(&[
                    self.meta as u8,         // version
                    (self.meta >> 8) as u8,  // codec
                    (self.meta >> 16) as u8, // hash_type
                    32,
                ])
                .map_err(|_| std::fmt::Error)?;
            cursor.write_all(&self.hash).map_err(|_| std::fmt::Error)?;
        }
        let mb = multibase::encode(multibase::Base::Base32Lower, &bytes);
        write!(f, "{}", mb)
    }
}
