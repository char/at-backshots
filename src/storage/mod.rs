use std::os::fd::AsFd;

use anyhow::Result;
use nix::{libc::off_t, sys::uio};

pub mod compacted_writer;
pub mod live_writer;

pub fn pread_all(fd: impl AsFd, buf: &mut [u8], offset: usize) -> Result<()> {
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

pub fn pwrite_all(fd: impl AsFd, buf: &[u8], offset: usize) -> Result<()> {
    let mut written = 0;
    while written < buf.len() {
        written += uio::pwrite(fd.as_fd(), &buf[written..], (offset + written) as off_t)?;
    }
    Ok(())
}
