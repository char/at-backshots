use anyhow::Result;
use backshots::{get_app_config, storage::live_guards::LiveWriteHandle, AppContext};

fn main() -> Result<()> {
    let cfg = get_app_config()?;
    let app = AppContext::new(&cfg)?;
    LiveWriteHandle::add_new(&app)?;
    Ok(())
}
