use anyhow::Result;
use backshots::{get_app_config, storage::guards::LiveStorageWriterGuard, AppContext};

fn main() -> Result<()> {
    let cfg = get_app_config()?;
    let app = AppContext::new(&cfg)?;
    LiveStorageWriterGuard::add_new(&app)?;
    Ok(())
}
