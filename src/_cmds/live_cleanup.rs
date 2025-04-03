use std::path::Path;

use anyhow::Result;
use backshots::{get_app_config, AppContext};
use nix::{libc::pid_t, sys::signal::kill};

pub fn has_running_procs(store_dir: &Path) -> Result<bool> {
    for file in std::fs::read_dir(store_dir)?.filter_map(Result::ok) {
        let Ok(file_name) = file.file_name().into_string() else {
            continue;
        };
        let Some(pid) = file_name.strip_suffix(".pid") else {
            continue;
        };
        let pid: pid_t = pid.parse()?;
        let pid = nix::unistd::Pid::from_raw(pid);
        let proc_running = !matches!(kill(pid, None), Err(nix::errno::Errno::ESRCH));
        if proc_running {
            return Ok(true);
        }
    }

    Ok(false)
}

fn main() -> Result<()> {
    let cfg = get_app_config()?;
    let app = AppContext::new(&cfg)?;

    let mut statement = app
        .db
        .prepare("SELECT name FROM data_stores WHERE type = 'live' ORDER BY id ASC")?;
    for store in statement
        .query_map((), |row| row.get::<_, String>(0))?
        .filter_map(Result::ok)
    {
        let store_dir = cfg.data_dir.join("live").join(store);
        if has_running_procs(&store_dir)? {
            continue;
        }

        std::fs::remove_dir_all(store_dir)?;
    }

    Ok(())
}
