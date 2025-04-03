use std::{collections::HashSet, path::Path};

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
    let stores = statement
        .query_map((), |row| row.get::<_, String>(0))?
        .filter_map(Result::ok)
        .collect::<HashSet<_>>();

    let live_dir = cfg.data_dir.join("live");

    for store_entry in live_dir.read_dir()?.filter_map(Result::ok) {
        if store_entry.file_type()?.is_file() {
            continue;
        }

        let store = store_entry.file_name();
        if store.to_str().map(|it| stores.contains(it)).unwrap_or(true) {
            continue;
        }

        let store_dir = live_dir.join(&store);
        if has_running_procs(&store_dir)? {
            continue;
        }

        println!("cleaning up: {}…", store.to_string_lossy());
        std::fs::remove_dir_all(store_dir)?;
    }

    Ok(())
}
