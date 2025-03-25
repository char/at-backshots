use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use backshots::{get_app_config, AppContext};

fn main() -> Result<()> {
    let cfg = get_app_config()?;
    let app = AppContext::new(&cfg)?;

    let mut last_count = 0;
    loop {
        let now = Instant::now();
        let count = {
            app.db.query_row(
                "SELECT count FROM counts WHERE key = 'backlinks'",
                (),
                |row| row.get::<_, u64>(0),
            )?
        };
        println!("+ {}", count - last_count);
        std::thread::sleep(
            now.checked_add(Duration::from_secs(1))
                .unwrap()
                .duration_since(Instant::now()),
        );
        last_count = count;
    }
}
