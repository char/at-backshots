use std::time::Duration;

use tokio::time::Instant;

use backshots::AppState;
use tokio::time::sleep_until;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let app = AppState::new("/dev/shm/backshots/data", "http://127.0.0.1:2485".into())?;

    let mut last_count = 0;
    loop {
        let now = Instant::now();
        let count = {
            app.db().query_row(
                "SELECT count FROM counts WHERE key = 'backlinks'",
                (),
                |row| row.get::<_, u64>(0),
            )?
        };
        println!("+ {}", count - last_count);
        sleep_until(now.checked_add(Duration::from_secs(1)).unwrap()).await;
        last_count = count;
    }
}
