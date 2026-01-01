use shared::TestProcessesConfigManual;
use std::env;

#[tokio::main]
async fn main() {
    let port_range_start: u16 = env::args()
        .nth(1)
        .unwrap_or_else(|| "6000".to_string())
        .parse()
        .unwrap_or_else(|_| {
            eprintln!("Expected a number for port_range_start. Example: cargo run -- 6000");
            std::process::exit(2);
        });

    let config = TestProcessesConfigManual::new(3, 6000);
    config.start_single(port_range_start as usize - 6000).await;
}