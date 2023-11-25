use accumulator::Configuration;
use clap::Parser;

/// Command line parser
#[derive(Parser)]
struct Cli {
    config_path: String,
    message: String,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let config = Configuration::from_file(&args.config_path);
    let mut client = accumulator::Client::new(&config).await;
    client.disseminate(&args.message).await;
}
