pub mod server;
use anyhow::anyhow;
use clap::{Parser, Subcommand};
use server::{generate_config, run, ServerConfig};
use std::net::Ipv4Addr;
use std::{fs::File, io::Write, process, sync::Arc};
use tracing::*;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser, Debug)]
#[command(author="Hetu Protocol", version="0.1", about="This server be used to test vlc network.", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "server.json")]
    server: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Run,
    Generate {
        #[arg(long, default_value = "/ip4/127.0.0.1/tcp/")]
        host: String,
        #[arg(long, default_value = "9600")]
        port: u16,
        #[arg(long, default_value = "vlc")]
        topic: String,
        #[arg(long)]
        bootnodes: Option<Vec<String>>,
        #[arg(long, default_value = "dht")]
        discover: String,
        #[arg(long, default_value = "5")]
        max_discover_node: u32, // max target peer count
        #[arg(long, default_value = "1.0")]
        disconnect_rate: f64, // disconnet rate with peers when memory exhausted
        #[arg(long, default_value = "false")]
        enable_tx_send: bool,
        #[arg(long, default_value = "0")]   // cpu cores
        concurrent_verify: u64,
        #[arg(long, default_value = "1")]
        time_window_s: u64,
        #[arg(long, default_value = "1000")]
        trigger_us: u64,
        #[arg(long, default_value = "80")]
        max_sys_memory_percent: u64,
        #[arg(long, default_value = "8192")]
        max_proc_memory_mb: u64,
        #[arg(long, default_value = "0")]
        init_clock_keys: u32,
        #[arg(long, default_value = "10")] // Hello Hetu
        payload_bytes_len: u64,
        #[arg(long, default_value = "false")]
        print_vlc: bool,
        #[arg(long, default_value = "false")]
        tokio_console: bool,
        #[arg(long, default_value = "6669")]
        tokio_console_port: u16,
        #[arg(long, default_value = "false")]
        pprof: bool,
    },
}

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    // set default log level: INFO
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let fmt_layer = fmt::layer().with_filter(EnvFilter::new(rust_log));
    let log_layers = tracing_subscriber::registry().with(fmt_layer);

    // graceful exit process
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
    let shutdown_signal = async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        shutdown_sender
            .send(())
            .expect("Failed to send shutdown signal");
    };

    let args = Args::parse();
    match &args.command {
        Command::Run => {
            let file = File::open(&args.server)
                .expect("Server configuration file is not exist,failed to open");
            let reader = std::io::BufReader::new(file);
            let config: ServerConfig = serde_json::from_reader(reader)
                .expect("Format of configuration file is not correct");
            if config.tokio_console {
                let console_layer = console_subscriber::ConsoleLayer::builder()
                    .server_addr((Ipv4Addr::LOCALHOST, config.tokio_console_port))
                    .spawn();
                log_layers.with(console_layer).init();
            } else {
                log_layers.init();
            }
            let is_pprof = if config.pprof {
                #[allow(non_upper_case_globals)]
                #[export_name = "malloc_conf"]
                pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";
                info!("prof:true,prof_active:true,lg_prof_sample:19");
                Some(true)
            } else {
                None
            };

            info!("start test vlc node server");
            tokio::select! {
                _ = shutdown_signal => {
                    info!("Received Ctrl+C, shutting down");
                    if let Some(_guard) = is_pprof {
                        let mut heap_file = File::create("heap.pb.gz").expect("Failed to create jemalloc heap file");
                        if let Ok(jemalloc) = jemalloc_activated().await {
                            heap_file.write_all(&jemalloc).expect("Failed to write to jemalloc heap file");
                        }
                    }
                }
                _ = shutdown_receiver => {
                    info!("Shutdown signal received");
                }
                result = run(Arc::new(config.clone())) => {
                    if let Err(e) = result {
                        error!("Server error: {:?}", e);
                    }
                }
            }
            process::exit(0);
        }
        Command::Generate {
            host,
            port,
            topic,
            bootnodes,
            discover,
            max_discover_node,
            enable_tx_send,
            concurrent_verify,
            trigger_us,
            max_sys_memory_percent,
            max_proc_memory_mb,
            disconnect_rate,
            time_window_s,
            init_clock_keys,
            payload_bytes_len,
            print_vlc,
            tokio_console,
            tokio_console_port,
            pprof,
            ..
        } => {
            info!("start generate vlc node config file");
            generate_config(
                host.clone(),
                *port,
                topic.clone(),
                bootnodes.clone(),
                discover.clone(),
                *max_discover_node,
                *enable_tx_send,
                *concurrent_verify,
                *trigger_us,
                *max_sys_memory_percent,
                *max_proc_memory_mb,
                *disconnect_rate,
                *time_window_s,
                *init_clock_keys,
                *payload_bytes_len,
                *print_vlc,
                *tokio_console,
                *tokio_console_port,
                *pprof,
                &args.server,
            );
        }
    }
}

pub async fn jemalloc_activated() -> Result<Vec<u8>, anyhow::Error> {
    let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
    if prof_ctl.activated() {
        let pprof = prof_ctl.dump_pprof()?;
        return Ok(pprof);
    }
    Err(anyhow!("jemalloc profiling is not activated"))
}
