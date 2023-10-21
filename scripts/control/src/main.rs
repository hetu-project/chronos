use std::{
    fmt::Write,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering::SeqCst},
        Arc,
    },
    time::Duration,
};

use control_messages::{App, BenchmarkClient, BenchmarkStats, Replica, Role, Task};
use reqwest::Client;
use tokio::{select, spawn, time::sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ycsb_app = App::Ycsb(control_messages::YcsbConfig {
        num_key: 10 * 1000,
        num_value: 100 * 1000,
        key_len: 64,
        value_len: 128,
        read_portion: 50,
        update_portion: 40,
        rmw_portion: 10,
    });
    match std::env::args().nth(1).as_deref() {
        Some("test") => {
            run(
                5,
                200,
                1,
                "hotstuff",
                App::Null,
                0.,
                1,
                &[],
                &mut std::io::empty(),
            )
            .await
        }
        Some("fpga") => {
            let saved = std::fs::read_to_string("saved-fpga.csv").unwrap_or_default();
            let saved_lines = Vec::from_iter(saved.lines());
            let mut out = std::fs::File::options()
                .create(true)
                .append(true)
                .open("saved-fpga.csv")
                .unwrap();
            run_clients(
                "unreplicated",
                [1].into_iter()
                    .chain((2..=20).step_by(2))
                    .chain((20..=100).step_by(10))
                    .chain((100..=200).step_by(20)),
                &saved_lines,
                &mut out,
            )
            .await;
            run_clients(
                "neo-pk",
                [1].into_iter()
                    .chain((2..=40).step_by(2))
                    .chain((40..=100).step_by(10))
                    .chain((100..=200).step_by(20)),
                &saved_lines,
                &mut out,
            )
            .await;
            run_clients(
                "neo-bn",
                [1].into_iter()
                    .chain((2..=60).step_by(2))
                    .chain((60..=100).step_by(10))
                    .chain((100..=300).step_by(20)),
                &saved_lines,
                &mut out,
            )
            .await;
            run_clients(
                "pbft",
                [1].into_iter()
                    .chain((2..=60).step_by(2))
                    .chain((60..=100).step_by(10)),
                &saved_lines,
                &mut out,
            )
            .await;
            run_clients(
                "zyzzyva",
                [1].into_iter().chain((2..=20).step_by(2)),
                &saved_lines,
                &mut out,
            )
            .await;
            run_clients(
                "zyzzyva-f",
                [1].into_iter().chain((2..=20).step_by(2)),
                &saved_lines,
                &mut out,
            )
            .await;
            run_clients(
                "hotstuff",
                [1].into_iter()
                    .chain((2..=60).step_by(2))
                    .chain((60..=100).step_by(10))
                    .chain((100..=200).step_by(20)),
                &saved_lines,
                &mut out,
            )
            .await;
            run_clients(
                "minbft",
                [1].into_iter()
                    .chain((2..=60).step_by(2))
                    .chain((60..=100).step_by(10))
                    .chain((100..=300).step_by(20)),
                &saved_lines,
                &mut out,
            )
            .await;

            for mode in [
                "unreplicated",
                "neo-pk",
                "neo-bn",
                "pbft",
                // "zyzzyva",
                // "zyzzyva-f",
                "hotstuff",
                "minbft",
            ] {
                run_full_throughput(mode, ycsb_app, 0., &saved_lines, &mut out).await
            }
            run(5, 10, 1, "zyzzyva", ycsb_app, 0., 1, &saved_lines, &mut out).await;
            run(
                5,
                6,
                1,
                "zyzzyva-f",
                ycsb_app,
                0.,
                1,
                &saved_lines,
                &mut out,
            )
            .await;

            for drop_rate in [1e-5, 5e-5, 1e-4, 5e-4, 1e-3] {
                run_full_throughput("neo-pk", App::Null, drop_rate, &saved_lines, &mut out).await
            }
        }
        Some("hmac") => {
            let saved = std::fs::read_to_string("saved-hmac.csv").unwrap_or_default();
            let saved_lines = Vec::from_iter(saved.lines());
            let mut out = std::fs::File::options()
                .create(true)
                .append(true)
                .open("saved-hmac.csv")
                .unwrap();

            run_clients(
                "neo-hm",
                [1].into_iter()
                    .chain((2..=40).step_by(2))
                    .chain((40..=100).step_by(10))
                    .chain((100..=200).step_by(20)),
                &saved_lines,
                &mut out,
            )
            .await;

            run_full_throughput("neo-hm", ycsb_app, 0., &saved_lines, &mut out).await;

            for drop_rate in [1e-5, 5e-5, 1e-4, 5e-4, 1e-3] {
                run_full_throughput("neo-hm", App::Null, drop_rate, &saved_lines, &mut out).await
            }
        }
        #[cfg(not(feature = "aws"))]
        Some("aws") => panic!("require enable aws feature"),
        #[cfg(feature = "aws")]
        Some("aws") => {
            let saved = std::fs::read_to_string("saved-aws.csv").unwrap_or_default();
            let saved_lines = Vec::from_iter(saved.lines());
            let mut out = std::fs::File::options()
                .create(true)
                .append(true)
                .open("saved-aws.csv")
                .unwrap();

            for num_faulty in 2..=33 {
                run(
                    1,
                    1,
                    match num_faulty {
                        n if n > 32 => 6,
                        n if n > 29 => 7,
                        n if n > 26 => 8,
                        n if n > 24 => 9,
                        n if n > 21 => 10,
                        n if n > 19 => 11,
                        n if n > 17 => 12,
                        n if n > 14 => 14,
                        n if n > 11 => 15,
                        n if n > 9 => 20,
                        n if n > 7 => 30,
                        n if n > 5 => 44,
                        n if n > 3 => 72,
                        3 => 80,
                        2 => 100,
                        _ => unreachable!(),
                    },
                    "neo-hm",
                    App::Null,
                    0.,
                    num_faulty,
                    &saved_lines,
                    &mut out,
                )
                .await
            }
            for num_faulty in 2..=33 {
                run(
                    1,
                    1,
                    100,
                    "neo-pk",
                    App::Null,
                    0.,
                    num_faulty,
                    &saved_lines,
                    &mut out,
                )
                .await
            }
        }

        _ => unimplemented!(),
    }
}

async fn run_full_throughput(
    mode: &str,
    app: App,
    drop_rate: f64,
    saved_lines: &[&str],
    out: impl std::io::Write,
) {
    run(5, 200, 1, mode, app, drop_rate, 1, saved_lines, out).await
}

async fn run_clients(
    mode: &str,
    num_clients_in_5_groups: impl Iterator<Item = usize>,
    saved_lines: &[&str],
    mut out: impl std::io::Write,
) {
    run(1, 1, 1, mode, App::Null, 0., 1, saved_lines, &mut out).await;
    for num_client in num_clients_in_5_groups {
        run(
            5,
            num_client,
            1,
            mode,
            App::Null,
            0.,
            1,
            saved_lines,
            &mut out,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    num_group: usize,
    num_client: usize,
    num_client_host: usize,
    mode: &str,
    app: App,
    drop_rate: f64,
    num_faulty: usize,
    saved_lines: &[&str],
    mut out: impl std::io::Write,
) {
    let client_addrs;
    let replica_addrs;
    let multicast_addr;
    let client_hosts;
    let replica_hosts;

    #[cfg(not(feature = "aws"))]
    {
        assert!(num_faulty <= 1);
        client_addrs = (20000..).map(|port| SocketAddr::from(([10, 0, 0, 10], port)));
        replica_addrs = vec![
            SocketAddr::from(([10, 0, 0, 1], 10000)),
            SocketAddr::from(([10, 0, 0, 2], 10000)),
            SocketAddr::from(([10, 0, 0, 3], 10000)),
            SocketAddr::from(([10, 0, 0, 4], 10000)),
        ];
        multicast_addr = SocketAddr::from(([10, 0, 0, 255], 60004));

        client_hosts = ["nsl-node10.d2"];
        assert_eq!(num_client_host, 1);
        replica_hosts = [
            "nsl-node1.d2",
            "nsl-node2.d2",
            "nsl-node3.d2",
            "nsl-node4.d2",
        ];
    }

    #[cfg(feature = "aws")]
    let output = neo_aws::Output::new_terraform();
    #[cfg(feature = "aws")]
    {
        use std::net::Ipv4Addr;
        client_addrs = output
            .client_ips
            .into_iter()
            .map(|ip| ip.parse::<Ipv4Addr>().unwrap())
            .flat_map(|ip| {
                (20000..)
                    .take(num_group * num_client)
                    .map(move |port| SocketAddr::from((ip, port)))
            });
        #[allow(clippy::int_plus_one)]
        {
            assert!(
                output.replica_ips.len() >= 2 * num_faulty + 1,
                "there are only {} replicas",
                output.replica_ips.len()
            )
        }
        replica_addrs = Vec::from_iter(
            output
                .replica_ips
                .into_iter()
                .map(|ip| SocketAddr::from((ip.parse::<Ipv4Addr>().unwrap(), 10000)))
                // TODO clarify this and avoid pitfall
                .chain((30000..).map(|port| SocketAddr::from(([127, 0, 0, 1], port))))
                .take(3 * num_faulty + 1),
        );
        multicast_addr =
            SocketAddr::from((output.sequencer_ip.parse::<Ipv4Addr>().unwrap(), 60004));
        client_hosts = output.client_hosts;
        replica_hosts = output.replica_hosts
    }

    assert!(client_hosts.len() >= num_client_host);
    let client_addrs = Vec::from_iter(client_addrs.take(num_group * num_client * num_client_host));
    let id = format!(
        "{mode},{},{drop_rate},{},{num_faulty}",
        match app {
            App::Null => "null",
            App::Ycsb(_) => "ycsb",
        },
        client_addrs.len(),
    );
    println!("* work on {id}");
    if saved_lines.iter().any(|line| line.starts_with(&id)) {
        println!("* skip because exist record found");
        return;
    }

    #[cfg(feature = "aws")]
    {
        std::process::Command::new("ssh")
            .args([
                &output.sequencer_host,
                "pkill",
                "-KILL",
                "--full",
                "neo-sequencer",
            ])
            .status()
            .unwrap();

        let status = std::process::Command::new("ssh")
                .arg(output.sequencer_host)
                .arg(format!(
                    "./neo-sequencer {} {} {} 1>./neo-sequencer-stdout.txt 2>./neo-sequencer-stderr.txt &",
                    match mode {
                        "neo-hm" => "half-sip-hash",
                        "neo-pk" => "k256",
                        _ => unimplemented!(),
                    },
                    num_faulty * 3 + 1,
                    output.relay_ips[0],
                ))
                .status()
                .unwrap();
        assert!(status.success());
    }

    let task = |role| Task {
        mode: String::from(mode),
        app,
        client_addrs: client_addrs.clone(),
        replica_addrs: replica_addrs.clone(),
        multicast_addr,
        num_faulty,
        drop_rate,
        seed: 3603269_3604874,
        role,
    };

    let cancel = CancellationToken::new();
    let hook = std::panic::take_hook();
    std::panic::set_hook({
        let cancel = cancel.clone();
        Box::new(move |info| {
            cancel.cancel();
            hook(info)
        })
    });

    let http_client = Arc::new(Client::new());
    let panic = Arc::new(AtomicBool::new(false));
    println!("* start replicas");
    let mut sessions = Vec::from_iter(
        replica_hosts
            .into_iter()
            .enumerate()
            .take(match mode {
                "unreplicated" => 1,
                "minbft" => num_faulty + 1,
                "zyzzyva" => 3 * num_faulty + 1,
                _ => 2 * num_faulty + 1,
            })
            .map(|(index, host)| {
                spawn(host_session(
                    host,
                    task(Role::Replica(Replica { index: index as _ })),
                    http_client.clone(),
                    cancel.clone(),
                    panic.clone(),
                ))
            }),
    );

    sleep(Duration::from_secs(1)).await;
    println!("* start clients");
    let mut benchmark = BenchmarkClient {
        num_group,
        num_client,
        offset: 0,
        duration: Duration::from_secs(10),
    };
    let mut delay = Duration::from_millis(100);
    for client_host in client_hosts.iter().take(num_client_host) {
        sessions.push(spawn(host_session(
            client_host.to_string(),
            task(Role::BenchmarkClient(benchmark)),
            http_client.clone(),
            cancel.clone(),
            panic.clone(),
        )));
        benchmark.offset += num_group * num_client;
        sleep(delay).await;
        delay = Duration::ZERO;
    }

    let mut throughput = 0.;
    let mut result = String::new();
    for (index, client_host) in client_hosts.into_iter().enumerate().take(num_client_host) {
        if index == 0 {
            sleep(Duration::from_secs(1)).await
        }
        loop {
            let response = http_client
                .get(format!("http://{client_host}:9999/benchmark"))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
            if let Some(stats) = response.json::<Option<BenchmarkStats>>().await.unwrap() {
                println!("* {stats:?}");
                assert_ne!(stats.throughput, 0.);
                writeln!(
                    &mut result,
                    "{id},{index},{},{}",
                    stats.throughput,
                    stats.average_latency.unwrap().as_nanos() as f64 / 1000.,
                )
                .unwrap();
                throughput += stats.throughput;
                break;
            }
            select! {
                _ = sleep(Duration::from_secs(1)) => {}
                _ = cancel.cancelled() => break,
            }
        }
    }

    cancel.cancel();
    for session in sessions {
        session.await.unwrap()
    }
    assert!(!panic.load(SeqCst));
    if num_client_host > 1 {
        println!("{throughput}");
        out.write_all(result.as_bytes()).unwrap()
    }
}

async fn host_session(
    host: impl Into<String>,
    task: Task,
    client: Arc<Client>,
    cancel: CancellationToken,
    panic: Arc<AtomicBool>,
) {
    let host = host.into();
    // println!("{host}");
    let endpoint = format!("http://{host}:9999");
    client
        .post(format!("{endpoint}/task"))
        .json(&task)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    loop {
        select! {
            _ = sleep(Duration::from_secs(1)) => {}
            _ = cancel.cancelled() => break,
        }
        let response = client
            .get(format!("{endpoint}/panic"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        if response.json::<bool>().await.unwrap() {
            println!("! {host} panic");
            panic.store(true, SeqCst);
            cancel.cancel();
            break;
        }
    }
    if !panic.load(SeqCst) {
        client
            .post(format!("{endpoint}/reset"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
}
