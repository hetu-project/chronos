// TODO overhaul with per-setup feature

use std::{
    process::{Command, Stdio},
    thread::{sleep, spawn},
    time::Duration,
};

#[cfg(not(feature = "neo-aws"))]
const HOSTS: &[&str] = &[
    "nsl-node1.d2",
    "nsl-node2.d2",
    "nsl-node3.d2",
    "nsl-node4.d2",
    "nsl-node10.d2",
];
#[cfg(not(feature = "neo-aws"))]
const LOCALHOST: &str = "nsl-node1.d2";
#[cfg(feature = "neo-aws")]
const LOCALHOST: &str = "localhost";
#[cfg(not(feature = "neo-aws"))]
const WORK_DIR: &str = "/local/cowsay/artifacts";
#[cfg(feature = "neo-aws")]
const WORK_DIR: &str = "/home/ubuntu";
const PROGRAM: &str = "permissioned-blockchain";

fn main() {
    let status = Command::new("cargo")
        .args(["build", "--release", "--workspace", "--bin", PROGRAM])
        .status()
        .unwrap();
    assert!(status.success());

    #[cfg(not(feature = "neo-aws"))]
    let hosts = Vec::from_iter(HOSTS.iter().map(ToString::to_string));
    #[cfg(feature = "neo-aws")]
    let hosts = {
        let output = neo_aws::Output::new_terraform();
        [&*output.client_hosts, &*output.replica_hosts].concat()
    };

    let rsync_threads =
        Vec::from_iter(hosts.iter().filter(|&host| host != LOCALHOST).map(|host| {
            let host = host.to_string();
            spawn(move || host_session(&host))
        }));
    for thread in rsync_threads {
        thread.join().unwrap()
    }
    if hosts.contains(&LOCALHOST.to_string()) {
        host_session(LOCALHOST)
    }
    println!()
}

fn host_session(host: &str) {
    let status = Command::new("rsync")
        .arg(format!("target/release/{PROGRAM}"))
        .arg(format!("{host}:{WORK_DIR}"))
        .status()
        .unwrap();
    assert!(status.success());
    let status = Command::new("ssh")
        .args([host, "pkill", "-INT", "--full", PROGRAM])
        .status()
        .unwrap();
    // sleep(Duration::from_secs(1));
    if status.success() {
        let status = Command::new("ssh")
            .args([host, "pkill", "-KILL", "--full", PROGRAM])
            .status()
            .unwrap();
        // sleep(Duration::from_secs(1));
        if status.success() {
            println!("! cleaned nonresponsive server on {host}")
        }
    }
    let status = Command::new("ssh")
        .arg(host)
        .arg(format!("{WORK_DIR}/{PROGRAM} 1>{WORK_DIR}/{PROGRAM}-stdout.txt 2>{WORK_DIR}/{PROGRAM}-stderr.txt &"))
        .status()
        .unwrap();
    assert!(status.success());
    sleep(Duration::from_secs(1));
    let status = Command::new("curl")
        .arg("--silent")
        .arg(format!("http://{host}:9999/panic"))
        .stdout(Stdio::null())
        // .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success());
    eprint!("* server started on {host}        \r")
}
