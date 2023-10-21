use std::{process::Command, thread::spawn};

fn main() {
    let status = Command::new("cargo")
        .args(["build", "--release", "--package", "relay"])
        .status()
        .unwrap();
    assert!(status.success());
    let status = Command::new("cargo")
        .args([
            "build",
            "--release",
            "--workspace",
            "--bin",
            "neo-sequencer",
        ])
        .status()
        .unwrap();
    assert!(status.success());

    let output = neo_aws::Output::new_terraform();
    let mut sessions = Vec::from_iter(output.replica_hosts.into_iter().map(|host| {
        spawn(move || {
            let status = Command::new("ssh")
                .arg(host)
                .arg(concat!(
                    "sudo ethtool -L ens5 combined 1 &&",
                    "sudo service irqbalance stop &&",
                    "IRQBALANCE_BANNED_CPULIST=0-1 sudo -E irqbalance --oneshot",
                ))
                .status()
                .unwrap();
            assert!(status.success());
        })
    }));
    let mut relay_args = vec![output.relay_ips[1..].join(" ")];
    for i in 0..5 {
        relay_args.push(
            Vec::from_iter(output.replica_ips.iter().skip(i * 14).take(14).cloned()).join(" "),
        )
    }
    sessions.extend(
        output
            .relay_hosts
            .into_iter()
            .zip(relay_args)
            .map(|(host, args)| {
                spawn(move || {
                    let status = Command::new("rsync")
                        .arg("target/release/relay")
                        .arg(format!("{host}:"))
                        .status()
                        .unwrap();
                    assert!(status.success());

                    Command::new("ssh")
                        .args([&host, "pkill", "-KILL", "--full", "relay"])
                        .status()
                        .unwrap();

                    let status = Command::new("ssh")
                        .arg(host)
                        .arg(format!(
                            "./relay {args} 1>./relay-stdout.txt 2>./relay-stderr.txt &"
                        ))
                        .status()
                        .unwrap();
                    assert!(status.success());
                })
            }),
    );
    sessions.push(spawn(move || {
        let status = Command::new("rsync")
            .arg("target/release/neo-sequencer")
            .arg(format!("{}:", output.sequencer_host))
            .status()
            .unwrap();
        assert!(status.success())
    }));
    for thread in sessions {
        thread.join().unwrap()
    }
}
