use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Output {
    pub client_hosts: Vec<String>,
    pub client_ips: Vec<String>,
    pub replica_hosts: Vec<String>,
    pub replica_ips: Vec<String>,
    pub sequencer_host: String,
    pub sequencer_ip: String,
    pub relay_hosts: Vec<String>,
    pub relay_ips: Vec<String>,
}

impl Output {
    pub fn new_terraform() -> Self {
        let output = std::process::Command::new("terraform")
            .args(["-chdir=scripts/neo-aws", "output"])
            .output()
            .unwrap();
        assert!(output.status.success());
        toml::from_str(std::str::from_utf8(&output.stdout).unwrap()).unwrap()
    }
}
