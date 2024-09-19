use tokio::process::Command;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // the path of dockerfile and app.eif putted path
    let item = std::env::args().nth(1);
    println!("* Install Nitro CLI");
    let status = Command::new("sh")
        .arg("-c")
        .arg(
            String::from("sudo dnf install -y tmux htop openssl-devel perl docker-24.0.5-1.amzn2023.0.3 aws-nitro-enclaves-cli aws-nitro-enclaves-cli-devel")
            + " && sudo usermod -aG ne ec2-user"
            + " && sudo usermod -aG docker ec2-user"
            + " && sudo systemctl restart docker"
            + " && sudo systemctl restart nitro-enclaves-allocator.service"
            + " && sudo systemctl enable --now nitro-enclaves-allocator.service"
            + " && sudo systemctl enable --now docker"
        )
        .status()
        .await?;
    anyhow::ensure!(status.success());

    println!("* Build artifact");
    let status = Command::new("cargo")
        .args([
            "build",
            "--target",
            "x86_64-unknown-linux-musl",
            "--profile",
            "artifact",
            "--features",
            "nitro-enclaves,tikv-jemallocator",
            "--bin",
            "tee_vlc",
        ])
        .status()
        .await?;
    anyhow::ensure!(status.success());

    println!("* cp artifact");
    let status = Command::new("cp")
        .arg("target/x86_64-unknown-linux-musl/artifact/tee_vlc")
        .arg(item.clone().ok_or(anyhow::format_err!("missing destination path"))?)
        .status()
        .await?;
    anyhow::ensure!(status.success());

    println!("* cd docker folder and build enclave image file");
    let status = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "cd {} && docker build . -t tee_vlc && nitro-cli build-enclave --docker-uri tee_vlc:latest --output-file tee_vlc.eif",
            item.clone().ok_or(anyhow::format_err!("missing destination path"))?
        ))
        .status()
        .await?;
    anyhow::ensure!(status.success());

    println!("* cd dockerfile folder and run enclave image");
    let status = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "cd {} && nitro-cli run-enclave --cpu-count 2 --memory 2048 --enclave-cid 16 --eif-path tee_vlc.eif",
            item.ok_or(anyhow::format_err!("missing destination path"))?
        ))
        .status()
        .await?;
    anyhow::ensure!(status.success());

    Ok(())
}
