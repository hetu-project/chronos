use std::{convert::identity, sync::OnceLock, time::Duration};

use messages::AddrBook;
use replication_control_messages as messages;
use tokio::{
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    CLIENT
        .set(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .build()?,
        )
        .unwrap();
    let result = benchmark_session(messages::Protocol::Unreplicated).await?;
    // let result = benchmark_session(messages::Protocol::Pbft).await?;
    println!("{result:?}");
    Ok(())
}

static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

async fn client_session(config: messages::Client, url: String) -> anyhow::Result<(f32, Duration)> {
    let client = CLIENT.get().unwrap();
    client
        .post(format!("{url}/run-client"))
        .json(&config)
        .send()
        .await?
        .error_for_status()?;
    loop {
        sleep(Duration::from_secs(1)).await;
        let result = client
            .post(format!("{url}/join-client"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(result) = result {
            break Ok(result);
        }
    }
}

async fn replica_session(url: String, shutdown: CancellationToken) -> anyhow::Result<()> {
    let client = CLIENT.get().unwrap();
    while timeout(Duration::from_secs(1), shutdown.cancelled())
        .await
        .is_err()
    {
        client
            .get(format!("{url}/ok"))
            .send()
            .await?
            .error_for_status()?;
    }
    client
        .post(format!("{url}/reset-replica"))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}

async fn benchmark_session(protocol: messages::Protocol) -> anyhow::Result<(f32, Duration)> {
    let client = CLIENT.get().unwrap();
    let mut addr_book = AddrBook::default();
    addr_book
        .client_addrs
        .insert(0, ([127, 0, 0, 1], 20000).into());
    for i in 0..4 {
        addr_book
            .replica_addrs
            .insert(i, ([127, 0, 0, 1], 30000 + i as u16).into());
    }
    let num_faulty = match protocol {
        messages::Protocol::Unreplicated => 0,
        _ => 1,
    };
    let num_replica = 3 * num_faulty + 1;

    let mut replica_sessions = JoinSet::new();
    let shutdown = CancellationToken::new();
    let urls = [
        "http://127.0.0.1:10000",
        "http://127.0.0.1:10001",
        "http://127.0.0.1:10002",
        "http://127.0.0.1:10003",
    ];
    for (i, url) in urls.into_iter().enumerate().take(num_replica) {
        let config = messages::Replica {
            id: i as _,
            addr_book: addr_book.clone(),
            num_replica,
            num_faulty,
            protocol,
        };
        client
            .post(format!("{url}/run-replica"))
            .json(&config)
            .send()
            .await?
            .error_for_status()?;
        replica_sessions.spawn(replica_session(url.into(), shutdown.clone()));
    }
    let mut client_sessions = JoinSet::new();
    let config = messages::Client {
        id_range: 0..1,
        addr_book,
        num_replica,
        num_faulty,
        protocol,
    };
    let url = "http://127.0.0.1:10004".into();
    client_sessions.spawn(client_session(config, url));
    let mut throughput_sum = 0.;
    let mut latency_sum = Duration::ZERO;
    while let Some(client_result) = tokio::select! {
        result = client_sessions.join_next() => result,
        result = replica_sessions.join_next() => Err(result.unwrap().map_err(Into::into).and_then(identity).unwrap_err())?,
    } {
        // println!("{client_result:?}");
        let (throughput, latency) = client_result??;
        throughput_sum += throughput;
        latency_sum += latency * throughput as _;
    }
    shutdown.cancel();
    while let Some(replica_result) = replica_sessions.join_next().await {
        replica_result??
    }
    Ok((throughput_sum, latency_sum / throughput_sum.ceil() as _))
}
