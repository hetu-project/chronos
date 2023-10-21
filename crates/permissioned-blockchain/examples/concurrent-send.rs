use std::{iter::repeat, sync::Arc};

use tokio::{net::UdpSocket, spawn};

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let socket = Arc::new(UdpSocket::bind("10.0.0.1:0").await.unwrap());
            let send_tasks = Vec::from_iter(repeat(socket).take(1000).map(|socket| {
                spawn(async move {
                    socket
                        .send_to(&vec![0; 1400], "10.0.0.10:10000")
                        .await
                        .unwrap()
                })
            }));
            for task in send_tasks {
                task.await.unwrap();
            }
        })
}
