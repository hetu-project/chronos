use std::{
    env::args,
    iter::repeat,
    net::{Ipv4Addr, UdpSocket},
    sync::Arc,
    thread::{available_parallelism, spawn},
};

use nix::{
    sched::{sched_setaffinity, CpuSet},
    unistd::Pid,
};

pub fn set_affinity(index: usize) {
    let mut cpu_set = CpuSet::new();
    cpu_set.set(index).unwrap();
    sched_setaffinity(Pid::from_raw(0), &cpu_set).unwrap()
}

fn main() {
    let ips = Vec::from_iter(args().skip(1).map(|ip| ip.parse::<Ipv4Addr>().unwrap()));
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:60004").unwrap());
    // let messages = flume::bounded::<Vec<_>>(1024);
    let messages = flume::unbounded::<Vec<_>>();
    for ((index, messages), (socket, ips)) in repeat(messages.1)
        .take(usize::from(available_parallelism().unwrap()) - 1)
        .enumerate()
        .zip(repeat((socket.clone(), ips)))
    {
        spawn(move || {
            set_affinity(index + 1);
            loop {
                let buf = messages.recv().unwrap();
                for &ip in &ips {
                    socket.send_to(&buf, (ip, 60004)).unwrap();
                }
            }
        });
    }

    // let counter = Arc::new(AtomicU32::new(0));
    // spawn({
    //     let counter = counter.clone();
    //     move || loop {
    //         std::thread::sleep(Duration::from_secs(1));
    //         let count = counter.swap(0, std::sync::atomic::Ordering::SeqCst);
    //         if count != 0 {
    //             println!("{count}")
    //         }
    //     }
    // });

    set_affinity(0);
    let mut buf = vec![0; 65536];
    loop {
        let (len, _) = socket.recv_from(&mut buf).unwrap();
        messages.0.send(buf[..len].to_vec()).unwrap();
        // counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
