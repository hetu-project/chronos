use std::{
    env::args,
    iter::repeat,
    net::{Ipv4Addr, UdpSocket},
    sync::Arc,
    thread::{available_parallelism, spawn},
};

use permissioned_blockchain::{common::set_affinity, context::ordered_multicast::Sequencer};

fn main() {
    // let ip = args().nth(1).unwrap().parse::<Ipv4Addr>().unwrap();
    let mut sequencer = match args().nth(1).as_deref() {
        Some("half-sip-hash") => {
            Sequencer::new_half_sip_hash(args().nth(2).unwrap().parse().unwrap())
        }
        Some("k256") => Sequencer::new_k256(),
        _ => unimplemented!(),
    };
    let multicast_ip = args().nth(3).unwrap().parse::<Ipv4Addr>().unwrap();

    let socket = Arc::new(UdpSocket::bind(("0.0.0.0", 60004)).unwrap());
    let messages = flume::bounded(1024);

    // this has to go first or compiler cannot guess `messages` type
    let mut run = || {
        set_affinity(0);
        let mut buf = vec![0; 65536];
        loop {
            let (len, _) = socket.recv_from(&mut buf).unwrap();
            let process = sequencer.process(buf[..len].to_vec());
            messages.0.send(process).unwrap()
        }
    };

    for ((index, messages), socket) in repeat(messages.1)
        .take(usize::from(available_parallelism().unwrap()) - 1)
        .enumerate()
        .zip(repeat(socket.clone()))
    {
        spawn(move || {
            set_affinity(index + 1);
            loop {
                let process = messages.recv().unwrap();
                process.apply(|buf| {
                    socket.send_to(buf, (multicast_ip, 60004)).unwrap();
                })
            }
        });
    }

    run()
}
