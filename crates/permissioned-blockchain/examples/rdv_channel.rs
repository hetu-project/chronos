fn main() {
    let ch0 = flume::bounded(0);
    let ch1 = flume::unbounded();
    let _ch0 = ch0.0.clone();
    let _ch1 = ch1.0.clone();
    let send0 = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let t0 = std::thread::spawn({
        let send0 = send0.clone();
        move || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async move {
                    ch0.0.send_async(0).await.unwrap();
                    send0.store(true, std::sync::atomic::Ordering::SeqCst)
                })
        }
    });
    let t1 = std::thread::spawn(move || {
        // std::thread::sleep(std::time::Duration::from_millis(1));
        ch1.0.send(1).unwrap();
    });
    for i in 0..2 {
        let rx = flume::Selector::new()
            .recv(&ch0.1, Result::unwrap)
            .recv(&ch1.1, Result::unwrap)
            .wait();
        if i == 0 && rx == 1 {
            assert!(!send0.load(std::sync::atomic::Ordering::SeqCst));
        }
    }
    t0.join().unwrap();
    t1.join().unwrap()
}
