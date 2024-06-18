use std::sync::Arc;
use websocket::ReceiveMessage;
use crate::node_factory::ZchronodFactory;

pub struct Zchronod {}

pub type ZchronodArc = Arc<Zchronod>;

impl Zchronod {
    pub fn zchronod_factory() -> ZchronodFactory {
        ZchronodFactory::init()
    }
}

pub(crate) async fn p2p_event_loop(arc_zchronod: Arc<Zchronod>) {}

/// sample handler
pub(crate) async fn handle_incoming_ws_msg() {
    let ws_config = Arc::new(websocket::WebsocketConfig::default());
    let l = websocket::WebsocketListener::bind(ws_config, "127.0.0.1:8080").await.unwrap();

    let addr = l.local_addr().unwrap();

    let (_send, mut recv) = l.accept().await.unwrap();

    loop {
        let res = recv.recv().await.unwrap();
        match res {
            ReceiveMessage::Request(data, res) => {
                res.respond(data).await.unwrap();
            }
            oth => panic!("unexpected: {oth:?}"),
        }
    }
}