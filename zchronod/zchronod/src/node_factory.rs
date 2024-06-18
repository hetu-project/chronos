use std::sync::Arc;
use node_api::config::ZchronodConfig;
use node_api::error::ZchronodResult;
use crate::{storage, zchronod};
use crate::zchronod::{Zchronod, ZchronodArc};

#[derive(Default)]
pub struct ZchronodFactory {
    pub config: ZchronodConfig,
}

impl ZchronodFactory {
    pub fn init() -> Self {
        Self::default()
    }

    pub fn set_config(mut self, config: ZchronodConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn produce(self) -> ZchronodResult<ZchronodArc> {
        let storage = storage::Storage::new(self.config.clone());

        /// p2p network pass send && recv
        // let (p2p_send, p2p_recv) = match zchronod_p2p::spawn_zchronod_p2p().await;


        /// create arc zchronod node
        let arc_zchronod: ZchronodArc = Arc::new(Zchronod {});

        tokio::task::spawn(zchronod::p2p_event_loop(arc_zchronod.clone()));


        // start client websocket
        tokio::task::spawn(zchronod::handle_incoming_ws_msg());

        todo!()
    }
}