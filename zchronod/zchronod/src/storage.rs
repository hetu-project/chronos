use std::collections::HashMap;
use std::sync::Arc;
use node_api::config::ZchronodConfig;
use db_sql::api::{DbKindZchronod, DbWrite};

pub struct Storage {
    zchronod_db: DbWrite<DbKindZchronod>,
}

impl Storage {
    pub fn new(config: ZchronodConfig) -> Self {
        let zchronod_db = DbWrite::open(
            config.storage_root_path.as_ref().unwrap().as_path(),
            DbKindZchronod,
        ).unwrap(); // todo error handling
        Self {
            zchronod_db,
        }
    }
    pub async fn get(&self, key: String) -> Option<String> {
        todo!()
    }

    pub async fn set(&self, key: String, value: String) -> bool {
        todo!()
    }

    pub async fn delete(&self, key: String) -> bool {
        todo!()
    }

    pub async fn get_all(&self) -> HashMap<String, String> {
        todo!()
    }
}