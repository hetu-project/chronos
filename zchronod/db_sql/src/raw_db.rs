use std::any::Any;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use once_cell::sync::Lazy;
use crate::api::{DbKindT, DbWrite};
use crate::error::DatabaseResult;

pub struct RawDb {
    dbs: parking_lot::RwLock<HashMap<PathBuf, Box<dyn Any + Send + Sync>>>,
}

pub static RAW_DB_HANDLER: Lazy<RawDb> = Lazy::new(|| {
    /// get panic info if error occurs
    let orig_handler = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        eprintln!("FATAL PANIC {:#?}", panic_info);
        orig_handler(panic_info);
    }));

    RawDb::init()
});

impl RawDb {
    pub fn init() -> Self {
        RawDb {
            dbs: parking_lot::RwLock::new(HashMap::new()),
        }
    }
    pub(super) fn get_or_insert<Kind, F>(
        &self,
        kind: &Kind,
        path_prefix: &Path,
        insert: F,
    ) -> DatabaseResult<DbWrite<Kind>>
        where
            Kind: DbKindT + Send + Sync + 'static,
            F: FnOnce(Kind) -> DatabaseResult<DbWrite<Kind>>,
    {
        let path = path_prefix.join(kind.filename());

        let ret = self
            .dbs
            .read()
            .get(&path)
            .and_then(|d| d.downcast_ref::<DbWrite<Kind>>().cloned());
        match ret {
            Some(ret) => Ok(ret),
            None => match self.dbs.write().entry(path) {
                // Note that this downcast is safe because the path is created
                // from the kind so will always be the correct type.
                std::collections::hash_map::Entry::Occupied(o) => Ok(o
                    .get()
                    .downcast_ref::<DbWrite<Kind>>()
                    .expect("Downcast to db kind failed. This is a bug")
                    .clone()),
                std::collections::hash_map::Entry::Vacant(v) => {
                    // If the db is missing we run the closure to create it.
                    // Note the `Kind` is enforced by the closure return type.
                    let db = insert(kind.clone())?;
                    v.insert(Box::new(db.clone()));
                    Ok(db)
                }
            },
        }
    }
}