use crate::{lmdb::{DbFlags, DbHandle, Environment}, ClockInfo, MergeLog};

#[derive(Clone)]
pub struct VLCLLDb {
    env: Environment,
    merge_log: DbHandle,
    clock_infos: DbHandle,
    _cur_count: DbHandle,
}

impl VLCLLDb {
    pub fn new(path: &str, mode: Option<u32>) -> Self {
        let mut user_dir_mode: u32 = 0o777;
        if let Some(mode) = mode {
            user_dir_mode = mode;
        }

        let env = Environment::new()
            .max_dbs(5)
            .open(path, user_dir_mode)
            .expect("Failed to open the environment");

        let merge_log = env
            .create_db("merge_log", DbFlags::empty())
            .expect("Failed to create the merge_log database");

        let clock_infos = env
            .create_db("clock_infos", DbFlags::empty())
            .expect("Failed to create the clock_infos database");

        let cur_count = env
            .create_db("cur_count", DbFlags::empty())
            .expect("Failed to create the cur_count database");

        VLCLLDb { env, merge_log, clock_infos, _cur_count: cur_count }
    }

    pub(crate) fn add_clock_infos(&mut self, key: String, clock_info: ClockInfo) {
        let txn = self.env.new_transaction().unwrap();
        let db = txn.bind(&self.clock_infos);

        let _ = db.set(&key, &serde_json::to_string(&clock_info).unwrap());
        println!("[insert clock to DB]: \nkey: {},\nvalue: {:#?}", key, clock_info);
    }

    pub(crate) fn add_merge_log(&mut self, key: String, merge_log: MergeLog) {
        let txn = self.env.new_transaction().unwrap();
        let db = txn.bind(&self.merge_log);

        let _ = db.set(&key, &serde_json::to_string(&merge_log).unwrap());
        println!("[insert merge_log to DB]:  \nkey: {},\nvalue: {:#?}", key, merge_log);
    }

    pub fn get_clock_info(&mut self, key: String) -> String {
        let txn = self.env.new_transaction().unwrap();
        let db = txn.bind(&self.clock_infos);
        db.get::<&str>(&key).unwrap_or("").to_string()
    }
}