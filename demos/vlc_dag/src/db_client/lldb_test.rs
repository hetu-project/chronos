use lmdb_rs as lmdb;
use lmdb::{DbFlags, DbHandle, Environment};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct Commit {
    pub id: String,
    parent_ids: HashSet<String>,
    message: String,
}

struct _Repository {
    env: Environment,
    commits_db: DbHandle,
}

impl _Repository {
    
    fn _new(path: &str, db: &str) -> Self {
        const USER_DIR: u32 = 0o777;
        let env = Environment::new()
            .max_dbs(5)
            .open(path, USER_DIR)
            .expect("Failed to open the environment");

        let commits_db = env
            .create_db(db, DbFlags::empty())
            .expect("Failed to create the commits database");

        _Repository { env, commits_db }
    }
 
    fn _add_commit(&mut self, commit: &Commit) {
        // let db = self.env.get_default_db(DbFlags::empty()).unwrap();
        let txn = self.env.new_transaction().unwrap();
        let db = txn.bind(&self.commits_db);

        let _ = db.set(&commit.id, &serde_json::to_string(&commit).unwrap());
        println!("Get From DB: {}", db.get::<&str>(&commit.id).unwrap());
    }

    // other methods
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn submit_to_repo() {
        let mut repo = _Repository::_new("./db", "commit");

        let commit1 = Commit {
            id: "commit1".to_string(),
            parent_ids: HashSet::new(),
            message: "Initial commit".to_string(),
        };
        repo._add_commit(&commit1);

        let commit2 = Commit {
            id: "commit2".to_string(),
            parent_ids: vec!["commit1".to_string()].into_iter().collect(),
            message: "Add feature X".to_string(),
        };
        repo._add_commit(&commit2);

        let commit3 = Commit {
            id: "commit3".to_string(),
            parent_ids: vec!["commit1".to_string()].into_iter().collect(),
            message: "Add feature Y".to_string(),
        };
        repo._add_commit(&commit3);

        let commit4 = Commit {
            id: "commit4".to_string(),
            parent_ids: vec!["commit2".to_string(), "commit3".to_string()]
                .into_iter()
                .collect(),
            message: "Merge feature X and Y".to_string(),
        };
        repo._add_commit(&commit4);
    }
}