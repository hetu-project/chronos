use std::path::PathBuf;
use std::path::Path;
use crate::error::{ZchronodConfigError, ZchronodConfigResult};
use serde::Deserialize;
use serde::Serialize;

/// Zchronod Node Config
#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct ZchronodConfig {
    pub storage_root_path: Option<StorageRootPath>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct StorageRootPath(PathBuf);

impl StorageRootPath {
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl ZchronodConfig {
    pub fn load_config(path: PathBuf) -> ZchronodConfigResult<ZchronodConfig> {
        let p: &Path = path.as_ref();
        let config_yaml = std::fs::read_to_string(p).map_err(|err| match err {
            e @ std::io::Error { .. } if e.kind() == std::io::ErrorKind::NotFound => {
                ZchronodConfigError::ConfigMissing(path.into())
            }
            _ => err.into(),
        })?;
        serde_yaml::from_str(&config_yaml).map_err(ZchronodConfigError::SerializationError)
    }
}