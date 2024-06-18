use std::path::PathBuf;
use thiserror::Error;

pub type ZchronodConfigResult<T> = Result<T, ZchronodConfigError>;

#[derive(Error, Debug)]
pub enum ZchronodConfigError {
    #[error("No conductor config found at this path: {0}")]
    ConfigMissing(PathBuf),

    #[error("Config deserialization error: {0}")]
    SerializationError(#[from] serde_yaml::Error),

    #[error("Error while performing IO for the Zchronod: {0}")]
    IoError(#[from] std::io::Error),
}


pub type ZchronodResult<T> = Result<T, ZchronodError>;

#[derive(Error, Debug)]
pub enum ZchronodError {}