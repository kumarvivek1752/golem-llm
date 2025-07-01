use crate::golem::search::types::SearchError;
use std::ffi::OsStr;

/// Gets an expected configuration value from the environment, and fails if it is not found
/// using the `fail` function. Otherwise, it runs `succeed` with the configuration value.
pub fn with_config_key<R>(
    key: impl AsRef<OsStr>,
    fail: impl FnOnce(SearchError) -> R,
    succeed: impl FnOnce(String) -> R,
) -> R {
    let key_str = key.as_ref().to_string_lossy().to_string();
    match std::env::var(key) {
        Ok(value) => succeed(value),
        Err(_) => {
            let error = SearchError::Internal(format!("Missing config key: {key_str}"));
            fail(error)
        }
    }
}

/// Gets an optional configuration value from the environment
pub fn get_optional_config(key: impl AsRef<OsStr>) -> Option<String> {
    std::env::var(key).ok()
}

/// Gets a configuration value with a default fallback
pub fn get_config_with_default(key: impl AsRef<OsStr>, default: impl Into<String>) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

/// Validates a required configuration key exists
pub fn validate_config_key(key: impl AsRef<OsStr>) -> Result<String, SearchError> {
    let key_str = key.as_ref().to_string_lossy().to_string();
    std::env::var(key).map_err(|_| SearchError::Internal(format!("Missing config key: {key_str}")))
}

/// Gets multiple expected configuration values from the environment
pub fn with_config_keys<R>(
    keys: &[&str],
    callback: impl FnOnce(Vec<String>) -> R,
) -> R {
    let mut values = Vec::new();
    for key in keys {
        match std::env::var(key) {
            Ok(value) => values.push(value),
            Err(_) => {
                return callback(Vec::new());
            }
        }
    }
    callback(values)
}