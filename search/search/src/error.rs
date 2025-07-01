use crate::golem::search::types::{SearchError};
use reqwest::StatusCode;

/// Creates a `SearchError` value representing that something is unsupported
pub fn unsupported(_what: impl AsRef<str>) -> SearchError {
    SearchError::Unsupported
}

/// Creates a `SearchError` value for invalid queries
pub fn invalid_query(message: impl AsRef<str>) -> SearchError {
    SearchError::InvalidQuery(message.as_ref().to_string())
}

/// Creates a `SearchError` value for internal errors
pub fn internal_error(message: impl AsRef<str>) -> SearchError {
    SearchError::Internal(message.as_ref().to_string())
}

/// Creates a `SearchError` value for index not found
pub fn index_not_found() -> SearchError {
    SearchError::IndexNotFound
}

/// Creates a `SearchError` value for timeout
pub fn timeout() -> SearchError {
    SearchError::Timeout
}

/// Creates a `SearchError` value for rate limiting
pub fn rate_limited() -> SearchError {
    SearchError::RateLimited
}

/// Converts a reqwest error to a SearchError
pub fn from_reqwest_error(details: impl AsRef<str>, err: reqwest::Error) -> SearchError {
    SearchError::Internal(format!("{}: {err}", details.as_ref()))
}

/// Maps HTTP status codes to appropriate SearchError variants
pub fn search_error_from_status(status: StatusCode) -> SearchError {
    match status {
        StatusCode::TOO_MANY_REQUESTS => SearchError::RateLimited,
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => SearchError::Timeout,
        StatusCode::NOT_FOUND => SearchError::IndexNotFound,
        StatusCode::BAD_REQUEST => SearchError::InvalidQuery("Bad request".to_string()),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN | StatusCode::PAYMENT_REQUIRED => {
            SearchError::Internal("Authentication failed".to_string())
        }
        _ if status.is_client_error() => {
            SearchError::InvalidQuery(format!("Client error: {}", status))
        }
        _ => SearchError::Internal(format!("Server error: {}", status)),
    }
}