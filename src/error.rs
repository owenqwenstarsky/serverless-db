use axum::{Json, http::StatusCode};
use thiserror::Error;

use crate::models::SqlResponse;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("sql not supported: {0}")]
    NotSupported(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("sql parse error: {0}")]
    Sql(#[from] sqlparser::parser::ParserError),
    #[error("internal error: {0}")]
    Internal(String),
}

impl AppError {
    pub fn into_http_response(self) -> (StatusCode, Json<SqlResponse>) {
        let status = match self {
            Self::InvalidRequest(_) | Self::Sql(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::NotSupported(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Io(_) | Self::Serde(_) | Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let message = self.to_string();
        (
            status,
            Json(SqlResponse::error(status.as_u16(), message)),
        )
    }
}

