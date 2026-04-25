use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct SqlRequest {
    pub database: Option<String>,
    pub sql: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SqlResponse {
    pub ok: bool,
    pub status: u16,
    pub result: Option<ExecutionResultDto>,
    pub error: Option<String>,
}

impl SqlResponse {
    pub fn success(result: crate::engine::ExecutionResult) -> Self {
        Self {
            ok: true,
            status: 200,
            result: Some(result.into()),
            error: None,
        }
    }

    pub fn error(status: u16, error: String) -> Self {
        Self {
            ok: false,
            status,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionResultDto {
    Acknowledged {
        message: String,
        rows_affected: usize,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<BTreeMap<String, ScalarValue>>,
        row_count: usize,
    },
}

impl From<crate::engine::ExecutionResult> for ExecutionResultDto {
    fn from(value: crate::engine::ExecutionResult) -> Self {
        match value {
            crate::engine::ExecutionResult::Acknowledged {
                message,
                rows_affected,
            } => Self::Acknowledged {
                message,
                rows_affected,
            },
            crate::engine::ExecutionResult::Rows { columns, rows } => Self::Rows {
                row_count: rows.len(),
                columns,
                rows,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsResponse {
    pub status: String,
    pub database_count: usize,
    pub table_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ScalarValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Null,
}

impl ScalarValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Int(value) => Some(*value as f64),
            Self::Float(value) => Some(*value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Int,
    Float,
    Bool,
    Text,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Catalog {
    pub databases: BTreeMap<String, DatabaseMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseMeta {
    pub name: String,
    pub tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFile {
    pub schema: TableSchema,
    pub rows: Vec<BTreeMap<String, ScalarValue>>,
}
