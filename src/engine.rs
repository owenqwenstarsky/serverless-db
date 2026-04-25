use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use sqlparser::{
    ast::{
        Assignment, AssignmentTarget, BinaryOperator, Expr, Ident, LimitClause, ObjectName,
        Query, SchemaName, SelectItem, SetExpr, Statement, TableFactor, TableObject, Value,
        ValueWithSpan,
    },
    dialect::GenericDialect,
    parser::Parser,
};

use crate::{
    error::AppError,
    models::{
        Catalog, ColumnSchema, ColumnType, DatabaseMeta, ScalarValue, SqlRequest, TableFile,
        TableSchema,
    },
};

pub type Row = BTreeMap<String, ScalarValue>;

#[derive(Debug, Clone)]
pub enum ExecutionResult {
    Acknowledged {
        message: String,
        rows_affected: usize,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Row>,
    },
}

pub struct Engine {
    root_dir: PathBuf,
    catalog: Catalog,
}

#[derive(Debug, Clone)]
pub struct EngineStats {
    pub database_count: usize,
    pub table_count: usize,
}

impl Engine {
    pub fn open(root_dir: PathBuf) -> Result<Self, AppError> {
        fs::create_dir_all(&root_dir)?;
        let catalog_path = catalog_path(&root_dir);
        let catalog = if catalog_path.exists() {
            serde_json::from_slice(&fs::read(catalog_path)?)?
        } else {
            Catalog::default()
        };

        Ok(Self { root_dir, catalog })
    }

    pub fn execute(&mut self, request: SqlRequest) -> Result<ExecutionResult, AppError> {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, &request.sql)?;
        if statements.len() != 1 {
            return Err(AppError::InvalidRequest(
                "exactly one SQL statement is allowed per request".to_string(),
            ));
        }

        let statement = statements.remove(0);
        match statement {
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                ..
            } => self.create_database(object_name_to_string(&db_name), if_not_exists),
            Statement::CreateTable(create) => {
                let database = request
                    .database
                    .or_else(|| object_name_database(&create.name))
                    .ok_or_else(|| {
                        AppError::InvalidRequest(
                            "database is required for CREATE TABLE".to_string(),
                        )
                    })?;

                let table_name = object_name_last(&create.name)?;
                let schema = TableSchema {
                    name: table_name.clone(),
                    columns: create
                        .columns
                        .into_iter()
                        .map(Self::column_from_ast)
                        .collect::<Result<Vec<_>, _>>()?,
                };

                if schema.columns.is_empty() {
                    return Err(AppError::InvalidRequest(
                        "CREATE TABLE requires at least one column".to_string(),
                    ));
                }

                self.create_table(database, table_name, schema, create.if_not_exists)
            }
            Statement::ShowDatabases { .. } => self.show_databases(),
            Statement::ShowTables { .. } => {
                let database = request.database.ok_or_else(|| {
                    AppError::InvalidRequest("database is required for SHOW TABLES".to_string())
                })?;
                self.show_tables(&database)
            }
            Statement::Insert(insert) => self.insert(request.database, insert),
            Statement::Query(query) => self.select(request.database, *query),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.update(request.database, table, assignments, selection),
            Statement::Delete(delete) => self.delete(request.database, delete),
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => self.create_database(schema_name_to_string(&schema_name), if_not_exists),
            Statement::ShowCreate { obj_type, obj_name } => Err(AppError::NotSupported(format!(
                "SHOW CREATE {:?} {} is not supported",
                obj_type, obj_name
            ))),
            other => Err(AppError::NotSupported(format!(
                "statement is not supported yet: {other}"
            ))),
        }
    }

    pub fn stats(&self) -> EngineStats {
        EngineStats {
            database_count: self.catalog.databases.len(),
            table_count: self
                .catalog
                .databases
                .values()
                .map(|database| database.tables.len())
                .sum(),
        }
    }

    fn create_database(
        &mut self,
        database: String,
        if_not_exists: bool,
    ) -> Result<ExecutionResult, AppError> {
        validate_name(&database)?;
        if self.catalog.databases.contains_key(&database) {
            if if_not_exists {
                return Ok(ExecutionResult::Acknowledged {
                    message: format!("database '{database}' already exists"),
                    rows_affected: 0,
                });
            }
            return Err(AppError::Conflict(format!(
                "database '{database}' already exists"
            )));
        }

        fs::create_dir_all(database_dir(&self.root_dir, &database))?;
        self.catalog.databases.insert(
            database.clone(),
            DatabaseMeta {
                name: database.clone(),
                tables: Vec::new(),
            },
        );
        self.persist_catalog()?;

        Ok(ExecutionResult::Acknowledged {
            message: format!("database '{database}' created"),
            rows_affected: 1,
        })
    }

    fn create_table(
        &mut self,
        database: String,
        table_name: String,
        schema: TableSchema,
        if_not_exists: bool,
    ) -> Result<ExecutionResult, AppError> {
        validate_name(&database)?;
        validate_name(&table_name)?;
        let existing_tables = self
            .catalog
            .databases
            .get(&database)
            .ok_or_else(|| AppError::NotFound(format!("database '{database}' does not exist")))?;

        if existing_tables.tables.iter().any(|item| item == &table_name) {
            if if_not_exists {
                return Ok(ExecutionResult::Acknowledged {
                    message: format!("table '{database}.{table_name}' already exists"),
                    rows_affected: 0,
                });
            }
            return Err(AppError::Conflict(format!(
                "table '{database}.{table_name}' already exists"
            )));
        }

        let primary_keys = schema.columns.iter().filter(|column| column.primary_key).count();
        if primary_keys > 1 {
            return Err(AppError::InvalidRequest(
                "only one PRIMARY KEY column is supported".to_string(),
            ));
        }

        let file = TableFile {
            schema,
            rows: Vec::new(),
        };
        self.write_table_file(&database, &table_name, &file)?;
        let database_meta = self
            .catalog
            .databases
            .get_mut(&database)
            .ok_or_else(|| AppError::NotFound(format!("database '{database}' does not exist")))?;
        database_meta.tables.push(table_name.clone());
        database_meta.tables.sort();
        self.persist_catalog()?;

        Ok(ExecutionResult::Acknowledged {
            message: format!("table '{database}.{table_name}' created"),
            rows_affected: 1,
        })
    }

    fn show_databases(&self) -> Result<ExecutionResult, AppError> {
        let rows = self
            .catalog
            .databases
            .keys()
            .map(|name| {
                let mut row = Row::new();
                row.insert("database".to_string(), ScalarValue::Text(name.clone()));
                row
            })
            .collect::<Vec<_>>();

        Ok(ExecutionResult::Rows {
            columns: vec!["database".to_string()],
            rows,
        })
    }

    fn show_tables(&self, database: &str) -> Result<ExecutionResult, AppError> {
        let database_meta = self
            .catalog
            .databases
            .get(database)
            .ok_or_else(|| AppError::NotFound(format!("database '{database}' does not exist")))?;

        let rows = database_meta
            .tables
            .iter()
            .map(|name| {
                let mut row = Row::new();
                row.insert("table".to_string(), ScalarValue::Text(name.clone()));
                row
            })
            .collect::<Vec<_>>();

        Ok(ExecutionResult::Rows {
            columns: vec!["table".to_string()],
            rows,
        })
    }

    fn insert(
        &mut self,
        database: Option<String>,
        insert: sqlparser::ast::Insert,
    ) -> Result<ExecutionResult, AppError> {
        let insert_table = insert_table_name(&insert.table)?;
        let database = database.or_else(|| object_name_database(insert_table)).ok_or_else(|| {
            AppError::InvalidRequest("database is required for INSERT".to_string())
        })?;
        let table_name = object_name_last(insert_table)?;
        let mut table = self.read_table_file(&database, &table_name)?;

        let source = insert
            .source
            .ok_or_else(|| AppError::NotSupported("INSERT without source is not supported".to_string()))?;
        let query = *source;
        let SetExpr::Values(values) = *query.body else {
            return Err(AppError::NotSupported(
                "only INSERT ... VALUES is supported".to_string(),
            ));
        };

        let target_columns = if insert.columns.is_empty() {
            table
                .schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        } else {
            insert
                .columns
                .iter()
                .map(identifier_to_string)
                .collect::<Vec<_>>()
        };

        let schema_lookup = schema_lookup(&table.schema);
        let mut inserted = 0usize;

        for row_values in values.rows {
            if row_values.len() != target_columns.len() {
                return Err(AppError::InvalidRequest(format!(
                    "INSERT column count ({}) does not match value count ({})",
                    target_columns.len(),
                    row_values.len()
                )));
            }

            let mut row = default_row(&table.schema);
            for (column_name, expr) in target_columns.iter().zip(row_values.iter()) {
                let value = scalar_from_expr(expr)?;
                let column = schema_lookup.get(column_name).ok_or_else(|| {
                    AppError::InvalidRequest(format!("unknown column '{column_name}'"))
                })?;
                validate_value_type(column_name, &column.column_type, &value)?;
                row.insert(column_name.clone(), value);
            }

            enforce_row_constraints(&table.schema, &table.rows, &row)?;
            table.rows.push(row);
            inserted += 1;
        }

        self.write_table_file(&database, &table_name, &table)?;
        Ok(ExecutionResult::Acknowledged {
            message: format!("inserted {inserted} row(s) into '{database}.{table_name}'"),
            rows_affected: inserted,
        })
    }

    fn select(
        &self,
        database: Option<String>,
        query: Query,
    ) -> Result<ExecutionResult, AppError> {
        let SetExpr::Select(select) = *query.body else {
            return Err(AppError::NotSupported(
                "only SELECT queries are supported".to_string(),
            ));
        };

        if select.from.len() != 1 {
            return Err(AppError::NotSupported(
                "SELECT currently supports exactly one table".to_string(),
            ));
        }

        let relation = &select.from[0].relation;
        let TableFactor::Table { name, .. } = relation else {
            return Err(AppError::NotSupported(
                "SELECT supports plain table references only".to_string(),
            ));
        };

        let database = database
            .or_else(|| object_name_database(name))
            .ok_or_else(|| AppError::InvalidRequest("database is required for SELECT".to_string()))?;
        let table_name = object_name_last(name)?;
        let table = self.read_table_file(&database, &table_name)?;

        let projection = projection_from_select(&table.schema, &select.projection)?;
        let selection = select.selection.as_ref();
        let limit = limit_clause_to_usize(query.limit_clause.as_ref())?;

        let mut rows = Vec::new();
        for row in &table.rows {
            if let Some(expr) = selection {
                if !evaluate_predicate(expr, row)? {
                    continue;
                }
            }

            let mut projected = Row::new();
            for column in &projection {
                projected.insert(
                    column.clone(),
                    row.get(column).cloned().unwrap_or(ScalarValue::Null),
                );
            }
            rows.push(projected);

            if limit.is_some_and(|value| rows.len() >= value) {
                break;
            }
        }

        Ok(ExecutionResult::Rows {
            columns: projection,
            rows,
        })
    }

    fn update(
        &mut self,
        database: Option<String>,
        table: TableWithJoinsCompat,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
    ) -> Result<ExecutionResult, AppError> {
        let table_name_ref = table_name_from_update_target(&table)?;
        let database = database
            .or_else(|| object_name_database(table_name_ref))
            .ok_or_else(|| AppError::InvalidRequest("database is required for UPDATE".to_string()))?;
        let table_name = object_name_last(table_name_ref)?;
        let mut table_file = self.read_table_file(&database, &table_name)?;
        let schema_lookup = schema_lookup(&table_file.schema);

        let updates = assignments
            .iter()
            .map(|assignment| {
                let name = assignment_target_name(&assignment.target)?;
                let value = scalar_from_expr(&assignment.value)?;
                let column_schema = schema_lookup.get(&name).ok_or_else(|| {
                    AppError::InvalidRequest(format!("unknown column '{name}'"))
                })?;
                validate_value_type(&name, &column_schema.column_type, &value)?;
                Ok((name, value))
            })
            .collect::<Result<Vec<_>, AppError>>()?;

        let mut updated = 0usize;
        for index in 0..table_file.rows.len() {
            let row_matches = if let Some(expr) = selection.as_ref() {
                evaluate_predicate(expr, &table_file.rows[index])?
            } else {
                true
            };
            if !row_matches {
                continue;
            }

            let mut candidate = table_file.rows[index].clone();
            for (column_name, value) in &updates {
                candidate.insert(column_name.clone(), value.clone());
            }
            enforce_row_constraints_except(&table_file.schema, &table_file.rows, &candidate, index)?;
            table_file.rows[index] = candidate;
            updated += 1;
        }

        self.write_table_file(&database, &table_name, &table_file)?;
        Ok(ExecutionResult::Acknowledged {
            message: format!("updated {updated} row(s) in '{database}.{table_name}'"),
            rows_affected: updated,
        })
    }

    fn delete(
        &mut self,
        database: Option<String>,
        delete: sqlparser::ast::Delete,
    ) -> Result<ExecutionResult, AppError> {
        let table_name_ref = delete_table_name(&delete)?;
        let database = database
            .or_else(|| object_name_database(table_name_ref))
            .ok_or_else(|| AppError::InvalidRequest("database is required for DELETE".to_string()))?;
        let table_name = object_name_last(table_name_ref)?;
        let mut table_file = self.read_table_file(&database, &table_name)?;

        let mut kept_rows = Vec::with_capacity(table_file.rows.len());
        let mut removed = 0usize;
        for row in &table_file.rows {
            let should_delete = if let Some(selection) = delete.selection.as_ref() {
                evaluate_predicate(selection, row)?
            } else {
                true
            };

            if should_delete {
                removed += 1;
            } else {
                kept_rows.push(row.clone());
            }
        }
        table_file.rows = kept_rows;
        self.write_table_file(&database, &table_name, &table_file)?;
        Ok(ExecutionResult::Acknowledged {
            message: format!("deleted {removed} row(s) from '{database}.{table_name}'"),
            rows_affected: removed,
        })
    }

    fn read_table_file(&self, database: &str, table_name: &str) -> Result<TableFile, AppError> {
        ensure_table_exists(&self.catalog, database, table_name)?;
        let path = table_path(&self.root_dir, database, table_name);
        let bytes = fs::read(path)?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn write_table_file(
        &self,
        database: &str,
        table_name: &str,
        table_file: &TableFile,
    ) -> Result<(), AppError> {
        let path = table_path(&self.root_dir, database, table_name);
        write_json_atomic(path, table_file)
    }

    fn persist_catalog(&self) -> Result<(), AppError> {
        write_json_atomic(catalog_path(&self.root_dir), &self.catalog)
    }

    fn column_from_ast(column: sqlparser::ast::ColumnDef) -> Result<ColumnSchema, AppError> {
        let name = identifier_to_string(&column.name);
        validate_name(&name)?;

        let column_type = match column.data_type.to_string().to_uppercase().as_str() {
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => ColumnType::Int,
            "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" => ColumnType::Float,
            "BOOLEAN" | "BOOL" => ColumnType::Bool,
            "TEXT" | "STRING" | "VARCHAR" | "CHAR" => ColumnType::Text,
            other => {
                return Err(AppError::NotSupported(format!(
                    "column type '{other}' is not supported"
                )))
            }
        };

        let mut nullable = true;
        let mut primary_key = false;
        for option in &column.options {
            let option_text = option.option.to_string().to_uppercase();
            if option_text == "NOT NULL" {
                nullable = false;
            }
            if option_text == "PRIMARY KEY" {
                primary_key = true;
                nullable = false;
            }
        }

        Ok(ColumnSchema {
            name,
            column_type,
            nullable,
            primary_key,
        })
    }
}

type TableWithJoinsCompat = sqlparser::ast::TableWithJoins;

fn catalog_path(root_dir: &Path) -> PathBuf {
    root_dir.join("catalog.json")
}

fn database_dir(root_dir: &Path, database: &str) -> PathBuf {
    root_dir.join(database)
}

fn table_path(root_dir: &Path, database: &str, table_name: &str) -> PathBuf {
    database_dir(root_dir, database).join(format!("{table_name}.json"))
}

fn write_json_atomic<T: serde::Serialize>(path: PathBuf, value: &T) -> Result<(), AppError> {
    let parent = path
        .parent()
        .ok_or_else(|| AppError::Internal("path missing parent".to_string()))?;
    fs::create_dir_all(parent)?;
    let temp_path = path.with_extension("tmp");
    fs::write(&temp_path, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temp_path, path)?;
    Ok(())
}

fn validate_name(name: &str) -> Result<(), AppError> {
    if name.is_empty() {
        return Err(AppError::InvalidRequest("identifier cannot be empty".to_string()));
    }

    if name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        && name
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
    {
        Ok(())
    } else {
        Err(AppError::InvalidRequest(format!(
            "identifier '{name}' must use [A-Za-z_][A-Za-z0-9_]*"
        )))
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| part.to_string())
        .collect::<Vec<_>>()
        .join(".")
}

fn schema_name_to_string(name: &SchemaName) -> String {
    match name {
        SchemaName::Simple(name) | SchemaName::NamedAuthorization(name, _) => {
            object_name_to_string(name)
        }
        SchemaName::UnnamedAuthorization(ident) => identifier_to_string(ident),
    }
}

fn object_name_last(name: &ObjectName) -> Result<String, AppError> {
    name.0
        .last()
        .map(|part| strip_quotes(&part.to_string()))
        .ok_or_else(|| AppError::InvalidRequest("missing object name".to_string()))
}

fn object_name_database(name: &ObjectName) -> Option<String> {
    if name.0.len() > 1 {
        name.0.first().map(|part| strip_quotes(&part.to_string()))
    } else {
        None
    }
}

fn identifier_to_string(ident: &Ident) -> String {
    strip_quotes(&ident.to_string())
}

fn strip_quotes(value: &str) -> String {
    value.trim_matches('"').trim_matches('`').to_string()
}

fn schema_lookup(schema: &TableSchema) -> BTreeMap<String, &ColumnSchema> {
    schema
        .columns
        .iter()
        .map(|column| (column.name.clone(), column))
        .collect()
}

fn default_row(schema: &TableSchema) -> Row {
    schema
        .columns
        .iter()
        .map(|column| (column.name.clone(), ScalarValue::Null))
        .collect()
}

fn enforce_row_constraints(schema: &TableSchema, existing_rows: &[Row], row: &Row) -> Result<(), AppError> {
    enforce_row_constraints_inner(schema, existing_rows, row, None)
}

fn enforce_row_constraints_except(
    schema: &TableSchema,
    existing_rows: &[Row],
    row: &Row,
    skip_index: usize,
) -> Result<(), AppError> {
    enforce_row_constraints_inner(schema, existing_rows, row, Some(skip_index))
}

fn enforce_row_constraints_inner(
    schema: &TableSchema,
    existing_rows: &[Row],
    row: &Row,
    skip_index: Option<usize>,
) -> Result<(), AppError> {
    for column in &schema.columns {
        let value = row
            .get(&column.name)
            .cloned()
            .unwrap_or(ScalarValue::Null);

        if !column.nullable && matches!(value, ScalarValue::Null) {
            return Err(AppError::InvalidRequest(format!(
                "column '{}' cannot be NULL",
                column.name
            )));
        }

        if column.primary_key {
            for (index, existing) in existing_rows.iter().enumerate() {
                if skip_index.is_some_and(|skip| skip == index) {
                    continue;
                }
                if existing.get(&column.name) == Some(&value) {
                    return Err(AppError::Conflict(format!(
                        "duplicate PRIMARY KEY value for column '{}'",
                        column.name
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_value_type(
    column_name: &str,
    column_type: &ColumnType,
    value: &ScalarValue,
) -> Result<(), AppError> {
    let valid = match (column_type, value) {
        (_, ScalarValue::Null) => true,
        (ColumnType::Int, ScalarValue::Int(_)) => true,
        (ColumnType::Float, ScalarValue::Int(_) | ScalarValue::Float(_)) => true,
        (ColumnType::Bool, ScalarValue::Bool(_)) => true,
        (ColumnType::Text, ScalarValue::Text(_)) => true,
        _ => false,
    };

    if valid {
        Ok(())
    } else {
        Err(AppError::InvalidRequest(format!(
            "value type does not match column '{column_name}'"
        )))
    }
}

fn scalar_from_expr(expr: &Expr) -> Result<ScalarValue, AppError> {
    match expr {
        Expr::Value(value) => scalar_from_value(value),
        Expr::UnaryOp { op, expr } if op.to_string() == "-" => match scalar_from_expr(expr)? {
            ScalarValue::Int(value) => Ok(ScalarValue::Int(-value)),
            ScalarValue::Float(value) => Ok(ScalarValue::Float(-value)),
            _ => Err(AppError::InvalidRequest(
                "unary minus requires a numeric literal".to_string(),
            )),
        },
        _ => Err(AppError::NotSupported(format!(
            "expression is not supported here: {expr}"
        ))),
    }
}

fn scalar_from_value(value: &ValueWithSpan) -> Result<ScalarValue, AppError> {
    match &value.value {
        Value::Number(number, _) => {
            if number.contains('.') {
                let parsed = number.parse::<f64>().map_err(|error| {
                    AppError::InvalidRequest(format!("invalid float literal '{number}': {error}"))
                })?;
                Ok(ScalarValue::Float(parsed))
            } else {
                let parsed = number.parse::<i64>().map_err(|error| {
                    AppError::InvalidRequest(format!("invalid integer literal '{number}': {error}"))
                })?;
                Ok(ScalarValue::Int(parsed))
            }
        }
        Value::SingleQuotedString(value) | Value::DoubleQuotedString(value) => {
            Ok(ScalarValue::Text(value.clone()))
        }
        Value::Boolean(value) => Ok(ScalarValue::Bool(*value)),
        Value::Null => Ok(ScalarValue::Null),
        other => Err(AppError::NotSupported(format!(
            "literal is not supported: {other}"
        ))),
    }
}

fn projection_from_select(
    schema: &TableSchema,
    projection: &[SelectItem],
) -> Result<Vec<String>, AppError> {
    let mut columns = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                columns.extend(schema.columns.iter().map(|column| column.name.clone()));
            }
            SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                columns.push(identifier_to_string(ident));
            }
            _ => {
                return Err(AppError::NotSupported(
                    "SELECT supports only bare column identifiers and *".to_string(),
                ))
            }
        }
    }

    let known = schema
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<HashSet<_>>();
    for column in &columns {
        if !known.contains(column) {
            return Err(AppError::InvalidRequest(format!(
                "unknown column '{column}' in SELECT projection"
            )));
        }
    }

    Ok(columns)
}

fn evaluate_predicate(expr: &Expr, row: &Row) -> Result<bool, AppError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(evaluate_predicate(left, row)? && evaluate_predicate(right, row)?),
            BinaryOperator::Or => Ok(evaluate_predicate(left, row)? || evaluate_predicate(right, row)?),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                let left_value = predicate_operand(left, row)?;
                let right_value = predicate_operand(right, row)?;
                compare_values(op, &left_value, &right_value)
            }
            _ => Err(AppError::NotSupported(format!(
                "operator '{op}' is not supported in WHERE clauses"
            ))),
        },
        Expr::Nested(expr) => evaluate_predicate(expr, row),
        _ => Err(AppError::NotSupported(format!(
            "WHERE clause is not supported: {expr}"
        ))),
    }
}

fn predicate_operand(expr: &Expr, row: &Row) -> Result<ScalarValue, AppError> {
    match expr {
        Expr::Identifier(identifier) => row
            .get(&identifier_to_string(identifier))
            .cloned()
            .ok_or_else(|| AppError::InvalidRequest(format!("unknown column '{}'", identifier))),
        _ => scalar_from_expr(expr),
    }
}

fn compare_values(
    operator: &BinaryOperator,
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<bool, AppError> {
    let result = match operator {
        BinaryOperator::Eq => left == right,
        BinaryOperator::NotEq => left != right,
        BinaryOperator::Gt | BinaryOperator::GtEq | BinaryOperator::Lt | BinaryOperator::LtEq => {
            match (left, right) {
                (ScalarValue::Text(left), ScalarValue::Text(right)) => match operator {
                    BinaryOperator::Gt => left > right,
                    BinaryOperator::GtEq => left >= right,
                    BinaryOperator::Lt => left < right,
                    BinaryOperator::LtEq => left <= right,
                    _ => unreachable!(),
                },
                _ => {
                    let left_number = left.as_f64().ok_or_else(|| {
                        AppError::InvalidRequest("numeric comparison requires numeric values".to_string())
                    })?;
                    let right_number = right.as_f64().ok_or_else(|| {
                        AppError::InvalidRequest("numeric comparison requires numeric values".to_string())
                    })?;
                    match operator {
                        BinaryOperator::Gt => left_number > right_number,
                        BinaryOperator::GtEq => left_number >= right_number,
                        BinaryOperator::Lt => left_number < right_number,
                        BinaryOperator::LtEq => left_number <= right_number,
                        _ => unreachable!(),
                    }
                }
            }
        }
        _ => unreachable!(),
    };
    Ok(result)
}

fn limit_to_usize(expr: &Expr) -> Result<usize, AppError> {
    let value = scalar_from_expr(expr)?;
    match value {
        ScalarValue::Int(value) if value >= 0 => Ok(value as usize),
        _ => Err(AppError::InvalidRequest(
            "LIMIT must be a non-negative integer literal".to_string(),
        )),
    }
}

fn limit_clause_to_usize(limit_clause: Option<&LimitClause>) -> Result<Option<usize>, AppError> {
    let Some(limit_clause) = limit_clause else {
        return Ok(None);
    };

    match limit_clause {
        LimitClause::LimitOffset { limit, .. } => limit.as_ref().map(limit_to_usize).transpose(),
        LimitClause::OffsetCommaLimit { limit, .. } => limit_to_usize(limit).map(Some),
    }
}

fn ensure_table_exists(catalog: &Catalog, database: &str, table_name: &str) -> Result<(), AppError> {
    let database_meta = catalog
        .databases
        .get(database)
        .ok_or_else(|| AppError::NotFound(format!("database '{database}' does not exist")))?;
    if database_meta.tables.iter().any(|table| table == table_name) {
        Ok(())
    } else {
        Err(AppError::NotFound(format!(
            "table '{database}.{table_name}' does not exist"
        )))
    }
}

fn table_name_from_update_target(table: &TableWithJoinsCompat) -> Result<&ObjectName, AppError> {
    let TableFactor::Table { name, .. } = &table.relation else {
        return Err(AppError::NotSupported(
            "UPDATE supports plain table references only".to_string(),
        ));
    };
    Ok(name)
}

fn delete_table_name(delete: &sqlparser::ast::Delete) -> Result<&ObjectName, AppError> {
    match &delete.from {
        sqlparser::ast::FromTable::WithFromKeyword(list)
        | sqlparser::ast::FromTable::WithoutKeyword(list) => {
            if list.len() != 1 {
                return Err(AppError::NotSupported(
                    "DELETE supports exactly one table".to_string(),
                ));
            }
            let TableFactor::Table { name, .. } = &list[0].relation else {
                return Err(AppError::NotSupported(
                    "DELETE supports plain table references only".to_string(),
                ));
            };
            Ok(name)
        }
    }
}

fn insert_table_name(table: &TableObject) -> Result<&ObjectName, AppError> {
    match table {
        TableObject::TableName(name) => Ok(name),
        TableObject::TableFunction(_) => Err(AppError::NotSupported(
            "INSERT into table functions is not supported".to_string(),
        )),
    }
}

fn assignment_target_name(target: &AssignmentTarget) -> Result<String, AppError> {
    match target {
        AssignmentTarget::ColumnName(name) => object_name_last(name),
        AssignmentTarget::Tuple(_) => Err(AppError::NotSupported(
            "tuple UPDATE assignments are not supported".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("serverless-db-{nonce}"))
    }

    fn execute(engine: &mut Engine, database: Option<&str>, sql: &str) -> ExecutionResult {
        engine
            .execute(SqlRequest {
                database: database.map(ToString::to_string),
                sql: sql.to_string(),
            })
            .expect("statement should succeed")
    }

    #[test]
    fn create_insert_select_update_delete_round_trip() {
        let root = temp_root();
        let mut engine = Engine::open(root).expect("engine should open");

        execute(&mut engine, None, "CREATE DATABASE app");
        execute(
            &mut engine,
            Some("app"),
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, active BOOL)",
        );
        execute(
            &mut engine,
            Some("app"),
            "INSERT INTO users (id, name, active) VALUES (1, 'owen', true), (2, 'sam', false)",
        );

        let selected = execute(
            &mut engine,
            Some("app"),
            "SELECT id, name FROM users WHERE id >= 1 LIMIT 10",
        );
        match selected {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!("expected rows"),
        }

        execute(
            &mut engine,
            Some("app"),
            "UPDATE users SET name = 'owen-updated' WHERE id = 1",
        );
        let selected = execute(
            &mut engine,
            Some("app"),
            "SELECT * FROM users WHERE id = 1",
        );
        match selected {
            ExecutionResult::Rows { rows, .. } => {
                assert_eq!(
                    rows[0].get("name"),
                    Some(&ScalarValue::Text("owen-updated".to_string()))
                );
            }
            _ => panic!("expected rows"),
        }

        execute(&mut engine, Some("app"), "DELETE FROM users WHERE id = 2");
        let selected = execute(&mut engine, Some("app"), "SELECT * FROM users");
        match selected {
            ExecutionResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!("expected rows"),
        }
    }
}
