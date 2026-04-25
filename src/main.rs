mod config;
mod engine;
mod error;
mod models;

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use config::Config;
use engine::Engine;
use error::AppError;
use models::{HealthResponse, MetricsResponse, SqlRequest, SqlResponse};
use tokio::{net::TcpListener, sync::Mutex, task};
use tracing::{error, info};

#[derive(Clone)]
struct AppState {
    engine: Arc<Mutex<Engine>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env();

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "serverless_db=info,serverless_db::engine=debug".to_string()),
        )
        .with_target(false)
        .compact()
        .init();

    let engine = Engine::open(config.data_dir.clone())?;
    let state = AppState {
        engine: Arc::new(Mutex::new(engine)),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/ping", get(health))
        .route("/metrics", get(metrics))
        .route("/sql", post(run_sql))
        .with_state(state);

    let listener = TcpListener::bind(config.bind_addr).await?;
    let local_addr = listener.local_addr()?;
    info!("serverless-db listening on {local_addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

async fn metrics(State(state): State<AppState>) -> Json<MetricsResponse> {
    let engine = state.engine.lock().await;
    let stats = engine.stats();
    Json(MetricsResponse {
        status: "ok".to_string(),
        database_count: stats.database_count,
        table_count: stats.table_count,
    })
}

async fn run_sql(
    State(state): State<AppState>,
    Json(request): Json<SqlRequest>,
) -> Result<Json<SqlResponse>, (StatusCode, Json<SqlResponse>)> {
    let engine = state.engine.clone();
    let handle = task::spawn_blocking(move || {
        let mut engine = engine.blocking_lock();
        engine.execute(request)
    });

    match handle.await {
        Ok(Ok(result)) => Ok(Json(SqlResponse::success(result))),
        Ok(Err(error)) => Err(error.into_http_response()),
        Err(join_error) => {
            error!("worker join error: {join_error}");
            Err(AppError::Internal(join_error.to_string()).into_http_response())
        }
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!("failed to listen for shutdown signal: {error}");
    }
}
