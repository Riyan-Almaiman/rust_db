use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::State,
    response::IntoResponse,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tower_http::services::ServeDir;

use crate::db::{ColumnType, DbCommand, DbResult, Value};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum JsonCommand {
    CreateTable {
        table: String,
        columns: Vec<(String, String)>,
    },
    Insert {
        table: String,
        values: Vec<serde_json::Value>,
    },
    Update {
        table: String,
        #[serde(rename = "rowId")]
        row_id: u64,
        updates: std::collections::HashMap<String, serde_json::Value>,
    },
    SelectAll {
        table: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum JsonResponse {
    Ok { ok: bool },
    Error { ok: bool, error: String },
    Rows {
        ok: bool,
        columns: Vec<String>,
        rows: Vec<serde_json::Value>,
    },
}

impl JsonCommand {
    pub fn to_db_command(self) -> Result<DbCommand, String> {
        match self {
            JsonCommand::CreateTable { table, columns } => {
                let cols = columns
                    .into_iter()
                    .map(|(name, ty)| {
                        let col_type = match ty.as_str() {
                            "int" => ColumnType::Int,
                            "text" => ColumnType::Text,
                            "bool" => ColumnType::Bool,
                            _ => return Err(format!("Unknown type: {}", ty)),
                        };
                        Ok((name, col_type))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DbCommand::CreateTable { table, columns: cols })
            }
            JsonCommand::Insert { table, values } => {
                let vals = values
                    .into_iter()
                    .map(json_to_value)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DbCommand::InsertRow { table, values: vals })
            }
            JsonCommand::Update { table, row_id, updates } => {
                let upd = updates
                    .into_iter()
                    .map(|(k, v)| json_to_value(v).map(|val| (k, val)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DbCommand::UpdateRow { table, row_id, updates: upd })
            }
            JsonCommand::SelectAll { table } => Ok(DbCommand::SelectAll { table }),
        }
    }
}

fn json_to_value(v: serde_json::Value) -> Result<Value, String> {
    match v {
        serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
        serde_json::Value::Number(n) => {
            n.as_i64()
                .map(Value::Int)
                .ok_or_else(|| "Invalid integer".to_string())
        }
        serde_json::Value::String(s) => Ok(Value::Text(s)),
        _ => Err("Unsupported value type".to_string()),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Bool(b) => serde_json::Value::Bool(*b),
    }
}

impl From<Result<DbResult, String>> for JsonResponse {
    fn from(result: Result<DbResult, String>) -> Self {
        match result {
            Ok(DbResult::Ok) => JsonResponse::Ok { ok: true },
            Ok(DbResult::Rows { columns, rows }) => {
                let json_rows: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|(id, values)| {
                        let mut map = serde_json::Map::new();
                        map.insert("_id".to_string(), serde_json::Value::Number((*id).into()));
                        for (col, val) in columns.iter().zip(values.iter()) {
                            map.insert(col.clone(), value_to_json(val));
                        }
                        serde_json::Value::Object(map)
                    })
                    .collect();
                JsonResponse::Rows {
                    ok: true,
                    columns,
                    rows: json_rows,
                }
            }
            Err(e) => JsonResponse::Error { ok: false, error: e },
        }
    }
}

pub struct WebCommand {
    pub cmd: DbCommand,
    pub respond_to: oneshot::Sender<Result<DbResult, String>>,
}

type AppState = Arc<mpsc::Sender<WebCommand>>;

pub fn create_router(tx: mpsc::Sender<WebCommand>) -> Router {
    let state: AppState = Arc::new(tx);

    Router::new()
        .route("/ws", get(ws_handler))
        .nest_service("/", ServeDir::new("web"))
        .with_state(state)
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, tx: AppState) {
    while let Some(Ok(msg)) = socket.recv().await {
        let Message::Text(text) = msg else {
            continue;
        };

        let response = match serde_json::from_str::<JsonCommand>(&text) {
            Ok(json_cmd) => match json_cmd.to_db_command() {
                Ok(db_cmd) => {
                    let (resp_tx, resp_rx) = oneshot::channel();
                    if tx.send(WebCommand { cmd: db_cmd, respond_to: resp_tx }).await.is_err() {
                        JsonResponse::Error { ok: false, error: "Database unavailable".to_string() }
                    } else {
                        match resp_rx.await {
                            Ok(result) => JsonResponse::from(result),
                            Err(_) => JsonResponse::Error { ok: false, error: "No response".to_string() },
                        }
                    }
                }
                Err(e) => JsonResponse::Error { ok: false, error: e },
            },
            Err(e) => JsonResponse::Error { ok: false, error: format!("Invalid JSON: {}", e) },
        };

        let json = serde_json::to_string(&response).unwrap();
        if socket.send(Message::Text(json.into())).await.is_err() {
            break;
        }
    }
}
