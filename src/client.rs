use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use tokio::net::TcpStream;
use tower_http::services::ServeDir;

use crate::config::{CLIENT_ADDRESS, CLIENT_SERVER, DB_ADDRESS};
use crate::db_types::Value;
use crate::commands::{DbCommand, DbResult};
use crate::protocol;

pub async fn run() {
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .nest_service("/", ServeDir::new("web"));

    let listener = tokio::net::TcpListener::bind(CLIENT_SERVER).await.unwrap();
    println!("Web client at {}", CLIENT_ADDRESS);
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    let mut tcp = match TcpStream::connect(DB_ADDRESS).await {
        Ok(s) => s,
        Err(e) => {
            let _ = send_error(&mut socket, format!("Failed to connect to database: {}", e)).await;
            return;
        }
    };

    while let Some(Ok(msg)) = socket.recv().await {
        let Message::Text(text) = msg else { continue };
   println!("Received command: {}", text);
        // Parse JSON directly to DbCommand
        let db_cmd: DbCommand = match serde_json::from_str(&text) {
         
            Ok(cmd) => cmd,
            Err(e) => {
                let _ = send_error(&mut socket, format!("Invalid JSON: {}", e)).await;
                continue;
            }
        };

        let binary = protocol::encode_command(&db_cmd);
        if let Err(e) = protocol::write_frame(&mut tcp, &binary).await {
            let _ = send_error(&mut socket, format!("TCP send error: {}", e)).await;
            return;
        }

        // Read and decode response
        let response_bytes = match protocol::read_frame(&mut tcp).await {
            Ok(Some(b)) => b,
            Ok(None) => {
                let _ = send_error(&mut socket, "Connection closed".into()).await;
                return;
            }
            Err(e) => {
                let _ = send_error(&mut socket, format!("TCP read error: {}", e)).await;
                return;
            }
        };

        let json = match protocol::decode_response(&response_bytes) {
            Ok(result) => result_to_json(&result),
            Err(e) => serde_json::json!({"ok": false, "error": e}),
        };

        if socket.send(Message::Text(json.to_string().into())).await.is_err() {
            return;
        }
    }
}

fn result_to_json(result: &DbResult) -> serde_json::Value {
    match result {
        DbResult::Ok => serde_json::json!({"ok": true}),
   
        DbResult::Rows { columns, rows } => {
            let json_rows: Vec<_> = rows
                .iter()
                .map(|(id, values)| {
                    let mut obj = serde_json::Map::new();
                    obj.insert("_id".into(), serde_json::json!(id));
                    for (col, val) in columns.iter().zip(values) {
                        obj.insert(col.clone(), value_to_json(val));
                    }
                    serde_json::Value::Object(obj)
                })
                .collect();
       
            serde_json::json!({
                "ok": true,
                "columns": columns,
                "rows": json_rows
            })
        }
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Int(i) => serde_json::json!(i),
        Value::Text(s) => serde_json::json!(s),
        Value::Bool(b) => serde_json::json!(b),
    }
}

async fn send_error(socket: &mut WebSocket, error: String) -> Result<(), axum::Error> {
    let json = serde_json::json!({"ok": false, "error": error});
    socket.send(Message::Text(json.to_string().into())).await
}
