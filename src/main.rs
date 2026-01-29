
use anyhow::Result;
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener, sync::mpsc, sync::oneshot};

mod db;
mod protocol;
mod web;

use crate::db::{Database, DbResult};
use crate::web::WebCommand;

enum Command {
    Tcp {
        data: Vec<u8>,
        respond_to: oneshot::Sender<Vec<u8>>,
    },
    Web(WebCommand),
}

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<Command>(1024);

    // Channel for web commands
    let (web_tx, mut web_rx) = mpsc::channel::<WebCommand>(1024);

    // Forward web commands to main channel
    let tx_for_web = tx.clone();
    tokio::spawn(async move {
        while let Some(cmd) = web_rx.recv().await {
            let _ = tx_for_web.send(Command::Web(cmd)).await;
        }
    });

    // Start web server
    let web_router = web::create_router(web_tx);
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
        println!("Web UI at http://localhost:3000");
        axum::serve(listener, web_router).await.unwrap();
    });

    // Database logic loop
    let _db_handle = tokio::spawn(async move {
        println!("Logic loop started");

        let mut db = Database::default();


        while let Some(cmd) = rx.recv().await {
       
            match cmd {
                Command::Tcp { data, respond_to } => {
                    let response = match protocol::parse_command(&data) {
                        Ok(db_cmd) => match db.execute(db_cmd) {
                            Ok(DbResult::Ok) => protocol::encode_ok(),
                            Ok(DbResult::Rows { columns, rows }) => {
                                protocol::encode_rows(&columns, &rows)
                            }
                            Err(e) => protocol::encode_error(&e),
                        },
                        Err(e) => protocol::encode_error(&format!("Protocol error: {}", e)),
                    };
                    let _ = respond_to.send(response);
                }
                Command::Web(WebCommand { cmd: db_cmd, respond_to }) => {
                    let result = db.execute(db_cmd);
                    let _ = respond_to.send(result);
                }
            }
        }
    });

    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    println!("TCP server on port 8080");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let tx = tx.clone();

        tokio::spawn(async move {
            loop {
                let frame = match read_frame(&mut socket).await {
                    Ok(Some(f)) => f,
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("TCP client error: {}", e);
                        break;
                    }
                };

                let (resp_tx, resp_rx) = oneshot::channel();

                if tx.send(Command::Tcp { data: frame, respond_to: resp_tx }).await.is_err() {
                    break;
                }

                if let Ok(response) = resp_rx.await {
                    if let Err(e) = write_frame(&mut socket, &response).await {
                        eprintln!("TCP write error: {}", e);
                        break;
                    }
                }
            }
        });
    }
}

async fn read_frame(socket: &mut tokio::net::TcpStream) -> anyhow::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];

    if socket.read_exact(&mut len_buf).await.is_err() {
        return Ok(None);
    }

    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 1024 * 1024 {
        anyhow::bail!("Frame too large");
    }

    let mut data = vec![0u8; len];
    socket.read_exact(&mut data).await?;

    Ok(Some(data))
}

async fn write_frame(socket: &mut tokio::net::TcpStream, data: &[u8]) -> anyhow::Result<()> {
    let len = (data.len() as u32).to_be_bytes();
    socket.write_all(&len).await?;
    socket.write_all(data).await?;
    Ok(())
}
