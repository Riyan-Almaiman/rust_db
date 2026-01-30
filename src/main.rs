use anyhow::Result;
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, net::TcpListener, sync::mpsc, sync::oneshot};

mod client;
mod config;
mod db;
mod db_types;
mod protocol;
mod commands;
use crate::db::Database;

struct Command {
    data: Vec<u8>,
    respond_to: oneshot::Sender<Vec<u8>>,
}

const ADDRESS: &str = concat!("0.0.0.0", ":",  "8080");

#[tokio::main]
async fn main() -> Result<()> {
tokio::spawn(client::run());
        let (tx, mut rx) = mpsc::channel::<Command>(1024);

    // Database logic loop
    tokio::spawn(async move {
        let mut db = Database::default();

        while let Some(cmd) = rx.recv().await {
            let response = match protocol::parse_command(&cmd.data) {
                Ok(db_cmd) => match db.execute(db_cmd) {
                    Ok(result) => protocol::encode_result(&result),
                    Err(e) => protocol::encode_error(&e),
                },
                Err(e) => protocol::encode_error(&format!("Protocol error: {}", e)),
            };
            let _ = cmd.respond_to.send(response);
        }
    });

    let listener = TcpListener::bind(ADDRESS).await?;
    println!("Database server on {}", ADDRESS);

    loop {
let (mut socket, addr) = listener.accept().await?;
println!("Client connected: {}", addr);
        let tx = tx.clone();

        tokio::spawn(async move {
            loop {
                let frame = match read_frame(&mut socket).await {
                    Ok(Some(f)) => f,
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("Client {} error: {}", addr, e);
                        break;
                    }
                };

                let (resp_tx, resp_rx) = oneshot::channel();

                if tx.send(Command { data: frame, respond_to: resp_tx }).await.is_err() {
                    break;
                }

                if let Ok(response) = resp_rx.await {
                    if let Err(e) = write_frame(&mut socket, &response).await {
                        eprintln!("Client {} write error: {}", addr, e);
                        break;
                    }
                }
            }
            println!("Client disconnected: {}", addr);
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
