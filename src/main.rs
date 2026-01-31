use anyhow::Result;
use tokio::sync::{mpsc, oneshot};
mod client;
mod commands;
mod config;
mod db;
mod db_types;
mod listener;
mod protocol;
use crate::db::Database;

struct Command {
    data: Vec<u8>,
    respond_to: oneshot::Sender<Vec<u8>>,
}

const ADDRESS: &str = concat!("0.0.0.0", ":", "8080");

#[tokio::main]
async fn main() -> Result<()> {
    let (tx, rx) = mpsc::channel::<Command>(1024);

    let mut db = Database::default();

    // Database logic loop
    tokio::spawn(async move {
        db.run(rx).await;
    });

    tokio::spawn(async move {
        client::run().await;
    });

    let listener = listener::Listener::new(ADDRESS).await?;
    listener.accept(tx).await;

    Ok(())
}
