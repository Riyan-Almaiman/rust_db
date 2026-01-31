use tokio::{net::TcpListener, sync::mpsc, sync::oneshot};

use crate::{Command, protocol};

pub struct Listener {
    listener: TcpListener,
}

impl Listener {
    pub async fn new(address: &str) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(address).await?;
        println!("Database server on {}", address);
        Ok(Self { listener })
    }

    pub async fn accept(&self, tx: mpsc::Sender<Command>) {
        loop {
            let (mut socket, addr) = self.listener.accept().await.unwrap();
            println!("Client connected: {}", addr);
            let tx = tx.clone();
            tokio::spawn(async move {
                loop {
                    let frame = match protocol::read_frame(&mut socket).await {
                        Ok(Some(f)) => f,
                        Ok(None) => break,
                        Err(e) => {
                            eprintln!("Client {} error: {}", addr, e);
                            break;
                        }
                    };

                    let (resp_tx, resp_rx) = oneshot::channel();

                    if tx
                        .send(Command {
                            data: frame,
                            respond_to: resp_tx,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }

                    if let Ok(response) = resp_rx.await {
                        if let Err(e) = protocol::write_frame(&mut socket, &response).await {
                            eprintln!("Client {} write error: {}", addr, e);
                            break;
                        }
                    }
                }
                println!("Client disconnected: {}", addr);
            });
        }
    }
}
