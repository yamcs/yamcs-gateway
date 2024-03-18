//! Handler for UDP telecommand
//! one UDP datagram = one TC packet

use std::net::SocketAddr;

use tokio::{net::UdpSocket, sync::mpsc::{Receiver, Sender}};

use crate::{msg::YgwMessage, YgwNode, Result};
use async_trait::async_trait;

pub struct TcUdpTarget {
    name: String,
    description: String,
    config: TcUdpConfig,
    socket: UdpSocket,
}

#[async_trait]
impl YgwNode for TcUdpTarget {
    fn properties(&self) -> &crate::YgwLinkNodeProperties {
        todo!()
    }
    
    fn sub_links(&self) -> &[crate::Link] {
        todo!()
    }
    
    async fn run(&mut self, node_id: u32, _tx: Sender<YgwMessage>, rx: Receiver<YgwMessage>) {
        todo!()
    }
}

impl TcUdpTarget {
    pub async fn new(
        config: TcUdpConfig,
        name: &str,
        description: &str,
    ) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.connect(config.addr).await?;
    
        Ok(Self {
            socket,
            name: name.to_string(),
            description: description.to_string(),
            config           
        })
    }
}


pub struct TcUdpConfig {
    pub id: u32,
    pub addr: SocketAddr,
    pub initial_bytes_to_strip: usize,
}





async fn run(_config: TcUdpConfig, socket: UdpSocket, mut rx: Receiver<YgwMessage>) -> Result<()> {
    while let Some(msg) = rx.recv().await {
        match msg {
            YgwMessage::TcPacket(_id, pc) => match pc.binary {
                Some(ref cmd_binary) => {
                    log::debug!("Sending command {:?}", pc);
                    if let Err(err) = socket.send(&cmd_binary[..]).await {
                        log::warn!("Got error when sending command {:?}", err);
                    }
                    //TODO send an acknowledgment back to Yamcs
                }
                None => log::warn!("Command has no binary {:?}", pc),
            },
            _ => log::warn!("Got unexpected message {:?}", msg),
        };
    }

    Ok(())
}
