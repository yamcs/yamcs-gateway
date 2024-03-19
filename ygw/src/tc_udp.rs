//! Handler for UDP telecommand
//! one UDP datagram = one TC packet

use std::net::SocketAddr;

use tokio::{net::UdpSocket, sync::mpsc::{Receiver, Sender}};

use crate::{msg::YgwMessage, Result, YgwLinkNodeProperties, YgwNode};
use async_trait::async_trait;

pub struct TcUdpNode {
    props: YgwLinkNodeProperties,
    socket: UdpSocket,
}

#[async_trait]
impl YgwNode for TcUdpNode {
    fn properties(&self) -> &crate::YgwLinkNodeProperties {
        &self.props
    }
    
    fn sub_links(&self) -> &[crate::Link] {
        &[]
    }
    
    async fn run(&mut self, node_id: u32, _tx: Sender<YgwMessage>, mut rx: Receiver<YgwMessage>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                YgwMessage::TcPacket(_id, pc) => match pc.binary {
                    Some(ref cmd_binary) => {
                        log::debug!("Sending command {:?}", pc);
                        if let Err(err) = self.socket.send(&cmd_binary[..]).await {
                            log::warn!("Got error when sending command {:?}", err);
                        }
                        //TODO send an acknowledgment back to Yamcs
                    }
                    None => log::warn!("Command has no binary {:?}", pc),
                },
                _ => log::warn!("Got unexpected message {:?}", msg),
            };
        }
    }
}

impl TcUdpNode {
    pub async fn new(
        name: &str,
        description: &str,
        addr: SocketAddr
    ) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.connect(addr).await?;
    
        Ok(Self {
            socket,
            props: YgwLinkNodeProperties{
                 name: name.to_string(),
                description: description.to_string(),
                tm: false,
                tc: true
            },   
        })
    }
}



