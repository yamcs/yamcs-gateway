//! Handler for UDP telecommand
//! one UDP datagram = one TC packet

use std::net::SocketAddr;

use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
};

use crate::{
    msg::{Addr, YgwMessage}, LinkStatus, Result, YgwLinkNodeProperties, YgwNode
};
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

    async fn run(self: Box<Self>, node_id: u32, tx: Sender<YgwMessage>, mut rx: Receiver<YgwMessage>)  -> Result<()> {
        let addr = Addr::new(node_id, 0);

        let mut link_status = LinkStatus::new(addr);

       
        //send an initial link status indicating that the link is up (it is always up for a UDP)
        link_status.send(&tx).await?;

        while let Some(msg) = rx.recv().await {
            match msg {
                YgwMessage::TcPacket(_id, pc) => match pc.binary {
                    Some(ref cmd_binary) => {
                        link_status.data_out(1, cmd_binary.len() as u64);

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

            link_status.send(&tx).await?;
        }
        Ok(())
    }
}

impl TcUdpNode {
    pub async fn new(name: &str, description: &str, addr: SocketAddr) -> Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.connect(addr).await?;

        Ok(Self {
            socket,
            props: YgwLinkNodeProperties {
                name: name.to_string(),
                description: description.to_string(),
                tm: false,
                tc: true,
            },
        })
    }
}
