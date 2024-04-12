//! Handler for UDP telecommand
//! one UDP datagram = one TC packet

use std::net::SocketAddr;

use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
};

use crate::{
    msg::{Addr, YgwMessage},
    yamcs::protobuf,
    Result, YgwLinkNodeProperties, YgwNode,
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

    async fn run(&mut self, node_id: u32, tx: Sender<YgwMessage>, mut rx: Receiver<YgwMessage>) {
        let mut link_status = protobuf::ygw::LinkStatus {
            data_in_count: 0,
            data_out_count: 0,
            data_in_size: 0,
            data_out_size: 0,
            state: protobuf::ygw::LinkState::Ok as i32,
        };
        let addr = Addr::new(node_id, 0);

        //send an initial link status indicating that the link is up (it is always up for a UDP)
        if let Err(_) = tx.send(YgwMessage::LinkStatus(addr, link_status.clone())).await {
            return;
        }

        while let Some(msg) = rx.recv().await {
            println!("got message via bla: {:?}", msg);
            match msg {
                YgwMessage::TcPacket(_id, pc) => match pc.binary {
                    Some(ref cmd_binary) => {
                        link_status.data_out_count += 1;
                        link_status.data_out_size += cmd_binary.len() as u64;

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

            if let Err(_) = tx.send(YgwMessage::LinkStatus(addr, link_status.clone())).await {
                return;
            }
        }
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
