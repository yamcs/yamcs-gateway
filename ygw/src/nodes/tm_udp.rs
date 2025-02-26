//! Handler for UDP telemetry
//! one UDP datagram = one TM packet
//! There is an option to strip some bytes from the beginning of the datagram (similar with the UdpTmDataLink from Yamcs)

use std::{
    net::SocketAddr,
    time::{self},
};

use async_trait::async_trait;
use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
};

use tokio::time as tokio_time;

use crate::{
    msg::{Addr, TmPacket, YgwMessage},
    LinkStatus, Result, YgwError, YgwLinkNodeProperties, YgwNode,
};
const MAX_DATAGRAM_LEN: usize = 2000;

pub struct TmUdpNode {
    props: YgwLinkNodeProperties,
    socket: UdpSocket,
    initial_bytes_to_strip: usize,
}

#[async_trait]
impl YgwNode for TmUdpNode {
    fn properties(&self) -> &crate::YgwLinkNodeProperties {
        &self.props
    }

    fn sub_links(&self) -> &[crate::Link] {
        &[]
    }

    async fn run(
        self: Box<Self>,
        node_id: u32,
        tx: Sender<YgwMessage>,
        mut rx: Receiver<YgwMessage>,
    ) -> Result<()> {
        let mut buf = [0; MAX_DATAGRAM_LEN];
        let ibs = self.initial_bytes_to_strip;

        let addr = Addr::new(node_id, 0);
        let mut link_status = LinkStatus::new(addr);

        let mut interval = tokio_time::interval(time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio_time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                len = self.socket.recv(&mut buf) => {
                    match len {
                        Ok(len) => {
                            link_status.data_in(1, len as u64);
                            log::trace!("Got packet of size {len}");

                            let pkt = TmPacket {
                                data: buf[ibs..len].to_vec(),
                                acq_time: crate::protobuf::now(),
                            };
                            if let Err(_) = tx.send(YgwMessage::TmPacket(addr, pkt)).await {
                                return Err(YgwError::ServerShutdown);
                            }
                        },
                        Err(err) => {
                            log::warn!("Error receiving data from UDP socket: {:?}", err);
                            return Err(YgwError::IOError("Error receiving data from UDP socket".into(), err));
                        }
                    }
                }
                _ = interval.tick() => {
                    link_status.send(&tx).await?
                }
                msg = rx.recv() => {
                    match msg {
                        Some(msg) => log::info!("Received unexpected message {:?}", msg),
                        None => break

                    }
                }
            }
        }

        log::debug!("TM UDP node exiting");
        Ok(())
    }
}

impl TmUdpNode {
    pub async fn new(
        name: &str,
        description: &str,
        addr: SocketAddr,
        initial_bytes_to_strip: usize,
    ) -> Result<Self> {
        let socket = UdpSocket::bind(addr)
            .await
            .map_err(|e| YgwError::IOError(format!("Failed to bind to {}", addr), e))?;

        Ok(Self {
            socket,
            initial_bytes_to_strip,
            props: YgwLinkNodeProperties::new(name, description)
                .tm_packet(true),
        })
    }
}

/*
#[cfg(test)]
mod tests {
    use std::ops::Add;

    use tokio::sync::mpsc::channel;

    use crate::msg::TmPacket;

    use super::*;

    #[tokio::test]
    async fn test1() {
        let (tx, mut rx) = channel(10);

        env_logger::init();
        let addr: SocketAddr = ([127, 0, 0, 1], 9090).into();
        let conf = TmUdpConfig {
            id: 3,
            addr,
            initial_bytes_to_strip: 2,
        };

        let udp_tm_handler = UdpTmHandle::start(conf, tx).await.unwrap();

        let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        socket.connect(addr).await.unwrap();

        let buf = vec![1, 2, 3, 4, 5, 6];
        socket.send(&buf).await.unwrap();

        let msg = rx.recv().await.unwrap();
        let addr = Addr::new(0, 0);

        assert!(
            matches!(msg, YgwMessage::TmPacket(addr1, TmPacket{data,..}) if addr1 == addr && data == &buf[2..])
        );

        udp_tm_handler.stop();
    }
}
 */
