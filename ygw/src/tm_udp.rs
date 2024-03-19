//! Handler for UDP telemetry
//! one UDP datagram = one TM packet
//! There is an option to strip some bytes from the beginning of the datagram (similar with the UdpTmDataLink from Yamcs)

use std::{
    net::SocketAddr,
    time::{self, SystemTime},
};

use async_trait::async_trait;
use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
};
use tokio::{sync::mpsc::error::SendError, time as tokio_time};

use crate::{
    msg::{Addr, TmPacket, YgwMessage},
    utc_converter::wallclock,
    yamcs::protobuf::{self, ygw::{value, ParameterData, ParameterValue, Value}},
    Result, YgwLinkNodeProperties, YgwNode,
};
const MAX_DATAGRAM_LEN: usize = 2000;

struct Stats {
    message_nr: u32,
    message_rate: u32,
    data_rate: u32,
}

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

    async fn run(&mut self, node_id: u32, tx: Sender<YgwMessage>, mut _rx: Receiver<YgwMessage>) {
        let mut buf = [0; MAX_DATAGRAM_LEN];
        let ibs = self.initial_bytes_to_strip;
        let mut stats = Stats {
            message_nr: 0,
            message_rate: 0,
            data_rate: 0,
        };
        let mut link_status = protobuf::ygw::LinkStatus{
            data_in_count: 0,
            data_out_count: 0,
            state: protobuf::ygw::LinkState::Ok as i32
        };

        let addr = Addr::new(node_id, 0);
        let mut interval = tokio_time::interval(time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio_time::MissedTickBehavior::Skip);
        let para_namespace = format!("/yamcs/ygw/{}", self.props.name);
        loop {
            tokio::select! {
                len = self.socket.recv(&mut buf) => {
                    match len {
                        Ok(len) => {
                            stats.message_rate += 1;
                            stats.data_rate += len as u32;
                            link_status.data_in_count +=1;
                            log::trace!("Got packet of size {len}");

                            let pkt = TmPacket {
                                data: buf[ibs..len].to_vec(),
                                acq_time: SystemTime::now(),
                            };
                            if let Err(_) = tx.send(YgwMessage::TmPacket(addr, pkt)).await {
                                break;
                            }
                        },
                        Err(err) => {
                            log::warn!("Error receiving data from UDP socket: {:?}", err);
                            break;
                        }
                    }

                }
                _ = interval.tick() => {
                    if let Err(_) = tx.send(YgwMessage::LinkStatus(addr, link_status.clone())).await {
                        break;
                    }

                    if let Err(_) = send_stats(&para_namespace, addr, &stats, &tx).await {
                        break;
                    }

                    stats.message_rate = 0;
                    stats.message_nr += 1;
                    stats.data_rate = 0;
                }
            }
        }
    }
}

impl TmUdpNode {
    pub async fn new(
        name: &str,
        description: &str,
        addr: SocketAddr,
        initial_bytes_to_strip: usize,
    ) -> Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        socket.connect(addr).await?;

        Ok(Self {
            socket,
            initial_bytes_to_strip,
            props: YgwLinkNodeProperties {
                name: name.to_string(),
                description: description.to_string(),
                tm: false,
                tc: true,
            },
        })
    }
}

async fn send_stats(
    para_namespace: &str,
    addr: Addr,
    stats: &Stats,
    tx: &Sender<YgwMessage>,
) -> std::result::Result<(), SendError<YgwMessage>> {
    let pr = ParameterValue {
        fqn: Some(format!("{para_namespace}/packet_rate")),
        eng_value: Some(Value {
            v: Some(value::V::Uint32Value(stats.message_rate)),
        }),
        ..Default::default()
    };
    let dr = ParameterValue {
        fqn: Some(format!("{para_namespace}/data_rate")),
        eng_value: Some(Value {
            v: Some(value::V::Uint32Value(stats.data_rate)),
        }),
        ..Default::default()
    };
    let mc = ParameterValue {
        fqn: Some(format!("{para_namespace}/message_number")),
        eng_value: Some(Value {
            v: Some(value::V::Uint32Value(stats.message_nr)),
        }),
        ..Default::default()
    };

    let pd = ParameterData {
        generation_time: Some(wallclock().into()),
        parameter: vec![pr, dr, mc],
        seq_num: stats.message_nr,
        group: para_namespace.to_string(),
    };

    tx.send(YgwMessage::Parameters(addr, pd)).await
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
