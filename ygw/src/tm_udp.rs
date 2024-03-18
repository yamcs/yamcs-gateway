//! Handler for UDP telemetry
//! one UDP datagram = one TM packet
//! There is an option to strip some bytes from the beginning of the datagram (similar with the UdpTmDataLink from Yamcs)

use std::{
    net::SocketAddr,
    time::{self, SystemTime},
};

use tokio::{net::UdpSocket, sync::mpsc::Sender, task::JoinHandle};
use tokio::{sync::mpsc::error::SendError, time as tokio_time};

use crate::{
    msg::{TmPacket, YgwMessage},
    utc_converter::wallclock,
    yamcs::protobuf::ygw::{value, Event, ParameterData, ParameterValue, Value},
    Result, TargetIf,
};
const MAX_DATAGRAM_LEN: usize = 2000;

pub struct TmUdpConfig {
    pub id: u32,
    pub addr: SocketAddr,
    pub initial_bytes_to_strip: usize,
}


struct Stats {
    message_nr: u32,
    message_rate: u32,
    data_rate: u32,
}

pub async fn new(
    config: TmUdpConfig,
    target_id: u32,
    name: &str,
    description: &str,
) -> Result<TargetIf> {
    let socket = UdpSocket::bind(config.addr).await?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    tokio::spawn(async move {
        // first message we should receive is TargetInit
        if let Some(YgwMessage::TargetInit(ygw_tx)) = rx.recv().await {
            if let Err(err) = run(config, socket, ygw_tx).await {
                log::error!("Error in Udp TM handle: {:?}", err);
            }
        }
    });

    Ok(TargetIf {
        id: target_id,
        name: name.to_string(),
        description: description.to_string(),
        tx,
    })
}

async fn run(config: TmUdpConfig, socket: UdpSocket, tx: Sender<YgwMessage>) -> Result<()> {
    let mut buf = [0; MAX_DATAGRAM_LEN];
    let ibs = config.initial_bytes_to_strip;
    let mut stats = Stats {
        message_nr: 0,
        message_rate: 0,
        data_rate: 0,
    };

    let mut interval = tokio_time::interval(time::Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio_time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            len = socket.recv(&mut buf) => {
                let len = len?;
                stats.message_rate += 1;
                stats.data_rate += len as u32;
                log::trace!("Got packet of size {len}");

                let pkt = TmPacket {
                    target_id: config.id,
                    data: buf[ibs..len].to_vec(),
                    acq_time: SystemTime::now(),
                };
                if let Err(_) = tx.send(YgwMessage::TmPacket(pkt)).await {
                    break;
                }
            }
            _ = interval.tick() => {
                if let Err(_) = send_stats(config.id, &stats, &tx).await {
                    break;
                }

                stats.message_rate = 0;
                stats.message_nr += 1;
                stats.data_rate = 0;
            }
        }
    }
    Ok(())
}

async fn send_stats(
    target_id: u32,
    stats: &Stats,
    tx: &Sender<YgwMessage>,
) -> std::result::Result<(), SendError<YgwMessage>> {
    let pr = ParameterValue {
        fqn: Some("/yamcs/ygw/tm_udp/packet_rate".to_string()),
        eng_value: Some(Value {
            v: Some(value::V::Uint32Value(stats.message_rate)),
        }),
        ..Default::default()
    };
    let dr = ParameterValue {
        fqn: Some("/yamcs/ygw/tm_udp/data_rate".to_string()),
        eng_value: Some(Value {
            v: Some(value::V::Uint32Value(stats.data_rate)),
        }),
        ..Default::default()
    };
    let mc = ParameterValue {
        fqn: Some("/yamcs/ygw/tm_udp/message_number".to_string()),
        eng_value: Some(Value {
            v: Some(value::V::Uint32Value(stats.message_nr)),
        }),
        ..Default::default()
    };

    let pd = ParameterData {
        generation_time: Some(wallclock().into()),
        parameter: vec![pr, dr, mc],
        seq_num: Some(stats.message_nr),
        ..Default::default()
    };

    tx.send(YgwMessage::Parameters(target_id, pd)).await
}

#[cfg(test)]
mod tests {
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

        assert!(
            matches!(msg, YgwMessage::TmPacket(TmPacket{target_id, data,..}) if target_id == 3 && data == &buf[2..])
        );

        udp_tm_handler.stop();
    }
}
