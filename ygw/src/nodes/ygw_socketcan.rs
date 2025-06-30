//! Provides a "raw" CAN ygw node
//! The node provides any intercepted message as TM packet and registers the following commands in Yamcs:
//! send_data_frame(id: u32, data: Binary) - injects data frames on the CAN bus
//! send_data_remote(id: u32) - injects a remote frame on the CAN bus
//!
//! TODO:
//!   add filters in order to receive only certain data

use crate::{
    msg::{Addr, TmPacket, YgwMessage},
    protobuf::{
        self, get_eng_arg,
        ygw::{CommandArgument, CommandDefinition, CommandDefinitionList, PreparedCommand},
    },
    Link, LinkStatus, Result, YgwError, YgwLinkNodeProperties, YgwNode,
};
use async_trait::async_trait;
use futures::StreamExt;
use socketcan::{
    tokio::CanSocket, CanDataFrame, CanFrame, CanRemoteFrame, EmbeddedFrame, ExtendedId, Frame,
};
use tokio::sync::mpsc::{Receiver, Sender};

const CMD_SEND_DATA_FRAME_ID: u32 = 0;
const CMD_SEND_REMOTE_FRAME_ID: u32 = 1;

pub struct CanNode {
    props: YgwLinkNodeProperties,
    socket: CanSocket,
}

struct CanRawState {
    addr: Addr,
    tx: Sender<YgwMessage>,
    rx: Receiver<YgwMessage>,
    link_status: LinkStatus,
}

#[async_trait]
impl YgwNode for CanNode {
    fn properties(&self) -> &YgwLinkNodeProperties {
        &self.props
    }

    fn sub_links(&self) -> &[Link] {
        &[]
    }

    async fn run(
        mut self: Box<Self>,
        node_id: u32,
        tx: Sender<YgwMessage>,
        rx: Receiver<YgwMessage>,
    ) -> Result<()> {
        let addr = Addr::new(node_id, 0);
        register_commands(addr, &tx).await?;
        let mut state = CanRawState {
            addr,
            tx,
            rx,
            link_status: LinkStatus::new(addr),
        };

        //send an initial link status indicating that the link is up
        state.link_status.send(&state.tx).await?;
        loop {
            tokio::select! {
                Some(data) = self.socket.next() => on_can_msg(&mut state, data).await?,
                msg = state.rx.recv() => {
                    if let Some(msg) = msg {
                        on_yamcs_msg(&mut state, &mut self.socket, msg).await?;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

impl CanNode {
    pub fn new(name: &str, can_if: &str) -> Result<Self> {
        let socket = CanSocket::open(can_if)?;
        let props = YgwLinkNodeProperties::new(
            name,
            "raw CAN interface providing as TM all the data captured and allowing to send raw messages")            
            .tm_packet(true);
        Ok(Self { props, socket })
    }
}

/// called at startup to register commands in Yamcs
async fn register_commands(addr: Addr, tx: &Sender<YgwMessage>) -> Result<()> {
    let cmd_data_frame = CommandDefinition {
        relative_name: "send_data_frame".into(),
        description: Some("Send a data frame with the given id".into()),
        ygw_cmd_id: CMD_SEND_DATA_FRAME_ID,
        arguments: vec![
            CommandArgument {
                name: "id".into(),
                description: Some("frame id".into()),
                unit: None,
                argtype: "uint32".into(),
                default_value: None,
            },
            CommandArgument {
                name: "data".into(),
                description: Some("frame data".into()),
                unit: None,
                argtype: "binary".into(),
                default_value: None,
            },
        ],
    };
    let cmd_remote_frame = CommandDefinition {
        relative_name: "send_remote_frame".into(),
        description: Some("Send a remote frame with the given id".into()),
        ygw_cmd_id: CMD_SEND_REMOTE_FRAME_ID,
        arguments: vec![
            CommandArgument {
                name: "id".into(),
                description: Some("frame id".into()),
                unit: None,
                argtype: "uint32".into(),
                default_value: None,
            },
            CommandArgument {
                name: "dlc".into(),
                description: Some("data length code (requested data length)".into()),
                unit: None,
                argtype: "uint32".into(),
                default_value: None,
            },
        ],
    };
    let cmd_defs = CommandDefinitionList {
        definitions: vec![cmd_data_frame, cmd_remote_frame],
    };

    tx.send(YgwMessage::CommandDefinitions(addr, cmd_defs))
        .await
        .map_err(|_| YgwError::ServerShutdown)
}

///called when a message is received on the CanSocket
async fn on_can_msg(
    state: &mut CanRawState,
    data: std::result::Result<CanFrame, socketcan::Error>,
) -> Result<()> {
    match data {
        Ok(frame) => {
            let mut data = Vec::with_capacity(4 + frame.data().len());
            data.extend_from_slice(&frame.id_word().to_be_bytes());
            data.extend_from_slice(frame.data());

            let pkt = TmPacket {
                data,
                acq_time: protobuf::now(),
            };

            if let Err(_) = state.tx.send(YgwMessage::TmPacket(state.addr, pkt)).await {
                return Err(YgwError::ServerShutdown);
            }
            state.link_status.data_in(1, frame.len() as u64);
        }
        Err(err) => {
            log::warn!("Error receiving can data {:?}", err);
        }
    }
    Ok(())
}

/// called when a message is received from Yamcs
async fn on_yamcs_msg(
    state: &mut CanRawState,
    socket: &mut CanSocket,
    msg: YgwMessage,
) -> Result<()> {
    match msg {
        YgwMessage::Tc(_, cmd) => execute_cmd(state, socket, cmd).await?,
        YgwMessage::LinkCommand(_, _) => todo!(),
        YgwMessage::ParameterUpdates(_, _) => todo!(),
        _ => log::warn!("Unexpected message received from Yamcs: {:?}", msg),
    }
    Ok(())
}

async fn execute_cmd(
    state: &mut CanRawState,
    socket: &mut CanSocket,
    cmd: PreparedCommand,
) -> Result<()> {
    let Some(ygw_cmd_id) = cmd.ygw_cmd_id else {
        log::warn!(
            "Received command without the ygw_cmd_id, ignored: {:?}",
            cmd
        );
        return Ok(());
    };

    match ygw_cmd_id {
        CMD_SEND_DATA_FRAME_ID => {
            let id: u32 = get_eng_arg(&cmd, "id")?;
            let data: Vec<u8> = get_eng_arg(&cmd, "data")?;
            let Some(id) = ExtendedId::new(id) else {
                return Err(YgwError::CommandError(format!("Invalid id {id}")));
            };
            let Some(frame) = CanDataFrame::new(id, &data) else {
                return Err(YgwError::CommandError(format!("Invalid data {:?}", data)));
            };
            log::debug!("Sending {:?}", frame);
            socket.write_frame(CanFrame::Data(frame))?.await?;
            state.link_status.data_out(1, frame.len() as u64);
        }
        CMD_SEND_REMOTE_FRAME_ID => {
            let id: u32 = get_eng_arg(&cmd, "id")?;
            let dlc: u32 = get_eng_arg(&cmd, "dlc")?;
            let Some(id) = ExtendedId::new(id) else {
                return Err(YgwError::CommandError(format!("Invalid id {id}")));
            };
            let Some(frame) = CanRemoteFrame::new_remote(id, dlc as usize) else {
                return Err(YgwError::CommandError(format!("Invalid dlc = {}", dlc)));
            };
            log::debug!("Sending {:?}", frame);
            socket.write_frame(CanFrame::Remote(frame))?.await?;
            state.link_status.data_out(1, frame.len() as u64);
        }
        _ => {
            log::warn!(
                "Unknown command received: ygw_cmd_id={} {:?} ",
                ygw_cmd_id,
                cmd.command_id.command_name
            );
            return Err(YgwError::CommandError(format!(
                "Command unknown: {:?}",
                cmd.command_id
            )));
        }
    }
    Ok(())
}
