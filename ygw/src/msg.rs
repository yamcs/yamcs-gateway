use std::time::SystemTime;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;

use crate::{
    protobuf::{
        self,
        ygw::MessageType,
    },
    Result, YgwError,
};

const VERSION: u8 = 0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Addr {
    node_id: u32,
    link_id: u32,
}
impl Addr {
    pub fn new(node_id: u32, link_id: u32) -> Self {
        Self { node_id, link_id }
    }
    pub fn node_id(&self) -> u32 {
        self.node_id
    }

    pub fn link_id(&self) -> u32 {
        self.link_id
    }
}
// messages exchanged on the internal channels
#[derive(Debug, PartialEq)]
pub enum YgwMessage {
    ParameterDefinitions(Addr, protobuf::ygw::ParameterDefinitionList),
    TmPacket(Addr, TmPacket),
    TcPacket(Addr, protobuf::ygw::PreparedCommand),
    Event(Addr, protobuf::ygw::Event),
    ParameterData(Addr, protobuf::ygw::ParameterData),
    LinkStatus(Addr, protobuf::ygw::LinkStatus),
    LinkCommand(Addr, protobuf::ygw::LinkCommand),
    ParameterUpdates(Addr, protobuf::ygw::ParameterUpdates),
    
}

#[derive(Debug, PartialEq)]
pub struct TmPacket {
    pub data: Vec<u8>,
    pub acq_time: SystemTime,
}

impl YgwMessage {
    /// encode a message to a BytesMut
    /// the first 4 bytes will be the data length
    pub(crate) fn encode(&self) -> BytesMut {
        match self {
            YgwMessage::TmPacket(addr, tm) => {
                let mut buf = buf_with_header(tm.data.len() + 12, MessageType::Tm, *addr);
                encode_time(&mut buf, tm.acq_time);
                buf.put(&tm.data[..]);
                buf
            }
            YgwMessage::TcPacket(addr, pc) => {
                encode_message(addr, MessageType::Tc, pc)
            }
            YgwMessage::Event(addr, ev) => {
                encode_message(addr, MessageType::Event, ev)
            }
            YgwMessage::ParameterData(addr, pdata) => {
                encode_message(addr, MessageType::ParameterData, pdata)
            }
            YgwMessage::ParameterDefinitions(addr, pdefs) => {
                encode_message(addr, MessageType::ParameterDefinitions, pdefs)
            }
            YgwMessage::LinkStatus(addr, link_status) => {
                encode_message(addr, MessageType::LinkStatus, link_status)
            }

            _ => unreachable!("Cannot encode message {:?}", self),
        }
    }

    /// decode a message from Bytes
    /// the data should start directly with the version (no data length)
    pub(crate) fn decode(buf: &mut Bytes) -> Result<Self> {
        if buf.len() < 10 {
            return Err(YgwError::DecodeError(format!(
                "Message too short: {} bytes(expected at least 10 bytes)",
                buf.len()
            )));
        }
        let version = buf.get_u8();
        if version != VERSION {
            return Err(YgwError::DecodeError(format!(
                "Invalid message version {} (expected {})",
                version, VERSION
            )));
        }
        let msg_type = buf.get_u8() as i32;
        let node_id = buf.get_u32();
        let link_id = buf.get_u32();
        let addr = Addr {
            node_id,
            link_id,
        };

        println!("Got message {:?}", msg_type);
        match msg_type {
            x if x == MessageType::Tc as i32 => match protobuf::ygw::PreparedCommand::decode(buf) {
                Ok(prep_cmd) => Ok(YgwMessage::TcPacket(addr, prep_cmd)),
                Err(e) => Err(YgwError::DecodeError(e.to_string())),
            },
            x if x == MessageType::ParameterUpdates as i32 => match protobuf::ygw::ParameterUpdates::decode(buf) {
                Ok(param_data) => Ok(YgwMessage::ParameterUpdates(addr, param_data)),
                Err(e) => Err(YgwError::DecodeError(e.to_string())),
            },
            x if x == MessageType::LinkCommand as i32 => match protobuf::ygw::LinkCommand::decode(buf) {
                Ok(link_cmd) => Ok(YgwMessage::LinkCommand(addr, link_cmd)),
                Err(e) => Err(YgwError::DecodeError(e.to_string())),
            },

            _ => Err(YgwError::DecodeError(format!(
                "Unexpected message type {}",
                msg_type
            ))),
        }
    }

    pub fn node_id(&self) -> u32 {
        match self {
            YgwMessage::TmPacket(addr, _) => addr.node_id,
            YgwMessage::TcPacket(addr, _) => addr.node_id,
            YgwMessage::Event(addr, _) => addr.node_id,
            YgwMessage::ParameterData(addr, _) => addr.node_id,
            YgwMessage::ParameterUpdates(addr, _) => addr.node_id,
            _ => todo!(),
        }
    }
}



pub(crate) fn encode_message<T: prost::Message>(
    addr: &Addr,
    msg_type: MessageType,
    msg: &T,
) -> BytesMut {
    let mut buf = buf_with_header(msg.encoded_len(), msg_type, *addr);
    msg.encode_raw(&mut buf);
    buf
}

pub(crate) fn encode_node_info(node_list: &protobuf::ygw::NodeList) -> BytesMut {
    //message length without the length itself
    let len = 2 + node_list.encoded_len();
    let mut buf = BytesMut::with_capacity(len);

    buf.put_u32(len as u32);
    buf.put_u8(VERSION);
    buf.put_u8(MessageType::NodeInfo as u8);
    node_list.encode_raw(&mut buf);

    buf
}

fn encode_time(buf: &mut BytesMut, time: SystemTime) {
    let d = time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let (seconds, nanos) = if d.as_secs() >= i64::MAX as u64 {
        (i64::MAX, 0)
    } else {
        (d.as_secs() as i64, d.subsec_nanos())
    };

    buf.put_i64(seconds);
    buf.put_u32(nanos);
}

fn buf_with_header(data_len: usize, msg_type: MessageType, addr: Addr) -> BytesMut {
     //message length without the length itself
    let len = 10 + data_len;
    let mut buf = BytesMut::with_capacity(len);
    buf.put_u32(len as u32);
    buf.put_u8(VERSION);
    buf.put_u8(msg_type as u8);
    buf.put_u32(addr.node_id);
    buf.put_u32(addr.link_id);
    buf
}
