use std::time::SystemTime;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;

use crate::{
    yamcs::protobuf::{self, ygw::MessageType},
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
    ParameterDefs(Addr, protobuf::ygw::ParameterDefinitionList),
    TmPacket(Addr, TmPacket),
    TcPacket(Addr, protobuf::ygw::PreparedCommand),
    Event(Addr, protobuf::ygw::Event),
    Parameters(Addr, protobuf::ygw::ParameterData),    
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
        fn buf_with_header(data_len: usize, msg_type: MessageType, addr: Addr) -> BytesMut {
            let mut buf = BytesMut::with_capacity(10 + data_len);
            buf.put_u32((data_len + 10) as u32);
            buf.put_u8(VERSION);
            buf.put_u8(msg_type as u8);
            buf.put_u32(addr.node_id);
            buf.put_u32(addr.link_id);
            buf
        }
        match self {
            YgwMessage::TmPacket(addr, tm) => {
                let mut buf = buf_with_header(tm.data.len() + 12, MessageType::Tm, *addr);
                encode_time(&mut buf, tm.acq_time);
                buf.put(&tm.data[..]);
                buf
            }
            YgwMessage::TcPacket(addr, pc) => {
                let mut buf = buf_with_header(pc.encoded_len(), MessageType::Tc, *addr);
                pc.encode_raw(&mut buf);
                buf
            }
            YgwMessage::Event(addr, e) => {
                let mut buf = buf_with_header(e.encoded_len(), MessageType::Event, *addr);
                e.encode_raw(&mut buf);
                buf
            }
            YgwMessage::Parameters(addr, pd) => {
                let mut buf = buf_with_header(pd.encoded_len(), MessageType::ParameterData, *addr);
                pd.encode_raw(&mut buf);
                buf
            }
            YgwMessage::ParameterDefs(addr, pdefs) => {
                let mut buf = buf_with_header(pdefs.encoded_len(), MessageType::ParameterData, *addr);
                pdefs.encode_raw(&mut buf);
                buf
            }
            _ => unreachable!("Cannot encode message {:?}", self),
        }
    }

    /// decode a message from Bytes
    /// the data should start directly with the version (no data length)
    pub(crate) fn decode(buf: &mut Bytes) -> Result<Self> {
        if buf.len() < 6 {
            return Err(YgwError::DecodeError(format!(
                "Message too short: {} bytes(expected at least 6 bytes)",
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
        let msg_type = buf.get_u8();
        let target_id = buf.get_u32();
        let link_id = buf.get_u32();
        let addr = Addr {
            node_id: target_id,
            link_id,
        };

        match msg_type.try_into() {
            Ok(MessageType::Tc) => match protobuf::ygw::PreparedCommand::decode(buf) {
                Ok(prep_cmd) => Ok(YgwMessage::TcPacket(addr, prep_cmd)),
                Err(e) => Err(YgwError::DecodeError(e.to_string())),
            },
            Ok(MessageType::ParameterData) => match protobuf::ygw::ParameterData::decode(buf) {
                Ok(param_data) => Ok(YgwMessage::Parameters(addr, param_data)),
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
            YgwMessage::Parameters(addr, _) => addr.node_id,
            _ => 0,
        }
    }
}

pub(crate) fn encode_node_info(node_list: &protobuf::ygw::NodeList) -> BytesMut {
    let len = 6 + node_list.encoded_len();
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

impl TryFrom<u8> for MessageType {
    type Error = ();

    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
        match v {
            x if x == MessageType::Tm as u8 => Ok(MessageType::Tm),
            x if x == MessageType::Tc as u8 => Ok(MessageType::Tc),
            x if x == MessageType::Event as u8 => Ok(MessageType::Event),
            x if x == MessageType::ParameterData as u8 => Ok(MessageType::ParameterData),
            x if x == MessageType::ParameterDefinitions as u8 => Ok(MessageType::ParameterDefinitions),
            _ => Err(()),
        }
    }
}
