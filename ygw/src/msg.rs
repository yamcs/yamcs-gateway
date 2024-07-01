use std::{ops::Deref, time::SystemTime};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;

use crate::{
    protobuf::{self, ygw::MessageType},
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
    CommandDefinitions(Addr, protobuf::ygw::CommandDefinitionList),
    TmPacket(Addr, TmPacket),
    TcPacket(Addr, protobuf::ygw::PreparedCommand),
    Event(Addr, protobuf::ygw::Event),
    ParameterData(Addr, protobuf::ygw::ParameterData),
    LinkStatus(Addr, protobuf::ygw::LinkStatus),
    LinkCommand(Addr, protobuf::ygw::LinkCommand),
    ParameterUpdates(Addr, protobuf::ygw::ParameterUpdates),
}

/// An encoded message contains 4 bytes length, 1 byte version, 8 bytes recording number (rn), 1 byte data type followed by the data
pub struct EncodedMessage(Bytes);

impl EncodedMessage {
    pub fn rn(&self) -> u64 {
        self.0.slice(5..13).get_u64()
    }

    /// returns a reference to the data - that is the encoded message without the size and without the recording number
    pub fn data(&self) -> &[u8] {
        &self.0[13..]
    }

    pub fn clone(&self) -> EncodedMessage {
        EncodedMessage(self.0.clone())
    }
}

impl Deref for EncodedMessage {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}

impl AsRef<[u8]> for EncodedMessage {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

struct EncodedMessageBuilder(BytesMut);

impl EncodedMessageBuilder {
    fn new(data_len: usize, rn: u64, msg_type: MessageType, addr: Addr) -> EncodedMessageBuilder {
        //message length without the length itself
        let len = 18 + data_len;
        let mut buf = BytesMut::with_capacity(len);
        buf.put_u32(len as u32);
        buf.put_u8(VERSION);
        buf.put_u64(rn);
        buf.put_u8(msg_type as u8);
        buf.put_u32(addr.node_id);
        buf.put_u32(addr.link_id);
        EncodedMessageBuilder(buf)
    }
    fn build(self) -> EncodedMessage {
        EncodedMessage(self.0.freeze())
    }

    fn put_time(&mut self, time: SystemTime) {
        let d = time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let (seconds, nanos) = if d.as_secs() >= i64::MAX as u64 {
            (i64::MAX, 0)
        } else {
            (d.as_secs() as i64, d.subsec_nanos())
        };
    
        self.0.put_i64(seconds);
        self.0.put_u32(nanos);
    }

    fn put(&mut self, buf: &[u8]) {
        self.0.put(buf);
    }
    
}

#[derive(Debug, PartialEq)]
pub struct TmPacket {
    pub data: Vec<u8>,
    pub acq_time: SystemTime,
}

impl YgwMessage {
    /// encode a message to a BytesMut
    /// the first 4 bytes will be the data length
    /// then 8 bytes recording number
    pub(crate) fn encode(&self, rn: u64) -> EncodedMessage {
        match self {
            YgwMessage::TmPacket(addr, tm) => {
                let mut emb = EncodedMessageBuilder::new(tm.data.len() + 12, rn, MessageType::Tm, *addr);
                emb.put_time(tm.acq_time);
                emb.put(&tm.data[..]);
                emb.build()
            }
            YgwMessage::TcPacket(addr, pc) => encode_message(rn, addr, MessageType::Tc, pc),
            YgwMessage::Event(addr, ev) => encode_message(rn, addr, MessageType::Event, ev),
            YgwMessage::ParameterData(addr, pdata) => {
                encode_message(rn, addr, MessageType::ParameterData, pdata)
            }
            YgwMessage::ParameterDefinitions(addr, pdefs) => {
                encode_message(rn, addr, MessageType::ParameterDefinitions, pdefs)
            }
            YgwMessage::LinkStatus(addr, link_status) => {
                encode_message(rn, addr, MessageType::LinkStatus, link_status)
            }

            _ => unreachable!("Cannot encode message {:?}", self),
        }
    }

    /// decode a message from Bytes
    /// the data should start directly with the version (no data length)
    ///  The data is:
    /// - 1 byte - version = 0
    /// - 1 byte - message type
    /// - 4 bytes - node id = FFFFFFFF if the node is not specified
    /// - 4 bytes - link id = 0 if it is for the node itself (and not a sub-link)
    /// - n bytes - sub_data
    ///
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
        let addr = Addr { node_id, link_id };

        match msg_type {
            x if x == MessageType::Tc as i32 => match protobuf::ygw::PreparedCommand::decode(buf) {
                Ok(prep_cmd) => Ok(YgwMessage::TcPacket(addr, prep_cmd)),
                Err(e) => Err(YgwError::DecodeError(e.to_string())),
            },
            x if x == MessageType::ParameterUpdates as i32 => {
                match protobuf::ygw::ParameterUpdates::decode(buf) {
                    Ok(param_data) => Ok(YgwMessage::ParameterUpdates(addr, param_data)),
                    Err(e) => Err(YgwError::DecodeError(e.to_string())),
                }
            }
            x if x == MessageType::LinkCommand as i32 => {
                match protobuf::ygw::LinkCommand::decode(buf) {
                    Ok(link_cmd) => Ok(YgwMessage::LinkCommand(addr, link_cmd)),
                    Err(e) => Err(YgwError::DecodeError(e.to_string())),
                }
            }

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
    rn: u64,
    addr: &Addr,
    msg_type: MessageType,
    msg: &T,
) -> EncodedMessage {
    let mut emb = EncodedMessageBuilder::new(msg.encoded_len(), rn, msg_type, *addr);
    msg.encode_raw(&mut emb.0);
    emb.build()
}

pub(crate) fn encode_node_info(node_list: &protobuf::ygw::NodeList) -> EncodedMessage {
    //message length without the length itself
    let len = 10 + node_list.encoded_len();
    let mut buf = BytesMut::with_capacity(len);

    buf.put_u32(len as u32);
    buf.put_u64(0); //rn
    buf.put_u8(VERSION);
    buf.put_u8(MessageType::NodeInfo as u8);
    node_list.encode_raw(&mut buf);

    EncodedMessage(buf.freeze())
}

