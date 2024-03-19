pub mod msg;
pub mod ygw_server;

pub mod tc_udp;
pub mod tm_udp;

pub mod utc_converter;
use msg::YgwMessage;
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};
use async_trait::async_trait;
use yamcs::protobuf;

pub mod yamcs {
    use core::f32;

    use self::protobuf::ygw::{value, Value};

    pub mod protobuf {
        include!(concat!(env!("OUT_DIR"), "/yamcs.protobuf.rs"));
        pub mod pvalue {
            include!(concat!(env!("OUT_DIR"), "/yamcs.protobuf.pvalue.rs"));
        }
        pub mod events {
            include!(concat!(env!("OUT_DIR"), "/yamcs.protobuf.events.rs"));
        }
        pub mod commanding {
            include!(concat!(env!("OUT_DIR"), "/yamcs.protobuf.commanding.rs"));
        }
        pub mod ygw {
            include!(concat!(env!("OUT_DIR"), "/yamcs.protobuf.ygw.rs"));
        }
    }

    pub fn v_f32(x: f32) -> Value {
        Value {
            v: Some(value::V::FloatValue(x)),
        }
    }
    pub fn v_f64(x: f64) -> Value {
        Value {
            v: Some(value::V::DoubleValue(x)),
        }
    }
}

/// A YGW node represents a connection to an end device.
/// The node appears as a link in Yamcs and can have sub-links.
///
/// The sub-link allow to separate data coming from a node into different streams in Yamcs. 
/// Different TM pre-processor/ CMD post-processor can be set for each sub-link.
#[async_trait]
pub trait YgwNode:Send {
    /// the properties of the node - will be communicated to Yamcs
    fn properties(&self) -> &YgwLinkNodeProperties;
    /// the list of sub links - will also be communicated to Yamcs
    fn sub_links(&self) -> &[Link];

    /// method called by the ygw server to run the node
    /// tx and rx are used to communicate between the node and the server
    /// the node_id is the id allocated to this node, it has to be used for all the messages sent to the server
    async fn run(&mut self, node_id: u32, tx: Sender<YgwMessage>, mut rx: Receiver<YgwMessage>);
}

/// properties for a link or node
#[derive(Clone, Debug)]
pub struct YgwLinkNodeProperties {
    /// the name of the node has to be unique for a server    
    name: String,
    /// a description for the node. May be shown in the Yamcs web interface
    description: String,

    /// if this is set to true, Yamcs will setup a TM pre-processor for this link/node
    tm: bool,
    /// if this is set to true, Yamcs will setup a TC post-processor for this link/node
    tc: bool,
}

#[derive(Clone, Debug)]
pub struct Link {
    id: u32,
    props: YgwLinkNodeProperties,
}
impl Link {
    fn to_proto(&self) -> protobuf::ygw::Link {
        protobuf::ygw::Link {
            id: self.id,
            name: self.props.name.clone(),
            description: Some(self.props.description.clone()),
            tm: if self.props.tm {Some(true)} else {None},
            tc: if self.props.tc {Some(true)} else {None},
        }
    }
}

pub type Result<T> = std::result::Result<T, YgwError>;

#[derive(Error, Debug)]
pub enum YgwError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("decoding error: {0}")]
    DecodeError(String),

    #[error("target has unexpectedly closed its channel: {0}")]
    TargetChannelClosed(u32),
}

pub fn hex8(data: &[u8]) -> String {
    let hex_strings: Vec<String> = data.iter().map(|x| format!("{:02X}", x)).collect();
    hex_strings.join(" ")
}

#[cfg(test)]
mod tests {
    use crate::yamcs::{
        protobuf::ygw::{ParameterValue, Value},
        *,
    };


    
}
