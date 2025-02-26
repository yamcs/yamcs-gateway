use crate::{
    hex8,
    msg::{Addr, TmFrame, YgwMessage},
    protobuf, LinkStatus, Result, YgwError, YgwLinkNodeProperties, YgwNode,
};
use async_trait::async_trait;
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use std::str;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_serial::SerialStream;
use tokio_util::codec::{Decoder, Encoder};

pub use tokio_serial::new as new_serial_port_builder;
pub use tokio_serial::SerialPortBuilder;

pub struct SyncFrameCodec;

pub struct TmTcSerialNode {
    props: YgwLinkNodeProperties,
    serial_stream: SerialStream,
    frame_delimiting: FrameDelimiting,
}

pub enum FrameDelimiting {
    FixedFrameSyncMarker(usize, Vec<u8>),
}

#[async_trait]
impl YgwNode for TmTcSerialNode {
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
        let addr = Addr::new(node_id, 0);

        let codec = match self.frame_delimiting {
            FrameDelimiting::FixedFrameSyncMarker(frame_length, sync_marker) => FrameCodec {
                sync_marker: sync_marker.clone(),
                frame_length,
            },
        };

        let mut framed = codec.framed(self.serial_stream);
        let mut link_status = LinkStatus::new(addr);

        loop {
            tokio::select! {
            msg = rx.recv() => {
                    match msg {
                        Some(msg) => {
                        if let YgwMessage::TcFrame(_addr, frame) = msg {
                            log::debug!("Sendig TC frame of length {} to serial port: {}", frame.binary.len(), hex8(&frame.binary));
                            if let Err(e) = framed.send(frame.binary).await {
                                return Err(YgwError::Other(Box::new(e)));
                            }
                        } else {
                            log::warn!("Unexpected message received {:?}", msg);
                        }
                     },
                     None => break
                    }
                }
                result = framed.next() => {
                    match result {
                        Some(Ok(data)) => {
                            log::trace!("Got TM frame of length {}: {}", data.len(), hex8(&data));
                            link_status.data_in(1, data.len() as u64);
                            let frame = TmFrame{ data, ert: protobuf::now() };
                            if tx.send(YgwMessage::TmFrame(addr, frame)).await.is_err() {
                                break;
                            }
                        }
                        Some(Err(e)) => return Err(YgwError::Other(Box::new(e))),
                        None => break,
                    }
                }
            }
        }

        log::debug!("TmTcSerialNode exiting");
        Ok(())
    }
}

impl TmTcSerialNode {
    pub fn new(
        name: &str,
        description: &str,
        serial_port_builder: SerialPortBuilder,
        frame_delimiting: FrameDelimiting,
    ) -> Result<Self> {
        let serial_stream = SerialStream::open(&serial_port_builder).map_err(|e| {
            YgwError::Generic(format!(
                "Failed to open serial port with {:?}: {}",
                serial_port_builder, e
            ))
        })?;
        let props = YgwLinkNodeProperties::new(name, description)
            .tm_frame(true)
            .tc_frame(true);

        Ok(Self {
            serial_stream,
            frame_delimiting,
            props,
        })
    }
}

struct FrameCodec {
    //sync marker used for the incoming frames
    sync_marker: Vec<u8>,
    // frame length (without the sync marker)
    frame_length: usize,
}

impl Decoder for FrameCodec {
    type Item = Vec<u8>;
    type Error = YgwError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Vec<u8>>> {
        let len = self.frame_length + self.sync_marker.len();

        while buf.len() >= len {
            if let Some(pos) = buf.windows(4).position(|w| w == self.sync_marker) {
                if buf.len() >= pos + len {
                    let frame = buf
                        .split_to(pos + len)
                        .split_off(self.sync_marker.len())
                        .to_vec();
                    println!("got frame {:?}", frame);
                    return Ok(Some(frame));
                }
            } else {
                buf.clear(); // No sync marker, discard buffer
                return Ok(None);
            }
        }

        Ok(None)
    }
}

impl Encoder<Vec<u8>> for FrameCodec {
    type Error = YgwError;

    fn encode(&mut self, _item: Vec<u8>, _dst: &mut BytesMut) -> Result<()> {
        Ok(())
    }
}
