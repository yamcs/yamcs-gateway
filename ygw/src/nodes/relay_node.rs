//! Accepts TCP connections from external clients and pushes data to Yamcs
//!
//! This allows connecting non-Rust (e.g. Python) clients
//!
//! The TCP protocol is similar with the one used for exchanging data with Yamcs but simplified.
//!
//! Each message exchanged on the TCP socket consists of:
//!
//! 1. `length`: uint32 - the length of the message without the length itself.
//! 2. `version`: uint8 - fixed to 0
//! 2. `msg_type`: uint8 - one of the message types below.
//! 3. `node_id`: uint32 - ignored; the node ID is fixed for one relay node, this field may be used in the future
//! 4. `link_id`: uint32 - the link ID of the sender.
//!
//! The node id and the number and name of the links have to be configured beforehand

use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender},
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use crate::Link;
use crate::{
    hex8,
    msg::{Addr, YgwMessage},
    LinkStatus, Result, YgwError, YgwLinkNodeProperties, YgwNode,
};

const MAX_FRAME_LENGTH: usize = 16 * 1024 * 1024;
pub struct RelayNode {
    props: YgwLinkNodeProperties,
    addr: SocketAddr,
    links: Vec<Link>,
}

#[async_trait]
impl YgwNode for RelayNode {
    fn properties(&self) -> &YgwLinkNodeProperties {
        &self.props
    }

    fn sub_links(&self) -> &[Link] {
        &self.links
    }

    async fn run(
        mut self: Box<Self>,
        node_id: u32,
        msg_tx: Sender<YgwMessage>,
        mut msg_rx: Receiver<YgwMessage>,
    ) -> Result<()> {
        let addr = Addr::new(node_id, 0);
        let link_status = LinkStatus::new(addr);
        link_status.send(&msg_tx).await?;

        let listener = TcpListener::bind(self.addr).await?;
        log::info!("RelayNode listening on {}", self.addr);

        //let link_status = Arc::new(Mutex::new(link_status));
        let (main_tx, mut main_rx) = mpsc::channel::<ClientCommand>(100);
        let mut client_map = HashMap::new();

        loop {
            tokio::select! {
                // Accept a new TCP client connection
                Ok((stream, client_addr)) = listener.accept() => {
                    log::info!("New client from {}", client_addr);
                    let msg_tx = msg_tx.clone();
                    let main_tx = main_tx.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, msg_tx, node_id, main_tx).await {
                            log::warn!("Client handling error: {}", e);
                        }
                    });
                }

                // A spawned client sends us its link_id and a channel to send messages back
                Some(cmd) = main_rx.recv() => {
                    match cmd {
                        ClientCommand::Register { link_id, sender } => {
                            client_map.insert(link_id, sender);
                            log::info!("Registered link_id {}", link_id);
                        }
                        ClientCommand::Unregister { link_id } => {
                            client_map.remove(&link_id);
                            log::info!("Unregistered link_id {}", link_id);
                        }
                    }
                }

                // Main message input from Yamcs — route to client by link_id
                msg = msg_rx.recv() => {
                    if let Some(msg) = msg {
                        let link_id = msg.link_id();

                        if let Some(client_tx) = client_map.get(&link_id) {
                            if let Err(e) = client_tx.send(msg).await {
                                log::warn!("Failed to send message to link_id {}: {}", link_id, e);
                            }
                        } else {
                            log::warn!("No client found for link_id {}", link_id);
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        log::debug!("RelayNode exiting");

        Ok(())
    }
}

async fn handle_client(
    stream: TcpStream,
    tx: Sender<YgwMessage>,
    node_id: u32,
    main_tx: mpsc::Sender<ClientCommand>,
) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    let (reader, mut writer) = stream.into_split();
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(MAX_FRAME_LENGTH);
    let mut framed_reader = FramedRead::new(reader, codec);

    let mut link_id: Option<u32> = None;
    let mut client_rx: Option<Receiver<YgwMessage>> = None;

    loop {
        tokio::select! {
                result = framed_reader.next() => {
                    match result {
                        Some(Ok(buf)) => {
                            let mut buf = buf.freeze();
                            match YgwMessage::decode(&mut buf) {
                                Ok(mut msg) => {
                                    msg.set_node_id(node_id);

                                    if link_id.is_none() {
                                        let (client_tx, rx) = tokio::sync::mpsc::channel(100);
                                        if let Err(_) = main_tx.send(ClientCommand::Register {
                                            link_id: msg.link_id(),
                                            sender: client_tx,
                                        }).await { //main task has closed the channel
                                            break;
                                        }
                                        client_rx = Some(rx);
                                        link_id.replace(msg.link_id());
                                    }

                                    if let Err(_) = tx.send(msg).await {
                                        break;
                                    }
                                }
                                Err(err) => log::warn!("Cannot decode data {}: {:?}", hex8(&buf), err),
                            }
                        }
                        Some(Err(e)) => {
                            //this can happen if the length of the data (first 4 bytes) is longer than the max
                            //also if the socket closes in the middle of a message
                            log::warn!("Error reading from {}: {:?}", peer_addr, e);
                            return Err(YgwError::IOError(format!("Error reading from {peer_addr}"), e));
                        }
                        None => {
                            log::info!("Client connection {} closed", peer_addr);
                            break;
                        }
                    }
                }
                Some(msg) = async {
                    match &mut client_rx {
                        Some(rx) => rx.recv().await,
                        None => None
                    }
                }, if client_rx.is_some() => {
                    let buf = msg.encode(0);
                    if let Err(e) = writer.write_all(&buf).await {
                        log::warn!("Error sending to client {}: {:?}", peer_addr, e);
                        break;
                    }
                }
        }
    }

    if let Some(link_id) = link_id {
        let _ = main_tx.send(ClientCommand::Unregister { link_id }).await;
    }

    Ok(())
}

enum ClientCommand {
    Register {
        link_id: u32,
        sender: Sender<YgwMessage>,
    },
    Unregister {
        link_id: u32,
    },
}

pub struct RelayNodeBuilder {
    name: String,
    addr: SocketAddr,
    links: Vec<Link>,
}

impl RelayNodeBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            addr: ([127, 0, 0, 1], 7899).into(),
            links: Vec::new(),
        }
    }
    pub fn set_addr(mut self, addr: SocketAddr) -> Self {
        self.addr = addr;
        self
    }

    pub fn add_link(mut self, id: u32, props: YgwLinkNodeProperties) -> Self {
        self.links.push(Link { id, props });
        self
    }

    pub fn build(self) -> RelayNode {
        RelayNode {
            props: YgwLinkNodeProperties::new(self.name, "relays data from external clients"),
            addr: self.addr,
            links: self.links,
        }
    }
}
