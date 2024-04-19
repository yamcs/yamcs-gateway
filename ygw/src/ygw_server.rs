//! Handles connections from multiple Yamcs clients (or other clients implementing the protocol)
//!
//! Based on tokio tasks:
//!  - one task accepts TCP connections and spawns a read and writer task for each connection
//!  - one encoder task receives messages from Targets, encodes them to binary and sends it to all Yamcs writers
//!  - one decoder task receives data from the Yamcs readers, decodes it to messages and sends the messages to the targets
//!
//! The TCP protocol is formed by the length delimited messages:
//!  - 4 bytes length = n (max 8MB)
//!  - n bytes data
//!
//! The data is:
//! - 1   byte version = 0
//! - 1   byte message type
//! - 4   bytes target id = FFFFFFFF if the target is not specified
//! - n-6 bytes sub_data
//!
//! For TM packets sub_data is:
//! - 12   bytes acquisition time
//! - n-18 bytes packet data
//!
//! For TC packets sub_data is:
//! - PreparedCommand protobuf encoded
//!
//!
use std::{collections::HashMap, net::SocketAddr};

use bytes::Bytes;

use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener,
    },
    select,
    sync::mpsc::{channel, error::SendError, Receiver, Sender},
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use crate::{
    hex8,
    msg::{self, Addr, YgwMessage},
    protobuf::{self, ygw::MessageType},
    Link, Result, YgwError, YgwLinkNodeProperties, YgwNode,
};

pub enum CtrlMessage {
    NewYamcsConnection(YamcsConnection),
    YamcsConnectionClosed(SocketAddr),
}

pub struct Server {
    nodes: Vec<Box<dyn YgwNode>>,
    addr: SocketAddr,
}

pub struct ServerBuilder {
    nodes: Vec<Box<dyn YgwNode>>,
    addr: SocketAddr,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerBuilder {
    /// creates a new server builder
    /// the listening address is set to 127.0.0.1:7897
    pub fn new() -> Self {
        Self {
            addr: ([127, 0, 0, 1], 7897).into(),
            nodes: Vec::new(),
        }
    }
    pub fn set_addr(mut self, addr: SocketAddr) -> Self {
        self.addr = addr;
        self
    }
    pub fn add_node(mut self, target: Box<dyn YgwNode>) -> Self {
        self.nodes.push(target);

        self
    }

    pub fn build(self) -> Server {
        Server {
            nodes: self.nodes,
            addr: self.addr,
        }
    }
}

pub struct ServerHandle {
    pub jh: JoinHandle<Result<()>>,
    pub addr: SocketAddr,
}

impl Server {
    /// Runs the YgwServer.
    ///
    /// Starts the following Tokio tasks:
    ///  * One acceptor task for accepting Yamcs connections.
    ///  * One encoder task for encoding messages from nodes and sending them to Yamcs
    ///  * One decoder task for decoding messages from Yamcs and sending them to nodes
    ///     
    ///
    pub async fn start(mut self) -> Result<ServerHandle> {
        let mut node_tx_map = HashMap::new();
        let mut node_data = HashMap::new();
        let mut node_id = 0;
        let mut handles = Vec::new();

        let (encoder_tx, encoder_rx) = tokio::sync::mpsc::channel(100);

        let socket = TcpListener::bind(self.addr).await?;
        let addr = socket.local_addr()?;

        for mut node in self.nodes.drain(..) {
            let props = node.properties();
            let (tx, rx) = tokio::sync::mpsc::channel(100);
            node_data.insert(node_id, NodeData::new(node_id, props, node.sub_links()));

            let encoder_tx = encoder_tx.clone();
            log::info!("Starting node {} with id {}", props.name, node_id);
            let jh = tokio::spawn(async move { node.run(node_id, encoder_tx, rx).await });
            handles.push(jh);

            node_tx_map.insert(node_id, tx);
            node_id += 1;
        }
        let (ctrl_tx, ctrl_rx) = channel(1);

        let (decoder_tx, decoder_rx) = tokio::sync::mpsc::channel(100);

        let accepter_jh =
            tokio::spawn(async move { accepter_task(ctrl_tx, socket, decoder_tx).await });

        tokio::spawn(async move { encoder_task(ctrl_rx, encoder_rx, node_data).await });

        tokio::spawn(async move { decoder_task(decoder_rx, node_tx_map).await });

        //TODO check result from join
        // futures::future::join_all(handles);

        Ok(ServerHandle {
            jh: accepter_jh,
            addr,
        })
    }
}

#[derive(Debug)]
pub struct YamcsConnection {
    addr: SocketAddr,
    writer_jh: JoinHandle<Result<()>>,
    reader_jh: JoinHandle<Result<()>>,
    chan_tx: Sender<Bytes>,
    //if this option is true, the encoder will drop the Yamcs connection if it cannot keep up with the data
    drop_if_full: bool,
}

impl PartialEq for YamcsConnection {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

// used in the encoder stores data about nodes
struct NodeData {
    node_id: u32,
    props: YgwLinkNodeProperties,
    links: Vec<Link>,
    /// collects the parameter definitions as sent by the node
    /// sent in bulk to Yamcs upon connection
    para_defs: protobuf::ygw::ParameterDefinitionList,
    /// collects the parameter values per group as sent by the node
    /// sent in bulk to Yamcs upon connection
    para_values: HashMap<String, protobuf::ygw::ParameterData>,

    /// collects the link status per link
    /// sent in bulk to Yamcs upon connection
    link_status: HashMap<u32, protobuf::ygw::LinkStatus>,
}
impl NodeData {
    fn new(node_id: u32, props: &YgwLinkNodeProperties, links: &[Link]) -> Self {
        Self {
            node_id,
            props: props.clone(),
            links: links.to_vec(),
            para_defs: protobuf::ygw::ParameterDefinitionList {
                definitions: Vec::new(),
            },
            para_values: HashMap::new(),
            link_status: HashMap::new(),
        }
    }

    fn node_to_proto(&self) -> protobuf::ygw::Node {
        protobuf::ygw::Node {
            id: self.node_id,
            name: self.props.name.clone(),
            description: Some(self.props.description.clone()),
            tm: if self.props.tm { Some(true) } else { None },
            tc: if self.props.tc { Some(true) } else { None },
            links: self.links.iter().map(|l| l.to_proto()).collect(),
        }
    }
}

/// listens for new messages from the targets
/// encodes them to bytes and sends the bytes to all writer tasks (to be sent to Yamcs)
///
async fn encoder_task(
    mut ctrl_rx: Receiver<CtrlMessage>,
    mut encoder_rx: Receiver<YgwMessage>,
    mut nodes: HashMap<u32, NodeData>,
) -> Result<()> {
    let mut connections: Vec<YamcsConnection> = Vec::new();

    loop {
        select! {
            msg = encoder_rx.recv() => {
                match msg {
                    Some(msg) => {
                        let buf = msg.encode().freeze();
                        send_data_to_all(&mut connections, buf).await;
                        match msg {
                            YgwMessage::ParameterDefinitions(addr, pdefs) => {
                                if let Some(node) = nodes.get_mut(&addr.node_id()) {
                                    node.para_defs.definitions.extend(pdefs.definitions);
                                }
                            },
                            YgwMessage::Parameters(addr, pvals) => {
                                if let Some(node) = nodes.get_mut(&addr.node_id()) {
                                    node.para_values.insert(pvals.group.clone(), pvals);
                                }
                            },
                            YgwMessage::LinkStatus(addr, link_status) => {
                                if let Some(node) = nodes.get_mut(&addr.node_id()) {
                                    node.link_status.insert(addr.link_id(), link_status);
                                }
                            },
                            _ => {}
                        }
                    },
                    None => break
                }
            }
            msg = ctrl_rx.recv() => {
                match msg {
                    Some(CtrlMessage::NewYamcsConnection(yc)) => {
                        if let Err(_)= send_initial_data(&yc, &nodes).await {
                            log::warn!("Error sending initial data message to {}", yc.addr);
                            continue;
                        }


                        connections.push(yc);
                    },
                    Some(CtrlMessage::YamcsConnectionClosed(addr)) => connections.retain(|yc| yc.addr != addr),
                    None => break
                }
            }
        }
    }
    Ok(())
}

/// send encoded message to all connected yamcs servers
async fn send_data_to_all(connections: &mut Vec<YamcsConnection>, buf: Bytes) {
    let mut idx = 0;
    while idx < connections.len() {
        let buf1 = buf.clone();
        let yc = &connections[idx];
        if yc.drop_if_full {
            if let Err(_) = yc.chan_tx.try_send(buf1) {
                log::warn!("Channel to {} is full, dropping connection", yc.addr);
                yc.reader_jh.abort();
                yc.writer_jh.abort();
                connections.remove(idx);
                continue;
            }
        } else if let Err(_) = yc.chan_tx.send(buf1).await {
            //channel closed, the writer quit
            // (it hopefully printed an informative log message so no need to log anything extra here)
            connections.remove(idx);
            continue;
        }
        idx += 1;
    }
}

/// Called when there is a new Yamcs connection
/// Sends node information, parameter definitions and parameter values
async fn send_initial_data(
    yc: &YamcsConnection,
    nodes: &HashMap<u32, NodeData>,
) -> std::result::Result<(), SendError<Bytes>> {
    //send the node information
    let nl = protobuf::ygw::NodeList {
        nodes: nodes
            .iter()
            .map(|(_, nd)| nd.node_to_proto())
            .collect(),
    };
    let buf = msg::encode_node_info(&nl);
    yc.chan_tx.send(buf.freeze()).await?;

    //send the parameter defintions
    for nd in nodes.values() {
        let buf = msg::encode_message(
            &Addr::new(nd.node_id, 0),
            MessageType::ParameterDefinitions,
            &nd.para_defs,
        );
        yc.chan_tx.send(buf.freeze()).await?;
    }

    //send the parameter values
    for nd in nodes.values() {
        for pdata in nd.para_values.values() {
            let buf =
                msg::encode_message(&Addr::new(nd.node_id, 0), MessageType::ParameterData, pdata);
            yc.chan_tx.send(buf.freeze()).await?;
        }
    }

    // send the link status
    for nd in nodes.values() {
        for (&link_id, lstatus) in nd.link_status.iter() {
            let buf = msg::encode_message(
                &Addr::new(nd.node_id, link_id),
                MessageType::LinkStatus,
                lstatus,
            );
            yc.chan_tx.send(buf.freeze()).await?;
        }
    }
    Ok(())
}

/// receives data from the yamcs readers, converts them to messages
/// and sends the messages to the targets
async fn decoder_task(
    mut decoder_rx: Receiver<Bytes>,
    mut nodes: HashMap<u32, Sender<YgwMessage>>,
) -> Result<()> {
    loop {
        match decoder_rx.recv().await {
            Some(mut buf) => match YgwMessage::decode(&mut buf) {
                Ok(msg) => {
                    let node_id = msg.node_id();
                    match nodes.get(&node_id) {
                        Some(tx) => {
                            if let Err(_) = tx.send(msg).await {
                                log::warn!("Channel to node {} closed", node_id);
                                nodes.remove(&node_id);
                            }
                        }
                        None => {
                            log::warn!("Received message for unknown target {} ", node_id);
                        }
                    }
                }
                Err(err) => log::warn!("Cannot decode data {}: {:?}", hex8(&buf), err),
            },
            None => break,
        };
    }

    Ok(())
}

/// accepts connections from Yamcs and spawns a new reader task and a new writer task for each connection
/// a channel to the writer task is created and is handed over to the encoder_task
async fn accepter_task(
    ctrl_tx: Sender<CtrlMessage>,
    srv_sock: TcpListener,
    decoder_tx: Sender<Bytes>,
) -> Result<()> {
    loop {
        let (sock, addr) = srv_sock.accept().await?;
        log::info!("New Yamcs connection from {}", addr);

        let (read_sock, write_sock) = sock.into_split();
        let (chan_tx, chan_rx) = channel(100);

        let dtx = decoder_tx.clone();
        let ctx = ctrl_tx.clone();

        let reader_jh = tokio::spawn(async move { reader_task(ctx, addr, read_sock, dtx).await });
        let writer_jh = tokio::spawn(async move { writer_task(write_sock, chan_rx).await });

        let yc = YamcsConnection {
            addr,
            reader_jh,
            writer_jh,
            chan_tx,
            drop_if_full: false,
        };

        if let Err(_) = ctrl_tx.send(CtrlMessage::NewYamcsConnection(yc)).await {
            // channel closed
            break;
        }
    }
    Ok(())
}

/// reads data from one Yamcs server and sends it to the decoder
/// if the connection is closed, send a message to the encoder
/// to know not to send messages to that yamcs server anymore
async fn reader_task(
    ctrl_tx: Sender<CtrlMessage>,
    addr: SocketAddr,
    read_sock: OwnedReadHalf,
    decoder_tx: Sender<Bytes>,
) -> Result<()> {
    let mut stream = FramedRead::new(read_sock, LengthDelimitedCodec::new());

    loop {
        match stream.next().await {
            Some(Ok(buf)) => {
                let buf = buf.freeze();
                log::trace!("Received message {:}", hex8(&buf));
                if let Err(_) = decoder_tx.send(buf).await {
                    //channel to decoder closed
                    break;
                }
            }
            Some(Err(e)) => {
                //this can happen if the length of the data (first 4 bytes) is longer than the max
                //also if the socket closes in the middle of a message
                log::warn!("Error reading from {}: {:?}", addr, e);
                let _ = ctrl_tx.send(CtrlMessage::YamcsConnectionClosed(addr)).await;
                return Err(YgwError::IOError(e));
            }
            None => {
                log::info!("Yamcs connection {} closed", addr);
                let _ = ctrl_tx.send(CtrlMessage::YamcsConnectionClosed(addr)).await;
                break;
            }
        }
    }
    Ok(())
}

//Writes data to one Yamcs server
async fn writer_task(mut sock: OwnedWriteHalf, mut chan: Receiver<Bytes>) -> Result<()> {
    loop {
        match chan.recv().await {
            Some(buf) => {
                //println!("writing to socket in thread {:?}", std::thread::current());
                sock.write_all(&buf).await?;
            }
            None => break,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::ErrorKind,
        time::{Duration, Instant, SystemTime},
    };

    use async_trait::async_trait;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        sync::mpsc,
    };
    use tokio_util::codec::Framed;

    use crate::{
        msg::{Addr, TmPacket},
        protobuf::{ygw::CommandId, ygw::PreparedCommand},
        Link,
    };

    use super::*;

    #[tokio::test]
    async fn test_frame_too_long() {
        let (addr, _node_id, _node_tx, _node_rx) = setup_test().await;

        let mut conn = TcpStream::connect(addr).await.unwrap();
        //max frame size is 8MB
        conn.write_u32(8 * 1024 * 1024 + 1).await.unwrap();
        let r = conn.read_u32_le().await.unwrap_err();
        assert_eq!(ErrorKind::UnexpectedEof, r.kind());
    }

    #[tokio::test]
    async fn test_tm() {
        //env_logger::init();
        let (addr, node_id, node_tx, _node_rx) = setup_test().await;

        let conn = TcpStream::connect(addr).await.unwrap();
        let mut stream = Framed::new(conn, LengthDelimitedCodec::new());

        tokio::task::yield_now().await;
        node_tx
            .send(YgwMessage::TmPacket(
                Addr::new(node_id, 0),
                TmPacket {
                    data: vec![1, 2, 3, 4],
                    acq_time: SystemTime::now(),
                },
            ))
            .await
            .unwrap();

        let buf = stream.next().await.unwrap().unwrap();
        assert_eq!(26, buf.len());
        assert_eq!([1, 2, 3, 4], buf[22..26]);
    }

    #[tokio::test]
    async fn test_tc() {
        //env_logger::init();
        let (addr, node_id, _node_tx, mut node_rx) = setup_test().await;

        let mut conn = TcpStream::connect(addr).await.unwrap();
        let pc = prepared_cmd();
        let msg = YgwMessage::TcPacket(Addr::new(node_id, 0), pc.clone());
        let buf = msg.encode();

        conn.write_all(&buf).await.unwrap();

        let msg1 = node_rx.recv().await.unwrap();

        assert_eq!(msg, msg1);
    }

    // performance test sending a TM packet from the target to a TCP client
    // this is able to send about 620k msg/sec multithreaded and about 530k msg/sec single threaded on an i7
    // That is about 1600 nanoseconds/message. Out of that, at least 1000 are spent on the syscall
    // overhead for sending to the socket and probably at least half of the remaining
    // on the memory allocation/deallocation for each packet (we have two allocations: one for the packet and
    //    one for the encoded message).
    // This throughput is probably 2-4 orders of magnitude better than what we expect the system to be used for
    // #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    // #[tokio::test(flavor = "current_thread")]
    async fn _test_performance() {
        //env_logger::init();
        let (addr, node_id, node_tx, _node_rx) = setup_test().await;

        let conn = TcpStream::connect(addr).await.unwrap();
        let mut stream = Framed::new(conn, LengthDelimitedCodec::new());
        let n = 1_000_000;
        let client_handle = tokio::spawn(async move {
            let mut count = 0;
            let mut t0 = Instant::now();

            while let Some(Ok(_)) = stream.next().await {
                if count == 0 {
                    t0 = Instant::now();
                }
                count += 1;
                if count == n {
                    break;
                }
            }
            let d = t0.elapsed();
            println!(
                "Received {} messages in {:?}: speed {:.2} msg/millisec",
                count,
                d,
                (count as f64) / (d.as_millis() as f64)
            );
        });
        tokio::time::sleep(Duration::from_secs(1)).await;

        tokio::spawn(async move {
            let t0 = Instant::now();
            for _ in 0..n {
                node_tx
                    .send(YgwMessage::TmPacket(
                        Addr::new(node_id, 0),
                        TmPacket {
                            data: vec![0; 1024],
                            acq_time: SystemTime::now(),
                        },
                    ))
                    .await
                    .unwrap();
            }
            let d = t0.elapsed();
            println!(
                "Sent {} messages; speed {:.2} msg/millisec {} nanosec/message",
                n,
                (n as f64) / (d.as_millis() as f64),
                d.as_nanos() / n
            );
        })
        .await
        .unwrap();

        client_handle.await.unwrap();
    }

    async fn setup_test() -> (SocketAddr, u32, Sender<YgwMessage>, Receiver<YgwMessage>) {
        let (tx, mut rx) = mpsc::channel(1);
        let props = YgwLinkNodeProperties {
            name: "test_node".into(),
            description: "test node".into(),
            tm: true,
            tc: true,
        };

        let dn = DummyNode { props, tx };
        let addr = ([127, 0, 0, 1], 56789).into();
        let server = ServerBuilder::new()
            .set_addr(addr)
            .add_node(Box::new(dn))
            .build();
        let server_handle = server.start().await.unwrap();

        let x = rx.recv().await.unwrap();
        (server_handle.addr, x.0, x.1, x.2)
    }

    fn prepared_cmd() -> PreparedCommand {
        PreparedCommand {
            command_id: CommandId {
                generation_time: 100,
                origin: String::from("test"),
                sequence_number: 10,
                command_name: None,
            },
            assignments: Vec::new(),
            extra: HashMap::new(),
            binary: Some(vec![1, 2, 3]),
        }
    }

    /// waits for the rx,tx channels used to communicate between the node and the server and passed them out to be used in the test
    struct DummyNode {
        props: YgwLinkNodeProperties,
        tx: mpsc::Sender<(u32, Sender<YgwMessage>, Receiver<YgwMessage>)>,
    }

    #[async_trait]
    impl YgwNode for DummyNode {
        fn properties(&self) -> &YgwLinkNodeProperties {
            &self.props
        }

        fn sub_links(&self) -> &[Link] {
            &[]
        }

        async fn run(&mut self, node_id: u32, tx: Sender<YgwMessage>, rx: Receiver<YgwMessage>) -> Result<()> {
            self.tx.send((node_id, tx, rx)).await.unwrap();

            tokio::time::sleep(std::time::Duration::from_secs(200)).await;
            Ok(())
        }
    }
}
