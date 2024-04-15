use std::net::SocketAddr;
pub mod mynode;

use mynode::MyNode;
use ygw::{
    protobuf::ygw::ParameterDefinition, tc_udp::TcUdpNode, tm_udp::TmUdpNode, ygw_server::ServerBuilder, Result
};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let node1 =
        TcUdpNode::new("TC_NODE1", "quickstart test TC UDP", ([127, 0, 0, 1], 10025).into()).await?;
    
    let node2 =
        TmUdpNode::new("TM_NODE2", "quickstart test TM UDP", ([127, 0, 0, 1], 10015).into(), 0).await?;

    let node3 =
        MyNode::new("TM_NODE2", "quickstart test TM UDP").await?;


    let server = ServerBuilder::new()
    .add_node(Box::new(node1))
    .add_node(Box::new(node2))
    .add_node(Box::new(node3))
    .build();

    let handle = server.start().await?;

    if let Err(err) = handle.jh.await {
        println!("server terminated with error {:?}", err);
    }

    let pd = ParameterDefinition{ ..Default::default()};
    Ok(())
}

