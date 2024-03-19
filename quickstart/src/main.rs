use std::net::SocketAddr;

use ygw::{
    tc_udp::TcUdpNode, tm_udp::TmUdpNode, ygw_server::ServerBuilder, Result
};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let node1 =
        TcUdpNode::new("TC_NODE1", "quickstart test", ([127, 0, 0, 1], 10025).into()).await?;
    
    let node2 =
        TmUdpNode::new("TM_NODE2", "quickstart test", ([127, 0, 0, 1], 10015).into(), 0).await?;

    let server = ServerBuilder::new()
    .add_node(Box::new(node1))
    .add_node(Box::new(node2))
    .build();

    let handle = server.start().await?;

    if let Err(err) = handle.jh.await {
        println!("server terminated with error {:?}", err);
    }

    Ok(())
}
