pub mod mynode;

use mynode::MyNode;
use ygw::{
    nodes::{
        pingnode::PingNodeBuilder,
        shellcmd::{ShellCmdArg, ShellCmdNodeBuilder},
        tc_udp::TcUdpNode,
        tm_udp::TmUdpNode,
        ygw_socketcan::CanNode,
    },
    protobuf::ygw::ParameterDefinition,
    ygw_server::ServerBuilder,
    Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let node1 = TcUdpNode::new(
        "TC_NODE1",
        "quickstart test TC UDP",
        ([127, 0, 0, 1], 10025).into(),
    )
    .await?;

    let node2 = TmUdpNode::new(
        "TM_NODE2",
        "quickstart test TM UDP",
        ([127, 0, 0, 1], 10015).into(),
        0,
    )
    .await?;

    let can_dev = "vcan0";
    let node3 = match CanNode::new("CAN_NODE3", &can_dev) {
        Ok(node3) => node3,
        Err(err) => {
            eprintln!(
                "Failed to open CAN socket on interface '{}' : {}",
                can_dev, err
            );
            return Err(err);
        }
    };

    let node4 = MyNode::new("MY_NODE", "quickstart test TM UDP").await?;

    let node5 = ShellCmdNodeBuilder::new("SHELL_CMD")
        .add_command(
            "ls".into(),
            Some("list directory".into()),
            "ls".into(),
            Some(vec![ShellCmdArg::YamcsArgument {
                name: "dir".into(),
                argtype: "string".into(),
                default_value: Some("/tmp".into()),
            }]),
            Some("ls_out_para".into()),
        )
        .add_command(
            "pwd".into(),
            Some("list directory".into()),
            "pwd".into(),
            None,
            None,
        )
        .build();

    let node6 = PingNodeBuilder::new("PING")
        .add_target("localhost", "localhost")
        .await?
        .add_target("fritz", "192.168.18.1")
        .await?
        .add_target("toto", "10.10.100.100")
        .await?
        .build();

    let server = ServerBuilder::new()
        .add_node(Box::new(node1))
        .add_node(Box::new(node2))
        .add_node(Box::new(node3))
        .add_node(Box::new(node4))
        .add_node(Box::new(node5))
        .add_node(Box::new(node6))
        .build();

    let handle = server.start().await?;

    if let Err(err) = handle.jh.await {
        println!("server terminated with error {:?}", err);
    }

    let _pd = ParameterDefinition {
        ..Default::default()
    };
    Ok(())
}
