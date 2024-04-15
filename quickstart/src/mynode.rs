use tokio::sync::mpsc::Sender;

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use ygw::{
    msg::{Addr, YgwMessage}, protobuf::{get_pv_eng, get_value, ygw::{ParameterData, ParameterDefinition, ParameterDefinitionList, ParameterValue, Timestamp, Value}}, Link, LinkStatus, Result, YgwLinkNodeProperties, YgwNode
};
use ygw_macros::{parameter_pool};

pub struct MyNode {
    props: YgwLinkNodeProperties,
}


#[parameter_pool]
pub struct MyParameters {
    //by default the relative name is the name of the field and it is writable
    a: u32,

    #[mdb(relative_name = "path/to/b", description = "some description", writable = false, unit = "KB/sec")]
    b: f32
}

#[async_trait]
impl YgwNode for MyNode {
    fn properties(&self) -> &YgwLinkNodeProperties {
        &self.props
    }

    fn sub_links(&self) -> &[Link] {
        &[]
    }

    async fn run(&mut self, node_id: u32, tx: Sender<YgwMessage>, mut rx: Receiver<YgwMessage>) -> Result<()> {
        let addr = Addr::new(node_id, 0);
        let mut link_status = LinkStatus::new(addr);
    
        //send an initial link status indicating that the link is up
        link_status.send(&tx).await?;

        //tx.send(YgwMessage::Parameters(addr, ()));

        while let Some(msg) = rx.recv().await {
            link_status.data_in(1, 0);

            println!("got message via bla: {:?}", msg);
            match msg {
                YgwMessage::Parameters(_id, pc) => {}
                _ => log::warn!("Got unexpected message {:?}", msg),
            };

            link_status.send(&tx).await?;
        }

        Ok(())
    }
}

impl MyNode {
    pub async fn new(name: &str, description: &str) -> Result<Self> {
        Ok(Self {
            props: YgwLinkNodeProperties {
                name: name.to_string(),
                description: description.to_string(),
                tm: false,
                tc: false,
            },
        })
    }
}

