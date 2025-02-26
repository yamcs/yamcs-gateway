use tokio::{sync::mpsc::Sender, time};

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;
use ygw::{
    msg::{Addr, YgwMessage},
    protobuf::{
        now,
        ygw::{ParameterDefinition, ParameterUpdates},
    },
    LinkStatus, Result, YgwError, YgwLinkNodeProperties, YgwNode,
};
use ygw_macros::parameter_group;

pub struct MyNode {
    props: YgwLinkNodeProperties,
}

#[allow(dead_code)]
pub struct BasicType {
    abcd: u32,
    xyz: f32,
}

#[parameter_group]
pub struct MyParameters {
    //by default the relative name is the name of the field and it is writable
    a: u32,

    #[mdb(
        relative_name = "path/to/b",
        description = "some description",
        writable = false,
        unit = "KB/sec"
    )]
    b: f32,
}

#[async_trait]
impl YgwNode for MyNode {
    fn properties(&self) -> &YgwLinkNodeProperties {
        &self.props
    }

    async fn run(
        mut self: Box<Self>,
        node_id: u32,
        tx: Sender<YgwMessage>,
        mut rx: Receiver<YgwMessage>,
    ) -> Result<()> {
        let addr = Addr::new(node_id, 0);
        let mut link_status = LinkStatus::new(addr);

        //send an initial link status indicating that the link is up
        link_status.send(&tx).await?;

        //
        let mut my_params = MyParameters {
            a: 10,
            b: -1.0,
            _meta: MyParametersMeta::new(addr, now()),
        };

        my_params.send_definitions(&tx).await?;

        my_params.send_values(&tx).await?;
        let mut interval = tokio::time::interval(time::Duration::from_secs(1));
        let t0 = tokio::time::Instant::now();
        loop {
            tokio::select! {
                    msg = rx.recv() => {
                        if let Some(msg) = msg {
                        log::info!("Got message {:?}", msg);
                        link_status.data_in(1, 0);

                        match msg {
                            YgwMessage::ParameterUpdates(_id, pdata) => {
                               if let Err(e) = update_params(&mut my_params, pdata) {
                                    log::warn!("Error updating parameters: {}", e);
                               }
                            },
                            _ => log::warn!("Got unexpected message {:?}", msg),
                        };

                        my_params.send_modified_values(&tx).await?;
                        link_status.send(&tx).await?;
                    } else {
                        break;
                    }
                },
                t = interval.tick() => {
                    my_params.set_b(f32::sin(t.duration_since(t0).as_secs_f32()/10.0), now());
                    my_params.send_modified_values(&tx).await?;
                }
            }
        }

        Ok(())
    }
}

impl MyNode {
    pub async fn new(name: &str, description: &str) -> Result<Self> {
        Ok(Self {
            props: YgwLinkNodeProperties::new(name, description),
        })
    }
}

fn update_params(my_params: &mut MyParameters, pdata: ParameterUpdates) -> Result<()> {
    for pv in pdata.parameters {
        if let Some(v) = pv.eng_value {
            if pv.id == my_params.id_a() {
                let gentime = pv.generation_time.unwrap_or(now());
                my_params.set_a(v.try_into()?, gentime);
            }
        } else {
            log::warn!(
                "Parameter Value without engineering value, ignored: {:?}",
                pv
            );
        }
    }

    Ok(())
}
