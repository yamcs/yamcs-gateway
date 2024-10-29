use crate::{
    msg::{Addr, YgwMessage},
    protobuf::{
        self,
        ygw::{ParameterData, ParameterDefinition, ParameterDefinitionList, ParameterValue, Value},
    },
    LinkStatus, Result, YgwError, YgwLinkNodeProperties, YgwNode,
};
/// `PingNode` is a Yamcs gateway node that pings multiple IP targets regularly and reports packet loss
/// statistics to Yamcs.
///
use async_trait::async_trait;
use rand::random;
use std::{net::IpAddr, time::Duration};
use surge_ping::{PingIdentifier, PingSequence};
use tokio::{
    net::lookup_host,
    sync::mpsc::{Receiver, Sender},
    time,
};

pub struct PingNode {
    props: YgwLinkNodeProperties,
    targets: Vec<PingTarget>,
    timeout_value: f32,
}

struct PingTarget {
    ip_addr: IpAddr,
    name: String,
    para_id: u32,
}

const PING_LEN: usize = 56;

#[async_trait]
impl YgwNode for PingNode {
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
        link_status.send(&tx).await?;

        let defs: Vec<ParameterDefinition> = self
            .targets
            .iter()
            .map(|t| ParameterDefinition {
                relative_name: format!("PING/{}", t.name),
                description: Some(format!("PING statistics for ip {}", t.ip_addr)),
                unit: Some("ms".into()),
                ptype: "float".into(),
                writable: Some(false),
                id: t.para_id,
            })
            .collect();
        let para_def =
            YgwMessage::ParameterDefinitions(addr, ParameterDefinitionList { definitions: defs });
        tx.send(para_def)
            .await
            .map_err(|_| YgwError::ServerShutdown)?;

        let client_v4 = surge_ping::Client::new(&surge_ping::Config::default())?;
        let client_v6 = surge_ping::Client::new(
            &surge_ping::Config::builder()
                .kind(surge_ping::ICMP::V6)
                .build(),
        )?;

        let mut pingers = Vec::with_capacity(self.targets.len());
        for tp in &self.targets {
            let client = match tp.ip_addr {
                IpAddr::V4(_) => client_v4.clone(),
                IpAddr::V6(_) => client_v6.clone(),
            };

            let mut pinger = client.pinger(tp.ip_addr, PingIdentifier(random())).await;
            pinger.timeout(Duration::from_secs(5));

            pingers.push((pinger, tp));
        }

        let mut seq = 0u32;
        let mut interval = time::interval(Duration::from_secs(1));

        loop {
            interval.tick().await;
            let now = protobuf::now();

            // Step 2: Ping all `pinger` instances and collect `ParameterValue`s
            let mut ping_futures = Vec::new();
            for (pinger, tp) in &mut pingers {
                ping_futures.push(ping(pinger, tp, seq, self.timeout_value));
            }

            // Wait for all pings to complete and collect their results
            let param_values: Vec<ParameterValue> = futures::future::join_all(ping_futures).await;

            let count_ok = param_values
                .iter()
                .filter(
                    |pv| match pv.eng_value.as_ref().unwrap().v.as_ref().unwrap() {
                        protobuf::ygw::value::V::FloatValue(val) => !val.is_infinite(),
                        _ => false,
                    },
                )
                .count();

            // Create a `ParameterData` containing all `ParameterValue`s
            let param_data = ParameterData {
                parameters: param_values,
                group: "ping".into(),
                seq_num: seq,
                generation_time: Some(now.clone()),
                acquisition_time: Some(now),
            };

            // Send the `ParameterData` to Yamcs
            tx.send(YgwMessage::ParameterData(addr.clone(), param_data))
                .await
                .map_err(|_| YgwError::ServerShutdown)?;

            link_status.data_out(pingers.len() as u64, (pingers.len() * PING_LEN) as u64);
            link_status.data_in(count_ok as u64, (count_ok * PING_LEN) as u64);

            link_status.send(&tx).await?;
            seq += 1;
        }
    }
}

async fn ping(pinger: &mut surge_ping::Pinger, target: &PingTarget, seq: u32, timeout_value: f32) -> ParameterValue {
    let payload = [0; PING_LEN];
    match pinger.ping(PingSequence(seq as u16), &payload).await {
        Ok((_, dur)) => get_param_value(target, dur.as_secs_f32()),
        Err(_) => get_param_value(target, timeout_value),
    }
}

fn get_param_value(target: &PingTarget, dur_secs: f32) -> ParameterValue {
    let millis = dur_secs * 1000.0;
    ParameterValue {
        id: target.para_id,
        raw_value: None,
        eng_value: Some(millis.into()),
        acquisition_time: None,
        generation_time: None,
        expire_millis: None,
    }
}

pub struct PingNodeBuilder {
    name: String,
    targets: Vec<PingTarget>,
    timeout_value: f32
}

impl PingNodeBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            targets: Vec::new(),
            timeout_value: f32::INFINITY,
        }
    }


    pub async fn add_target(mut self, name: &str, host: &str) -> Result<Self> {
        match lookup_host((host, 0)).await {
            Ok(mut lookup) => {
                if let Some(socket_addr) = lookup.next() {
                    self.targets.push(PingTarget {
                        ip_addr: socket_addr.ip(),
                        name: name.to_string(),
                        para_id: self.targets.len() as u32,
                    });
                    Ok(self)
                } else {
                    Err(YgwError::Unresolvable(host.to_string(), 0))
                }
            }
            Err(_) => Err(YgwError::Unresolvable(host.to_string(), 0)),
        }
    }

    /// Sets the value of the parameter published by the pinger in case of timeout
    /// By default it is set to f32::infinity
    pub fn with_timeout_value(&mut self, timeout_value: f32) {
        self.timeout_value = timeout_value;
    }

    pub fn build(self) -> PingNode {
        PingNode {
            props: YgwLinkNodeProperties {
                name: self.name,
                description: "pings regularly some hosts and generates packet loss stats".into(),
                tm: false,
                tc: false,
            },
            targets: self.targets,
            timeout_value: self.timeout_value
        }
    }
}
