use std::vec;

use crate::{
    hex8,
    msg::{Addr, YgwMessage},
    protobuf::{
        self, get_pv_eng,
        ygw::{
            command_ack::AckStatus, CommandAck, CommandArgument, CommandDefinition,
            CommandDefinitionList, ParameterData, ParameterDefinition, ParameterDefinitionList,
            PreparedCommand, Value,
        },
    },
    LinkStatus, Result, YgwError, YgwLinkNodeProperties, YgwNode,
};
/// `CmdNode` is a Yamcs gateway node that executes shell commands. The commmands and their arguments are registered in Yamcs.
///
use async_trait::async_trait;
use log::{debug, warn};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug)]
pub struct ShellCmd {
    pub name: String,
    pub description: Option<String>,
    pub cmd: String,
    pub args: Option<Vec<ShellCmdArg>>,
    pub out_para: Option<(u32, String)>,
}

impl ShellCmd {
    fn yamcs_definition(&self, namespace: &str, ygw_cmd_id: u32) -> CommandDefinition {
        let arguments = match self.args.as_ref() {
            None => vec![],
            Some(args) => args
                .iter()
                .filter_map(|arg| {
                    if let ShellCmdArg::YamcsArgument {
                        name,
                        argtype,
                        default_value,
                    } = arg
                    {
                        Some(CommandArgument {
                            name: name.clone(),
                            description: None,
                            unit: None,
                            argtype: argtype.clone(),
                            default_value: default_value.as_ref().map(|x| x.clone().into()),
                        })
                    } else {
                        None
                    }
                })
                .collect(),
        };

        CommandDefinition {
            relative_name: format!("{}/{}", namespace, self.name),
            description: self.description.clone(),
            arguments,
            ygw_cmd_id,
        }
    }
}

#[derive(Debug)]
pub enum ShellCmdArg {
    Simple(String),
    YamcsArgument {
        name: String,
        argtype: String,
        default_value: Option<String>,
    },
}

pub struct ShellCmdNode {
    props: YgwLinkNodeProperties,
    commands: Vec<ShellCmd>,
}

pub struct ShellCmdNodeBuilder {
    props: YgwLinkNodeProperties,
    commands: Vec<ShellCmd>,
}

impl ShellCmdNodeBuilder {
    pub fn new(name: &str) -> Self {
        ShellCmdNodeBuilder {
            props: YgwLinkNodeProperties {
                name: name.into(),
                description: "executes predefined commands".into(),
                tm: false,
                tc: false,
            },
            commands: Vec::new(),
        }
    }

    pub fn add_command(
        mut self,
        name: String,
        description: Option<String>,
        cmd: String,
        args: Option<Vec<ShellCmdArg>>,
        out_para_name: Option<String>,
    ) -> Self {
        let out_para = out_para_name.map(|x| (self.commands.len() as u32 * 3 + 4, x));
        let command = ShellCmd {
            name,
            description,
            cmd,
            args,
            out_para,
        };
        self.commands.push(command);
        self
    }

    pub fn build(self) -> ShellCmdNode {
        ShellCmdNode {
            props: self.props,
            commands: self.commands,
        }
    }
}

#[async_trait]
impl YgwNode for ShellCmdNode {
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

        self.register_cmds(addr, &tx).await?;
        self.register_params(addr, &tx).await?;
        let mut seq_num = 0;
        let mut link_status = LinkStatus::new(addr);
        link_status.send(&tx).await?;

        loop {
            match rx.recv().await {
                Some(YgwMessage::TcPacket(_, cmd)) => {
                    log::debug!("Received command {:?}", cmd);
                    let (cnt_out, cnt_in, data_out, data_in) =
                        self.execute_cmd(addr, cmd, &tx, seq_num).await?;
                    seq_num += 1;
                    link_status.data_out(cnt_out as u64, data_out as u64);
                    link_status.data_in(cnt_in as u64, data_in as u64);
                    link_status.send(&tx).await?;
                }
                Some(_) => {
                    warn!("Unexpected message received");
                }
                None => break,
            }
        }

        Ok(())
    }
}

impl ShellCmdNode {
    pub async fn new(name: &str, commands: Vec<ShellCmd>) -> Result<Self> {
        Ok(Self {
            props: YgwLinkNodeProperties {
                name: name.to_string(),
                description: "executes shell commands".into(),
                tm: false,
                tc: false,
            },
            commands,
        })
    }

    pub async fn register_cmds(&self, addr: Addr, tx: &Sender<YgwMessage>) -> Result<()> {
        let definitions = self
            .commands
            .iter()
            .enumerate()
            .map(|(i, cmd)| cmd.yamcs_definition(&self.props.name, i as u32))
            .collect();

        tx.send(YgwMessage::CommandDefinitions(
            addr,
            CommandDefinitionList { definitions },
        ))
        .await
        .map_err(|_| YgwError::ServerShutdown)
    }

    pub async fn register_params(&self, addr: Addr, tx: &Sender<YgwMessage>) -> Result<()> {
        let mut definitions = Vec::new();
        let (p_out, p_err, p_code) = get_defs(1, &self.props.name, None);

        definitions.push(p_out);
        definitions.push(p_err);
        definitions.push(p_code);

        for (para_id, para_name) in self.commands.iter().filter_map(|cmd| cmd.out_para.as_ref()) {
            let (p_out, p_err, p_code) = get_defs(*para_id, &self.props.name, Some(para_name));
            definitions.push(p_out);
            definitions.push(p_err);
            definitions.push(p_code);
        }

        tx.send(YgwMessage::ParameterDefinitions(
            addr,
            ParameterDefinitionList { definitions },
        ))
        .await
        .map_err(|_| YgwError::ServerShutdown)
    }

    pub async fn execute_cmd(
        &self,
        addr: Addr,
        pc: PreparedCommand,
        tx: &Sender<YgwMessage>,
        seq_num: u32,
    ) -> Result<(usize, usize, usize, usize)> {
        let Some(ygw_cmd_id) = pc.ygw_cmd_id else {
            log::warn!("Command has no ygw_cmd_id set {:?}", pc);
            return Ok((0, 0, 0, 0));
        };

        let Some(cmd) = self.commands.get(ygw_cmd_id as usize) else {
            log::warn!("Unknown command: {}", ygw_cmd_id);
            return Ok((0, 0, 0, 0));
        };
        let mut data_out = 0;

        let mut command = tokio::process::Command::new(&cmd.cmd);
        if let Some(args) = cmd.args.as_ref() {
            for cmdarg in args.iter() {
                match cmdarg {
                    ShellCmdArg::Simple(x) => {
                        data_out += x.len();
                        command.arg(x)
                    }
                    ShellCmdArg::YamcsArgument {
                        name,
                        argtype: _,
                        default_value: _,
                    } => {
                        let Some(a) = pc.assignments.iter().find(|&a| a.name == *name) else {
                            log::warn!("Argument {} not found", name);
                            //TODO send ack
                            return Ok((0, 0, 0, 0));
                        };
                        let Some(strv) = to_string(&a.eng_value) else {
                            log::warn!("Argument {} has no value", name);
                            //TODO send ack
                            return Ok((0, 0, 0, 0));
                        };
                        data_out += strv.len();
                        command.arg(strv)
                    }
                };
            }
        }
        debug!("Executing command {:?}", command);

        let cmd_out = command.output().await;

        debug!("command output: {:?}", cmd_out);
        let now = protobuf::now();

        let mut data_in = 0;

        let (ack, message, return_pv) = match cmd_out {
            Ok(cmd_out) => {
                let stdout = String::from_utf8_lossy(&cmd_out.stdout).into_owned();
                let stderr = String::from_utf8_lossy(&cmd_out.stderr).into_owned();
                data_in += stdout.len();
                data_in += stderr.len();
                let para_id = cmd.out_para.as_ref().map(|x| x.0).unwrap_or(1);

                let pv_out = get_pv_eng(para_id, None, stdout);
                let pv_err = get_pv_eng(para_id + 1, None, stderr);
                let pv_code = get_pv_eng(para_id + 2, None, cmd_out.status.code().unwrap());

                tx.send(YgwMessage::ParameterData(
                    addr,
                    ParameterData {
                        parameters: vec![pv_out.clone(), pv_err.clone(), pv_code],
                        group: self.props.name.clone(),
                        seq_num,
                        generation_time: Some(now),
                        acquisition_time: None,
                    },
                ))
                .await
                .map_err(|_| YgwError::ServerShutdown)?;

                if cmd_out.status.success() {
                    (AckStatus::Ok, None, Some(pv_out))
                } else {
                    (
                        AckStatus::Nok,
                        Some(String::from_utf8_lossy(&cmd_out.stderr).into_owned()),
                        None,
                    )
                }
            }
            Err(err) => (AckStatus::Nok, Some(err.to_string()), None),
        };

        let cnt_out = 1;
        let cnt_in = if return_pv.is_some() { 1 } else { 0 };

        tx.send(YgwMessage::TcAck(
            addr,
            CommandAck {
                command_id: pc.command_id.clone(),
                ack: ack as i32,
                key: "CommandComplete".into(),
                time: protobuf::now(),
                message,
                return_pv,
            },
        ))
        .await
        .map_err(|_| YgwError::ServerShutdown)?;

        return Ok((cnt_out, cnt_in, data_out, data_in));
    }
}

fn to_string(v: &Option<Value>) -> Option<String> {
    let Some(Value { v: Some(ref v) }) = v else {
        return None;
    };

    let vstr = match v {
        protobuf::ygw::value::V::FloatValue(x) => x.to_string(),
        protobuf::ygw::value::V::DoubleValue(x) => x.to_string(),
        protobuf::ygw::value::V::Sint32Value(x) => x.to_string(),
        protobuf::ygw::value::V::Uint32Value(x) => x.to_string(),
        protobuf::ygw::value::V::BinaryValue(x) => hex8(x),
        protobuf::ygw::value::V::StringValue(x) => x.clone(),
        protobuf::ygw::value::V::TimestampValue(x) => format!("{:?}", x), //TODO
        protobuf::ygw::value::V::Uint64Value(x) => x.to_string(),
        protobuf::ygw::value::V::Sint64Value(x) => x.to_string(),
        protobuf::ygw::value::V::BooleanValue(x) => x.to_string(),
        protobuf::ygw::value::V::EnumeratedValue(x) => x.string_value.clone(),
        protobuf::ygw::value::V::AggregateValue(x) => format!("{:?}", x),
        protobuf::ygw::value::V::ArrayValue(x) => format!("{:?}", x),
    };

    Some(vstr)
}

fn get_defs(
    para_id: u32,
    node_name: &str,
    name: Option<&str>,
) -> (
    ParameterDefinition,
    ParameterDefinition,
    ParameterDefinition,
) {
    let rel_name = match name {
        Some(name) => format!("{}/{}", node_name, name),
        None => format!("{}", node_name),
    };

    let p_out = ParameterDefinition {
        relative_name: format!("{}/STDOUT", rel_name),
        description: Some("standard output of the last executed command".into()),
        unit: None,
        ptype: "string".into(),
        writable: None,
        id: para_id,
    };
    let p_err = ParameterDefinition {
        relative_name: format!("{}/STDERR", rel_name),
        description: Some("standard error of the last executed command".into()),
        unit: None,
        ptype: "string".into(),
        writable: None,
        id: para_id + 1,
    };
    let p_code = ParameterDefinition {
        relative_name: format!("{}/EXITCODE", rel_name),
        description: Some("exit code of the last executed command".into()),
        unit: None,
        ptype: "sint32".into(),
        writable: None,
        id: para_id + 2,
    };

    (p_out, p_err, p_code)
}
