use crate::BusMessage;

use super::{Result, RtAddr};
use std::time::Duration;

/// all messages configured on the MIL-1553 devices, are identified by a 32 bit id
/// the implementors of the Mil1553BcDev trait will map that to whatever is required for the specific device
pub type MsgId = u32;

pub struct MsgIdGenerator {
    current: MsgId,
}

impl MsgIdGenerator {
    pub fn new() -> Self {
        Self { current: 0 }
    }

    pub fn next(&mut self) -> MsgId {
        let id = self.current;
        self.current = self.current.wrapping_add(1);
        id
    }
}

pub struct Schedule {
    pub major_frame: Vec<MinorFrame>,
    pub messages: Vec<ScheduledMessage>,
}

/// a message that is part of the schedule
#[derive(Debug)]
pub struct ScheduledMessage {
    /// id given by the user
    pub msg_id: MsgId,
    pub data: MessageData,
    /// If true, this message will be part of the event log
    /// That means it will generate a BusMessage each time it is sent or received on the MIL-1553 bus.
    /// The log is generated even if there is an error sending/receiving
    pub log: bool,
}
pub struct MinorFrame {
    pub duration: Duration,
    pub entries: Vec<FrameEntry>,
}

impl MinorFrame {
    //make a new frame with the given duration and messages
    pub fn new(duration: Duration, entries: Vec<MsgId>) -> Self {
        let entries: Vec<FrameEntry> = entries
            .into_iter()
            .map(|msg_id| FrameEntry {
                start_time: None,
                msg_id,
            })
            .collect();
        Self { entries, duration }
    }
}
pub struct FrameEntry {
    /// start time is relative to the beginning of the frame
    /// if present, it is interpreted as "do not start before this time"
    pub start_time: Option<Duration>,
    pub msg_id: MsgId,
}

#[derive(Debug)]
pub enum MessageData {
    /// rt addr, sub_addr, data
    BcToRtMessage(RtAddr, RtAddr, Vec<u16>),
    /// rt addr, sub_addr, data length
    RtToBcMessage(RtAddr, RtAddr, u8),
    /// rt addr, sub_addr(has to be 0 or 31), mode, data
    BcToRtModeCommand(RtAddr, RtAddr, u8, Option<u16>),
}

/// This trait specifies the necessary behaviors that must be implemented to control STD-MIL-1553b devices
/// to make them compatible with the Yamcs Frontend.
/// It is expected that each such device contains a scheduler which can be programmed to transmit/receive messages.
///
/// The scheduler is structured with major and minor frames in which a a series of messages, and mode commands are scheduled.
/// The messages get send in a cyclic fashion by the scheduler and can be read and written at any time
/// The event log is used to know when to a specific message is sent/received.
/// Messages can be enabled and disabled.
///
/// Each message is identified by an u32 chosen by the user.
pub trait Mil1553BcDev {
    /// Init the card with a new schedule.
    ///  Overrides any previously stored schedule.
    fn init(&mut self, schedule: Schedule) -> Result<()>;

    /// Start executing the schedule
    fn start(&self) -> Result<()>;

    /// Disable the message with the given id from being transmitted.
    /// If the message was already disabled, this function has no effect
    fn disable_msg(&mut self, msgid: MsgId) -> Result<()>;

    /// Enable the message with the given id. If the message was disabled, it will be transmitted again.
    /// If the message was already enabled, this function has no effect
    fn enable_msg(&mut self, msgid: MsgId) -> Result<()>;

    /// Read data from the specified message.
    /// /// TODO: there is no guarantee that data is not being written by the device at the same time
    fn read_msg_data(&self, msgid: MsgId) -> Result<Vec<u16>>;

    /// Write data to the specified message.
    /// TODO: there is no guarantee that data is not being read by the device at the same time
    fn write_msg_data(&self, msgid: MsgId, data: &[u16]) -> Result<()>;

    /// Reads the next message from the event log
    fn next_msg(&mut self) -> Result<Option<(MsgId, BusMessage)>>;

    /// Read one unscheduled message from the RT.
    /// This function does not return until transmission is complete.    
    fn get_unscheduled_from_rt(
        &mut self,
        addr: RtAddr,
        sub_addr: RtAddr,
        len: u8,
    ) -> Result<BusMessage>;

    /// Send one unscheduled message to the RT.
    /// This function does not return until transmission is complete.    
    fn send_unscheduled_to_rt(
        &mut self,
        addr: RtAddr,
        sub_addr: RtAddr,
        data: &[u16],
    ) -> Result<()>;
}
