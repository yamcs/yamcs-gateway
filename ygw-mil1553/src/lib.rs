pub mod mil1553dev;
use bitflags::bitflags;
use mil1553dev::MsgId;
use thiserror::Error;

/// maximum number of words (u16) in a MIL-1553 message
pub const MAX_DATA_SIZE: usize = 32;

#[derive(Error, Debug)]
pub enum Mil1553Error {
    #[error("device driver error ({0}): {1}")]
    DevError(i32, String),
    #[error("invalid message id: {0}")]
    InvalidMessageId(MsgId),
    #[error("duplicate message id: {0}")]
    DuplicateMessageId(MsgId),
    #[error("too many messages: {0}")]
    TooManyMessages(MsgId),
    #[error("too many frames: {0}")]
    TooManyFrames(MsgId),
    #[error("too many opcodes: {0}")]
    TooManyOpCodes(MsgId),
    #[error("invalid state: {0}")]
    InvalidState(String),
}

pub type Result<T> = std::result::Result<T, Mil1553Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RtAddr {
    a: u8,
}

impl RtAddr {
    pub const fn new(a: u8) -> RtAddr {
        if a > 31 {
            panic!("Address should be between 0 and 31")
        }
        RtAddr { a }
    }
    pub const fn addr(&self) -> u8 {
        self.a
    }
}

pub enum TxRx {
    BcToRt = 0,
    RtToBc = 1,
}

#[derive(Debug)]
pub enum MilbusData {
    HealthStatus(Vec<u8>),
    Telemetry(Vec<u8>),
}

#[derive(Debug)]
pub enum MilbusCommand {
    Telecommand(RtAddr, Vec<u8>),
}

bitflags! {
    /// A set of possible errors related to message transmission
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct MsgError: u32 {
        /// no error
        const NOERROR = 0x0000_0000;
        /// No response was received from the RT
        const NORESP = 0x0000_0001;
        ///  protocol error occurred
        const PROTOCOL = 0x0000_0002;
        /// Wrong polarity of the sync pulse
        const SYNC = 0x0000_0004;
        /// Too many/too few data words
        const DATACOUNT = 0x0000_0008;
        /// Manchester error
        const MANCHESTER = 0x0000_0010;
        /// Parity error
        const PARITY = 0x0000_0020;
        /// Internal error
        const INTERNAL = 0x0000_0040;
        /// Concurrency error - the message was being transmitted/received when being read/changed
        const CONCURRENCY = 0x0000_0080;
        /// Test loop error
        const TESTLOOP = 0x0000_0100;
    }
}
pub struct RtMessageId {
    pub id: u16,
    pub sub_addr: RtAddr,
}

/// a message transmitted on the bus
#[derive(Debug)]
pub struct BusMessage {
    pub timetag: u64,
    cmdwrd: u16,
    
    pub data: Vec<u16>,

    /// status received from the BC if any
    pub status: Option<u16>,
    /// if there was an error transmitting/receiving
    pub error: MsgError,
}

impl BusMessage {
    pub fn new(
        timetag: u64,
        cmdwrd: u16,
        data: Vec<u16>,
        error: MsgError,
        status: Option<u16>,
    ) -> Self {
        BusMessage {
            timetag,
            cmdwrd,
            data,
            error,
            status,
        }
    }

    pub fn addr(&self) -> RtAddr {
        RtAddr::new((self.cmdwrd >> 11) as u8)
    }

    pub fn sub_addr(&self) -> RtAddr {
        RtAddr::new(((self.cmdwrd >> 5) & 0x1F) as u8)
    }

    pub fn txrx(&self) -> TxRx {
        if (self.cmdwrd >> 10) & 1 == 0 {
            TxRx::BcToRt
        } else {
            TxRx::RtToBc
        }
    }
}
pub fn hex(data: &[u16]) -> String {
    let hex_strings: Vec<String> = data.iter().map(|x| format!("{:04X}", x)).collect();
    hex_strings.join(" ")
}

pub fn hex8(data: &[u8]) -> String {
    let hex_strings: Vec<String> = data.iter().map(|x| format!("{:02X}", x)).collect();
    hex_strings.join(" ")
}

//builds the u16 command word
pub fn pack_cwd(addr: RtAddr, sub_addr: RtAddr, txrx: TxRx, len_or_mode: u8) -> u16 {
    ((addr.a as u16) << 11)
        | ((txrx as u16) << 10)
        | ((sub_addr.a as u16) << 5)
        | ((len_or_mode & 0x1F) as u16)
}
