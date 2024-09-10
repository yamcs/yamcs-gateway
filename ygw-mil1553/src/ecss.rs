/// Implements ECSS-E-ST-50-13C - Interface and communication protocol for MIL-STD-1553B data bus onboard spacecraft
///
/// For the moment it only works as BC and only supports a BTI1553 Milbus card. It also supports one single RT.
///
/// The following services are part of the standard and supported:
/// - Time service
/// - Synchronization service
/// - Health&Status RT->BC
/// - Data Block (TM) RT->BC
/// - Data Block (TC) BC->RT
use std::{
    sync::mpsc::{Receiver, Sender, TryRecvError},
    time::{SystemTime, UNIX_EPOCH},
};

use super::{
    mil1553card::{self, Mil1553Card},
    //bti1553::{Bti1553Card, MsgAddr},
    MilbusCommand, MilbusData, Result, RtAddr,
};


// use super::bti1553::MSGCRT1553_DEFAULT;
// use super::bti1553::MSGCRT1553_LOG;

const TIME_LEN: u8 = 5;

const BROADCAST_ADDR: RtAddr = RtAddr::new(31);
const MODE_SUBADDR: RtAddr = RtAddr::new(31);

// Health&Status subaddress
const HS_SUBADDR: RtAddr = RtAddr::new(1);

// at this sub-address the datablocks for transfering TM/TC start
const DATA_BLOCK_START_SUBADDR: u8 = 11;

// distribution transfer - used by the BC to signal sending data (TC) and for the RT to confirm
// reception
const DT_SUBADDR: RtAddr = RtAddr::new(27);

//acquisition transfer request/confirmation - used by the RT to request to send data (TM) and by BC to
//grant the request
const AT_SUBADDR: RtAddr = RtAddr::new(28);

//used to broadcast time
const TIME_SUBADDR: RtAddr = RtAddr::new(29);

pub struct Ecss5013Config {
    pub num_slots: u8,
    //slot duration in microseconds
    pub slot_duration: u32,
    //if this is true, the first sync message is without data, as specified in the standard
    //however the NGRM instrument does not like it so we use the
    pub tsync_with_data: bool,
    pub rt_conf: Vec<RtConfig>,
}

/// Configuration for one Remote Terminal
pub struct RtConfig {
    pub addr: RtAddr,
    pub hs_len: u8,
    pub adb_num: u8,
    pub ddb_num: u8,
}

pub struct Ecss5013C<T: Mil1553Card> {
    //synchronization messages (one per slot) BC > broadcast
    _sync_msg: Vec<MsgAddr>,

    //time message BC -> Broadcast
    time_msg: MsgAddr,
    card: T,
    rts: Vec<RtHandler>,
}

impl Ecss5013C {
    pub fn new<T:Mil1553Card>(mut card: T, config: &Ecss5013Config) -> Result<Self> {
        let mut sync_msgs = create_sync_msgs(&mut card, config.num_slots, config.tsync_with_data);
        //time message
        let data = cuc_time();
        let mut time_msg = card.create_msg_to_rt(BROADCAST_ADDR, TIME_SUBADDR, MSGCRT1553_LOG, TIME_LEN, &data);

        let mut rt_handlers = Vec::with_capacity(config.rt_conf.len());
        for rt_conf in config.rt_conf.iter() {
            rt_handlers.push(RtHandler::new(&mut card, rt_conf)?);
        }

        //schedule messages
        // TODO: make the schedule configurable
        for slot in 0..config.num_slots {
            card.sched_frame(config.slot_duration)?;
            card.sched_msg(&mut sync_msgs[slot as usize])?;
            if slot == 0 {
                card.sched_msg(&mut time_msg)?;
            }

            for rt in rt_handlers.iter_mut() {
                rt.schedule(&mut card, slot)?;
            }
        }

        Ok(Ecss5013C {
            _sync_msg: sync_msgs,
            time_msg,
            card,
            rts: rt_handlers,
        })
    }

    pub fn run(&mut self, cmd_rx: Receiver<MilbusCommand>, data_tx: Sender<MilbusData>) -> Result<()> {
        self.card.config_event_log(1024)?;
        self.card.start()?;
        for rt in self.rts.iter() {
            rt.reset_hs(&mut self.card)?;
        }

        let mut count = 0;
        loop {
            if count == 3000 {
                break;
            }
            if let Some((event_type, event_info)) = self.card.read_event_log() {
                if event_type == super::bti1553::EVENTTYPE_1553MSG && self.time_msg.equals(event_info) {
                    // TODO: fix the time
                    // According to the standard the time that is set here is the time of the next time sync message - that is the sync message in slot 0
                    //
                    let data = cuc_time();
                    log::trace!("Updating time to {}", hex(&data));
                    self.card.write_message(&self.time_msg, &data);
                }

                for rt in self.rts.iter_mut() {
                    if rt.process_event(&mut self.card, event_type, event_info)? {
                        break;
                    }
                }
                count += 1;
            }

            match cmd_rx.try_recv() {
                Err(err) => if err == TryRecvError::Disconnected {
                    return Ok(())
                }
                Ok(cmd) => {
                   self.execute_cmd(cmd);
                }
            }
        }

        Ok(())
    }

    fn execute_cmd(&mut self, cmd: MilbusCommand) {
        match cmd {
            MilbusCommand::Telecommand(addr, tc) => {
                for rt in self.rts.iter_mut() {
                    if rt.addr == addr {
                        if rt.tc_data == None {
                            rt.tc_data = Some(tc);
                            break;
                        } else {
                            log::warn!("RT {:?} is already transferring a command; dropping new command", addr)
                        }
                    }
                }
            },
        }
    }

}

//create sync messages one for each slot
fn create_sync_msgs<T: Mil1553Card> (card: &mut T, num_slots: u8, tsync_with_data: bool) -> Vec<MsgAddr> {
    let mut sync_msgs = Vec::new();
    let mut data = [u16; 32];
    for i in 0..num_slots {
        data[0] = i as u16;
        sync_msgs.push(if i == 0 {
            //1 = synchronize without data
            let x = if tsync_with_data { 17 } else { 1 };
            card.create_msg_to_rt(BROADCAST_ADDR, MODE_SUBADDR, MSGCRT1553_DEFAULT, x, &data)
        } else {
            //17 = synchronize with data
            card.create_msg_to_rt(BROADCAST_ADDR, MODE_SUBADDR, MSGCRT1553_DEFAULT, 17, &data)
        });
    }
    sync_msgs
}

fn cuc_time() -> [u16; 5] {
    //TODO use simulation time
    let mut data = [0; 5];
    data[0] = 0x002F; //CCSDS CUC P-field indicating 4 bytes coarse time, 3 bytes fine time

    let now = SystemTime::now();
    let since_unix = now.duration_since(UNIX_EPOCH).unwrap();
    let seconds = since_unix.as_secs();
    let fsec = since_unix.subsec_nanos() as u64 * 0x1000000 / 1_000_000_000;

    data[1] = (seconds >> 16) as u16;
    data[2] = (seconds & 0xFFFF) as u16;
    data[3] = (fsec >> 16) as u16;
    data[4] = (fsec & 0xFFFF) as u16;

    data
}

fn hex(data: &[u16]) -> String {
    let hex_strings: Vec<String> = data.iter().map(|x| format!("{:04X}", x)).collect();
    hex_strings.join(" ")
}

/// Implements schedule and message processing for one RT (as seen from BC)
pub struct RtHandler {
    addr: RtAddr,

    //Health&Status message RT -> BC
    hs_msg: MsgAddr,

    //Acquisition Data block (TM) RT->BC
    adb_msg: Vec<MsgAddr>,

    //Distribution Data Block (TC) BC->RT
    ddb_msg: Vec<MsgAddr>,

    //Aquisition Transfer Request
    atr_msg: MsgAddr,

    //Distribution Transfer Descriptor
    dtd_msg: MsgAddr,
    dtd: AtrDtc,

    //Distribution Transfer Confirmation
    dtc_msg: MsgAddr,

    // received data is build up in this builder
    tm_data: Option<DataBuilder>,

    //command to be sent
    //TODO: turn this into a queue?
    tc_data: Option<Vec<u8>>,
}

impl RtHandler {
    fn new<T:Mil1553Card>(card: &mut T, config: &RtConfig) -> Result<Self> {
        let addr = config.addr;

        //health and status messsage
        let hs_msg = card.create_msg_from_rt(addr, HS_SUBADDR, MSGCRT1553_LOG, config.hs_len);
        println!("Created hs_msg {:?}", hs_msg);

        // acquisition transfer request
        let atr_msg = card.create_msg_from_rt(addr, AT_SUBADDR, MSGCRT1553_LOG, 2);
        println!("Created atr message: {:?}", atr_msg);

        // acquisition data blocks
        let mut adb_msg = Vec::with_capacity(config.adb_num as usize);
        for i in 0..config.adb_num {
            let msg = card.create_msg_from_rt(addr, RtAddr::new(DATA_BLOCK_START_SUBADDR + i), MSGCRT1553_LOG, 32);
            adb_msg.push(msg);
        }

        // distribution transfer descriptor
        let dtd = AtrDtc::reset();
        let dtd_msg = card.create_msg_to_rt(addr, DT_SUBADDR, MSGCRT1553_DEFAULT, 2, &dtd.pack());

        // distribution transfer confirmation
        let dtc_msg = card.create_msg_from_rt(addr, DT_SUBADDR, MSGCRT1553_LOG, 2);
        println!("Created dtc message: {:?}", dtc_msg);

        let data = [0u16; 32];
        let mut ddb_msg = Vec::new();
        for i in 0..config.ddb_num {
            let msg = card.create_msg_to_rt(
                addr,
                RtAddr::new(DATA_BLOCK_START_SUBADDR + i),
                MSGCRT1553_LOG,
                32,
                &data,
            );
            ddb_msg.push(msg);
        }

        Ok(RtHandler {
            addr,
            hs_msg,
            adb_msg,
            ddb_msg,
            atr_msg,
            dtd_msg,
            dtd,
            dtc_msg,
            tm_data: None,
            tc_data: None,
        })
    }

    fn schedule<T:Mil1553Card>(&mut self, card: &mut T, slot: u8) -> Result<()> {
        if slot == 0 {
            card.sched_msg(&mut self.hs_msg)?;
        } else if slot == 2 {
            card.sched_msg(&mut self.atr_msg)?;
        } else if slot == 3 {
            for m in self.adb_msg.iter_mut() {
                card.sched_msg(m)?;
                card.set_one_shot(m)?;
                card.set_skip(m, true)?;
            }
        } else if slot == 4 {
            for m in self.ddb_msg.iter_mut() {
                card.sched_msg(m)?;
                card.set_skip(m, true)?;
            }
            card.sched_msg(&mut self.dtd_msg)?;
        } else if slot == 6 {
            card.sched_msg(&mut self.dtc_msg)?;
        }

        Ok(())
    }

    fn reset_hs<T:Mil1553Card>(&self, card: &mut T) -> Result<()> {
        card.send_unscheduled_to_rt(self.addr, HS_SUBADDR, &[0xFFFF])
    }

    // Called when an event is received from the Milbus card.
    // We have to look through the list of messages for this RT to see if it is destined for this RT
    // If it's for this RT, return true, otherwise return false
    fn process_event<T:Mil1553Card>(&mut self, card: &mut T, event_type: u32, event_info: u32) -> Result<bool> {
        if event_type != super::bti1553::EVENTTYPE_1553MSG {
            return Ok(false);
        }

        if self.hs_msg.equals(event_info) {
            let data = card.read_message(&self.hs_msg);
            println!("Got HS: {}", hex(&data));
            return Ok(true);
        } else if self.atr_msg.equals(event_info) {
            let mut data = [0; 2];
            card.read_message_into(&self.atr_msg, &mut data);
            let atr = AtrDtc::unpack(data[0], data[1]);
            self.process_atr(card, atr)?;
            return Ok(true);
        } else if self.dtc_msg.equals(event_info) {
            let mut data = [0; 2];
            card.read_message_into(&self.dtc_msg, &mut data);
            let dtc = AtrDtc::unpack(data[0], data[1]);
            self.process_dtc(card, dtc)?;
            return Ok(true);
        } else {
            for (idx, m) in self.adb_msg.iter().enumerate() {
                if m.equals(event_info) {
                    let data = card.read_message(m);
                    self.process_data(card, idx, data)?;

                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// ATR is the message received when the RT wants to trasnfer a TM packet
    fn process_atr<T:Mil1553Card>(&mut self, card: &mut T, atr: AtrDtc) -> Result<()> {
        log::trace!("Got ATR data: {:?}", atr);

        if atr.reset {
            self.tm_data = None;
            log::info!("Resetting ATR ");
            card.send_unscheduled_to_rt(self.addr, AT_SUBADDR, &[0, 0x4000])?;
            return Ok(());
        }
        if atr.mode {
            // ATR mode 1 means that one single sub-address is used to receive all the data
            // we do not support this mode; instead we only support receiving data on different sub-addresses
            log::warn!("ATR with mode 1 not supported, ignored");
            return Ok(());
        }

        if atr.sa != 11 {
            //NGRM sends this when it does not have anything to transfer
            log::trace!("Got ATR sub-address {}, expected 11, ignored", atr.sa);
            return Ok(());
        }
        if atr.size == 0 {
            log::warn!("Got ATR size 0, ignoring ATR");
            return Ok(());
        }

        let max_size = self.adb_msg.len() * 64;
        if atr.size as usize > max_size {
            log::warn!(
                "Received ATR with size {} larger than max {}; ignored",
                atr.size,
                max_size
            );
            return Ok(());
        }
        match &self.tm_data {
            None => self.start_new_data_builder(card, atr)?,
            Some(block) => {
                if block.atr.count != atr.count {
                    if !block.is_complete() {
                        log::warn!("Dropping incomplete data block");
                    }

                    self.start_new_data_builder(card, atr)?;
                } //else same ATR like before, ignore
            }
        }

        Ok(())
    }

    fn process_data<T:Mil1553Card>(&mut self, card: &mut T, block_num: usize, data: [u16; 32])  -> Result<()> {
        if let Some(dbuilder) = &mut self.tm_data {
            dbuilder.add_block(block_num, &data);
            if dbuilder.is_complete() {
                println!("Got TM data complete: {:?}", super::hex8(&dbuilder.data));
                //acknowledging the packet
                let data = dbuilder.atr.pack();
                card.send_unscheduled_to_rt(self.addr, AT_SUBADDR, &data)?;
            }
        } else {
            //TODO: should we ignore this - maybe it happens following a reset?
            log::warn!("Received data but have no data builder");
        }

        Ok(())
    }

    //enable reception of data in the ADB sub-addresses
    fn start_new_data_builder<T:Mil1553Card>(&mut self, card: &mut T, atr: AtrDtc) -> Result<()>{
        let db = DataBuilder::new(atr);
        let num_blocks = db.num_blocks();

        self.tm_data = Some(db);
        for i in 0..num_blocks {
            card.set_skip(&self.adb_msg[i as usize], false)?;
        }

        Ok(())
    }

    /// DTC is the message received when the RT confirms the reception of a TC
    fn process_dtc<T:Mil1553Card>(&mut self, card: &mut T, dtc: AtrDtc) -> Result<()> {
        log::trace!("Processing DTC {:?}", dtc);
        
        if self.dtd.reset {
            //we requested a reset
            if dtc.reset {
                log::debug!("Data transfer (TC) reset accepted");
                self.dtd.reset = false;
                self.dtd.count = 0;
                card.write_message(&self.dtd_msg, &self.dtd.pack());
            } else {
                log::debug!("Data transfer(TC) reset not accepted");
            }
            log::trace!("Sending DTD {:?}: {}", self.dtd, hex(&self.dtd.pack()));
        } else if self.dtd.size > 0 {
            //command in progress
            if dtc.qos_err {
                log::warn!("Data transfer(TC): peer indicates error");
                //TODO reset after a few attempts??
            } else if self.dtd == dtc {
                log::debug!("Data transfer(TC): completed successfully");
                self.dtd.size = 0;
                card.write_message(&self.dtd_msg, &self.dtd.pack());

                for m in self.ddb_msg.iter() {
                    card.set_skip(m, true)?;
                }
                //TODO ack command?
                self.tc_data = None;
            }
        } else {
            self.send_tc_if_any(card)?;
        }

        Ok(())
    }

    fn send_tc_if_any<T:Mil1553Card>(&mut self, card: &mut T) -> Result<()>{
        if let Some(tc_data) = &self.tc_data {
            let num_blocks = (tc_data.len() - 1) / 64 + 1;

            let mut n = 0;
            for i in 0..num_blocks {
                let mut data = [0; 32];
                for j in 0..32 {
                    if n >= tc_data.len() {
                        break;
                    }
                    data[j] = (tc_data[n] as u16) << 8;
                    n += 1;
                    if n >= tc_data.len() {
                        break;
                    }
                    data[j] |= tc_data[n] as u16;
                    n += 1;
                }
                log::trace!("Set message ddb[{}] to {}", i, hex(&data));
                card.write_message(&self.ddb_msg[i], &data);
                card.set_skip(&self.ddb_msg[i], false)?;
            }
            self.dtd.size = tc_data.len() as u16;
            self.dtd.count+=1;
            
            card.write_message(&self.dtd_msg, &self.dtd.pack());
            log::debug!("Wrote new command with dtd {:?}", self.dtd);
        }

        Ok(())
    }
}

/// structure for the following:
// Distribution transfer confirmation
// Distribution transfer descriptor
// Acquisition Transfer Request
// Acquisition Transfer Confirmation
//  all have the same structure
#[derive(Debug, PartialEq)]
struct AtrDtc {
    size: u16,
    qos_err: bool,
    reset: bool,
    mode: bool,
    sa: u8,
    count: u8,
}
impl AtrDtc {
    fn reset() -> Self {
        Self {
            size: 0,
            qos_err: false,
            reset: true,
            mode: false,
            sa: 0,
            count: 0,
        }
    }
    fn new(size: u16, reset: bool, count: u8) -> Self {
        Self {
            size,
            qos_err: false,
            reset,
            mode: false,
            sa: 11,
            count,
        }
    }

    //parse from two data words
    fn unpack(w1: u16, w2: u16) -> Self {
        Self {
            size: w1 & 0xFFF,
            qos_err: (w2 >> 15) & 1 == 1,
            reset: (w2 >> 14) & 1 == 1,
            mode: (w2 >> 13) & 1 == 1,
            sa: ((w2 >> 8) & 0x1F) as u8,
            count: (w2 & 0xFF) as u8,
        }
    }

    fn pack(&self) -> [u16; 2] {
        let w2 = ((self.qos_err as u16) << 15)
            | ((self.reset as u16) << 14)
            | ((self.mode as u16) << 13)
            | ((self.sa as u16) << 8)
            | (self.count as u16);

        [self.size, w2]
    }
}

// builds data by blocks of 32 words (64 bytes)
#[derive(Debug)]
struct DataBuilder {
    atr: AtrDtc,
    data: Vec<u8>,
    index: u32,
}

impl DataBuilder {
    fn new(atr: AtrDtc) -> Self {
        Self {
            data: vec![0; atr.size as usize],
            atr,
            index: 0,
        }
    }

    fn is_complete(&self) -> bool {
        let num_blocks = (self.data.len() - 1) / 64 + 1;
        self.index == (1 << num_blocks) - 1
    }

    // add a new block (this also changes data from little to big endian, we can't just use memcopy)
    fn add_block(&mut self, block_num: usize, data: &[u16; 32]) {
        let mut n = 64 * block_num as usize;

        for i in 0..32 {
            if n >= self.data.len() {
                break;
            }
            self.data[n] = (data[i] >> 8) as u8;
            n += 1;
            if n >= self.data.len() {
                break;
            }
            self.data[n] = (data[i] & 0xFF) as u8;
            n += 1;
        }
        self.index |= 1 << block_num;
    }

    fn num_blocks(&self) -> usize {
        (self.data.len() - 1) / 64 + 1
    }
}
