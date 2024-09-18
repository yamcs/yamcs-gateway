//! File recording and replay
//!
//! The recording files are composed of a header and multiple segments. The segments are compressed with zstd.
//! The format of the file is:
//! part 1 - header
//! - MAGIC - the string YGW_REC (7 bytes)
//! - format version (1 byte) - current version is 1
//! - first record number (8 bytes)
//! - maximum number of segments (4 bytes)
//!
//! then for each segment 9 bytes segment information consisting of:
//! - metadata (1 byte) - currently only if the segment contains parameter definitions
//! - size (4 bytes)
//! - the number of the last record in the segment, relative to the first record number defined above (4 bytes)
//!
//! part 2 - data - formed by segments one after each-other
//! the segments are zstd compressed data composed of records.
//! Each record consists of:
//! - record length  (without the size itself) (4 bytes)
//! - record number relative to the first record number defined above (4 bytes)
//! - record data (length - 4)
//!
//! The trick with the zstd decoder is that it can decode segments concatenated one after eachother;
//! zstd has itself the concept of a frame so our segments are basically zstd frames.
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::{Result, YgwError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use zstd;

const MAX_REC_SIZE: u32 = 8 * 1024 * 1024;

/// the size of each segment info
const SEG_INFO_SIZE: usize = 9;

const MAGIC: &[u8] = "YGW_REC".as_bytes();

const FORMAT_VERSION: u8 = 1;

const MAX_SEGMENTS_PER_FILE: u32 = 1000;

/// where in the file starts the segment info
/// after MAGIC, format version, first record number and maximum number of segments
const SEG_INFO_START: u64 = 20;

#[derive(Debug)]
struct SegmentInfo {
    /// true if the segment contains parameter definitions
    has_pdef: bool,
    /// the size in bytes of the segment (only known when the segment is closed)
    size: u32,
    /// the number of the last record in the segment, relative to the first record number of the file
    last_rn: u32,
}
impl SegmentInfo {
    fn to_bytes(&self) -> [u8; SEG_INFO_SIZE] {
        let mut bytes = [0u8; SEG_INFO_SIZE];

        bytes[0] = if self.has_pdef { 1 } else { 0 };

        let offset_bytes = self.size.to_be_bytes();
        bytes[1..5].copy_from_slice(&offset_bytes);

        let num_rec_bytes = self.last_rn.to_be_bytes();
        bytes[5..9].copy_from_slice(&num_rec_bytes);

        bytes
    }

    fn from_bytes(bytes: &[u8; SEG_INFO_SIZE]) -> Self {
        let has_pdef = bytes[0] != 0;

        let size = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);

        let last_rn = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);

        Self {
            has_pdef,
            size,
            last_rn,
        }
    }
}

/// recorder for one file
pub struct FileRecorder<'a> {
    /// maximum number of segments in this file.
    /// each segment will have a preallocated segment info at the beginning of the file,
    /// so this number cannot change once the file has been created
    max_num_segments: u32,
    /// first record number
    first_rec_num: u64,
    /// encoder for the current segment.
    /// None it means the file is full, no more data can be added
    encoder: Option<zstd::Encoder<'a, File>>,
    /// number of the segment that is being recorded
    seg_num: u32,
    /// true if the current segment has parameter definitions
    has_pdef: bool,
    /// number of records in the current segment
    num_records: u32,
    /// offset where the current segment starts
    offset: u64,
    file: File,
    /// maximum number of records in each segment
    max_nrps: u32,
    /// number of the last record written (relative to the first_rec_num)
    last_rn: u32,
}

impl<'a> FileRecorder<'_> {
    ///Create a new file recorder
    /// # Arguments
    ///
    /// * `path` - The path of the file to be recorded.
    /// * `first_rec_num` - The first record number. Al the record numbers in the file are relative to this one
    /// * `max_num_seg` - The maximum number of segments that can be stored in the file. This determines the size of the header
    /// * `max_nrps` - The maximum number of records per segment
    pub fn new(
        path: &Path,
        first_rec_num: u64,
        max_num_segments: u32,
        max_nrps: u32,
    ) -> Result<Self> {
        assert!(max_num_segments < MAX_SEGMENTS_PER_FILE);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .map_err(|e| YgwError::IOError(format!("Error opening {}", path.display()), e))?;

        file.write_all(&MAGIC)?;
        file.write_u8(FORMAT_VERSION)?;
        file.write_u64::<BigEndian>(first_rec_num)?;
        file.write_u32::<BigEndian>(max_num_segments)?;
        let zeros = vec![0u8; (max_num_segments as usize) * SEG_INFO_SIZE];
        file.write_all(&zeros)?;
        file.sync_data()?;

        let f = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|e| YgwError::IOError(format!("Error opening {}", path.display()), e))?;

        let encoder = Some(zstd::Encoder::new(f, 0)?);

        Ok(FileRecorder {
            max_num_segments,
            first_rec_num,
            encoder,
            seg_num: 0,
            has_pdef: false,
            num_records: 0,
            offset: get_data_start_offset(max_num_segments),
            file,
            max_nrps,
            last_rn: 0,
        })
    }

    /// append the record to the end of the file
    pub fn append(&mut self, rn: u64, msg: &[u8], is_pdef: bool) -> Result<()> {
        let Some(ref mut encoder) = self.encoder else {
            return Err(crate::YgwError::RecordingFileFull(self.max_num_segments));
        };
        let rn =  (rn-self.first_rec_num).try_into().expect(&format!("the record number {rn} is too high compared with the first record number of the file {}", self.first_rec_num));

        encoder.write_u32::<BigEndian>(4 + msg.len() as u32)?;
        encoder.write_u32::<BigEndian>(rn)?;
        encoder.write_all(msg)?;
        self.has_pdef |= is_pdef;
        self.num_records += 1;
        self.last_rn = rn;

        if self.num_records >= self.max_nrps {
            self.end_segment(true)?;
        }

        Ok(())
    }

    pub fn is_full(&self) -> bool {
        return self.encoder.is_none();
    }

    pub fn flush(&mut self) -> Result<()> {
        if let Some(ref mut encoder) = self.encoder {
            encoder.flush()?
        }
        Ok(())
    }

    fn end_segment(&mut self, start_next: bool) -> Result<()> {
        let mut f = if let Some(encoder) = self.encoder.take() {
            encoder.finish()?
        } else {
            panic!("unexpected state");
        };
        let offset = f.seek(SeekFrom::Current(0))?;
        log::debug!(
            "Ending segment {} at offset {}; last_rn: {}",
            self.seg_num,
            offset,
            self.last_rn
        );

        println!(
            "Ending segment {}; start: {},  end {}, last_rn: {}",
            self.seg_num, self.offset, offset, self.last_rn
        );

        let size = seg_size(offset, self.offset);

        let seg_info = SegmentInfo {
            has_pdef: self.has_pdef,
            size,
            last_rn: self.last_rn,
        };
        let seg_info_offset = SEG_INFO_START + (self.seg_num as u64) * (SEG_INFO_SIZE as u64);
        self.file.seek(SeekFrom::Start(seg_info_offset))?;
        self.file.write_all(&seg_info.to_bytes())?;
        self.file.sync_data()?;

        if start_next {
            //next segment
            self.seg_num += 1;
            if self.seg_num >= self.max_num_segments {
                //file full
                return Ok(());
            }
            self.offset = offset;
            self.has_pdef = false;
            self.num_records = 0;

            self.encoder = Some(zstd::Encoder::new(f, 0)?);
        }
        Ok(())
    }
}

fn get_data_start_offset(num_segments: u32) -> u64 {
    return SEG_INFO_START + (num_segments as u64) * (SEG_INFO_SIZE as u64);
}
fn seg_size(offset1: u64, offset2: u64) -> u32 {
    (offset1 - offset2).try_into().expect("segment too large")
}

impl<'a> Drop for FileRecorder<'_> {
    fn drop(&mut self) {
        if !self.is_full() && self.num_records > 0 {
            if let Err(e) = self.end_segment(false) {
                log::error!("Error in closing the file {:?}", e);
            }
        }
    }
}

pub struct FilePlayer {
    first_rec_num: u64,
    segments: Vec<SegmentInfo>,
    file: File,
}

impl<'a> FilePlayer {
    /// open a file for replay
    /// An CorruptedRecordingFile may be returned if the header of the file is not correct:
    /// - magic string not present
    /// - format version not matching the expected version (1)
    /// - first record number not matching  the argument of the function
    /// - maximum number of segments is too large
    /// - the size of the segment data according to the header is smaller than the size of the file
    /// - the last record numbers of the segments are not
    ///
    /// If the size of the file is larger than the sum of the segments from the header, then an attempt is to parse the
    /// last segment to find its size. This is because if the recording is not closed properly,
    ///  the last segment information will not be present in the header. This is not considered a data corruption.
    pub fn open(path: &Path, first_rec_num: u64) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(false).open(path)?;
        let mut magic = vec![0u8; MAGIC.len()];
        file.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(crate::YgwError::CorruptedRecordingFile(format!(
                "Incorrect magic read from {}: {:?}",
                path.display(),
                magic
            )));
        }
        let format = file.read_u8()?;
        if format != FORMAT_VERSION {
            return Err(crate::YgwError::CorruptedRecordingFile(format!(
                "Incorrect format version read from {}: {} (expected {})",
                path.display(),
                format,
                FORMAT_VERSION
            )));
        }

        let frn = file.read_u64::<BigEndian>()?;
        if frn != first_rec_num {
            return Err(crate::YgwError::CorruptedRecordingFile(format!(
                "Incorrect first record number read from from {}: {} (expected {})",
                path.display(),
                frn,
                first_rec_num
            )));
        }

        let max_num_segments = file.read_u32::<BigEndian>()?;

        if max_num_segments > MAX_SEGMENTS_PER_FILE {
            return Err(crate::YgwError::CorruptedRecordingFile(format!(
                "The file {} contains {} segments. Maximum expected is {}",
                path.display(),
                max_num_segments,
                MAX_SEGMENTS_PER_FILE
            )));
        }
        let mut segments = Vec::with_capacity(max_num_segments as usize);

        // read the segment information
        for _ in 0..max_num_segments {
            let mut buf = [0u8; SEG_INFO_SIZE];
            file.read_exact(&mut buf)?;
            segments.push(SegmentInfo::from_bytes(&buf));
        }
        verify_consistency(&segments)?;

        let hdr_size = get_data_start_offset(max_num_segments);
        let file_size = file.metadata()?.len();
        let seg_size: u64 = segments.iter().map(|s| s.size as u64).sum();
        log::debug!(
            "{}: file_size: {file_size} hdr_size: {hdr_size} segment_data_size: {seg_size} ",
            path.display(),
        );

        let mut player = Self {
            first_rec_num,
            segments,
            file,
        };

        if hdr_size + seg_size < file_size {
            if let Some(idx) = player.segments.iter().position(|s| s.size == 0) {
                player.recover_last_segment(idx)?;
            } else {
                return Err(YgwError::CorruptedRecordingFile(format!("{}: header + size of the segment data is smaller than the size of the file: {hdr_size} + {seg_size} < {file_size}) yet no segment is incomplete according to the header", path.display())));
            }
        } else if hdr_size + seg_size > file_size {
            return Err(YgwError::CorruptedRecordingFile(format!("{}: header + size of the segment data is greater than the size of the file: {hdr_size} + {seg_size} > {file_size})", path.display())));
        }

        Ok(player)
    }

    /// parse the last segment to find its size and last record number
    fn recover_last_segment(&mut self, idx: usize) -> Result<()> {
        let offset = self
            .segments
            .iter()
            .take(idx)
            .map(|s| s.size as u64)
            .sum::<u64>()
            + self.get_data_start_offset();
        let mut file = self.file.try_clone()?;

        file.seek(SeekFrom::Start(offset))?;
        let mut decoder = zstd::Decoder::new(file)?;
        let mut last_rn = 0;
        while let Ok((rn, _)) = read_record(&mut decoder) {
            last_rn = rn;
        }
        let mut file = decoder.finish();
        let offset1 = file.stream_position()?;

        self.segments[idx].last_rn = last_rn;
        self.segments[idx].size = seg_size(offset1, offset);
        //TODO
        //self.segments[idx].has_pdef =

        Ok(())
    }

    /// returns an iterator that iterates over all records
    /// the iterator does not take into account the segment information
    /// it just uses the zstd decoder to decode all segments
    pub fn iter(&mut self) -> Result<FilePlayerIterator> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(get_data_start_offset(
            self.segments.len() as u32
        )))?;
        let decoder = Some(zstd::Decoder::new(file)?);

        Ok(FilePlayerIterator { fr: self, decoder })
    }

    /// returns an iterator that iterates over all records starting with seg_num segment
    /// the position of the segment is based on the correct metadata information for the previous segments
    pub fn iter_from(&mut self, seg_num: u32) -> Result<FilePlayerIterator> {
        let mut file = self.file.try_clone()?;

        let offset = self
            .segments
            .iter()
            .take(seg_num as usize)
            .map(|s| s.size as u64)
            .sum::<u64>()
            + self.get_data_start_offset();
        file.seek(SeekFrom::Start(offset))?;

        let decoder = Some(zstd::Decoder::new(file)?);

        Ok(FilePlayerIterator { fr: self, decoder })
    }

    pub fn get_data_start_offset(&self) -> u64 {
        get_data_start_offset(self.segments.len() as u32)
    }

    pub fn get_last_record_number(&self) -> Option<u64> {
        self.segments
            .iter()
            .rev()
            .find(|s| s.size > 0)
            .map(|s| s.last_rn)
            .map(|rn| rn as u64 + self.first_rec_num)
    }
}

/// return an error:
///  - if there is a segment of size>0 following a segment of size 0
///  - if the last record numbers are not increasing
fn verify_consistency(segments: &[SegmentInfo]) -> Result<()> {
    println!("verifying consistency of segments {:?}", segments);

    for i in 1..segments.len() {
        if segments[i - 1].size == 0 && segments[i].size > 0 {
            return Err(YgwError::CorruptedRecordingFile(format!(
                "The segment {i} of size>0 is following a segment of size 0"
            )));
        }
        if segments[i].size > 0 && segments[i].last_rn <= segments[i - 1].last_rn {
            return Err(YgwError::CorruptedRecordingFile(format!(
                "The last record numbers are not increasing"
            )));
        }
    }
    Ok(())
}

pub struct FilePlayerIterator<'b> {
    fr: &'b FilePlayer,
    decoder: Option<zstd::Decoder<'b, BufReader<File>>>,
}

impl<'b> Iterator for FilePlayerIterator<'b> {
    type Item = Result<(u64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.decoder {
            None => None,
            Some(ref mut decoder) => match read_record(decoder) {
                Ok((rec_num, data)) => Some(Ok((self.fr.first_rec_num + rec_num as u64, data))),
                Err(YgwError::IOError(_, e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    self.decoder.take();
                    None
                }
                Err(e) => Some(Err(e)),
            },
        }
    }
}

fn read_record(decoder: &mut zstd::Decoder<BufReader<File>>) -> Result<(u32, Vec<u8>)> {
    let size = decoder.read_u32::<BigEndian>()?;
    if size > MAX_REC_SIZE {
        return Err(YgwError::DecodeError(format!(
            "Found record larger than the maximum allowed ({} > {})",
            size, MAX_REC_SIZE
        )));
    }
    if size < 4 {
        return Err(YgwError::DecodeError(format!(
            "Invalid record size {} (expected at least 4)",
            size
        )));
    }
    let rec_num = decoder.read_u32::<BigEndian>()?;
    let mut buf = vec![0; size as usize - 4];
    decoder.read_exact(&mut buf)?;

    Ok((rec_num, buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;
    use tempfile::tempdir;

    #[test]
    fn test1() {
        let tmp_dir = tempdir().unwrap();
        let path = tmp_dir.path().join("test1");

        let mut recorder = FileRecorder::new(&path, 1000, 2, 2).unwrap();

        //first_rec_num does not correspond to the recording
        let r = FilePlayer::open(&path, 300);
        assert!(matches!(r, Err(_)));

        let mut player = FilePlayer::open(&path, 1000).unwrap();
        let mut it = player.iter().unwrap();
        assert!(it.next().is_none());

        let data = vec![1u8; 10];

        recorder.append(1000, &data, false).unwrap();
        recorder.flush().unwrap();

        let mut player = FilePlayer::open(&path, 1000).unwrap();
        assert_eq!(player.first_rec_num, 1000);
        assert_eq!(player.segments.len(), 2);

        let mut it = player.iter().unwrap();
        check_equals(it.next(), 1000, &data);
        assert!(it.next().is_none());

        recorder.append(2000, &data, false).unwrap();
        recorder.flush().unwrap();

        let mut player = FilePlayer::open(&path, 1000).unwrap();
        let mut it = player.iter().unwrap();
        check_equals(it.next(), 1000, &data);
        check_equals(it.next(), 2000, &data);
        assert!(it.next().is_none());

        recorder.append(3000, &data, false).unwrap();
        assert_eq!(false, recorder.is_full());
        recorder.append(3001, &data, false).unwrap();
        assert_eq!(true, recorder.is_full());

        let mut player = FilePlayer::open(&path, 1000).unwrap();
        let mut it = player.iter().unwrap();
        check_equals(it.next(), 1000, &data);
        check_equals(it.next(), 2000, &data);
        check_equals(it.next(), 3000, &data);
        check_equals(it.next(), 3001, &data);
        assert!(it.next().is_none());
    }

    fn check_equals(
        actual: Option<Result<(u64, Vec<u8>)>>,
        expected_rec_num: u64,
        expected_data: &[u8],
    ) {
        if let Some(Ok((rec_num, ref data))) = actual {
            assert_eq!(rec_num, expected_rec_num);
            assert_eq!(data, expected_data);
        } else {
            panic!(
                "did not match, expected ({expected_rec_num}, {:?})  but got {:?}",
                expected_data, actual
            );
        }
    }
}
