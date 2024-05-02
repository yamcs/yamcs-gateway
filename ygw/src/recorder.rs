//! The recorder handles multiple recording files.
//! The files have the name ygw_rec_<rn>.dat where <rn> is the number of the first record in hexadecimal.
//!
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use tokio::sync::mpsc::Receiver;

use crate::{
    msg::EncodedMessage,
    record_file::{FilePlayer, FileRecorder},
    Result, YgwError,
};

pub struct Recorder {
    /// directory where data is being recorded
    dir: PathBuf,

    /// record files indexed by the first record number
    files: BTreeMap<u64, PathBuf>,
}

impl Recorder {
    ///Creates a new recorder
    /// scans all the existing recording files returning also the number of the last record if any
    pub fn new(dir: &Path) -> Result<(Self, Option<u64>)> {
        let mut files = BTreeMap::new();

        //read all files that match the ygw_rec_
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let filename = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue, // Ignore files with non-UTF8 names
            };

            if filename.starts_with("ygw_rec_") && filename.ends_with(".dat") {
                let hex = &filename[8..filename.len() - 4];
                if let Ok(rn) = u64::from_str_radix(hex, 16) {
                    files.insert(rn, path);
                } else {
                    log::warn!("Failed to parse record number from: {:?}", path);
                }
            }
        }

        // parse the last file to get the last record number
        let last_rn = match files.last_key_value() {
            Some((rn, path)) => {
                let player = FilePlayer::open(path, *rn)?;
                if let Some(rn) = player.get_last_record_number() {
                    Some(rn)
                } else {
                    return Err(YgwError::CorruptedRecordingFile(format!(
                        "{}: file has no record",
                        path.display()
                    )));
                }
            }
            None => None,
        };

        let recorder = Recorder {
            dir: dir.to_owned(),
            files,
        };

        Ok((recorder, last_rn))
    }

    pub async fn record(&mut self, mut rx: Receiver<EncodedMessage>) -> Result<()> {
        let mut file_recorder: Option<FileRecorder> = None;

        loop {
            match rx.recv().await {
                Some(msg) => {
                    let rn = msg.rn();
                    if file_recorder.is_none() {
                        let filename = format!("ygw_rec_{:016x}.dat", rn);
                        log::info!("Starting a new recording {filename}");
                        let path = self.dir.join(filename);
                        file_recorder = Some(FileRecorder::new(&path, rn, 100, 1000)?);
                        self.files.insert(msg.rn(), path);
                    }
                    if let Some(ref mut fr) = file_recorder {
                        fr.append(rn, msg.data(), false)?;
                        if fr.is_full() {
                            file_recorder = None;
                        }
                    }
                }
                None => break,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::record_file::FileRecorder;
    use tempfile::tempdir;

    use super::*;
    fn last_rn(dir: &Path) -> Result<Option<u64>> {
        let r = Recorder::new(dir)?;
        Ok(r.1)
    }
    #[test]
    fn test2() {
        let data = vec![1u8; 10];
        let tmp_dir = tempdir().unwrap();
        let tmp_path = tmp_dir.path();

        assert_eq!(None, last_rn(tmp_path).unwrap());

        let mut recorder =
            FileRecorder::new(&tmp_path.join(&format!("ygw_rec_{:x}.dat", 0)), 0, 2, 2).unwrap();
        recorder.append(0, &data, false).unwrap();
        drop(recorder);

        let mut recorder =
            FileRecorder::new(&tmp_path.join(&format!("ygw_rec_{:x}.dat", 10)), 10, 2, 2).unwrap();

        recorder.append(10, &data, false).unwrap();
        recorder.flush().unwrap();

        assert_eq!(Some(10), last_rn(tmp_path).unwrap());

        recorder.append(11, &data, false).unwrap();
        drop(recorder);

        assert_eq!(Some(11), last_rn(tmp_path).unwrap());
    }
}
