//! The recorder handles multiple recording files.
//! The files have the name ygw_rec_<rn>.dat where <rn> is the number of the first record in hexadecimal.
//!
use std::{
    fs,
    path::{Path, PathBuf},
};

use tokio::sync::{mpsc::Receiver, oneshot};

use crate::{
    msg::EncodedMessage,
    record_file::{FilePlayer, FileRecorder},
    Result,
};

pub struct Recorder {
    /// directory where data is being recorded
    dir: PathBuf,

    /// record files sorted by the first record number
    files: Vec<(u64, PathBuf)>,

    flush_after_each_record: bool,
}

///this is used to query the recorder on the files containg the records between start and stop (both inclusive)
/// the recorder will respond with a vector of files and start record numbers
pub struct FileQuery {
    pub rn_start: u64,
    pub rn_stop: u64,
    pub reply: oneshot::Sender<Vec<(u64, PathBuf)>>,
}

impl Recorder {
    ///Creates a new recorder
    /// scans all the existing recording files returning also the number of the last record if any
    pub fn new(dir: &Path) -> Result<(Self, Option<u64>)> {
        let mut files = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let filename = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };

            if filename.starts_with("ygw_rec_") && filename.ends_with(".dat") {
                let hex = &filename[8..filename.len() - 4];
                if let Ok(rn) = u64::from_str_radix(hex, 16) {
                    files.push((rn, path));
                } else {
                    log::warn!("Invalid filename: {:?}", filename);
                }
            }
        }
        files.sort_by_key(|(rn, _)| *rn);

        let last_rn = match files.last() {
            Some((start_rn, path)) => {
                let player = FilePlayer::open(&path, *start_rn)?;
                player.get_last_record_number()
            }
            None => None,
        };
        log::debug!(
            "Read the following recording files {:?}, last_rn: {:?}",
            files,
            last_rn
        );
        let recorder = Recorder {
            dir: dir.to_owned(),
            files,
            flush_after_each_record: false,
        };

        Ok((recorder, last_rn))
    }

    pub async fn record(
        &mut self,
        mut rx: Receiver<EncodedMessage>,
        mut query_rx: Receiver<FileQuery>,
    ) -> Result<()> {
        let mut file_recorder: Option<FileRecorder> = None;

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    let rn = msg.rn();
                    if file_recorder.is_none() {
                        let filename = format!("ygw_rec_{:016x}.dat", rn);
                        log::info!("Starting a new recording {filename}");
                        let path = self.dir.join(&filename);
                        file_recorder = Some(FileRecorder::new(&path, rn, 100, 1000)?);
                        self.files.push((rn, path));
                    }
                    if let Some(ref mut fr) = file_recorder {
                        fr.append(rn, msg.data(), false)?;

                        if fr.is_full() {
                            file_recorder = None;
                        } else if self.flush_after_each_record {
                            fr.flush()?;
                        }
                    }
                }

                Some(query) = query_rx.recv() => {
                    //received query from the replay server, find all the files containing records of interest

                    let result = self.find_files_covering_range(query.rn_start, query.rn_stop);

                    //flush the current file to make sure the replay server will find the data when opening the file for replay
                    if let Some(ref mut fr) = file_recorder {
                        fr.flush()?;
                    }
                    let _ = query.reply.send(result); // Ignore error if receiver is dropped
                }

                else => break,
            }
        }

        if let Some(ref mut fr) = file_recorder {
            fr.flush()?;
        }
        log::debug!("Recorder quitting");
        Ok(())
    }

    pub fn find_files_covering_range(&self, rn_start: u64, rn_stop: u64) -> Vec<(u64, PathBuf)> {
        let files = &self.files;

        // Binary search to find the insertion point for rn_start
        let idx = files
            .binary_search_by_key(&rn_start, |(start_rn, _)| *start_rn)
            .unwrap_or_else(|i| i.saturating_sub(1));

        let mut result = Vec::new();

        for i in idx..files.len() {
            let (current_start, current_path) = &files[i];
            let next_start = files.get(i + 1).map(|(next_start, _)| *next_start);

            let overlaps = match next_start {
                Some(next_rn) => *current_start <= rn_stop && next_rn > rn_start,
                None => *current_start <= rn_stop,
            };

            if overlaps {
                result.push((*current_start, current_path.clone()));
            } else if *current_start > rn_stop {
                break;
            }
        }

        result
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
