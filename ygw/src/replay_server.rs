use std::net::SocketAddr;

use bytes::Buf;
use prost::Message;
use tokio::{
    io::AsyncWriteExt,
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    sync::{mpsc::Sender, oneshot},
};
use tokio_stream::StreamExt;
use tokio_util::{
    codec::{FramedRead, LengthDelimitedCodec},
    sync::CancellationToken,
};

use crate::{
    hex8, msg::VERSION, protobuf, record_file::FilePlayer, recorder::FileQuery, Result, YgwError,
};

pub async fn start_replay_server(
    addr: SocketAddr,
    query_tx: Sender<FileQuery>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let socket = TcpListener::bind(addr).await?;
    log::info!("Starting replay server listening to {}", addr);

    loop {
        let (stream, addr) = socket.accept().await?;
        log::debug!("Accepted connection from {}", addr);
        let query_tx = query_tx.clone();
        let ct = cancel_token.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, query_tx, ct).await {
                log::warn!("Replay ended with error {:?}", e);
            }
        });
    }
}

async fn handle_client(
    socket: TcpStream,
    query_tx: Sender<FileQuery>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let addr = socket.peer_addr()?;
    let (reader, writer) = socket.into_split();
    let mut codec = LengthDelimitedCodec::new();
    codec.set_max_frame_length(100);
    let mut stream = FramedRead::new(reader, codec);

    tokio::select! {
        result = stream.next() => {
            match result {
                Some(Ok(buf)) => {
                    let mut buf = buf.freeze();
                    log::trace!("Received message {:}", hex8(&buf));
                    let version = buf.get_u8();
                    if version != crate::msg::VERSION {
                        return Err(YgwError::DecodeError(format!(
                            "Invalid message version {} received on the replay socket(expected {})",
                            version, crate::msg::VERSION
                        )));
                    }
                    match protobuf::ygw::ReplayRequest::decode(buf) {
                        Ok(req) => {
                            execute_replay(query_tx, writer, req).await?;
                        }
                        Err(e) =>  {
                            log::warn!("Error decoding replay request from {}: {:?}", addr, e);
                        }
                    }
                }
                Some(Err(e)) => {
                    //this can happen if the length of the data (first 4 bytes) is longer than the max
                    //also if the socket closes in the middle of a message
                    log::warn!("Error reading replay request from {}: {:?}", addr, e);
                }
                None => {
                    log::warn!("Yamcs connection {} closed", addr);
                }
            }
        }
        _ = cancel_token.cancelled() => {
            log::debug!("Replay task for {} received cancel signal", addr);
        }
    }

    Ok(())
}

async fn execute_replay(
    query_tx: Sender<FileQuery>,
    mut writer: OwnedWriteHalf,
    req: protobuf::ygw::ReplayRequest,
) -> Result<()> {
    log::info!("Executing replay request {:?}", req);
    //first query the recorder for the files containing the transactions
    let (resp_tx, resp_rx) = oneshot::channel();
    let query = FileQuery {
        rn_start: req.start_rn,
        rn_stop: req.stop_rn,
        reply: resp_tx,
    };

    if query_tx.send(query).await.is_err() {
        log::warn!("Recorder unavailable\n");
    }

    let Ok(paths) = resp_rx.await else {
        log::warn!("Recorder failed to respond");
        return Err(YgwError::Generic("No response from recorder".into()));
    };
    log::debug!("Recorder send the following paths {:?}", paths);

    for (rn, path) in paths {
        log::debug!("Replaying file {}", path.display());
        let mut player = match FilePlayer::open(&path, rn) {
            Ok(p) => p,
            Err(e) => {
                log::warn!("Failed to open file {}: {}", path.display(), e);
                continue; // skip this file
            }
        };

        let mut it = player.iter_from_segment_with_rn(req.start_rn)?;
        while let Some(record) = it.next() {
            match record {
                Ok((rec_num, data)) => {
                    if rec_num < req.start_rn {
                        continue;
                    }
                    if rec_num > req.stop_rn {
                        break;
                    }
                    let len = data.len() + 9;
                    writer.write_u32(len as u32).await?;
                    writer.write_u8(VERSION).await?;
                    writer.write_u64(rec_num).await?;
                    writer.write_all(&data).await?;
                }
                Err(e) => {
                    log::warn!("Error reading record from {}: {}", path.display(), e);
                    break; // Skip rest of file on error
                }
            }
        }
    }

    Ok(())
}
