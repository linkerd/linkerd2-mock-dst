use crate::DstSender;
use crate::Endpoints;
use inotify::{Event, EventMask, Inotify, WatchMask};
use inotify_sys as ffi;
use serde_json;
use std::error::Error;
use std::ffi::OsString;
use std::mem;
use std::{fs, path::PathBuf};
use tokio::stream::StreamExt;

use crate::Dst;
use crate::EndpointMeta;

const EVENT_BUF_SZ: usize =
    mem::size_of::<ffi::inotify_event>() + (libc::FILENAME_MAX as usize) + 1;

#[derive(Debug)]
pub struct FsWatcher {
    endpoints_dir: PathBuf,
    dst_sender: DstSender,
}

impl FsWatcher {
    pub fn new(endpoints_dir: PathBuf, dst_sender: DstSender) -> Self {
        Self {
            endpoints_dir,
            dst_sender,
        }
    }

    #[tracing::instrument(skip(self), name = "FsWatcher::handle_event", level = "info")]
    fn handle_event(
        &mut self,
        ev: Event<OsString>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        if let Some(file_name) = ev.name.and_then(|s| s.to_str().map(|s| s.to_string())) {
            let dst: Dst = file_name.parse()?;
            if ev.mask == EventMask::DELETE {
                tracing::info!(?dst, "deleted");
                self.dst_sender.delete_dst(dst);
            } else {
                let path = self.endpoints_dir.join(file_name);
                let contents = fs::read_to_string(path)?;
                let dsts = serde_json::from_str::<Vec<EndpointMeta>>(&contents)?
                    .into_iter()
                    .map(|e| (e.address, e))
                    .collect();
                let endpoints = Endpoints(dsts);
                tracing::info!(?endpoints, "added");
                self.dst_sender.send_endpoints(dst, endpoints)?
            }
        }
        Ok(())
    }

    pub async fn watch(&mut self) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let mut inotify = Inotify::init()?;
        let mask = WatchMask::MODIFY | WatchMask::DELETE;
        inotify.add_watch(self.endpoints_dir.clone(), mask)?;
        let stream = inotify.event_stream(vec![0; EVENT_BUF_SZ])?;
        stream
            .map(|event| {
                match event {
                    Ok(event) => {
                        if let Err(e) = self.handle_event(event) {
                            tracing::error!(?e, "error handing event");
                        }
                    }
                    Err(e) => tracing::error!(?e, "inotify stream error"),
                }
                Ok(())
            })
            .collect()
            .await
    }
}
