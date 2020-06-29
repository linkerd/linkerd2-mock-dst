use crate::Dst;
use crate::DstSender;
use crate::EndpointMeta;
use crate::Endpoints;
use crate::Error;
use inotify::{Event, EventMask, Inotify, WatchMask};
use inotify_sys as ffi;
use serde_json;
use serde_yaml;
use std::ffi::OsString;
use std::mem;
use std::path::PathBuf;
use std::string::String;
use tokio::stream::StreamExt;

const EVENT_BUF_SZ: usize =
    mem::size_of::<ffi::inotify_event>() + (libc::FILENAME_MAX as usize) + 1;

#[derive(Debug)]
pub struct FsWatcher {
    endpoints_dir: PathBuf,
    dst_sender: DstSender,
}

#[derive(Debug)]
enum FileType {
    Yaml,
    Json,
}

#[derive(Debug)]
pub struct FsWatcherError {
    reason: String,
}

macro_rules! fs_watcher_error {
    ($reason:expr) => {{
        return Err(Box::new(FsWatcherError { reason: $reason }));
    }};
}

// === impl FsWatcherError ===

impl std::fmt::Display for FsWatcherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.reason, f)
    }
}

impl std::error::Error for FsWatcherError {}

impl FsWatcher {
    pub fn new(endpoints_dir: PathBuf, dst_sender: DstSender) -> Self {
        Self {
            endpoints_dir,
            dst_sender,
        }
    }

    fn parse_dst(file_name: &str) -> Result<(Dst, FileType), Error> {
        let mut parts = file_name.rsplitn(2, ".");
        match (parts.next(), parts.next()) {
            (Some(ext), Some(name)) => {
                let dst: Dst = name.parse()?;
                let ft = match ext {
                    "yaml" | "yml" => FileType::Yaml,
                    "json" => FileType::Json,
                    _ => fs_watcher_error!(format!("invalid file ext {}", ext)),
                };
                Ok((dst, ft))
            }
            _ => fs_watcher_error!(format!("invalid file name {}", file_name)),
        }
    }

    async fn parse_file(&self, file_name: &str, ft: FileType) -> Result<Endpoints, Error> {
        let path = self.endpoints_dir.join(file_name);
        let contents = tokio::fs::read_to_string(path).await?;
        let destinations = match ft {
            FileType::Json => {
                serde_json::from_str::<Vec<EndpointMeta>>(&contents).map_err(Into::into)
            }
            FileType::Yaml => {
                serde_yaml::from_str::<Vec<EndpointMeta>>(&contents).map_err(Into::into)
            }
        };

        destinations.map(|dsts| Endpoints(dsts.into_iter().map(|e| (e.address, e)).collect()))
    }

    #[tracing::instrument(skip(self), name = "FsWatcher::handle_event", level = "info")]
    async fn handle_event(&mut self, ev: Event<OsString>) -> Result<(), Error> {
        if let Some(file_name) = ev.name.and_then(|s| s.to_str().map(|s| s.to_string())) {
            let (dst, ft) = Self::parse_dst(&file_name)?;
            if ev.mask == EventMask::DELETE {
                tracing::info!(?dst, "deleted");
                self.dst_sender.delete_dst(dst).await;
            } else {
                let endpoints = self.parse_file(&file_name, ft).await?;
                tracing::info!(?endpoints, "added");
                self.dst_sender.send_endpoints(dst, endpoints).await?;
            }
        }
        Ok(())
    }

    pub async fn watch(&mut self) -> Result<(), Error> {
        let mut inotify = Inotify::init()?;
        let mask = WatchMask::MODIFY | WatchMask::DELETE;
        inotify.add_watch(self.endpoints_dir.clone(), mask)?;
        let mut stream = inotify.event_stream(vec![0; EVENT_BUF_SZ])?;
        while let Some(event) = stream.next().await {
            match event {
                Ok(event) => {
                    if let Err(e) = self.handle_event(event).await {
                        tracing::error!(%e, "error handing event");
                    }
                }
                Err(e) => tracing::error!(%e, "inotify stream error"),
            }
        }
        Ok(())
    }
}
