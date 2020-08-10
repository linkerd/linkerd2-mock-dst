use futures::prelude::*;
use linkerd2_proxy_api::destination::{
    self as pb,
    destination_server::{Destination, DestinationServer},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    default::Default,
    fmt,
    hash::Hash,
    net::SocketAddr,
    sync::{Arc, Weak},
};
use tokio::sync::{mpsc, watch, RwLock};
use tracing_futures::Instrument;

pub use self::fs_watcher::FsWatcher;
pub use self::identity::IdentityService;
pub use self::spec::{EndpointsSpec, OverridesSpec, ParseError};

mod fs_watcher;
mod identity;
mod spec;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Clone, Debug)]
pub struct DstService {
    inner: Arc<Inner>,
}

#[derive(Debug)]
pub struct DstSender {
    endpoints: HashMap<Dst, watch::Sender<Endpoints>>,
    overrides: HashMap<Dst, watch::Sender<Overrides>>,
    inner: Weak<Inner>,
}

impl DstSender {
    #[tracing::instrument(skip(self), name = "DstSender::send_endpoints", level = "info")]
    pub async fn send_endpoints(&mut self, dst: Dst, endpoints: Endpoints) -> Result<(), Error> {
        if let Some(sender) = self.endpoints.get(&dst) {
            tracing::info!("Dst present");
            sender.broadcast(endpoints)?;
        } else {
            tracing::info!("Dst non present");
            if let Some(inner) = self.inner.upgrade() {
                let (tx, rx) = watch::channel(endpoints);
                self.endpoints.insert(dst.clone(), tx);
                inner.endpoints.write().await.insert(dst, rx);
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), name = "DstSender::delete_dst", level = "info")]
    pub async fn delete_dst(&mut self, dst: Dst) {
        if let Some(sender) = self.endpoints.remove(&dst) {
            tracing::info!("dropping sender");
            drop(sender);
            if let Some(inner) = self.inner.upgrade() {
                inner.endpoints.write().await.remove(&dst);
            }
        } else {
            tracing::info!("Dst not found");
        }
    }
}

#[derive(Debug)]
pub struct Inner {
    endpoints: RwLock<HashMap<Dst, watch::Receiver<Endpoints>>>,
    overrides: RwLock<HashMap<Dst, watch::Receiver<Overrides>>>,
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
pub struct Endpoints(HashMap<SocketAddr, EndpointMeta>);

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EndpointMeta {
    address: SocketAddr,
    h2: bool,
    weight: u32,
    #[serde(default)]
    metric_labels: BTreeMap<String, String>,
    tls_identity: Option<String>,
    authority_override: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
struct Overrides(HashMap<Dst, u32>);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Dst {
    name: String,
    port: u16,
}

type GrpcResult<T> = Result<T, tonic::Status>;

// === impl DstService ===

impl DstService {
    pub fn empty() -> (DstSender, DstService) {
        DstService::new(
            EndpointsSpec {
                dsts: HashMap::new(),
            },
            OverridesSpec {
                dsts: HashMap::new(),
            },
        )
    }

    pub fn new(endpoints: EndpointsSpec, overrides: OverridesSpec) -> (DstSender, DstService) {
        let mut endpoints_txs = HashMap::new();
        let mut endpoints_rxs = HashMap::new();
        for (dst, eps) in endpoints.dsts.into_iter() {
            let (tx, rx) = watch::channel(eps.clone());
            endpoints_txs.insert(dst.clone(), tx);
            endpoints_rxs.insert(dst.clone(), rx);
            tracing::info!(?dst, ?eps, "added");
        }

        let mut overrides_txs = HashMap::new();
        let mut overrides_rxs = HashMap::new();
        for (dst, eps) in overrides.dsts.into_iter() {
            let (tx, rx) = watch::channel(eps.clone());
            overrides_txs.insert(dst.clone(), tx);
            overrides_rxs.insert(dst.clone(), rx);
            tracing::info!(?dst, ?eps, "added");
        }

        let inner = Arc::new(Inner {
            endpoints: RwLock::new(endpoints_rxs),
            overrides: RwLock::new(overrides_rxs),
        });
        let sender = DstSender {
            endpoints: endpoints_txs,
            overrides: overrides_txs,
            inner: Arc::downgrade(&inner),
        };
        (sender, Self { inner })
    }

    /// Serves the mock Destination service on the specified socket address.
    pub async fn serve(self, addr: impl Into<SocketAddr>) -> Result<(), Error> {
        let addr = addr.into();
        let span = tracing::info_span!("DstService::serve", listen.addr = %addr);
        tracing::info!(parent: &span, "Starting destination server...");

        tonic::transport::Server::builder()
            .trace_fn(|headers| tracing::debug_span!("request", ?headers))
            .add_service(DestinationServer::new(self))
            .serve(addr)
            .instrument(span)
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self), level = "info")]
    async fn stream_endpoints(&self, dst: &Dst) -> mpsc::Receiver<GrpcResult<pb::Update>> {
        let mut endpoints_rx = match self.inner.endpoints.read().await.get(dst) {
            Some(rx) => rx.clone(),
            None => {
                tracing::info!("Does not exist");
                let (mut tx, rx) = mpsc::channel(1);
                let _ = tx
                    .send(Err(tonic::Status::invalid_argument("not configured")))
                    .await;
                return rx;
            }
        };

        let concrete_name = dst.to_string();

        tracing::info!("Serving endpoints");
        let (mut tx, rx) = mpsc::channel(8);
        tokio::spawn(
            async move {
                let mut prev = HashMap::new();

                while let Some(Endpoints(curr)) = endpoints_rx.recv().await {
                    if curr.is_empty() {
                        tx.send(Ok(pb::Update {
                            update: Some(pb::update::Update::NoEndpoints(pb::NoEndpoints {
                                exists: true,
                            })),
                        }))
                        .await?
                    } else {
                        let added = curr
                            .values()
                            .filter(|meta| meta.is_add(&prev))
                            .map(EndpointMeta::to_weighted_addr)
                            .collect::<Vec<_>>();
                        if !added.is_empty() {
                            tracing::debug!(?added);

                            let mut metric_labels = HashMap::default();
                            metric_labels.insert("concrete".to_string(), concrete_name.clone());

                            tx.send(Ok(pb::Update {
                                update: Some(pb::update::Update::Add(pb::WeightedAddrSet {
                                    addrs: added,
                                    metric_labels,
                                })),
                            }))
                            .await?;
                        }

                        let removed = prev
                            .keys()
                            .filter(|addr| !curr.contains_key(addr))
                            .map(Into::into)
                            .collect::<Vec<_>>();
                        if !removed.is_empty() {
                            tracing::debug!(?removed);
                            tx.send(Ok(pb::Update {
                                update: Some(pb::update::Update::Remove(pb::AddrSet {
                                    addrs: removed,
                                })),
                            }))
                            .await?;
                        }
                    }

                    prev = curr;
                }
                tracing::debug!("Watch ended");
                tx.send(Ok(pb::Update {
                    update: Some(pb::update::Update::NoEndpoints(pb::NoEndpoints {
                        exists: false,
                    })),
                }))
                .await?;
                Ok(())
            }
            .map_err(|_: mpsc::error::SendError<_>| tracing::info!("Lookup closed"))
            .in_current_span(),
        );

        rx
    }

    #[tracing::instrument(skip(self), level = "info")]
    async fn stream_overrides(
        &self,
        dst: &Dst,
    ) -> mpsc::Receiver<GrpcResult<pb::DestinationProfile>> {
        let mut overrides_rx = match self.inner.overrides.read().await.get(dst) {
            Some(rx) => rx.clone(),
            None => {
                tracing::info!("Does not exist");
                let (mut tx, rx) = mpsc::channel(1);
                let _ = tx
                    .send(Err(tonic::Status::invalid_argument("not configured")))
                    .await;
                return rx;
            }
        };

        tracing::info!("Serving endpoints");
        let (mut tx, rx) = mpsc::channel(8);
        tokio::spawn(
            async move {
                while let Some(Overrides(overrides)) = overrides_rx.recv().await {
                    tracing::debug!(?overrides);

                    let dst_overrides = overrides
                        .iter()
                        .map(|(dst, weight)| pb::WeightedDst {
                            authority: dst.to_string(),
                            weight: *weight,
                        })
                        .collect::<Vec<_>>();

                    tx.send(Ok(pb::DestinationProfile {
                        dst_overrides,
                        ..Default::default()
                    }))
                    .await?;
                }
                tracing::debug!("Watch ended");
                Ok(())
            }
            .map_err(|_: mpsc::error::SendError<_>| tracing::info!("Lookup closed"))
            .in_current_span(),
        );

        rx
    }
}

#[tonic::async_trait]
impl Destination for DstService {
    type GetStream = mpsc::Receiver<GrpcResult<pb::Update>>;
    type GetProfileStream = mpsc::Receiver<GrpcResult<pb::DestinationProfile>>;

    async fn get(
        &self,
        req: tonic::Request<pb::GetDestination>,
    ) -> GrpcResult<tonic::Response<Self::GetStream>> {
        let pb::GetDestination { path, .. } = req.into_inner();
        let dst = path
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("invalid dst"))?;
        let stream = self.stream_endpoints(&dst).await;
        Ok(tonic::Response::new(stream))
    }

    async fn get_profile(
        &self,
        req: tonic::Request<pb::GetDestination>,
    ) -> GrpcResult<tonic::Response<Self::GetProfileStream>> {
        let pb::GetDestination { path, .. } = req.into_inner();
        let dst = path
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("invalid dst"))?;
        let stream = self.stream_overrides(&dst).await;
        Ok(tonic::Response::new(stream))
    }
}

// === impl Dst ===

impl fmt::Display for Dst {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.name, self.port)
    }
}

// === impl EndpointMeta ===

impl EndpointMeta {
    fn to_weighted_addr(&self) -> pb::WeightedAddr {
        let protocol_hint = if self.h2 {
            Some(pb::ProtocolHint {
                protocol: Some(pb::protocol_hint::Protocol::H2(
                    pb::protocol_hint::H2::default(),
                )),
            })
        } else {
            None
        };

        let mut metric_labels = HashMap::default();
        metric_labels.insert("addr".to_string(), self.address.to_string());
        metric_labels.insert("h2".to_string(), self.h2.to_string());
        metric_labels.extend(self.metric_labels.clone());

        let ref addr = self.address;
        pb::WeightedAddr {
            addr: Some(addr.into()),
            weight: self.weight,
            protocol_hint,
            metric_labels,
            tls_identity: self.tls_identity.clone().map(|name| pb::TlsIdentity {
                strategy: Some(pb::tls_identity::Strategy::DnsLikeIdentity(
                    pb::tls_identity::DnsLikeIdentity { name },
                )),
            }),
            authority_override: self
                .authority_override
                .clone()
                .map(|authority_override| pb::AuthorityOverride { authority_override }),
        }
    }

    fn is_add(&self, prev: &HashMap<SocketAddr, EndpointMeta>) -> bool {
        match prev.get(&self.address) {
            Some(prev_ep) => prev_ep != self,
            None => true,
        }
    }
}
