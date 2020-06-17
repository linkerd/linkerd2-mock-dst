use futures::prelude::*;
use linkerd2_proxy_api::destination::{
    self as pb,
    destination_server::{Destination, DestinationServer},
};
use std::{
    collections::HashMap,
    error::Error,
    fmt,
    hash::Hash,
    net::SocketAddr,
    sync::{Arc, Weak},
};
use tokio::sync::{mpsc, watch, RwLock};
use tracing_futures::Instrument;

pub use self::spec::{EndpointsSpec, OverridesSpec, ParseError};
mod spec;

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

#[derive(Debug)]
pub struct Inner {
    endpoints: RwLock<HashMap<Dst, watch::Receiver<Endpoints>>>,
    overrides: RwLock<HashMap<Dst, watch::Receiver<Overrides>>>,
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
struct Endpoints(HashMap<SocketAddr, EndpointMeta>);

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct EndpointMeta {
    h2: bool,
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
struct Overrides(HashMap<Dst, u32>);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Dst {
    name: String,
    port: u16,
}

type GrpcResult<T> = Result<T, tonic::Status>;

// === impl DstService ===

impl DstService {
    pub fn new(endpoints: EndpointsSpec, overrides: OverridesSpec) -> (DstSender, DstService) {
        let mut endpoints_txs = HashMap::new();
        let mut endpoints_rxs = HashMap::new();
        for (dst, eps) in endpoints.dsts.into_iter() {
            let (tx, rx) = watch::channel(eps);
            endpoints_txs.insert(dst.clone(), tx);
            endpoints_rxs.insert(dst, rx);
        }

        let mut overrides_txs = HashMap::new();
        let mut overrides_rxs = HashMap::new();
        for (dst, eps) in overrides.dsts.into_iter() {
            let (tx, rx) = watch::channel(eps);
            overrides_txs.insert(dst.clone(), tx);
            overrides_rxs.insert(dst, rx);
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
    pub async fn serve(
        self,
        addr: impl Into<SocketAddr>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
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
                    .send(Ok(pb::Update {
                        update: Some(pb::update::Update::NoEndpoints(pb::NoEndpoints {
                            exists: false,
                        })),
                    }))
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
                    let added = curr
                        .iter()
                        .filter(|(addr, _)| !prev.contains_key(*addr))
                        .map(|(addr, EndpointMeta { h2 })| {
                            let protocol_hint = if *h2 {
                                Some(pb::ProtocolHint {
                                    protocol: Some(pb::protocol_hint::Protocol::H2(
                                        pb::protocol_hint::H2::default(),
                                    )),
                                })
                            } else {
                                None
                            };

                            let mut metric_labels = HashMap::default();
                            metric_labels.insert("addr".to_string(), addr.to_string());
                            metric_labels.insert("h2".to_string(), h2.to_string());

                            pb::WeightedAddr {
                                addr: Some(addr.into()),
                                protocol_hint,
                                metric_labels,
                                ..Default::default()
                            }
                        })
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

                    prev = curr;
                }
                tracing::debug!("Watch ended");
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
                let _ = tx.send(Ok(pb::DestinationProfile::default())).await;
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
