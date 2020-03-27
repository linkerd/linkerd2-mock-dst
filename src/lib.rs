use futures::prelude::*;
use linkerd2_proxy_api_tonic::destination as pb;
pub use pb::destination_server::DestinationServer;
use pb::{destination_server::Destination, DestinationProfile, GetDestination, Update};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    net::SocketAddr,
    sync::{Arc, Weak},
};
use tracing_futures::Instrument;

use tokio::sync::{mpsc, watch, RwLock};

pub use self::spec::{DstSpec, ParseError};
mod spec;

#[derive(Debug, Clone)]
pub struct DstService {
    state: Arc<SharedState<Endpoints>>,
}

#[derive(Debug)]
pub struct DstHandle {
    dsts: HashMap<Dst, watch::Sender<Endpoints>>,
    rxs: Weak<SharedState<Endpoints>>,
}

type SharedState<T> = RwLock<HashMap<Dst, watch::Receiver<T>>>;

#[derive(Debug, PartialEq, Eq, Default, Clone)]
struct Endpoints(HashSet<SocketAddr>);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Dst {
    scheme: String,
    name: String,
}

type GrpcResult<T> = Result<T, tonic::Status>;

impl DstSpec {
    pub fn into_svc(self) -> (DstHandle, DstService) {
        DstService::from_spec(self)
    }
}

// === impl DstService ===

impl DstService {
    fn from_spec(spec: DstSpec) -> (DstHandle, DstService) {
        let mut txs = HashMap::new();
        let mut rxs = HashMap::new();
        for (dst, eps) in spec.dsts.into_iter() {
            let (tx, rx) = watch::channel(eps);
            txs.insert(dst.clone(), tx);
            rxs.insert(dst, rx);
        }
        let state = Arc::new(RwLock::new(rxs));
        let rxs = Arc::downgrade(&state);
        let handle = DstHandle { dsts: txs, rxs };
        (handle, Self { state })
    }

    /// Serves the mock Destination service on the specified socket address.
    pub async fn serve(
        self,
        addr: impl Into<SocketAddr>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let addr = addr.into();
        let span = tracing::info_span!("DstService::serve", listen.addr = %addr);
        tracing::info!(parent: &span, "starting destination server...");

        tonic::transport::Server::builder()
            .add_service(DestinationServer::new(self))
            .serve(addr)
            .instrument(span)
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self), level = "info")]
    async fn stream_destination(
        &self,
        scheme: String,
        name: String,
    ) -> mpsc::Receiver<GrpcResult<Update>> {
        let state = self.state.read().await.get(&Dst { scheme, name }).cloned();
        if let Some(mut state) = state {
            tracing::info!("destination exists");
            let (mut tx, rx) = mpsc::channel(8);
            tokio::spawn(
                async move {
                    let mut prev = HashSet::new();
                    while let Some(Endpoints(curr)) = state.recv().await {
                        let added = curr
                            .difference(&prev)
                            .map(|addr| pb::WeightedAddr {
                                addr: Some(addr.into()),
                                ..Default::default()
                            })
                            .collect::<Vec<_>>();
                        let removed = prev.difference(&curr).map(Into::into).collect::<Vec<_>>();
                        if !removed.is_empty() {
                            tracing::debug!(?removed);
                            tx.send(Ok(pb::Update {
                                update: Some(pb::update::Update::Remove(pb::AddrSet {
                                    addrs: removed,
                                })),
                            }))
                            .await?;
                        }
                        if !added.is_empty() {
                            tracing::debug!(?added);
                            tx.send(Ok(pb::Update {
                                update: Some(pb::update::Update::Add(pb::WeightedAddrSet {
                                    addrs: added,
                                    ..Default::default()
                                })),
                            }))
                            .await?;
                        }
                        prev = curr;
                    }
                    tracing::debug!("watch ended");
                    Ok(())
                }
                .map_err(|_: mpsc::error::SendError<_>| tracing::info!("lookup closed"))
                .in_current_span(),
            );
            rx
        } else {
            tracing::info!("destination does not exist");
            let (mut tx, rx) = mpsc::channel(1);
            let _ = tx
                .send(Ok(pb::Update {
                    update: Some(pb::update::Update::NoEndpoints(pb::NoEndpoints {
                        exists: true,
                    })),
                }))
                .await;
            rx
        }
    }
}

#[tonic::async_trait]
impl Destination for DstService {
    type GetStream = mpsc::Receiver<GrpcResult<Update>>;

    type GetProfileStream = mpsc::Receiver<GrpcResult<DestinationProfile>>;
    async fn get(
        &self,
        req: tonic::Request<GetDestination>,
    ) -> GrpcResult<tonic::Response<Self::GetStream>> {
        let req_span = tracing::debug_span!("dst_request", remote.addr = ?req.remote_addr(), metadata = ?req.metadata());
        async {
            tracing::trace!("received destination request");
            let GetDestination {
                scheme,
                path,
                context_token,
            } = req.into_inner();
            tracing::debug!(?context_token);
            let stream = self.stream_destination(scheme, path).await;
            Ok(tonic::Response::new(stream))
        }
        .instrument(req_span)
        .await
    }

    async fn get_profile(
        &self,
        req: tonic::Request<GetDestination>,
    ) -> GrpcResult<tonic::Response<Self::GetProfileStream>> {
        let req_span = tracing::debug_span!("profile_request", remote.addr = ?req.remote_addr(), metadata = ?req.metadata());
        async {
            tracing::trace!("received profile request");
            let GetDestination {
                scheme,
                path,
                context_token,
            } = req.into_inner();
            tracing::debug!(?context_token);
            tracing::info!(?scheme, ?path, "profiles are not yet implemented");
            Err(tonic::Status::invalid_argument(
                "profiles are not yet implemented",
            ))
        }
        .instrument(req_span)
        .await
    }
}
