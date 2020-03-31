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
use tokio::sync::{mpsc, watch, RwLock};
use tracing_futures::Instrument;

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
    name: String,
    port: u16,
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
    async fn stream_destination(&self, dst: &Dst) -> mpsc::Receiver<GrpcResult<Update>> {
        let state = self.state.read().await.get(dst).cloned();
        if let Some(mut state) = state {
            tracing::info!("Serving endpoints");
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
                    tracing::debug!("Watch ended");
                    Ok(())
                }
                .map_err(|_: mpsc::error::SendError<_>| tracing::info!("Lookup closed"))
                .in_current_span(),
            );
            rx
        } else {
            tracing::info!("Does not exist");
            let (mut tx, rx) = mpsc::channel(1);
            let _ = tx
                .send(Ok(pb::Update {
                    update: Some(pb::update::Update::NoEndpoints(pb::NoEndpoints {
                        exists: false,
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
        let GetDestination { path, .. } = req.into_inner();
        let dst = path
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("invalid dst"))?;
        let stream = self.stream_destination(&dst).await;
        Ok(tonic::Response::new(stream))
    }

    async fn get_profile(
        &self,
        _: tonic::Request<GetDestination>,
    ) -> GrpcResult<tonic::Response<Self::GetProfileStream>> {
        tracing::debug!("Not implemented");
        Err(tonic::Status::invalid_argument("not implemented"))
    }
}
