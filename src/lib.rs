use futures::prelude::*;
use linkerd2_proxy_api_tonic::destination as pb;
pub use pb::destination_server::DestinationServer;
use pb::{destination_server::Destination, DestinationProfile, GetDestination, Update};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, Weak},
};
use tracing_futures::Instrument;

use tokio::sync::{mpsc, watch, RwLock};
use tracing_error::TracedError;
#[derive(Debug, Default)]
pub struct DstSpec {
    dsts: HashMap<Dst, Endpoints>,
}

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

#[derive(Debug)]
pub struct ParseError {
    reason: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Dst {
    scheme: String,
    name: String,
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
struct Endpoints(HashSet<SocketAddr>);

macro_rules! parse_error {
    ($reason:expr) => {{
        return tracing_error::InstrumentResult::in_current_span(Err(ParseError {
            reason: $reason,
        }));
    }};
}

type GrpcResult<T> = Result<T, tonic::Status>;

// === impl DstSpec ===

impl FromStr for DstSpec {
    type Err = TracedError<ParseError>;
    #[tracing::instrument(name = "DstSpec::from_str", level = "error")]
    fn from_str(spec: &str) -> Result<Self, Self::Err> {
        #[tracing::instrument(level = "info")]
        fn parse_entry(entry: &str) -> Result<(Dst, Endpoints), TracedError<ParseError>> {
            let mut parts = entry.split('=');
            match (parts.next(), parts.next(), parts.next()) {
                (_, _, Some(_)) => parse_error!("too many '='s"),
                (None, _, _) | (_, None, None) => parse_error!("no destination or endpoints"),
                (Some(dst), Some(endpoints), None) => {
                    let dst = dst.parse()?;
                    let endpoints = endpoints.parse()?;
                    tracing::trace!(?dst, ?endpoints, "parsed");
                    Ok((dst, endpoints))
                }
            }
        }

        let dsts = spec.split(';').map(parse_entry).collect::<Result<_, _>>()?;
        Ok(Self { dsts })
    }
}

impl DstSpec {
    pub fn into_svc(self) -> (DstHandle, DstService) {
        let mut txs = HashMap::new();
        let mut rxs = HashMap::new();
        for (dst, eps) in self.dsts.into_iter() {
            let (tx, rx) = watch::channel(eps);
            txs.insert(dst.clone(), tx);
            rxs.insert(dst, rx);
        }
        let state = Arc::new(RwLock::new(rxs));
        let rxs = Arc::downgrade(&state);
        let handle = DstHandle { dsts: txs, rxs };
        (handle, DstService { state })
    }
}

// === impl DstService ===

impl DstService {
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

// === impl ParseError ===

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.reason, f)
    }
}

impl Error for ParseError {}

// === impl Dst ===

impl FromStr for Dst {
    type Err = TracedError<ParseError>;
    #[tracing::instrument(name = "Dst::from_str", level = "error")]
    fn from_str(dst: &str) -> Result<Self, Self::Err> {
        let mut parts = dst.split("://");
        match (parts.next(), parts.next(), parts.next()) {
            (_, _, Some(_)) => parse_error!("too many schemes"),
            (None, _, _) | (_, None, None) => parse_error!("no scheme"),
            (Some(scheme), Some(name), None) => Ok(Self {
                scheme: scheme.to_owned(),
                name: name.to_owned(),
            }),
        }
    }
}

// === impl Endpoints ===

impl FromStr for Endpoints {
    type Err = TracedError<ParseError>;

    #[tracing::instrument(name = "Endpoints::from_str", level = "error")]
    fn from_str(endpoints: &str) -> Result<Self, Self::Err> {
        let endpoints = endpoints
            .split(',')
            .map(|addr| {
                let span = tracing::error_span!("parse_addr", ?addr);
                let _g = span.enter();
                match addr.parse() {
                    Ok(ep) => Ok(ep),
                    Err(_) => parse_error!("invalid socket address"),
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(Self(endpoints))
    }
}
