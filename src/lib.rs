mod destination;
mod fs_watcher;
mod identity;
mod spec;

pub use self::destination::{Dst, DstSender, DstService, EndpointMeta, Endpoints, Overrides};
pub use self::fs_watcher::FsWatcher;
pub use self::identity::IdentityService;
pub use self::spec::{EndpointsSpec, OverridesSpec};

use linkerd2_proxy_api::{
    destination::destination_server::DestinationServer, identity::identity_server::IdentityServer,
};
use std::net::SocketAddr;
use tracing_futures::Instrument;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct Controller {
    dst_svc: DstService,
    identity_svc: IdentityService,
}

impl Controller {
    pub fn new(dst_svc: DstService, identity_svc: IdentityService) -> Controller {
        Controller {
            dst_svc,
            identity_svc,
        }
    }

    pub async fn serve(self, addr: impl Into<SocketAddr>) -> Result<(), Error> {
        let addr = addr.into();
        let span = tracing::info_span!("Controller::serve", listen.addr = %addr);
        tracing::info!(parent: &span, "Starting controller server...");

        tonic::transport::Server::builder()
            .trace_fn(|headers| tracing::debug_span!("request", ?headers))
            .add_service(DestinationServer::new(self.dst_svc))
            .add_service(IdentityServer::new(self.identity_svc))
            .serve(addr)
            .instrument(span)
            .await?;

        Ok(())
    }
}
