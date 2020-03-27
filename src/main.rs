use linkerd2_mock_dst::{DestinationServer, DstSpec};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "linkerd2-mock-dst",
    about = "A mock Linkerd 2 Destination server."
)]
struct CliOpts {
    /// The address that the mock destination service will listen on.
    #[structopt(short = "a", long = "addr", default_value = "0.0.0.0:8086")]
    addr: SocketAddr,

    /// A list of mock destinations to serve.
    ///
    /// This is parsed as a list of `DESTINATION=ENDPOINTS` pairs, where
    /// `DESTINATION` is a scheme, DNS name, and port, and `ENDPOINTS` is a
    /// comma-separated list of socket addresses. Each pair is separated by
    /// semicolons.
    #[structopt(name = "DSTS", env = "LINKERD2_MOCK_DSTS", parse(try_from_str = parse_dsts))]
    dsts: DstSpec,
}

#[tokio::main]
async fn main() -> Result<(), Termination> {
    use tracing_subscriber::prelude::*;

    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_subscriber::fmt::Layer::default())
        .with(tracing_error::ErrorLayer::default())
        .with(tracing_subscriber::EnvFilter::from_default_env());
    tracing::subscriber::set_global_default(subscriber)?;

    let opts = CliOpts::from_args();
    tracing::trace!(opts = format_args!("{:#?}", opts));
    let CliOpts { dsts, addr } = opts;

    let (_handle, svc) = dsts.into_svc();
    svc.serve(addr).await?;
    Ok(())
}

fn parse_dsts(dsts: &str) -> Result<DstSpec, Termination> {
    dsts.parse().map_err(Into::into)
}

struct Termination(Box<dyn Error>);

impl fmt::Debug for Termination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut error = Some(self.0.as_ref());

        while let Some(err) = error {
            writeln!(f, "{}", err)?;
            error = err.source();
        }

        Ok(())
    }
}

impl fmt::Display for Termination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<E: Into<Box<dyn Error + Send + Sync + 'static>>> From<E> for Termination {
    fn from(e: E) -> Self {
        Self(e.into())
    }
}
