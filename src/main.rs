use linkerd2_mock_dst::{DstService, EndpointsSpec, FsWatcher, OverridesSpec};
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
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

    /// A list of destination endpoints to serve.
    ///
    /// This is parsed as a list of `DESTINATION=ENDPOINTS` pairs, where `DESTINATION` is a DNS name
    /// and port, and `ENDPOINTS` is a comma-separated list of endpoints. Each pair is separated by
    /// semicolons. An endpoint consists of a an`IP:PORT` and an optional `#h2` suffix, if the
    /// endpoint supports meshed protocol upgrading.
    #[structopt(long = "endpoints", env = "LINKERD2_MOCK_DST_ENDPOINTS", default_value = "", parse(try_from_str = parse_endpoints))]
    endpoints: EndpointsSpec,

    /// A list of destination overrides to serve.
    ///
    /// This is parsed as a list of `DESTINATION=OVERRIDES` pairs, where `DESTINATION` is a DNS name
    /// and port, and `OVERRIDES` is a comma-separated list of overrides. Each pair is separated by
    /// semicolons. An override consists of a an `NAME:PORT` and an optional `*WEIGHT` suffix.
    /// `WEIGHT`s are integers. If unspecifified, the default weight of 1000 is used.
    #[structopt(long = "overrides", env = "LINKERD2_MOCK_DST_OVERRIDES", default_value = "", parse(try_from_str = parse_overrides))]
    overrides: OverridesSpec,

    /// A directory that is dynamically watched for endpoints updates
    ///
    /// The directory contains files with names in the form of {dst.name}:{port}. Each file should
    /// contain the json representation of a list of `EndpointMeta` objects. Not that if such a
    /// directory is provided the `endpoints` and `overrides` opts will be ignored and the discovery
    /// state will be derived from the contents of the directory only.
    #[structopt(
        long = "endpoints_dir",
                env = "LINKERD2_MOCK_DST_ENDPOINTS_DIR",  conflicts_with_all = &["overrides", "endpoints"],
    )]
    endpoints_dir: Option<PathBuf>,
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
    let CliOpts {
        endpoints,
        overrides,
        addr,
        endpoints_dir,
    } = opts;
    tracing::debug!(?endpoints, ?overrides, ?addr, ?endpoints_dir);

    match endpoints_dir {
        Some(endpoints) => {
            let (sender, svc) = DstService::empty();
            let mut fs_watcher = FsWatcher::new(endpoints, sender);
            futures::try_join!(svc.serve(addr), fs_watcher.watch())?;
        }
        None => {
            let (_sender, svc) = DstService::new(endpoints, overrides);
            svc.serve(addr).await?;
        }
    };
    Ok(())
}

fn parse_endpoints(s: &str) -> Result<EndpointsSpec, Termination> {
    s.parse().map_err(Into::into)
}

fn parse_overrides(s: &str) -> Result<OverridesSpec, Termination> {
    s.parse().map_err(Into::into)
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
