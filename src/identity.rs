use linkerd2_proxy_api::identity::{
    self as pb,
    identity_server::{Identity, IdentityServer},
};
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufReader, ErrorKind},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tracing_futures::Instrument;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Default)]
pub struct IdentityService {
    identities: HashMap<String, Certificates>,
}

struct Certificates {
    leaf: Vec<u8>,
    intermediates: Vec<Vec<u8>>,
}

impl IdentityService {
    pub fn new(mut path: PathBuf) -> Result<IdentityService, io::Error> {
        let mut identities = HashMap::new();

        for entry in path.read_dir().expect("read_dir call failed") {
            if let Ok(entry) = entry {
                if entry.file_type().map_or(false, |ft| ft.is_dir()) {
                    path.push(entry.file_name());
                    path.push("crt.pem");
                    let certs = Certificates::load(&path)
                        .expect("failed to load certificates from crt.pem");
                    let local_name = entry.file_name().into_string().unwrap();
                    tracing::info!(?local_name, "added");
                    identities.insert(local_name, certs);
                }
            }
        }

        Ok(IdentityService { identities })
    }

    pub fn empty() -> IdentityService {
        IdentityService::default()
    }

    pub async fn serve(self, addr: impl Into<SocketAddr>) -> Result<(), Error> {
        let addr = addr.into();
        let span = tracing::info_span!("IdentityService::serve", listen.addr = %addr);
        tracing::info!(parent: &span, "Starting identity server...");

        tonic::transport::Server::builder()
            .trace_fn(|headers| tracing::debug_span!("request", ?headers))
            .add_service(IdentityServer::new(self))
            .serve(addr)
            .instrument(span)
            .await?;

        Ok(())
    }
}

impl Certificates {
    // Taken from: https://github.com/linkerd/linkerd2-proxy/blob/23995e7fb6eae5ede81048bdf9e4f68f7e81c7a9/linkerd/app/integration/src/identity.rs#L44-L64
    fn load<P>(path: P) -> Result<Certificates, io::Error>
    where
        P: AsRef<Path>,
    {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let certs = rustls::internal::pemfile::certs(&mut reader)
            .map_err(|_| io::Error::new(ErrorKind::Other, "rustls error reading certs"))?;
        let leaf = certs
            .first()
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "no certs in pemfile"))?
            .as_ref()
            .into();
        let intermediates = certs[1..].iter().map(|i| i.as_ref().into()).collect();
        Ok(Certificates {
            leaf,
            intermediates,
        })
    }
}

#[tonic::async_trait]
impl Identity for IdentityService {
    async fn certify(
        &self,
        request: tonic::Request<pb::CertifyRequest>,
    ) -> Result<tonic::Response<pb::CertifyResponse>, tonic::Status> {
        let pb::CertifyRequest { identity, .. } = request.into_inner();
        if let Some(certs) = self.identities.get(&identity) {
            // Ideally we'd load the `not_after` value from the `crt.pem`, but
            // that does not seem to be an option with rustls. Therefore,
            // create a fake expiration.
            let not_after = SystemTime::now() + Duration::from_secs(123_456);
            let response = pb::CertifyResponse {
                leaf_certificate: certs.leaf.clone(),
                intermediate_certificates: certs.intermediates.clone(),
                valid_until: Some(not_after.into()),
            };
            return Ok(tonic::Response::new(response));
        }
        Err(tonic::Status::not_found(format!(
            "'{}' identity does not exist",
            identity
        )))
    }
}
