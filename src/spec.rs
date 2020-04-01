use super::{Dst, EndpointMeta, Endpoints};
use std::{collections::HashMap, error::Error, str::FromStr};
use tracing_error::{prelude::*, TracedError};

#[derive(Debug, Default)]
pub struct DstSpec {
    pub(super) dsts: HashMap<Dst, Endpoints>,
}

#[derive(Debug)]
pub struct ParseError {
    reason: &'static str,
}

macro_rules! parse_error {
    ($reason:expr) => {{
        return Err(ParseError { reason: $reason }).in_current_span();
    }};
}

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
        let mut parts = dst.splitn(2, ":");
        match (parts.next(), parts.next()) {
            (Some(name), Some(port)) => match port.parse() {
                Ok(port) => Ok(Dst {
                    port,
                    name: name.into(),
                }),
                Err(_) => parse_error!("invalid port"),
            },
            _ => parse_error!("invalid destination"),
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

                let mut parts = addr.splitn(2, '#');
                match (parts.next(), parts.next()) {
                    (Some(addr), h2) => match addr.parse() {
                        Ok(addr) => Ok((
                            addr,
                            EndpointMeta {
                                h2: h2.map(|proto| proto == "h2").unwrap_or(false),
                            },
                        )),
                        Err(_) => parse_error!("invalid socket address"),
                    },
                    _ => parse_error!("empty socket address"),
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(Self(endpoints))
    }
}
