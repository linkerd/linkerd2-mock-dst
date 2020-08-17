use crate::{Dst, EndpointMeta, Endpoints, Overrides};
use std::collections::{BTreeMap, HashMap};
use std::{default::Default, error::Error, str::FromStr};
use tracing_error::{prelude::*, TracedError};

#[derive(Debug, Default)]
pub struct EndpointsSpec {
    pub(super) dsts: HashMap<Dst, Endpoints>,
}

#[derive(Debug, Default)]
pub struct OverridesSpec {
    pub(super) dsts: HashMap<Dst, Overrides>,
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

// === impl EndpointsSpec ===

impl FromStr for EndpointsSpec {
    type Err = TracedError<ParseError>;

    #[tracing::instrument(name = "EndpointsSpec::from_str", level = "error")]
    fn from_str(spec: &str) -> Result<Self, Self::Err> {
        if spec.is_empty() {
            return Ok(Self::default());
        }

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
                Ok(port) => Ok(Dst::new(name.into(), port)),
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

                // Endpoints can be configured for h2 upgrading and identity with the '#' suffix:
                // - `#h2` supports h2 upgrading
                // - `#h2#<IDENTITY>` supports h2 upgrading and has the `<IDENTITY>` TLS identity
                // - `##<IDENTITY>` has the `<IDENTITY>` TLS identity
                let mut parts = addr.splitn(3, '#');
                match (parts.next(), parts.next(), parts.next()) {
                    (Some(addr), h2, identity) => match addr.parse() {
                        Ok(addr) => Ok((
                            addr,
                            EndpointMeta::new(
                                addr,
                                h2.map(|proto| proto == "h2").unwrap_or(false),
                                10_000,
                                BTreeMap::default(),
                                identity.map(str::to_owned),
                                None,
                            ),
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

// === impl OverridesSpec ===

impl FromStr for OverridesSpec {
    type Err = TracedError<ParseError>;

    #[tracing::instrument(name = "OverridesSpec::from_str", level = "error")]
    fn from_str(spec: &str) -> Result<Self, Self::Err> {
        if spec.is_empty() {
            return Ok(Self::default());
        }

        #[tracing::instrument(level = "info")]
        fn parse_entry(entry: &str) -> Result<(Dst, Overrides), TracedError<ParseError>> {
            let mut parts = entry.split('=');
            match (parts.next(), parts.next(), parts.next()) {
                (_, _, Some(_)) => parse_error!("too many '='s"),
                (None, _, _) | (_, None, None) => parse_error!("no destination or endpoints"),
                (Some(dst), Some(overrides), None) => {
                    let dst = dst.parse()?;
                    let overrides = overrides.parse()?;
                    tracing::trace!(?dst, ?overrides, "parsed");
                    Ok((dst, overrides))
                }
            }
        }

        let dsts = spec.split(';').map(parse_entry).collect::<Result<_, _>>()?;
        Ok(Self { dsts })
    }
}

// === impl Overrides ===

impl FromStr for Overrides {
    type Err = TracedError<ParseError>;

    #[tracing::instrument(name = "Overrides::from_str", level = "error")]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let dsts = s
            .split(',')
            .map(|addr| {
                let span = tracing::error_span!("parse_addr", ?addr);
                let _g = span.enter();

                // Destinations may have the suffix `*W` to indicate a weight.
                let mut parts = addr.splitn(2, '*');
                match (parts.next(), parts.next()) {
                    (Some(dst), weight) => match (dst.parse(), weight) {
                        (Ok(dst), None) => Ok((dst, 1_000)),
                        (Ok(dst), Some(weight)) => match weight.parse() {
                            Ok(weight) => Ok((dst, weight)),
                            Err(_) => parse_error!("invalid weight"),
                        },
                        (Err(_), _) => parse_error!("invalid socket address"),
                    },
                    _ => parse_error!("empty socket address"),
                }
            })
            .collect::<Result<_, _>>()?;

        Ok(Overrides::new(dsts))
    }
}
