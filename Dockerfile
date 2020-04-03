ARG RUST_IMAGE=rust:1.42.0-buster
ARG RUNTIME_IMAGE=debian:buster-slim

FROM $RUST_IMAGE as build
RUN rustup component add rustfmt
WORKDIR /usr/src/linkerd2-mock-dst
COPY . .
RUN cargo fetch --locked
RUN cargo build --frozen --release

FROM $RUNTIME_IMAGE as runtime
COPY --from=build \
    /usr/src/linkerd2-mock-dst/target/release/linkerd2-mock-dst \
    /usr/bin/linkerd2-mock-dst
ENV RUST_LOG=linkerd=info,warn
ENTRYPOINT ["/usr/bin/linkerd2-mock-dst"]
