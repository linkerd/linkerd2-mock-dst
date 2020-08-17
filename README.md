# linkerd2-mock-dst

This repository contains a mock implementation of Linkerd 2's [Destination
service][dst-svc], intended for proxy testing and benchmarking. 

Unlike the real Destination service, which serves service discovery requests
from proxies based on information in the Kubernetes API, the mock implementation
currently serves a fixed set of destinations and endpoints. In the future, it
will also support simulating changes in cluster state for testing purposes.

The mock Destination service may be run in a standalone process as a
command-line application. Additionally, it can also be used as a Rust library,
to embed a mock Destination service in Rust tests.

[dst-svc]: https://linkerd.io/2/reference/architecture/#destination

## Usage

```
linkerd2-mock-dst 0.1.0
A mock Linkerd 2 Destination server.

USAGE:
    linkerd2-mock-dst [OPTIONS] <DSTS>

FLAGS:
    -h, --help       
            Prints help information

    -V, --version    
            Prints version information


OPTIONS:
    -a, --addr <addr>    
            The address that the mock destination service will listen on [default: 0.0.0.0:8086]


ARGS:
    <DSTS>    
            A list of mock destinations to serve.
            
            This is parsed as a list of `DESTINATION=ENDPOINTS` pairs, where `DESTINATION` is a scheme, DNS name, and
            port, and `ENDPOINTS` is a comma-separated list of socket addresses. Each pair is separated by semicolons.
            [env: LINKERD2_MOCK_DSTS=]
```

## Examples

Mock destinations for the `foo.ns.svc.cluster.local` service:

```console
:; RUST_LOG=linkerd2_mock_dst=info \
   LINKERD2_MOCK_DSTS='http://foo.ns.svc.cluster.local:8080=127.0.0.1:1234,127.0.0.1:1235;http://bar.ns.svc.cluster.local:8081=127.0.0.1:4321' \
   cargo run
```

Mock identity for the `foo-ns1-ca1` identity name:

```console
:; ls /path/to/identities
foo-ns1-ca1/

:; ls /path/to/identities/foo-ns1-ca1/
crt.pem csr.der key.p8

:; RUST_LOG=linkerd2_mock_dst=info \
   LINKERD2_MOCK_DST_IDENTITIES_DIR='/path/to/identities/' \
   cargo run
```
