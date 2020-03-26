# linkerd2-mock-dst

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

```console
:; RUST_LOG=linkerd2_mock_dst=info \
   LINKERD2_MOCK_DSTS='http://foo.ns.svc.cluster.local:8080=127.0.0.1:1234,127.0.0.1:1235;http://bar.ns.svc.cluster.local:8081=127.0.0.1:4321' \
   cargo run
```