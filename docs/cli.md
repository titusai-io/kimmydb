# `kimmy` — the terminal client

[← Documentation index](README.md)

The `kimmy` terminal client moved out of this repository into its own
([ADR-193](decisions.md)), with the client libraries it is built on. It is
**frozen, and not currently distributed**: releases of the server no longer
build it, and it will be brought up to date with the libraries once the
server is stable and performing well.

Until then the HTTP API is the interface. Everything the CLI did, it did over
that API, held to the same grants as any other client.

## Next

- [HTTP API](http-api.md) — the endpoints
- [Clients](clients.md) — what a client is expected to do
- [Federation](federation.md) — obtaining a token from an identity provider
