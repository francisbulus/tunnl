### How It Works

`tunnl` is a lightweight reverse tunnel prototype split into two apps:

- `proxy/`: public HTTP endpoint + Socket.IO broker
- `cli/`: local tunnel agent that forwards requests to your local port

Run the CLI with:

`tunnel -p <local-port> --remote <proxy-url>`

After connecting, the CLI prints a tunnel key. Remote callers can access your tunnel with either:

- `Authorization: Bearer <key>`
- `?token=<key>` on the URL

The proxy uses that key to route each HTTP request to the correct connected tunnel socket.

### Hardening Controls

The proxy now includes basic production guards:

- Keyed tunnel routing (no global fallback socket)
- Request rate limiting (`RATE_LIMIT_WINDOW_MS`, `RATE_LIMIT_MAX_REQUESTS`)
- Max request payload guard (`MAX_REQUEST_BODY_BYTES`)
- Request/response timeout guard (`REQUEST_TIMEOUT_MS`)
- Hop-by-hop header stripping in both directions
- Optional HTTPS/WSS enforcement (`ENFORCE_HTTPS=true`; defaults true in production)

Optional subdomain routing is supported by setting:

- `TUNNEL_BASE_DOMAIN` (for example `example.com`, enabling `<key>.example.com`)

### Development

- Proxy UI: `GET /connect`
- Proxy health check: `GET /healthz`
- Proxy integration tests: `cd proxy && npm test`

### References

- [ngrok](https://github.com/inconshreveable/ngrok)
- [web-tunnel](https://github.com/web-tunnel)
