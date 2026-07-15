# flowsurface-server

A trade data store daemon. Connects to crypto exchange WebSocket
streams via [flowsurface-exchange](https://crates.io/crates/flowsurface-exchange), persists trades to an embedded DuckDB database, and serves them over
a REST API.

## Quick start

```bash
# 1. Configure
cp config.example.toml config.toml
# edit config.toml with your ticker whitelist

# 2. Run
./flowsurface-server                              # config.toml next to binary or CWD
./flowsurface-server --config /path/to/config.toml
```

Use `--config <path>` to specify a custom config location. The server looks for
`config.toml` next to the binary first, then in the current working directory.

The server auto-detects everything else:

| Situation                         | Behaviour                                                          |
| --------------------------------- | ------------------------------------------------------------------ |
| `bind_address = "127.0.0.1:8080"` | Plain HTTP, no auth                                                |
| `bind_address = "0.0.0.0:8080"`   | HTTPS (self-signed cert), auth token + cert. fingerprint generated |

## Configuration

### `config.toml`

```toml
bind_address = "127.0.0.1:8080"  # use 0.0.0.0:8080 for remote access
data_dir = "./data"

base_assets = ["BTC", "ETH"]

[whitelist.binance]
spot = ["USDT"]
linear = ["USDT", "USDC"]
inverse = ["USD"]
```

See `config.example.toml` for all available options.

### Discovery mode

By default, the server fetches metadata for **all** supported exchanges on
startup, so `/exchanges` is fully populated with every available ticker.
Set `discovery_mode = false` in `config.toml` to skip exchanges not in your
whitelist and reduce startup time.

### Authentication (`AUTH_TOKEN`)

When binding to a non-loopback address, the server generates an
auth token on first boot and saves it as `data/.auth_token`.
The token's first 4 characters are shown at startup; retrieve the
full token with:

```bash
cat data/.auth_token
```

The token is reused across restarts.

You can also set a specific token manually via env var:

```bash
echo 'AUTH_TOKEN="your-token"' > .env
```

The token is **never** stored in `config.toml`. Sent by clients as
`Authorization: Bearer <token>`.

## Remote deployment (VPS)

The server automatically enables HTTPS with a **self-signed certificate**
when binding to a non-loopback address (e.g. `0.0.0.0:8080`).

1. Set `bind_address = "0.0.0.0:8080"` in `config.toml`
2. Start the server — it generates:
    - `data/cert.pem` + `data/key.pem` (self-signed TLS cert)
    - `data/.auth_token` (random 256-bit token, if none set)
3. (Optional) The server logs the certificate's SHA-256 fingerprint at startup.
4. Clients can now use `https://vps-ip:8080` with the generated auth token.

#### Connecting via a domain name

By default the self-signed cert only identifies itself as
`flowsurface-server`. If you point a domain at your VPS and want
clients to actually _verify_ the certificate (e.g. with `--cacert`
instead of `-k`), set `tls_domain` so the cert matches:

```toml
tls_domain = "data.mydomain.com"
```

Then connect via `https://data.mydomain.com:8080` — raw-IP
connections are not supported for verified TLS. The cert's SHA-256
fingerprint is still logged at startup for pinning.

### Local development

For local-only use, keep `bind_address = "127.0.0.1:8080"`:

- Plain HTTP (no TLS overhead)
- No auth token required
- `curl http://127.0.0.1:8080/status` works directly

## API endpoints

`/status` is **public** (no auth) — suitable for health checks.
All other endpoints require `Authorization: Bearer <token>` when
auth is configured.

| Method | Path              | Auth     | Description                                        |
| ------ | ----------------- | -------- | -------------------------------------------------- |
| GET    | `/status`         | ✗ public | Server uptime, DB connectivity check               |
| GET    | `/exchanges`      | required | Available ticker symbols per exchange              |
| GET    | `/pairs`          | required | Configured pairs with time bounds & tracked count  |
| GET    | `/trades`         | required | Trade data (filtered by venue, symbol, time range) |
| GET    | `/trades/grouped` | required | Aggregated trades (tick-aligned price buckets)     |

### Query parameters for `/trades`

| Param    | Type   | Description                                  |
| -------- | ------ | -------------------------------------------- |
| `venue`  | string | Exchange name (e.g. `binance`)               |
| `market` | string | `spot`, `linear`, or `inverse`               |
| `symbol` | string | **Required.** Ticker symbol (e.g. `btcusdt`) |
| `from`   | int    | Unix ms lower bound                          |
| `to`     | int    | Unix ms upper bound                          |
| `limit`  | int    | Max records (default 1000, max 10 000)       |

### Query parameters for `/trades/grouped`

| Param    | Type   | Description                                                             |
| -------- | ------ | ----------------------------------------------------------------------- |
| `venue`  | string | Exchange name (e.g. `binance`)                                          |
| `market` | string | `spot`, `linear`, or `inverse`                                          |
| `symbol` | string | **Required.** Ticker symbol (e.g. `btcusdt`)                            |
| `from`   | int    | Unix ms lower bound                                                     |
| `to`     | int    | Unix ms upper bound                                                     |
| `limit`  | int    | Max records (default 1000, max 10 000)                                  |
| `step`   | int    | Price bucket width multiplier (default 1). Bucket = min_ticksize × step |

## Architecture

```
Exchange WS ─▶ flowsurface-server ─▶ DuckDB
                    │
                    ▼
              REST API (HTTP/HTTPS)
                    │
                    ▼
                  Client
```

- `flowsurface-exchange` adapters connect to exchange WebSocket streams
- Trades are buffered in memory and flushed to DuckDB in batches
- Data retention is enforced (old trades purged periodically)
- The HTTP API serves queries directly from DuckDB
- A self-signed TLS cert is auto-generated for remote access; its SHA-256 fingerprint is logged at startup for optional client-side pinning
