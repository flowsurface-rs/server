# flowsurface-server

A trade data collector for crypto markets, with an embedded database and REST API.

- Connects to exchange WebSocket streams via [flowsurface-exchange](https://crates.io/crates/flowsurface-exchange)
- Persists trades to [DuckDB](https://duckdb.org)
- Serves data via a REST API, as JSON or [Arrow IPC](https://arrow.apache.org/) stream formats

It's self-contained, single-binary portable, designed to run on a small VPS for individual use; not for production or shared use.

## Quick start

1. **Copy the template**:

```bash
cp config.example.toml config.toml
```

> See the [basic settings](#basic) and edit `config.toml`

2. **Run**

```bash
# looks for `config.toml` next to the binary.
./flowsurface-server
```

Or to use a custom config path:

```bash
./flowsurface-server --config /path/to/config.toml
```

> If you run without a config file, the server will write the
> template to the given path and then exit. Simply edit the generated file and re-run.

## Configuration

See [`config.example.toml`](config.example.toml) for all
available options with inline documentation.

### Basic

| Option                 | Default | Description                                                                                             |
| ---------------------- | ------- | ------------------------------------------------------------------------------------------------------- |
| `bind_address`         | —       | Socket address to bind (`127.0.0.1:8080` = local-only plain HTTP; `0.0.0.0:8080` = remote HTTPS + auth) |
| `base_assets`          | —       | Base assets expanded via whitelist templates (e.g. `["BTC", "ETH"]`)                                    |
| `max_storage_mb`       | `4096`  | Hard cap on database file size (MB); `0` = unlimited.                                                   |
| `data_retention_hours` | `168`   | Trades older than this are purged; `0` = keep all indefinitely.                                         |

### Advanced

| Option                | Default                | Description                                                                       |
| --------------------- | ---------------------- | --------------------------------------------------------------------------------- |
| `discovery_mode`      | `true`                 | Fetch metadata for all exchange variants so `/exchanges` is fully populated       |
| `tls_domain`          | `"flowsurface-server"` | Domain in the self-signed TLS cert's SAN (only needed for verified TLS)           |
| `flush_interval_ms`   | `2000`                 | How often buffered trades are written to disk (ms); higher = fewer writes         |
| `max_buffered_trades` | `200000`               | Max trades in memory buffer before dropping (OOM guard)                           |
| `max_requests`        | `500`                  | Per-IP rate limit (req/10s); `0` = disabled. Burst up to 500, sustained ~50 req/s |

### Whitelist templates

The whitelist defines which pairs to track per venue and market kind:

```toml
[whitelist.binance]
spot = ["USDT"]
linear = ["USDT", "USDC"]
inverse = ["USD"]

[whitelist.bybit]
spot = ["USDT"]
linear = ["USDT"]

[whitelist.hyperliquid]
linear = ["USDC"]

[whitelist.okex]
spot = ["USDT"]
linear = ["USDT"]
```

The server combines `base_assets × quote_assets` per market kind and
resolves the correct exchange-specific ticker strings (handling
separators, `_PERP`, `-SWAP` suffixes, etc.).

Use the `/exchanges` endpoint to browse available symbols before
deciding which pairs to track.

### Authentication

When binding to a **non-loopback** address, the server:

1. Generates a random 256-bit auth token on first boot and persists
   it as `data/.auth_token`.
2. The token's first 4 characters are printed at startup.
3. Retrieve the full token at any time:
    ```bash
    cat data/.auth_token
    ```

The token is reused across restarts. To set a specific token set the `AUTH_TOKEN` environment variable
(via `.env` or the environment).

## Remote deployment (VPS)

The server automatically enables HTTPS with a **self-signed certificate**
when binding to a non-loopback address (e.g. `0.0.0.0:8080`).

1. Set `bind_address = "0.0.0.0:8080"` in `config.toml`
2. Start the server — it generates:
    - `data/cert.pem` + `data/key.pem` (self-signed TLS cert/key pair)
    - `data/.auth_token` (random 256-bit token, unless already set)
3. Clients connect via `https://vps-ip:8080` using the auth token.
   The cert is self-signed, so most HTTP clients need an explicit flag
   to accept it (`curl -k`, `verify=False` in `requests`,
   `.danger_accept_invalid_certs(true)` in `reqwest`, etc.) unless
   verifying against `cert.pem` directly.

> **Port reachability:** Some cloud providers block inbound ports by
> default. Configure a firewall / security-group rule to allow traffic
> on your chosen port.

#### Connecting via a domain name

By default the self-signed cert only identifies itself as
`flowsurface-server`. If you point a domain at your VPS and want
clients to actually _verify_ the certificate, set `tls_domain` so the
cert's Subject Alternative Name matches:

```toml
tls_domain = "data.mydomain.com"
```

Then connect via `https://data.mydomain.com:8080` — raw-IP connections
are not supported for verified TLS. The cert's SHA-256 fingerprint is
logged at startup for pinning.

### Local development

For local-only use, keep `bind_address = "127.0.0.1:8080"`:

- Plain HTTP (no TLS overhead)
- No auth token required
- `curl http://127.0.0.1:8080/pairs` works directly

## API endpoints

`/status` is **public** (no auth) — suitable for health checks.
All other endpoints require `Authorization: Bearer <token>` when
auth is configured.

| Method | Path            | Auth     | Description                                        |
| ------ | --------------- | -------- | -------------------------------------------------- |
| GET    | `/status`       | ✗ public | Server uptime, DB connectivity check               |
| GET    | `/exchanges`    | required | Available ticker symbols per exchange              |
| GET    | `/pairs`        | required | Configured pairs with time bounds & tracked count  |
| GET    | `/trades`       | required | Trade data (filtered by venue, symbol, time range) |
| GET    | `/trades.arrow` | required | Trade data as Arrow IPC stream                     |

### GET /status

Returns the server health status. No authentication required — suitable for
load balancer health checks.

#### Response fields

| Field         | Type   | Description                   |
| ------------- | ------ | ----------------------------- |
| `status`      | string | Always `"ok"` while running   |
| `uptime_secs` | int    | Seconds since server start    |
| `db_ok`       | bool   | `true` if DuckDB is reachable |

#### Example

```bash
curl http://127.0.0.1:8080/status
```

```json
{
    "status": "ok",
    "uptime_secs": 7,
    "db_ok": true
}
```

### GET /exchanges

Returns every ticker symbol discovered on each exchange, grouped by
canonical exchange name. Useful for browsing available tickers before
configuring the whitelist.

```bash
curl -H "Authorization: Bearer <token>" \
  http://127.0.0.1:8080/exchanges
```

```json
{
    "exchanges": {
        "Binance Linear": ["BTCUSDT", "ETHUSDT", ...],
        "Binance Spot": ["BTCUSDT", "ETHUSDT", ...],
        "Bybit Linear": ["BTCUSDT", ...],
        ...
    }
}

```

### GET /pairs

Returns all configured pairs with their stored time ranges. Pairs that have
been configured but have not yet received any trades appear with `earliest`
and `latest` as `null`.

#### Response fields

| Field           | Type  | Description                      |
| --------------- | ----- | -------------------------------- |
| `pairs`         | array | Array of tracked pair objects    |
| `tracked_count` | int   | Total number of configured pairs |

Each pair object:

| Field      | Type   | Description                               |
| ---------- | ------ | ----------------------------------------- |
| `ticker`   | string | Ticker ID (`Exchange:pair`)               |
| `earliest` | int    | Unix ms of oldest stored trade, or `null` |
| `latest`   | int    | Unix ms of newest stored trade, or `null` |

#### Example

```bash
curl -H "Authorization: Bearer <token>" \
  http://127.0.0.1:8080/pairs
```

```json
{
    "pairs": [
        {
            "ticker": "BinanceSpot:btcusdt",
            "earliest": 1783873393043,
            "latest": 1784372262447
        },
        {
            "ticker": "HyperliquidLinear:btcusdc",
            "earliest": 1784372175005,
            "latest": 1784372262010
        }
    ],
    "tracked_count": 18
}
```

### GET /trades

Returns trade records matching the given filter.

#### Query parameters

| Param    | Type   | Description                                  |
| -------- | ------ | -------------------------------------------- |
| `venue`  | string | **Required.** Exchange name (e.g. `binance`) |
| `market` | string | **Required.** `spot`, `linear`, or `inverse` |
| `symbol` | string | **Required.** Ticker symbol (e.g. `btcusdt`) |
| `from`   | int    | Unix ms lower bound (inclusive)              |
| `to`     | int    | Unix ms upper bound (inclusive)              |
| `limit`  | int    | Max records (default 1000, max 10 000)       |

#### Response fields

| Field     | Type  | Description                        |
| --------- | ----- | ---------------------------------- |
| `trades`  | array | Array of matching trades           |
| `ts`      | int   | Unix millisecond timestamp         |
| `price`   | float | Trade price                        |
| `qty`     | float | Trade quantity                     |
| `is_sell` | bool  | `true` if a sell, `false` if a buy |

#### Example

```bash
curl -H "Authorization: Bearer <token>" \
  'http://127.0.0.1:8080/trades?venue=bybit&market=linear&symbol=btcusdt&limit=2'
```

```json
{
    "trades": [
        {
            "ts": 1784108317317,
            "price": 64774.8,
            "qty": 0.111,
            "is_sell": false
        },
        {
            "ts": 1784108317964,
            "price": 64774.7,
            "qty": 0.003,
            "is_sell": true
        }
    ]
}
```

### GET /trades.arrow

Same filtering as `/trades` but returns an **Arrow IPC streaming
format** payload (`Content-Type: application/vnd.apache.arrow.stream`)
with 4 columns: `ts (int64)`, `price (float64)`, `qty (float64)`,
`is_sell (bool)`.

This is ideal for high-volume data transfer to clients that support Arrow natively.

| Param    | Type   | Description                                  |
| -------- | ------ | -------------------------------------------- |
| `venue`  | string | **Required.** Exchange name (e.g. `binance`) |
| `market` | string | **Required.** `spot`, `linear`, or `inverse` |
| `symbol` | string | **Required.** Ticker symbol (e.g. `btcusdt`) |
| `from`   | int    | Unix ms lower bound (inclusive)              |
| `to`     | int    | Unix ms upper bound (inclusive)              |
| `limit`  | int    | Max records (default 100 000, max 1 000 000) |
