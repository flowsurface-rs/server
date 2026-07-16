# flowsurface-server

A trade data store daemon. Connects to crypto exchange WebSocket
streams via [flowsurface-exchange](https://crates.io/crates/flowsurface-exchange), persists trades to an embedded DuckDB database, and serves them over
a REST API.

## Quick start

```bash
# 1. Configure
cp config.example.toml config.toml

# 2. Run
./flowsurface-server                              # config.toml next to binary or CWD
./flowsurface-server --config /path/to/config.toml
```

## Configuration

| Situation                         | Behaviour                                                          |
| --------------------------------- | ------------------------------------------------------------------ |
| `bind_address = "127.0.0.1:8080"` | Plain HTTP, no auth                                                |
| `bind_address = "0.0.0.0:8080"`   | HTTPS (self-signed cert), auth token + cert. fingerprint generated |

### `config.toml`

```toml
bind_address = "127.0.0.1:8080"   # use 0.0.0.0:8080 for remote access
data_dir = "./data"               # data, auth token, TLS certs, DuckDB

base_assets = ["BTC", "ETH"]      # assets expanded via whitelist templates

[whitelist.binance]               # per-exchange whitelist section
spot = ["USDT"]                   # spot markets: base_assets paired with USDT
linear = ["USDT", "USDC"]         # linear futures: base_assets paired with USDT/USDC
inverse = ["USD"]                 # inverse futures: base_assets paired with USD
```

See `config.example.toml` for all available options.

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

## Remote deployment (VPS)

The server automatically enables HTTPS with a **self-signed certificate**
when binding to a non-loopback address (e.g. `0.0.0.0:8080`).

1. Set `bind_address = "0.0.0.0:8080"` in `config.toml`
2. Start the server — it generates:
    - `data/cert.pem` + `data/key.pem` (self-signed TLS cert. for optional setup)
    - `data/.auth_token` (random 256-bit token, if none set)
3. Clients can now use `https://vps-ip:8080` with the generated auth
   token — note the cert is self-signed, so most HTTP clients need an
   explicit flag to accept it (e.g. `curl -k`, `verify=False` in
   `requests`, `.danger_accept_invalid_certs(true)` in `reqwest`, etc.) unless verifying against `cert.pem` directly.

> If you can't connect, check that the port is reachable: some cloud providers
> block inbound ports by default and require an explicit
> firewall/security-group rule.

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
- `curl http://127.0.0.1:8080/pairs` works directly

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
| `venue`  | string | **Required.** Exchange name (e.g. `binance`) |
| `market` | string | **Required.** `spot`, `linear`, or `inverse` |
| `symbol` | string | **Required.** Ticker symbol (e.g. `btcusdt`) |
| `from`   | int    | Unix ms lower bound (inclusive)              |
| `to`     | int    | Unix ms upper bound (inclusive)              |
| `limit`  | int    | Max records (default 1000, max 10 000)       |

#### Response fields

| Field      | Type   | Description                                         |
| ---------- | ------ | --------------------------------------------------- |
| `trades`   | array  | Array of matching trades                            |
| `exchange` | string | Canonical exchange identifier (e.g. `Binance Spot`) |
| `symbol`   | string | Ticker in lowercase (e.g. `btcusdt`)                |
| `ts`       | int    | Unix millisecond timestamp                          |
| `price`    | float  | Trade price                                         |
| `qty`      | float  | Trade quantity                                      |
| `is_sell`  | bool   | `true` if a sell, `false` if a buy                  |

#### Response example

`GET /trades?venue=bybit&market=linear&symbol=btcusdt&from=1784108317135&limit=2`

```json
{
    "trades": [
        {
            "exchange": "Bybit Linear",
            "symbol": "btcusdt",
            "ts": 1784108317317,
            "price": 64774.8,
            "qty": 0.111,
            "is_sell": false
        },
        {
            "exchange": "Bybit Linear",
            "symbol": "btcusdt",
            "ts": 1784108317964,
            "price": 64774.7,
            "qty": 0.003,
            "is_sell": true
        }
    ]
}
```

### Query parameters for `/trades/grouped`

| Param    | Type   | Description                                                             |
| -------- | ------ | ----------------------------------------------------------------------- |
| `venue`  | string | **Required.** Exchange name (e.g. `binance`)                            |
| `market` | string | **Required.** `spot`, `linear`, or `inverse`                            |
| `symbol` | string | **Required.** Ticker symbol (e.g. `btcusdt`)                            |
| `from`   | int    | Unix ms lower bound (inclusive)                                         |
| `to`     | int    | Unix ms upper bound (inclusive)                                         |
| `limit`  | int    | Max records (default 1000, max 10 000)                                  |
| `step`   | int    | Price bucket width multiplier (default 1). Bucket = min_ticksize × step |

#### Response fields

| Field         | Type  | Description                                  |
| ------------- | ----- | -------------------------------------------- |
| `trades`      | array | Array of aggregated price buckets            |
| `price_level` | float | Tick-aligned price bucket label              |
| `buy_volume`  | float | Total buy quantity in this bucket            |
| `sell_volume` | float | Total sell quantity in this bucket           |
| `buy_count`   | int   | Number of buy trades in this bucket          |
| `sell_count`  | int   | Number of sell trades in this bucket         |
| `first_ts`    | int   | Earliest trade timestamp in bucket (Unix ms) |
| `last_ts`     | int   | Latest trade timestamp in bucket (Unix ms)   |

#### Response example

`GET /trades/grouped?venue=binance&market=linear&symbol=btcusdt&step=100&limit=1`

```json
{
    "trades": [
        {
            "price_level": 64780.0,
            "buy_volume": 1104.205,
            "sell_volume": 1082.517,
            "buy_count": 9183,
            "sell_count": 8842,
            "first_ts": 1784043116723,
            "last_ts": 1784107852242
        }
    ]
}
```
