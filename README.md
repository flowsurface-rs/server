# flowsurface-server

A trade data collector for crypto markets, with an embedded database and a REST API.

- Connects to exchange WebSocket streams via [flowsurface-exchange](https://crates.io/crates/flowsurface-exchange)
- Persists trades to [DuckDB](https://duckdb.org)
- Serves data via REST API, as JSON or [Arrow IPC](https://arrow.apache.org/) stream formats

It's a self-contained, portable server, designed to run on a small VPS for individual use; not for production or shared use.

## Quick start

### Prebuilt binaries

1. **Get the latest [release](https://github.com/akenshaw/fs-server/releases/latest)**
   (`linux-x86_64` or `linux-aarch64`).

2. **Copy the config template**:

    ```bash
    # in the extracted directory
    cp config.example.toml config.toml
    ```

3. **Run**:

    ```bash
    ./flowsurface-server
    ```

    Or with a custom config path:

    ```bash
    ./flowsurface-server --config /path/to/config.toml
    ```

### Build from source

```bash
cargo build --release

cp config.example.toml config.toml

# run
cargo run --release
```

By default the server looks for `config.toml` in the project root when the
executable path contains `/target/`; otherwise it looks next
to the executable (deployed binary). Use `--config /path/to/config.toml` to
override.

> If `config.toml` doesn't exist, the server writes the default
> template to that path and **exits** (code 2). Edit the created file to suit
> your needs, then re-run.

## Configuration

See [`config.example.toml`](config.example.toml) for all
available options with inline documentation.

### `[network]`

| Option         | Default          | Description                                      |
| -------------- | ---------------- | ------------------------------------------------ |
| `bind_address` | `127.0.0.1:8080` | Listen address; non-loopback enables TLS + auth. |
| `max_requests` | `500`            | Per-IP rate limit (req/10s); `0` = off.          |

### `[storage]`

| Option                 | Default  | Description                                   |
| ---------------------- | -------- | --------------------------------------------- |
| `data_dir`             | `"data"` | Directory for DB, auth token, and TLS certs.  |
| `max_storage_mb`       | `4096`   | Hard cap on DB+WAL (MB); `0` = unlimited.     |
| `data_retention_hours` | `168`    | Purge trades older than this; `0` = keep all. |

### `[pairs]`

| Option           | Default | Description                                                      |
| ---------------- | ------- | ---------------------------------------------------------------- |
| `base_assets`    | `[]`    | Bases combined with whitelist quotes to determine tracked pairs. |
| `discovery_mode` | `true`  | Fetch metadata for all exchanges (populates `/exchanges`).       |

### Whitelist templates

The whitelist defines which pairs to track per venue and market kind.
See the `[whitelist.*]` sections in [`config.example.toml`](config.example.toml)
for the template. The server combines `base_assets × quote_assets`
per market kind and resolves the correct exchange-specific ticker strings
(handling separators, `_PERP`, `-SWAP` suffixes, etc.).

> **Wildcard:** An empty string `""` as a quote asset includes **every**
> ticker on that exchange whose base matches (e.g.
> `linear = [""]` would track all linear perpetuals with a matching base
> asset regardless of quote currency).

Pairs are only tracked when **both** `base_assets` and a matching whitelist
entry are configured. If either is empty, no pairs are tracked.
Use the `/exchanges` endpoint to browse available symbols before
deciding which pairs to track.

### Authentication

When binding to a **non-loopback** address, the server:

1. Generates a random 256-bit auth token on first boot and persists
   it as `.auth_token` inside the configured `data_dir` (see [`[storage]`](#storage)).
2. The token's first 4 characters are printed at startup.
3. Retrieve the full token at any time:
    ```bash
    # data_dir = "data"
    cat data/.auth_token
    ```

The token is reused across restarts. To set a specific token set the `AUTH_TOKEN` environment variable
(via `.env` or the environment).

## Remote deployment (VPS)

The server automatically enables HTTPS with a **self-signed certificate**
when binding to a non-loopback address (e.g. `0.0.0.0:8080`).

1. Set `bind_address = "0.0.0.0:8080"` in `config.toml`
2. Start the server, it generates (inside the configured `data_dir`, see [`[storage]`](#storage)):
    - `cert.pem` + `key.pem` (self-signed TLS cert/key pair)
    - `.auth_token` (random 256-bit token, unless already set)
3. Clients connect via `https://vps-ip:8080` using the auth token.
   The cert is self-signed, use `curl -k` or equivalent to accept
   it, or verify against `cert.pem` directly.

> **Port reachability:** Some cloud providers block inbound ports by
> default. Configure a firewall / security-group rule to allow traffic
> on your chosen port.

#### Connecting via a domain name

Set `tls_domain` to have the self-signed cert identify as your domain:

```toml
tls_domain = "data.mydomain.com"
```

When binding to `0.0.0.0`, verified TLS requires a domain name (no IP SAN
is added). When binding to a concrete IP, an IP SAN is added automatically.

The cert's SHA-256 fingerprint is logged at startup for pinning.

### Local development

For local-only use, keep `bind_address = "127.0.0.1:8080"`:

- Plain HTTP (no TLS overhead)
- No auth token required
- `curl http://127.0.0.1:8080/pairs` works directly

## Design limitations

This server is not a tick-by-tick market data recorder and shouldn't be
treated as one:

- **No delivery guarantees**: WebSocket disconnections may cause gaps.
  Exchange-side replays may introduce duplicates. The server uses
  [flowsurface-exchange](https://github.com/flowsurface-rs/flowsurface/tree/main/exchange)
  for market feeds, which is built for charting rather than archival.

- **In-memory buffering**: trades are held in memory for up to
  `flush_interval_ms` (default 2s) before batch-writing to DuckDB.
  Under extreme load, if the buffer exceeds `max_buffered_trades`,
  the oldest entries are shed to prevent OOM crashes.

- **Configuration changes require a restart**: editing tracked pairs
  or anything in `config.toml` needs a server restart. In-memory
  buffered trades are flushed to disk during shutdown, but there
  will be a data gap until the server restarts and feeds reconnect.

It prioritizes **convenience** over guaranteed delivery, as it's simply
made as a companion for [flowsurface](https://github.com/flowsurface-rs/flowsurface).

## API endpoints

`/status` is **public** (no auth).
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

Returns the server health status. No authentication required.

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

> **Sort order:** When `from` is specified, rows are returned oldest-first.
> When `from` is omitted, rows are returned newest-first.

#### Response fields

| Field    | Type  | Description                     |
| -------- | ----- | ------------------------------- |
| `trades` | array | Array of matching trade objects |

Each trade object:

| Field     | Type  | Description                                                                        |
| --------- | ----- | ---------------------------------------------------------------------------------- |
| `ts`      | int   | Unix millisecond timestamp                                                         |
| `price`   | float | Price                                                                              |
| `qty`     | float | Normalized quantity (base units for spot/linear, quote notional for inverse perps) |
| `is_sell` | bool  | `true` if a sell, `false` if a buy                                                 |

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
format** payload (`Content-Type: application/vnd.apache.arrow.stream`
with `Content-Disposition: attachment; filename="trades.arrow"`)
with 4 columns:

> `ts (int64)`, `price (float64)`, `qty (float64)`, `is_sell (bool)`

This is ideal for high-volume data transfer to clients that support Arrow natively.

| Param    | Type   | Description                                  |
| -------- | ------ | -------------------------------------------- |
| `venue`  | string | **Required.** Exchange name (e.g. `binance`) |
| `market` | string | **Required.** `spot`, `linear`, or `inverse` |
| `symbol` | string | **Required.** Ticker symbol (e.g. `btcusdt`) |
| `from`   | int    | Unix ms lower bound (inclusive)              |
| `to`     | int    | Unix ms upper bound (inclusive)              |
| `limit`  | int    | Max records (default 50 000, max 400 000)    |

> **Sort order:** Same as `/trades`.
