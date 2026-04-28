# Pricoin Mining Pool

PPLNS pool for the privacy-mandatory [Pricoin](https://pricoin.org) chain.

Miners connect with their **stealth address** as the username; the pool
mines coinbase rewards to its own transparent P2WPKH address, converts to
its own confidential balance via `walletsendct`, and pays out batches of
~50 users per `walletsendct_multi` transaction once each user's balance
clears their personal `min_payout`.

## Architecture

```
miners ─stratum/v1─►  pool ──getblocktemplate──► pricoind
                       │                            ▲
                       ├── rxshare (FULL_MEM RandomX, double-buffered)
                       │
                       ├── PPLNS share window (sqlite)
                       │
                       ├── vardiff per connection (target ~10 s/share)
                       │
block found  ──────────►  pool                      submitblock
                       │
                       └── payouts: walletsendct_multi every 10 min
```

## Components

- `rxshare/` — C extension wrapping RandomX in `FULL_MEM` mode for fast
  share verification; double-buffered datasets so epoch rotation never
  pauses the verifier.
- `pool/` — Python pool process. asyncio stratum server, PPLNS share
  accounting, vardiff, payout daemon, web UI.
- `scripts/` — operational helpers (compare-hash sanity check, etc.).
- `tests/` — pytest smoke tests; first one verifies `rxshare.hash` matches
  pricoind byte-for-byte on the same seed/header.

## Build

```bash
make -C rxshare           # builds rxshare/librxshare.so
pip install -e .
```

`rxshare/Makefile` links against the static `librandomx.a` produced by the
main pricoin build at `~/bitcoin/build_pric/src/crypto/randomx/`. Override
with `RANDOMX_LIB=/path/to/librandomx.a` if it lives elsewhere.

## Run

```bash
cp config.toml.example config.toml
# Edit pool.coinbase_address (transparent P2WPKH) and pricoind.wallet
python3 -m pool.main --config config.toml --db pool.sqlite
```

`SIGINT` / `SIGTERM` shuts the server down gracefully (drains in-flight
shares, flushes the sqlite DB, releases the rxshare datasets).

### As a systemd service

```bash
sudo useradd -m -d /home/pool pool
sudo install -d -o pool -g pool /var/lib/pricoinpool
sudo cp systemd/pricoinpool.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pricoinpool
journalctl -u pricoinpool -f
```

The unit pulls in `pricoind.service` automatically. Strongly recommended:

```bash
echo 'vm.nr_hugepages = 1280' | sudo tee /etc/sysctl.d/90-pricoinpool.conf
sudo sysctl -p /etc/sysctl.d/90-pricoinpool.conf
```

(RandomX's FULL_MEM dataset is ~2 GB; with two of them resident for hot
epoch rotation, ~4 GB of hugepages keeps share verification fast.)

## Runtime requirements

- ≥ 6 GB RAM (two ~2.08 GB RandomX datasets resident for double-buffering)
- Linux hugepages strongly recommended:
  `sudo sysctl -w vm.nr_hugepages=1280`
- A running `pricoind` with `-server=1` and rpcauth/cookie reachable
- Pool's wallet pre-funded so the first payout cycle has something to send

## Status

Phase 2 in progress. Working: `rxshare` + Python wrapper. All stratum /
PPLNS / payout pieces are stubs — they will be filled in Phase 3 onward.
