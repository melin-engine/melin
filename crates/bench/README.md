# Benchmark Suite

Latency and throughput benchmark for the trading engine with three modes, from bare matching engine to full network round-trip.

All modes use the realistic order flow generator: a mix of limit orders and cancels with power-law price/size distributions, multiple accounts, and resting book depth. Events are pre-generated before the measured run so RNG overhead doesn't pollute per-order timing.

## Modes

### `--mode=engine`

Calls `Exchange::execute()` and `Exchange::cancel()` directly in a tight loop. No disruptor, no journal, no I/O. Measures pure matching engine throughput and per-order latency with realistic order flow.

```sh
cargo run --release -p melin-bench -- --mode=engine 1000000
```

### `--mode=pipeline`

Builds the full disruptor pipeline (journal + matching stages on separate OS threads) but bypasses network transport. The bench thread publishes `InputSlot`s directly to the input `Producer` and drains `OutputSlot`s from the output SPSC queue. Isolates pipeline latency from TCP/UDS overhead.

```sh
cargo run --release -p melin-bench -- --mode=pipeline 1000000
cargo run --release -p melin-bench --features no-persist -- --mode=pipeline 1000000   # skip journal I/O
```

### `--mode=roundtrip` (default)

Full end-to-end benchmark. Boots the server in-process, connects via TCP (default) or Unix domain socket, and measures client-perceived round-trip latency through the entire pipeline: transport, queuing, journaling, matching, and response dispatch.

```sh
cargo run --release -p melin-bench -- 1000000                          # TCP, default settings
cargo run --release -p melin-bench -- --uds 1000000                    # Unix domain socket
cargo run --release -p melin-bench -- --clients=32 --window=8 1000000  # 32 concurrent clients
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--mode=MODE` | `roundtrip` | Benchmark mode: `engine`, `pipeline`, or `roundtrip` |
| `--uds` | off | Use Unix domain socket instead of TCP (roundtrip only) |
| `--clients=N` | `1` | Number of concurrent client connections (roundtrip only) |
| `--window=N` | `64` | In-flight orders per client (roundtrip, pipeline) |
| `--bench-threads=N` | `4` | io_uring client threads (roundtrip only) |
| `--group-commit-us=N` | `0` | Journal fsync coalescing delay in microseconds (roundtrip, pipeline) |
| `<order_pairs>` | `1000000` | Number of order pairs (total orders = pairs x 2) |

## Order Flow Generator

The `generator` module produces synthetic order streams that mimic real exchange order flow:

- **High cancel ratio** — ~90% conditional cancel probability (realized ~50% because each cancel removes a live order)
- **Book depth** — limit orders rest at multiple price levels, building a realistic order book
- **Power-law price placement** — orders cluster near the mid-price with a long tail
- **Power-law order sizes** — small orders are common, large orders are rare
- **Multiple accounts** — Zipf-distributed activity (few heavy traders, many light)
- **Multiple instruments** — configurable number of symbols

### Empirical basis

Generator parameters are drawn from published academic research on limit order book microstructure:

- **Bouchaud, Mézard & Potters (2002)** — "[Statistical properties of stock order books: empirical results and models](https://arxiv.org/abs/cond-mat/0203511)". Limit order prices follow a power-law around the current price; order sizes follow a power-law distribution; book shape is hump-shaped (peak liquidity a few ticks from best quote).

- **Cont, Stoikov & Talreja (2010)** — "[A stochastic model for order book dynamics](https://www.columbia.edu/~ww2040/orderbook.pdf)". Order arrivals modeled as Poisson processes with rates decreasing with distance from best bid/ask. Cancellation rates proportional to queue depth. Simple parameter estimation from observed data.

- **Gould, Porter, Williams, McDonald, Fenn & Howison (2013)** — "[Limit order books](https://www.math.ucla.edu/~mason/papers/gould-qf-final.pdf)". Comprehensive survey of LOB stylized facts including cancel-to-trade ratios, order flow autocorrelation, and spread distributions.

## Feature Flags

| Feature | Effect |
|---------|--------|
| `no-persist` | Skip all journal I/O (no writes, no fsync) |
| `io-uring` | Use io_uring for async fsync with group commit |
| `latency-trace` | Print per-stage latency histograms on shutdown |
| `pipeline-stats` | Print per-stage busy/idle utilization on shutdown |
