# MSTR BTC Poller

A single-file Python watcher that polls SEC EDGAR for new **Strategy Inc**
(formerly MicroStrategy, ticker **MSTR**, CIK `0001050446`) 8-K filings, parses
the weekly Bitcoin-treasury update out of them, and fires alerts — with special
emphasis on detecting **BTC sales** (vs the usual purchases).

Strategy files a weekly 8-K (usually Monday) with a table of BTC bought/sold,
average price, and total holdings. Historically these were all purchases; the
first **sale** appeared in mid-2026 with a new table layout. This tool is built
to catch any future sale fast and reliably, through several independent
detection layers plus an optional LLM second opinion.

> **Disclaimer:** This is an informational tool, not financial advice. It tells
> you *what a filing says*, not whether to trade. SEC filings are public-domain
> government records. Use at your own risk; see the MIT `LICENSE`.

## Features

- **Multi-layer sale detection** — fires a `🚨 SALE SIGNAL` if *any* layer trips:
  1. **Header text** — table header says "BTC Sold" / "BTC Disposed" / "Average Sale Price".
  2. **Negative delta** — a negative value in the BTC-acquired column (handles `(535)`, `-535`, `−535`).
  3. **Prose scan** — past-tense sale language ("sold 1,500 bitcoin"); forward-looking funding language is noted but doesn't sound the alarm.
  4. **Holdings delta** — any decrease vs the last known holdings baseline (the most layout-robust layer).
- **New-asset watch** — every new 8-K (BTC update or not) is also scanned for non-BTC crypto-asset language (ETH/SOL/etc names and tickers, "digital assets other than bitcoin", or a non-BTC `X Acquired`/`X Holdings` table header). A hit fires a distinct `🟡 NEW ASSET SIGNAL` alert — coverage for a first-ever diversification disclosure the BTC parser would ignore.
- **Optional LLM second opinion** (Groq Llama 3.3 70B) — runs on **every new 8-K** after the primary regex alerts have already been sent (its verdict arrives as a separate message and never delays them). Checks both for BTC sales AND for non-BTC asset disclosures in any wording. If it flags something the regex missed — a sale or a new asset — you get an escalation alert. Catch-all for novel layouts and novel phrasing.
- **Two SEC feeds** (`--feed`): `submissions` (per-CIK JSON, never misses), `getcurrent` (global firehose, often lower latency), or `both` — poll the two concurrently and alert from whichever detects a new filing first (shared seen-state guarantees exactly one alert; total request rate to SEC is unchanged; one throttled feed can't blind the poller).
- **Telegram broadcast** — auto-discovers subscribers (anyone who sends `/start`) and broadcasts alerts to all of them.
- **Health monitoring** — degraded/recovered alerts after consecutive poll failures, plus periodic heartbeat so a dead process is visible.
- Stdlib + `requests` only.

## Install

```bash
pip install -r requirements.txt
```

## Configure

```bash
cp config.env.example config.env   # Windows: copy config.env.example config.env
```

Then edit `config.env`. The only **required** key is `SEC_UA` — set it to your
own contact email (SEC rate-limits/blocks generic User-Agents). Telegram and
Groq are optional. `config.env` is gitignored — never commit real secrets.

## Usage

```bash
# Sanity check: parse a past filing through the full pipeline (no state change)
python mstr_btc_poller.py --backtest 0001193125-26-249768

# Verify Telegram delivery end-to-end (no filing needed).
# Sends ONLY to TELEGRAM_CHAT_ID (the owner) — subscribers never see tests.
python mstr_btc_poller.py --test-telegram

# Prime once before going live: mark existing 8-Ks as seen + set holdings baseline
python mstr_btc_poller.py --prime

# Go live
python mstr_btc_poller.py
```

### CLI flags

`--feed`, `--interval`, `--prime`, `--once`, `--backtest`, `--test-telegram`,
`--open`, `--open-only-btc`, `--no-audible`, `--groq-api-key`, `--llm-timeout`,
`--telegram-token`, `--telegram-chat-id`, `--health-fail-threshold`,
`--heartbeat-secs`, `--log-file`, `--user-agent`.

Precedence: CLI flag > environment variable > `config.env` > built-in default.

## State files

State persists in your home directory (not in the repo):

- `~/.mstr_btc_poller_state.json` — seen filings + holdings baseline.
- `~/.mstr_btc_poller_subscribers.json` — Telegram subscribers (kept separate so
  deleting the state file to re-prime does not wipe your subscribers).

## Operational notes

- SEC permits up to 10 req/sec for automated access — always send a descriptive
  User-Agent with a contact email. Default interval is ~5 req/sec.
- The poller does **not** survive laptop sleep, lid-close, or a closed terminal.
  For 24/7 reliability run it on an always-on machine or a VPS.
- The parser is positional/regex-based and assumes specific column orders and
  header strings; Strategy has changed their layout before. The holdings-delta
  layer and the LLM are the format-robust backstops.

## License

MIT — see [LICENSE](LICENSE).
