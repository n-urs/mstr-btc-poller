#!/usr/bin/env python3
"""
MSTR (Strategy / MicroStrategy) 8-K poller.

Watches the SEC EDGAR submissions feed for CIK 0001050446 and, as soon as a
new 8-K appears, fetches the primary document, looks for the weekly BTC
update language (purchase OR sale), extracts the numbers, and prints / logs
the result. Optionally sends a Telegram alert.

Rate limit: SEC permits up to 10 req/sec for automated access; we cap at
~3 req/sec by default. ALWAYS send a descriptive User-Agent — SEC blocks
generic UAs.

Author: built for Nursat, May 2026.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import signal
import sys
import time
import webbrowser
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

CIK = "0001050446"  # Strategy (formerly MicroStrategy)
CIK_INT = int(CIK)  # 1050446 — used to match against feed entries
SUBMISSIONS_URL = f"https://data.sec.gov/submissions/CIK{CIK}.json"
ARCHIVE_BASE = "https://www.sec.gov/Archives/edgar/data"

# The global "latest filings" Atom feed — SEC's live firehose of every filing
# as it's accepted. We filter client-side for our CIK. Often lower-latency
# than the per-CIK submissions JSON because it's the live acceptance view.
GETCURRENT_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
    "&type=8-K&company=&dateb=&owner=include&count=100&output=atom"
)

# SEC requires a real, identifiable UA. Set SEC_UA in config.env to your own
# contact email — this placeholder default will get you rate-limited / blocked.
DEFAULT_UA = "MSTR-BTC-Poller (set SEC_UA to your-email@example.com)"

# Polling
DEFAULT_INTERVAL = 0.20  # ~5 req/sec; comfortably under SEC's 10/sec ceiling
REQUEST_TIMEOUT = 10

# Persisted state (so restarts don't re-alert on already-seen filings)
STATE_FILE = Path.home() / ".mstr_btc_poller_state.json"

# Telegram subscriber list lives in its OWN file, separate from STATE_FILE, so
# that deleting the state file to re-prime (a routine operation) does NOT wipe
# the auto-discovered subscribers. These keys are split out of `state` on save
# and merged back in on load, so the rest of the code can keep treating them as
# ordinary `state[...]` entries.
SUBSCRIBERS_FILE = Path.home() / ".mstr_btc_poller_subscribers.json"
SUBSCRIBER_KEYS = ("telegram_subscribers", "telegram_update_offset")

# Optional config file: key=value pairs, one per line. Loaded at startup.
# Looked up next to the script first, then in CWD. CLI args override.
CONFIG_FILENAME = "config.env"


def load_config_env() -> dict:
    """Load key=value pairs from config.env if present. Returns {} if missing.

    Format: one KEY=VALUE per line. Lines starting with '#' are comments.
    Values may be quoted with single or double quotes (quotes are stripped).
    Whitespace around '=' is allowed.
    """
    candidates = [
        Path(__file__).resolve().parent / CONFIG_FILENAME,
        Path.cwd() / CONFIG_FILENAME,
    ]
    for path in candidates:
        if path.is_file():
            cfg = {}
            try:
                for raw in path.read_text(encoding="utf-8").splitlines():
                    line = raw.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    key, _, val = line.partition("=")
                    key = key.strip()
                    val = val.strip()
                    # Strip matching surrounding quotes.
                    if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                        val = val[1:-1]
                    if key:
                        cfg[key] = val
                return cfg
            except Exception as e:
                logging.warning(f"Could not read {path}: {e}")
                return {}
    return {}

# Regex patterns for the BTC update block.
# These are designed to be tolerant of small wording / whitespace / HTML changes.
# Number capture supports:
#   - Plain numbers:           535, 43.0, 818,869
#   - Leading minus:           -535, −535 (U+2212)
#   - Accounting negatives:    (535), (43.0)
#   - Dashes meaning zero are handled separately as "cells" (see CELL_RE below)
# The captured group is the numeric portion only; sign info is captured
# separately by the cell-level regex.
NUM = r"([\d,]+(?:\.\d+)?)"  # captures 535 / 43.0 / 818,869 / 80,340

PATTERNS = {
    # "During Period May 4, 2026 to May 10, 2026"
    "period": re.compile(
        r"During\s+Period\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})\s+to\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        re.IGNORECASE,
    ),
    # "As of May 10, 2026"
    "as_of": re.compile(
        r"As\s+of\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        re.IGNORECASE,
    ),
    # Look for any of these action words to figure out purchase vs sale.
    "action_purchase": re.compile(
        r"\b(BTC\s+Acquired|bitcoin\s+purchases?|purchased\s+bitcoin)\b",
        re.IGNORECASE,
    ),
    "action_sale": re.compile(
        r"\b(BTC\s+Sold|BTC\s+Disposed|bitcoin\s+sales?|bitcoin\s+sold|sold\s+bitcoin|disposed\s+of\s+bitcoin)\b",
        re.IGNORECASE,
    ),
    # Used to locate the "data row" — the sequence of 6 numbers after the
    # header block ending with "Average Purchase Price" (the 2nd one).
    "data_row_anchor": re.compile(
        r"Aggregate\s+BTC\s+Holdings.*?Average\s+Purchase\s+Price\s*(?:\([^)]*\))?",
        re.IGNORECASE | re.DOTALL,
    ),
    # All numeric tokens (incl. commas / decimals) — used after the anchor.
    "numbers": re.compile(NUM),
}

import html as _html_module

# Strip HTML tags / squeeze whitespace before regexing.
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def clean(text: str) -> str:
    text = TAG_RE.sub(" ", text)
    text = _html_module.unescape(text)  # &#160; -> non-breaking space, &amp; -> & etc.
    text = text.replace("\xa0", " ")
    text = WS_RE.sub(" ", text)
    return text


# ----------------------------------------------------------------------------
# Data model
# ----------------------------------------------------------------------------


@dataclass
class BtcUpdate:
    accession: str
    filed_at: str
    primary_doc_url: str
    action: str  # "purchase" | "sale" | "unknown"
    period_start: Optional[str]
    period_end: Optional[str]
    as_of: Optional[str]
    btc_delta: Optional[float]            # BTC acquired or sold this week
    agg_price_week: Optional[float]       # raw value from the cell
    agg_price_week_unit: Optional[str]    # "M" (millions) or "B" (billions)
    avg_price_week: Optional[float]
    aggregate_holdings: Optional[float]
    agg_price_total_bn: Optional[float]   # $ billions
    avg_price_lifetime: Optional[float]

    def pretty(self) -> str:
        flag = "🟢 PURCHASE" if self.action == "purchase" else (
            "🔴 SALE" if self.action == "sale" else "⚪ UNKNOWN-ACTION"
        )
        unit = self.agg_price_week_unit or "?"
        lines = [
            "=" * 68,
            f"{flag}  |  filed {self.filed_at}",
            f"Accession: {self.accession}",
            f"URL: {self.primary_doc_url}",
            "-" * 68,
            f"Period:           {self.period_start} → {self.period_end}",
            f"As of:            {self.as_of}",
            f"BTC delta (week): {self.btc_delta}",
            f"Spent/Received:   ${self.agg_price_week}{unit}  @ avg ${self.avg_price_week}",
            f"Total holdings:   {self.aggregate_holdings} BTC",
            f"Cost basis (tot): ${self.agg_price_total_bn}B  @ avg ${self.avg_price_lifetime}",
            "=" * 68,
        ]
        return "\n".join(lines)


# ----------------------------------------------------------------------------
# State persistence
# ----------------------------------------------------------------------------


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except Exception:
            logging.warning("Could not parse state file; starting fresh.")
            state = {"seen_accessions": []}
    else:
        state = {"seen_accessions": []}

    # Merge in the separately-persisted subscriber list. If the subscribers file
    # exists it is authoritative for SUBSCRIBER_KEYS; otherwise we keep whatever
    # was in STATE_FILE (one-time migration from the old combined format — the
    # next save_state() will write it out to SUBSCRIBERS_FILE).
    if SUBSCRIBERS_FILE.exists():
        try:
            subs = json.loads(SUBSCRIBERS_FILE.read_text())
            for k in SUBSCRIBER_KEYS:
                if k in subs:
                    state[k] = subs[k]
        except Exception:
            logging.warning("Could not parse subscribers file; keeping any "
                            "subscribers found in state file.")
    return state


def save_state(state: dict) -> None:
    # Subscriber data is persisted to its own file (see SUBSCRIBERS_FILE) so it
    # survives deletion of STATE_FILE during a re-prime.
    main_state = {k: v for k, v in state.items() if k not in SUBSCRIBER_KEYS}
    try:
        STATE_FILE.write_text(json.dumps(main_state, indent=2))
    except Exception as e:
        logging.warning(f"Could not save state: {e}")

    subs = {k: state[k] for k in SUBSCRIBER_KEYS if k in state}
    if subs:
        try:
            SUBSCRIBERS_FILE.write_text(json.dumps(subs, indent=2))
        except Exception as e:
            logging.warning(f"Could not save subscribers: {e}")


# ----------------------------------------------------------------------------
# HTTP helpers
# ----------------------------------------------------------------------------


def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": "",  # set per-request
    })
    return s


def fetch_submissions(session: requests.Session) -> dict:
    headers = {"Host": "data.sec.gov"}
    r = session.get(SUBMISSIONS_URL, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_getcurrent(session: requests.Session) -> str:
    """Fetch the global 'latest filings' Atom feed (raw XML text)."""
    headers = {"Host": "www.sec.gov"}
    r = session.get(GETCURRENT_URL, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text


# Atom-feed entry parsing. We avoid a full XML parser dependency and use
# targeted regexes — the feed format is stable and simple.
_ENTRY_RE = re.compile(r"<entry>(.*?)</entry>", re.DOTALL)
_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.DOTALL)
_ACCNO_RE = re.compile(r"accession-number=([\d-]+)")
_INDEX_HREF_RE = re.compile(r'href="([^"]*-index\.htm)"')
_CIK_IN_TITLE_RE = re.compile(r"\((\d{10})\)")
_UPDATED_RE = re.compile(r"<updated>(.*?)</updated>", re.DOTALL)


def iter_getcurrent_8k(feed_xml: str):
    """Yield (accession, filed_date, index_url, updated) for 8-K entries in
    the getcurrent Atom feed that match our CIK.

    Note: the getcurrent feed gives an *index* URL, not the primary document
    directly. The caller resolves the primary doc separately.
    """
    for entry_match in _ENTRY_RE.finditer(feed_xml):
        entry = entry_match.group(1)
        title_m = _TITLE_RE.search(entry)
        title = title_m.group(1) if title_m else ""
        # Only 8-Ks (feed is already filtered, but be safe).
        if not title.strip().startswith("8-K"):
            continue
        # Match CIK — title contains "(0001050446)".
        cik_hits = _CIK_IN_TITLE_RE.findall(title)
        if not any(int(c) == CIK_INT for c in cik_hits):
            continue
        accno_m = _ACCNO_RE.search(entry)
        index_m = _INDEX_HREF_RE.search(entry)
        updated_m = _UPDATED_RE.search(entry)
        if not accno_m or not index_m:
            continue
        accession = accno_m.group(1)
        index_url = index_m.group(1)
        if index_url.startswith("/"):
            index_url = "https://www.sec.gov" + index_url
        updated = updated_m.group(1).strip() if updated_m else ""
        # filed_date: derive from updated timestamp (YYYY-MM-DD prefix).
        filed_date = updated[:10] if len(updated) >= 10 else ""
        yield accession, filed_date, index_url, updated


def resolve_primary_from_index(session: requests.Session, index_url: str):
    """Given a filing index page URL, find the primary .htm document URL.

    The getcurrent feed only gives us the index page. Rather than scrape the
    HTML index, we fetch the accession directory's index.json (small, fast to
    parse) and pick the primary 8-K document.

    Returns the primary doc URL or None.
    """
    # Derive the accession directory from the index URL, e.g.
    #   .../Archives/edgar/data/1050446/000119312526215754/0001193125-26-215754-index.htm
    # -> .../Archives/edgar/data/1050446/000119312526215754/
    m = re.search(r"(.*/Archives/edgar/data/\d+/\d+)/", index_url)
    if not m:
        return _resolve_primary_via_html(session, index_url)
    acc_dir = m.group(1)

    headers = {"Host": "www.sec.gov"}
    try:
        r = session.get(acc_dir + "/index.json", headers=headers,
                         timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        items = r.json().get("directory", {}).get("item", [])
        # Candidate primary docs: .htm files that aren't the index pages and
        # aren't the XBRL R-files (R1.htm, R2.htm, ...).
        candidates = []
        for it in items:
            name = it.get("name", "")
            low = name.lower()
            if not low.endswith(".htm"):
                continue
            if "-index" in low or "index-headers" in low:
                continue
            if re.fullmatch(r"r\d+\.htm", low):  # XBRL viewer fragments
                continue
            candidates.append(name)
        if candidates:
            # Prefer the issuer-named doc (e.g. mstr-YYYYMMDD.htm) — typically
            # the only remaining candidate, but if several, take the longest
            # name which is almost always the real document vs a stub.
            primary = sorted(candidates, key=len, reverse=True)[0]
            return f"{acc_dir}/{primary}"
    except Exception as e:
        logging.warning(f"index.json resolution failed ({e}); "
                        f"falling back to HTML scrape.")

    return _resolve_primary_via_html(session, index_url)


def _resolve_primary_via_html(session: requests.Session, index_url: str):
    """Fallback: scrape the HTML index page for the primary document link.

    Handles EDGAR's inline-XBRL viewer links (`/ix?doc=<path>`) and skips
    site-navigation links.
    """
    headers = {"Host": "www.sec.gov"}
    r = session.get(index_url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    html = r.text

    m = re.search(r"(/Archives/edgar/data/\d+/\d+)/", index_url)
    archive_dir = m.group(1) if m else None

    ix_links = re.findall(r'href="/ix\?doc=([^"]+\.htm[^"]*)"', html)
    for doc_path in ix_links:
        fname = doc_path.rsplit("/", 1)[-1]
        if "-index" in fname:
            continue
        return "https://www.sec.gov" + (doc_path if doc_path.startswith("/")
                                        else "/" + doc_path)

    if archive_dir:
        all_links = re.findall(r'href="([^"]+\.htm[^"]*)"', html)
        for link in all_links:
            if archive_dir not in link:
                continue
            fname = link.rsplit("/", 1)[-1]
            if "-index" in fname:
                continue
            return "https://www.sec.gov" + (link if link.startswith("/")
                                            else "/" + link)
    return None


def fetch_primary_doc(session: requests.Session, accession: str, primary_filename: str) -> str:
    """Fetch the primary document for a filing."""
    accession_nodash = accession.replace("-", "")
    cik_int = int(CIK)  # strips leading zeros for URL path
    url = f"{ARCHIVE_BASE}/{cik_int}/{accession_nodash}/{primary_filename}"
    headers = {"Host": "www.sec.gov"}
    r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text, url


def fetch_doc_by_url(session: requests.Session, url: str):
    """Fetch a document given its full URL (used by the getcurrent path)."""
    headers = {"Host": "www.sec.gov"}
    r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.text, url


# ----------------------------------------------------------------------------
# Parsing
# ----------------------------------------------------------------------------


def _to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


# Prose-level sale-language patterns. These run even when the structured
# table doesn't parse — they're the last line of defense against a sale
# being disclosed in a layout we don't recognize.
#
# We split into two tiers:
#   STRONG = phrases that imply a sale ALREADY HAPPENED (past tense, specific
#            amounts, definite language)
#   WEAK   = phrases that COULD indicate sale-discussion but might be
#            forward-looking (e.g. "proceeds from the sale of bitcoin" as a
#            stated funding source for a future repurchase). Weak hits get
#            flagged but with a "possible" caveat.
SALE_LANGUAGE_STRONG_RE = re.compile(
    r"\b("
    r"sold\s+(?:approximately\s+)?[\d,]+\s+(?:bitcoin|BTC)"
    r"|disposed\s+of\s+(?:approximately\s+)?[\d,]+\s+(?:bitcoin|BTC)"
    r"|liquidat(?:ed|ion\s+of)\s+(?:approximately\s+)?[\d,]+\s+(?:bitcoin|BTC)"
    r"|BTC\s+(?:Sold|Disposed)\s+\(\d+\)"   # column header in a table
    r"|net\s+(?:reduction|decrease)\s+(?:in|of)\s+(?:bitcoin|BTC)\s+holdings"
    r")\b",
    re.IGNORECASE,
)
SALE_LANGUAGE_WEAK_RE = re.compile(
    r"\b("
    r"bitcoin\s+(?:sales?|dispositions?)"
    r"|sale\s+of\s+bitcoin"
    r"|proceeds\s+from\s+the\s+sale\s+of\s+bitcoin"
    r")\b",
    re.IGNORECASE,
)


def scan_for_sale_language(html: str):
    """Returns (tier, context) where tier is 'strong', 'weak', or None.

    'strong' = sale disclosure language with specific amounts or past tense.
    'weak'   = sale-related language that might be forward-looking or
               describing potential funding sources rather than actual sales.
    """
    text = clean(html)
    m = SALE_LANGUAGE_STRONG_RE.search(text)
    if m:
        start = max(0, m.start() - 40)
        end = min(len(text), m.end() + 40)
        return ("strong", text[start:end].strip())
    m = SALE_LANGUAGE_WEAK_RE.search(text)
    if m:
        start = max(0, m.start() - 40)
        end = min(len(text), m.end() + 40)
        return ("weak", text[start:end].strip())
    return (None, None)


def parse_btc_update(html: str, accession: str, filed_at: str, url: str) -> Optional[BtcUpdate]:
    """
    Returns a BtcUpdate if this filing looks like a weekly BTC update,
    otherwise None.
    """
    text = clean(html)

    # Quick reject: must have some BTC-update-ish language
    has_period = PATTERNS["period"].search(text) is not None
    has_btc_keyword = (
        PATTERNS["action_purchase"].search(text) is not None
        or PATTERNS["action_sale"].search(text) is not None
        or "BTC Acquired" in text
        or "Aggregate BTC Holdings" in text
    )
    if not (has_period and has_btc_keyword):
        return None

    # Action — determined PRIMARILY by the BTC-update block's table header,
    # not by stray keywords elsewhere in the document (the dashboard
    # boilerplate contains "bitcoin purchases" which would otherwise create
    # false "mixed" classifications). We refine this below once we've located
    # the BTC block. For now, set a provisional value from the whole document.
    has_sale = PATTERNS["action_sale"].search(text) is not None
    has_purchase = PATTERNS["action_purchase"].search(text) is not None
    if has_sale and not has_purchase:
        action = "sale"
    elif has_purchase and not has_sale:
        action = "purchase"
    elif has_sale and has_purchase:
        action = "mixed"
    else:
        action = "unknown"

    # Locate the BTC-update block(s). There can be MULTIPLE "During Period"
    # blocks in the same 8-K:
    #   - unrelated blocks (e.g. ATM share sales) — skipped, no BTC keywords;
    #   - SEVERAL BTC blocks in a single filing. Strategy splits a reporting
    #     week into sub-periods, each with its own BTC Sold/Acquired row and
    #     its own running holdings total (first seen 2026-07-06: 1,363 BTC sold
    #     in one sub-period, then 2,225 in the next). EVERY BTC block has to be
    #     parsed and combined — reading only the first silently understates
    #     both the weekly delta and the holdings baseline.
    period_matches = list(PATTERNS["period"].finditer(text))
    btc_period_matches = []
    for i, pm in enumerate(period_matches):
        # Bound the look-ahead by the start of the NEXT period block, or 600
        # chars, whichever is smaller. This avoids leaking into a later block.
        next_start = period_matches[i + 1].start() if i + 1 < len(period_matches) else len(text)
        window_end = min(pm.start() + 600, next_start)
        window = text[pm.start():window_end]
        if re.search(
            r"BTC\s+Acquired|BTC\s+Sold|BTC\s+Disposed|Aggregate\s+BTC\s+Holdings",
            window, re.IGNORECASE,
        ):
            btc_period_matches.append(pm)
    if not btc_period_matches and period_matches:
        btc_period_matches = [period_matches[0]]
    period_match = btc_period_matches[0] if btc_period_matches else None
    last_period_match = btc_period_matches[-1] if btc_period_matches else None

    # Period / as-of. The reported window spans the FIRST BTC block's start
    # through the LAST block's end; "as of" comes from the last block, which
    # carries the most recent holdings figure.
    period_start = period_end = as_of = None
    if period_match:
        period_start = period_match.group(1)
        period_end = last_period_match.group(2)
        as_of_m = PATTERNS["as_of"].search(text, last_period_match.start())
        if as_of_m:
            as_of = as_of_m.group(1)

    # Numbers. There are TWO known layouts:
    #
    #  A) PURCHASE layout (one contiguous row of 6 numbers):
    #     BTC Acquired | Agg Purchase Price (M/B) | Avg Purchase Price |
    #     Aggregate BTC Holdings | Agg Purchase Price (B) | Avg Purchase Price
    #
    #  B) SALE layout (split into two sub-tables):
    #     BTC Sold | Aggregate Sale Price (M) | Average Sale Price   -> 3 cells
    #     [then separately] Aggregate BTC Holdings | Agg Purchase Price (B) |
    #     Average Purchase Price                                     -> 3 cells
    #
    # We detect which layout by looking for "BTC Sold"/"Average Sale Price"
    # vs "BTC Acquired" in the BTC block header.
    btc_delta = agg_week = avg_week = holdings = agg_total_bn = avg_life = None
    agg_week_unit = None
    btc_block_is_sale = False

    # Cell tokenizer (shared by both layouts).
    CELL_RE = re.compile(
        r"\$?\s*(?:"
        r"\(\s*\$?\s*(?P<paren>[\d,]+(?:\.\d+)?)\s*\)"
        r"|(?P<sign>[-−])(?P<signed>[\d,]+(?:\.\d+)?)"
        r"|\+(?P<plus>[\d,]+(?:\.\d+)?)"
        r"|(?P<plain>[\d,]+(?:\.\d+)?)"
        r"|(?<![\d.])(?P<dash>[-–—])(?![\d.])"
        r")"
    )

    def _cell_value(m):
        if m.group("paren") is not None:
            return -_to_float(m.group("paren"))
        if m.group("signed") is not None:
            return -_to_float(m.group("signed"))
        if m.group("plus") is not None:
            return _to_float(m.group("plus"))
        if m.group("plain") is not None:
            return _to_float(m.group("plain"))
        if m.group("dash") is not None:
            return 0.0
        return None

    def _cells_after(anchor_regex, src, n):
        """Return up to n cell values appearing after the last match of
        anchor_regex in src."""
        hits = list(re.finditer(anchor_regex, src, re.IGNORECASE))
        start = hits[-1].end() if hits else 0
        out = []
        for m in CELL_RE.finditer(src, pos=start):
            out.append(_cell_value(m))
            if len(out) >= n:
                break
        return out

    def _block_text(pm):
        """Text of ONE BTC block: from its "During Period" anchor up to the
        next period block, the footnote marker, or a 2000-char cap."""
        block_start = pm.start()
        footnote_m = re.search(
            r"\(\s*1\s*\)\s+(?:The\s+bitcoin|No\s+bitcoin|Proceeds\s+from|"
            r"Aggregate\s+and\s+average)",
            text[block_start:], re.IGNORECASE,
        )
        try:
            pm_idx = period_matches.index(pm)
            next_pm = period_matches[pm_idx + 1] if pm_idx + 1 < len(period_matches) else None
        except ValueError:
            next_pm = None
        candidates = [block_start + 2000]
        if footnote_m:
            candidates.append(block_start + footnote_m.start())
        if next_pm is not None:
            candidates.append(next_pm.start())
        block = text[block_start:min(candidates)]
        # Strip single-digit footnote markers (1)–(9).
        block = re.sub(r"\(\s*[1-9]\s*\)", " ", block)
        # Strip dates so their digits aren't counted as data.
        block = re.sub(
            r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
            r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|"
            r"Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},?\s+\d{4}",
            " ", block, flags=re.IGNORECASE,
        )
        # Strip time-of-day stamps like "4:00 p.m." that some filings include.
        block = re.sub(r"\d{1,2}:\d{2}\s*[ap]\.?m\.?", " ", block, flags=re.IGNORECASE)
        return block

    def _parse_block(block):
        """Parse ONE BTC sub-period block into its constituent figures."""
        # Detect SALE layout: header has "BTC Sold" / "Average Sale Price".
        is_sale = bool(re.search(
            r"BTC\s+Sold|BTC\s+Disposed|Average\s+Sale\s+Price|Aggregate\s+Sale\s+Price",
            block, re.IGNORECASE,
        ))
        unit = None

        if is_sale:
            # --- SALE layout ---
            # Sub-table 1: BTC Sold | Aggregate Sale Price | Average Sale Price
            # The 3 cells come right after "Average Sale Price".
            sale_cells = _cells_after(r"Average\s+Sale\s+Price", block, 3)
            while len(sale_cells) < 3:
                sale_cells.append(None)
            btc_sold_qty, agg_sale_price, avg_sale_price = sale_cells
            # A sale is recorded as a NEGATIVE delta.
            delta = -btc_sold_qty if btc_sold_qty is not None else None
            week_agg = agg_sale_price
            week_avg = avg_sale_price

            # Sub-table 2: Aggregate BTC Holdings | Agg Purchase Price (B) |
            # Average Purchase Price. Cells come after the LAST
            # "Average Purchase Price".
            hold_cells = _cells_after(r"Average\s+Purchase\s+Price", block, 3)
            while len(hold_cells) < 3:
                hold_cells.append(None)
            hold, total_bn, life = hold_cells

            # Unit of the sale price column.
            unit_hits = list(re.finditer(
                r"\(\s*in\s+(millions?|billions?)\s*\)", block, re.IGNORECASE))
            if unit_hits:
                u = unit_hits[0].group(1).lower()
                unit = "B" if u.startswith("billion") else "M"

        else:
            # --- PURCHASE layout (classic 6-cell contiguous row) ---
            avg_iter = list(re.finditer(
                r"Average\s+Purchase\s+Price", block, re.IGNORECASE))
            scan_from = avg_iter[-1].end() if avg_iter else 0
            cells = []
            for m in CELL_RE.finditer(block, pos=scan_from):
                cells.append(_cell_value(m))
                if len(cells) >= 6:
                    break
            while len(cells) < 6:
                cells.append(None)
            delta, week_agg, week_avg, hold, total_bn, life = cells

            header_part = block[:avg_iter[-1].end()] if avg_iter else block
            unit_hits = list(re.finditer(
                r"\(\s*in\s+(millions?|billions?)\s*\)",
                header_part, re.IGNORECASE))
            if unit_hits:
                u = unit_hits[0].group(1).lower()
                unit = "B" if u.startswith("billion") else "M"

        return {
            "is_sale": is_sale,
            "delta": delta,
            "agg_week": week_agg,
            "avg_week": week_avg,
            "holdings": hold,
            "agg_total_bn": total_bn,
            "avg_life": life,
            "unit": unit,
            "has_acquired": bool(re.search(r"BTC\s+Acquired", block, re.IGNORECASE)),
        }

    blocks = [_parse_block(_block_text(pm)) for pm in btc_period_matches]

    if blocks:
        if len(blocks) > 1:
            logging.info(f"Filing reports {len(blocks)} BTC sub-periods — "
                         f"combining them into a single weekly figure.")
        btc_block_is_sale = any(b["is_sale"] for b in blocks)

        # Weekly delta = sum over every sub-period.
        deltas = [b["delta"] for b in blocks if b["delta"] is not None]
        btc_delta = sum(deltas) if deltas else None

        # Weekly $ column = sum over sub-periods. Units are normally identical;
        # if they ever differ, normalise everything to millions.
        weeks = [(b["agg_week"], b["unit"]) for b in blocks
                 if b["agg_week"] is not None]
        units = {u for _, u in weeks if u}
        if weeks:
            if len(units) > 1:
                agg_week = sum(v * (1000.0 if u == "B" else 1.0) for v, u in weeks)
                agg_week_unit = "M"
            else:
                agg_week = sum(v for v, _ in weeks)
                agg_week_unit = next(iter(units), None)
            agg_week = round(agg_week, 4)  # tidy float-summation noise

        # Average price = quantity-weighted across sub-periods.
        weighted = [(b["avg_week"], abs(b["delta"])) for b in blocks
                    if b["avg_week"] is not None and b["delta"]]
        wsum = sum(w for _, w in weighted)
        if wsum:
            avg_week = round(sum(v * w for v, w in weighted) / wsum, 2)
        else:
            avgs = [b["avg_week"] for b in blocks if b["avg_week"] is not None]
            avg_week = avgs[-1] if avgs else None

        # Running totals: the LAST sub-period carries the current figures.
        for b in blocks:
            if b["holdings"] is not None:
                holdings = b["holdings"]
                agg_total_bn = b["agg_total_bn"]
                avg_life = b["avg_life"]

        if btc_block_is_sale:
            action = "sale"  # header is authoritative here
        elif btc_delta is not None and btc_delta < 0 and action in ("purchase", "mixed", "unknown"):
            # Negative delta in a purchase-layout table = sale.
            logging.info(
                f"Negative BTC delta ({btc_delta}) — overriding "
                f"action='{action}' to 'sale'."
            )
            action = "sale"

        # Final guard: if the block clearly is a purchase (BTC Acquired header,
        # positive delta) but whole-doc keywords made it "mixed", trust the block.
        if not btc_block_is_sale and any(b["has_acquired"] for b in blocks):
            if action == "mixed" and (btc_delta is None or btc_delta >= 0):
                action = "purchase"

    return BtcUpdate(
        accession=accession,
        filed_at=filed_at,
        primary_doc_url=url,
        action=action,
        period_start=period_start,
        period_end=period_end,
        as_of=as_of,
        btc_delta=btc_delta,
        agg_price_week=agg_week,
        agg_price_week_unit=agg_week_unit,
        avg_price_week=avg_week,
        aggregate_holdings=holdings,
        agg_price_total_bn=agg_total_bn,
        avg_price_lifetime=avg_life,
    )


# ----------------------------------------------------------------------------
# Submissions feed handling
# ----------------------------------------------------------------------------


def iter_recent_8k(submissions: dict):
    """Yield (accession, filed_date, primary_document) for recent 8-K filings."""
    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    dates = recent.get("filingDate", [])
    primaries = recent.get("primaryDocument", [])
    for i, form in enumerate(forms):
        if form == "8-K":
            yield accessions[i], dates[i], primaries[i]


# ----------------------------------------------------------------------------
# Optional Telegram
# ----------------------------------------------------------------------------


def telegram_send(token: str, chat_id: str, text: str) -> None:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        r.raise_for_status()
    except Exception as e:
        logging.warning(f"Telegram send to {chat_id} failed: {e}")


def telegram_discover_subscribers(token: str, state: dict,
                                  offset: Optional[int] = None) -> int:
    """Poll getUpdates to find chat IDs of everyone who has messaged the bot.

    Stores discovered chat IDs in state['telegram_subscribers'] (a list) and
    tracks the update offset in state['telegram_update_offset'] so we don't
    reprocess old updates. Replies with a welcome message to brand-new
    subscribers. Returns the number of NEW subscribers found this call.

    Telegram rule: a bot can only message users who have messaged it first,
    so this is how the audience opts in — by sending /start (or anything).
    """
    if not token:
        return 0
    subs = set(str(s) for s in state.get("telegram_subscribers", []))
    use_offset = state.get("telegram_update_offset")
    params = {"timeout": 0}
    if use_offset is not None:
        params["offset"] = use_offset
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{token}/getUpdates",
            params=params, timeout=10,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logging.warning(f"telegram getUpdates failed: {e}")
        return 0

    new_count = 0
    max_update_id = use_offset - 1 if use_offset is not None else -1
    for upd in data.get("result", []):
        update_id = upd.get("update_id", 0)
        if update_id > max_update_id:
            max_update_id = update_id
        # A message (or other event) carries a chat we can reply to.
        msg = upd.get("message") or upd.get("my_chat_member") or {}
        chat = msg.get("chat", {})
        chat_id = chat.get("id")
        if chat_id is None:
            continue
        chat_id = str(chat_id)
        if chat_id not in subs:
            subs.add(chat_id)
            new_count += 1
            # Welcome / confirm subscription.
            name = chat.get("first_name") or chat.get("title") or "there"
            telegram_send(
                token, chat_id,
                f"✅ Subscribed to MSTR BTC filing alerts, {name}. "
                f"You'll get a message when a new Strategy 8-K BTC update is "
                f"detected — especially if it looks like a sale."
            )
            logging.info(f"New Telegram subscriber: {chat_id} ({name})")

    state["telegram_subscribers"] = sorted(subs)
    if max_update_id >= 0:
        # Next poll starts after the highest update we've seen (acks them).
        state["telegram_update_offset"] = max_update_id + 1
    return new_count


def telegram_broadcast(token: str, state: dict, text: str) -> int:
    """Send `text` to every subscriber chat ID stored in state.
    Returns the number of successful sends."""
    if not token:
        return 0
    subs = state.get("telegram_subscribers", [])
    sent = 0
    for chat_id in subs:
        try:
            telegram_send(token, chat_id, text)
            sent += 1
        except Exception as e:
            logging.warning(f"broadcast to {chat_id} failed: {e}")
    return sent


def telegram_notify_all(token: str, configured_chat_id: Optional[str],
                        state: dict, text: str) -> int:
    """Send to the configured chat ID (if set) AND all discovered subscribers,
    de-duplicating so nobody gets the same message twice. Returns send count."""
    if not token:
        return 0
    targets = set()
    if configured_chat_id:
        targets.add(str(configured_chat_id))
    for s in state.get("telegram_subscribers", []):
        targets.add(str(s))
    sent = 0
    for chat_id in targets:
        try:
            telegram_send(token, chat_id, text)
            sent += 1
        except Exception as e:
            logging.warning(f"notify to {chat_id} failed: {e}")
    return sent


# ----------------------------------------------------------------------------
# LLM analysis (Groq, Llama 3.3 70B)
# ----------------------------------------------------------------------------

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

LLM_SYSTEM_PROMPT = """You are a precise financial-disclosure analyst.

Your ONE job: given the text of a Strategy (formerly MicroStrategy) 8-K filing,
determine whether the filing discloses an ACTUAL BITCOIN SALE that has already
occurred during the reporting period.

CRITICAL DISTINCTIONS:
- A bitcoin PURCHASE is NOT a sale, even if the company also sold shares (ATM).
- "Proceeds from the sale of bitcoin" listed as a POTENTIAL future funding
  source for note repurchases is NOT a sale — it's forward-looking language.
- A weekly update showing zero bitcoin purchased (with dashes "-") is NOT a sale.
- A "BTC Acquired" value of zero or a dash is NOT a sale.
- An actual sale requires past-tense, specific-amount language like "sold X
  bitcoin" or "disposed of X BTC", OR a table with a negative bitcoin delta,
  OR explicit "BTC Sold" / "BTC Disposed" column headers with non-zero values.

Respond with ONLY a JSON object, no other text, with these fields:
  - sale_detected: boolean
  - confidence: one of "low", "medium", "high"
  - btc_sold: number or null (BTC sold this period, null if not stated)
  - reasoning: string (one sentence, max 200 chars)

Example of a forward-looking funding mention (NOT a sale):
{"sale_detected": false, "confidence": "high", "btc_sold": null,
 "reasoning": "Filing mentions sale of bitcoin only as a potential funding source for future note repurchases, not an executed sale."}

Example of an actual sale:
{"sale_detected": true, "confidence": "high", "btc_sold": 1500,
 "reasoning": "Filing states the company sold 1,500 BTC during the week to fund convertible note repurchases."}
"""


def analyze_with_llm(html: str, regex_summary: str, api_key: str,
                     timeout: float = 15.0) -> Optional[dict]:
    """Send the cleaned filing text to Groq's Llama 3.3 70B for a sale check.
    Returns parsed JSON dict or None on any failure. Never raises."""
    if not api_key:
        return None
    text = clean(html)
    # Real BTC-update 8-Ks clean down to ~6,500 chars after HTML stripping.
    # 20,000 gives ~3x headroom for future filings with appendices or longer
    # prose, while still capping disaster cases (e.g. SEC returning a huge
    # document by mistake). At ~$0.04/M input tokens, even hitting the cap
    # costs <$0.0002 per call.
    MAX_LLM_CHARS = 20000
    truncated = len(text) > MAX_LLM_CHARS
    if truncated:
        text = text[:MAX_LLM_CHARS]
        logging.info(
            f"LLM input truncated to {MAX_LLM_CHARS:,} chars "
            f"(original was {len(text):,}); BTC content is normally near "
            f"the top so this is usually safe."
        )

    user_msg = (
        f"Regex analysis already found: {regex_summary}\n\n"
        f"Now independently analyze the following 8-K text:\n\n{text}"
    )

    body = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": LLM_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        "temperature": 0.0,           # deterministic
        "max_tokens": 300,
        "response_format": {"type": "json_object"},
    }
    try:
        r = requests.post(
            GROQ_API_URL,
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json=body,
            timeout=timeout,
        )
        r.raise_for_status()
        data = r.json()
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        # Validate the shape we expect.
        if not isinstance(parsed, dict):
            return None
        if "sale_detected" not in parsed:
            return None
        return parsed
    except requests.Timeout:
        logging.warning(f"LLM request timed out after {timeout}s.")
        return None
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        logging.warning(f"LLM HTTP error {status}: {e}")
        return None
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logging.warning(f"LLM returned malformed response: {e}")
        return None
    except Exception as e:
        logging.warning(f"LLM call failed: {e}")
        return None


def audible_alert() -> None:
    """Make an audible noise on Windows / macOS / Linux. Best-effort."""
    try:
        if sys.platform == "win32":
            import winsound
            # Three short beeps — distinctive vs system sounds.
            for _ in range(3):
                winsound.Beep(880, 200)
                time.sleep(0.1)
        else:
            # ASCII BEL — works on most terminals.
            sys.stdout.write("\a")
            sys.stdout.flush()
    except Exception:
        pass


class HealthMonitor:
    """Tracks consecutive failures and fires alerts when health degrades or recovers.

    Behaviour:
      - After N consecutive failures, fires a single "degraded" alert.
      - On the next success, fires a single "recovered" alert.
      - Emits a heartbeat log line every `heartbeat_secs` so silence is visible.
    """

    def __init__(self, telegram_token=None, telegram_chat_id=None,
                 fail_threshold=5, heartbeat_secs=300, audible=True,
                 state=None):
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self.fail_threshold = fail_threshold
        self.heartbeat_secs = heartbeat_secs
        self.audible = audible
        self.state = state if state is not None else {}

        self.consecutive_failures = 0
        self.alerted_degraded = False
        self.last_success_at = time.monotonic()
        self.last_heartbeat_at = time.monotonic()
        self.last_failure_reason = ""

    def _send(self, msg: str) -> None:
        logging.warning(msg)
        if self.telegram_token:
            telegram_notify_all(self.telegram_token, self.telegram_chat_id,
                                self.state, msg)
        if self.audible:
            audible_alert()

    def record_success(self) -> None:
        self.last_success_at = time.monotonic()
        if self.alerted_degraded:
            self._send(
                "✅ MSTR poller RECOVERED — successfully polling SEC again."
            )
            self.alerted_degraded = False
        self.consecutive_failures = 0
        self.last_failure_reason = ""

    def record_failure(self, reason: str) -> None:
        self.consecutive_failures += 1
        self.last_failure_reason = reason
        if (self.consecutive_failures >= self.fail_threshold
                and not self.alerted_degraded):
            secs_since_ok = int(time.monotonic() - self.last_success_at)
            self._send(
                f"⚠️ MSTR poller DEGRADED — {self.consecutive_failures} "
                f"consecutive failures, no successful poll in {secs_since_ok}s. "
                f"Last error: {reason}"
            )
            self.alerted_degraded = True

    def heartbeat(self) -> None:
        now = time.monotonic()
        if now - self.last_heartbeat_at >= self.heartbeat_secs:
            secs_since_ok = int(now - self.last_success_at)
            status = "OK" if self.consecutive_failures == 0 else (
                f"DEGRADED ({self.consecutive_failures} fails, "
                f"last err: {self.last_failure_reason})"
            )
            logging.info(
                f"♥ heartbeat — status: {status}, "
                f"{secs_since_ok}s since last successful poll."
            )
            self.last_heartbeat_at = now


# ----------------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------------


def main():
    # The alert output (and logs) contain emoji — 🔴 SALE, 🚨 banners, 🤖 LLM,
    # etc. The default Windows console is cp1252, which can't encode them and
    # raises UnicodeEncodeError mid-alert (i.e. exactly when a sale fires). Force
    # the std streams to UTF-8 so the primary platform doesn't crash on its most
    # important output. errors="replace" is a belt-and-suspenders fallback for
    # any console that still can't render a given glyph.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass  # non-reconfigurable stream (e.g. redirected/older Python)

    # Load config.env (if present) and merge with environment variables.
    # Precedence: CLI flag > os.environ > config.env > built-in default.
    cfg = load_config_env()

    def cfg_get(key, default=None):
        return os.environ.get(key) or cfg.get(key) or default

    ap = argparse.ArgumentParser(
        description="Poll SEC EDGAR for new MSTR 8-K filings and extract BTC updates. "
                    "Settings can also come from config.env (same directory as the script).",
    )
    ap.add_argument("--user-agent", default=cfg_get("SEC_UA", DEFAULT_UA),
                    help="SEC requires a real UA with contact info.")
    ap.add_argument("--interval", type=float,
                    default=float(cfg_get("INTERVAL", DEFAULT_INTERVAL)),
                    help="Seconds between polls (default 0.20s ≈ 5/s).")
    ap.add_argument("--feed", default=cfg_get("FEED", "submissions"),
                    choices=["submissions", "getcurrent"],
                    help="Which SEC feed to poll. 'submissions' = per-CIK JSON "
                         "(data.sec.gov, 1 fetch per filing). 'getcurrent' = "
                         "global latest-filings Atom feed (often lower latency, "
                         "2 fetches per filing). Default: submissions.")
    ap.add_argument("--prime", action="store_true",
                    help="On startup, mark all existing 8-Ks as seen (don't alert on history).")
    ap.add_argument("--once", action="store_true",
                    help="Run one pass and exit (for cron / testing).")
    ap.add_argument("--telegram-token", default=cfg_get("TELEGRAM_BOT_TOKEN"))
    ap.add_argument("--telegram-chat-id", default=cfg_get("TELEGRAM_CHAT_ID"))
    ap.add_argument("--test-telegram", action="store_true",
                    help="Discover subscribers, send a one-off TEST broadcast to the "
                         "configured chat + all subscribers, report the count, and exit. "
                         "Use to verify end-to-end Telegram delivery without a real filing.")
    ap.add_argument("--open", action="store_true",
                    default=cfg_get("OPEN_IN_BROWSER", "").lower() in ("1", "true", "yes"),
                    help="Open each new 8-K in your default browser as soon as it's detected.")
    ap.add_argument("--open-only-btc", action="store_true",
                    default=cfg_get("OPEN_ONLY_BTC", "").lower() in ("1", "true", "yes"),
                    help="With --open: only open filings that look like BTC updates (skip ATM filings etc).")
    ap.add_argument("--backtest", default=None,
                    help="Comma-separated list of past accession numbers to fetch and parse "
                         "as if they just arrived (no polling, no state mutation). "
                         "Example: --backtest 0001193125-26-215754,0001193125-26-202611")
    ap.add_argument("--health-fail-threshold", type=int,
                    default=int(cfg_get("HEALTH_FAIL_THRESHOLD", 5)),
                    help="Send a 'degraded' alert after this many consecutive poll failures (default 5).")
    ap.add_argument("--heartbeat-secs", type=int,
                    default=int(cfg_get("HEARTBEAT_SECS", 300)),
                    help="Log a heartbeat line every N seconds (default 300 = 5 min) so silence is visible.")
    ap.add_argument("--no-audible", action="store_true",
                    default=cfg_get("NO_AUDIBLE", "").lower() in ("1", "true", "yes"),
                    help="Disable audible beep alerts (Windows: winsound; other: terminal BEL).")
    ap.add_argument("--groq-api-key", default=cfg_get("GROQ_API_KEY"),
                    help="If set, send each new BTC update to Groq's Llama 3.3 70B "
                         "for an independent second-opinion sale check.")
    ap.add_argument("--llm-timeout", type=float,
                    default=float(cfg_get("LLM_TIMEOUT", 15)),
                    help="Max seconds to wait for the LLM response (default 15).")
    ap.add_argument("--log-file", default=cfg_get("LOG_FILE"))
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            # Explicit UTF-8 — log lines contain emoji (✅ 🚨 🔴 …); the default
            # locale encoding (cp1252 on Windows) would crash on them.
            *([logging.FileHandler(args.log_file, encoding="utf-8")] if args.log_file else []),
        ],
    )

    # Log config source
    if cfg:
        logging.info(f"Loaded {len(cfg)} setting(s) from config.env.")
    else:
        logging.info("No config.env found — using environment vars / CLI args / defaults.")

    if args.user_agent == DEFAULT_UA:
        logging.warning("Using default User-Agent. Set SEC_UA in config.env (or pass "
                        "--user-agent) with your real email — SEC will rate-limit "
                        "generic UAs.")

    session = make_session(args.user_agent)
    state = load_state()
    seen = set(state.get("seen_accessions", []))

    # Test-telegram mode: exercise the REAL broadcast path (discover subscribers,
    # then telegram_notify_all to configured chat + all subscribers) so you can
    # confirm end-to-end delivery without waiting for a filing. Does not poll SEC.
    if args.test_telegram:
        if not args.telegram_token:
            logging.error("--test-telegram needs TELEGRAM_BOT_TOKEN (config.env) "
                          "or --telegram-token.")
            sys.exit(1)
        try:
            n = telegram_discover_subscribers(args.telegram_token, state)
            save_state(state)
            logging.info(f"Subscriber discovery: {len(state.get('telegram_subscribers', []))} "
                         f"total ({n} new).")
        except Exception as e:
            logging.warning(f"Subscriber discovery failed: {e}")
        msg = ("🧪 TEST ALERT — mstr8k poller\n\nIf you can read this, end-to-end "
               "Telegram delivery works. This is a manual test, not a real filing.")
        sent = telegram_notify_all(args.telegram_token, args.telegram_chat_id, state, msg)
        if sent:
            logging.info(f"✅ Test broadcast sent to {sent} recipient(s).")
        else:
            logging.warning("No recipients. Send /start to the bot first, or set "
                            "TELEGRAM_CHAT_ID in config.env.")
        return

    # Backtest mode: fetch the given accessions and parse them as if they
    # just arrived. Does not poll, does not modify state. Honours --open /
    # --open-only-btc and --feed (so the getcurrent resolution path can be
    # exercised against past filings).
    if args.backtest:
        accessions = [a.strip() for a in args.backtest.split(",") if a.strip()]
        logging.info(f"Backtest mode: parsing {len(accessions)} past 8-K(s) "
                     f"via '{args.feed}' feed path.")
        try:
            subs = fetch_submissions(session)
        except Exception as e:
            logging.error(f"Failed to fetch submissions index: {e}")
            sys.exit(1)

        # Build lookup: accession -> (filed_date, primary_doc)
        lookup = {acc: (date, prim) for acc, date, prim in iter_recent_8k(subs)}

        for acc in accessions:
            if acc not in lookup:
                logging.warning(f"Accession {acc} not found in recent 8-K index. Skipping.")
                continue
            filed_date, primary = lookup[acc]
            try:
                if args.feed == "getcurrent":
                    # Exercise the REAL getcurrent path: build the index URL
                    # from the accession (deterministic), feed it through the
                    # actual feed parser + index.json resolver.
                    acc_nodash = acc.replace("-", "")
                    index_url = (f"{ARCHIVE_BASE}/{CIK_INT}/{acc_nodash}/"
                                 f"{acc}-index.htm")
                    # Round-trip through iter_getcurrent_8k by synthesizing a
                    # one-entry feed, so the feed parser itself is tested too.
                    synthetic_feed = (
                        '<feed xmlns="http://www.w3.org/2005/Atom"><entry>'
                        f'<title>8-K - Strategy Inc ({CIK}) (Filer)</title>'
                        f'<link rel="alternate" type="text/html" href="{index_url}"/>'
                        f'<id>urn:tag:sec.gov,2008:accession-number={acc}</id>'
                        f'<updated>{filed_date}T16:30:00-04:00</updated>'
                        '<category scheme="https://www.sec.gov/" '
                        'label="form type" term="8-K"/>'
                        '</entry></feed>'
                    )
                    parsed_entries = list(iter_getcurrent_8k(synthetic_feed))
                    if not parsed_entries:
                        logging.error(f"getcurrent feed parser produced no entry "
                                       f"for {acc} — feed-parsing bug!")
                        continue
                    _acc, _date, _index_url, _updated = parsed_entries[0]
                    logging.info(f"  [getcurrent] feed parser OK: acc={_acc}, "
                                 f"index={_index_url}")
                    primary_url = resolve_primary_from_index(session, _index_url)
                    if primary_url is None:
                        logging.error(f"  [getcurrent] could not resolve primary "
                                       f"doc for {acc}")
                        continue
                    logging.info(f"  [getcurrent] resolved primary: {primary_url}")
                    html, url = fetch_doc_by_url(session, primary_url)
                else:
                    # submissions path
                    html, url = fetch_primary_doc(session, acc, primary)

                update = parse_btc_update(html, acc, filed_date, url)

                # Open in browser if requested.
                should_open = False
                if args.open:
                    if args.open_only_btc:
                        should_open = update is not None
                    else:
                        should_open = True
                if should_open:
                    try:
                        webbrowser.open_new_tab(url)
                        logging.info(f"Opened in browser: {url}")
                    except Exception as e:
                        logging.warning(f"Could not open browser: {e}")

                if update is None:
                    print(f"\n[NOT A BTC UPDATE]  {acc}  filed {filed_date}\n  {url}\n")
                else:
                    print()
                    print(update.pretty())

                # LLM second-opinion in backtest mode too.
                if args.groq_api_key:
                    regex_summary = (
                        f"action={update.action}, btc_delta={update.btc_delta}, "
                        f"holdings={update.aggregate_holdings}"
                        if update is not None else "no parseable table"
                    )
                    llm_result = analyze_with_llm(
                        html, regex_summary, args.groq_api_key,
                        timeout=args.llm_timeout,
                    )
                    if llm_result is not None:
                        print(f"🤖 LLM: sale_detected={llm_result.get('sale_detected')}, "
                              f"conf={llm_result.get('confidence')}, "
                              f"btc_sold={llm_result.get('btc_sold')}")
                        print(f"   reasoning: {llm_result.get('reasoning')}")
                    else:
                        print("🤖 LLM: call failed or returned malformed response")
            except Exception as e:
                logging.exception(f"Error processing {acc}: {e}")
            time.sleep(args.interval)  # be polite to SEC even in backtest
        return

    # Optional priming — mark existing 8-Ks as already-seen on first run.
    if args.prime:
        try:
            subs = fetch_submissions(session)
            # Find the most recent 8-K that's actually a BTC update so we
            # can save its holdings figure as a baseline for the delta check.
            most_recent_btc_holdings = None
            most_recent_btc_acc = None
            for acc, filed_date, primary in iter_recent_8k(subs):
                seen.add(acc)
                # Only try to parse if we haven't already found a baseline.
                if most_recent_btc_holdings is None:
                    try:
                        html, _ = fetch_primary_doc(session, acc, primary)
                        update = parse_btc_update(html, acc, filed_date, "")
                        if update is not None and update.aggregate_holdings is not None:
                            most_recent_btc_holdings = update.aggregate_holdings
                            most_recent_btc_acc = acc
                        time.sleep(args.interval)  # be polite to SEC
                    except Exception as e:
                        logging.warning(f"Could not parse {acc} during prime: {e}")
            state["seen_accessions"] = sorted(seen)
            if most_recent_btc_holdings is not None:
                state["last_holdings"] = most_recent_btc_holdings
                logging.info(
                    f"Primed state with {len(seen)} 8-K accessions and holdings "
                    f"baseline = {most_recent_btc_holdings} BTC "
                    f"(from {most_recent_btc_acc})."
                )
            else:
                logging.warning(
                    f"Primed {len(seen)} accessions but could not find a "
                    f"parseable BTC update in recent filings to set the "
                    f"holdings baseline. Holdings-delta detection will "
                    f"activate after the first BTC update is parsed live."
                )
            save_state(state)
            return
        except Exception as e:
            logging.error(f"Priming failed: {e}")
            sys.exit(1)

    # Graceful shutdown
    stop = {"flag": False}
    def handle_sig(signum, frame):
        stop["flag"] = True
        logging.info("Shutdown signal received; exiting after current poll.")
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    logging.info(f"Polling SEC every {args.interval:.2f}s "
                 f"(~{1/args.interval:.1f} req/s) via '{args.feed}' feed "
                 f"for new MSTR 8-Ks. Already tracking {len(seen)} seen accessions.")

    backoff = args.interval
    health = HealthMonitor(
        telegram_token=args.telegram_token,
        telegram_chat_id=args.telegram_chat_id,
        fail_threshold=args.health_fail_threshold,
        heartbeat_secs=args.heartbeat_secs,
        audible=not args.no_audible,
        state=state,
    )

    # Telegram subscriber discovery: poll getUpdates every N seconds to pick
    # up anyone who has messaged the bot (/start). They opt in; no whitelist.
    last_sub_poll = 0.0
    SUB_POLL_INTERVAL = 30.0  # seconds
    if args.telegram_token:
        try:
            n = telegram_discover_subscribers(args.telegram_token, state)
            save_state(state)
            total = len(state.get("telegram_subscribers", []))
            logging.info(f"Telegram subscriber discovery on: {total} subscriber(s) "
                         f"({n} new). Anyone who sends /start to the bot is added.")
        except Exception as e:
            logging.warning(f"Initial subscriber discovery failed: {e}")
        last_sub_poll = time.monotonic()

    while not stop["flag"]:
        loop_started = time.monotonic()

        # Periodically discover new Telegram subscribers (cheap, throttled).
        if args.telegram_token and (time.monotonic() - last_sub_poll) >= SUB_POLL_INTERVAL:
            try:
                new_subs = telegram_discover_subscribers(args.telegram_token, state)
                if new_subs:
                    save_state(state)
            except Exception as e:
                logging.warning(f"Subscriber discovery failed: {e}")
            last_sub_poll = time.monotonic()

        try:
            # Discover new filings via the configured feed. Both paths
            # produce `new_filings`: a list of dicts with keys
            #   accession, filed_date, fetch  (fetch is a () -> (html, url) callable)
            new_filings = []

            if args.feed == "getcurrent":
                feed_xml = fetch_getcurrent(session)
                backoff = args.interval
                health.record_success()
                for acc, filed_date, index_url, updated in iter_getcurrent_8k(feed_xml):
                    if acc in seen:
                        continue
                    # Bind index_url via default-arg to avoid late-binding bug.
                    def _fetch(idx=index_url):
                        primary_url = resolve_primary_from_index(session, idx)
                        if primary_url is None:
                            raise RuntimeError(f"Could not resolve primary doc from {idx}")
                        return fetch_doc_by_url(session, primary_url)
                    new_filings.append({
                        "accession": acc, "filed_date": filed_date, "fetch": _fetch,
                    })
            else:  # "submissions" (default)
                subs = fetch_submissions(session)
                backoff = args.interval
                health.record_success()
                for acc, filed_date, primary in iter_recent_8k(subs):
                    if acc in seen:
                        continue
                    def _fetch(a=acc, p=primary):
                        return fetch_primary_doc(session, a, p)
                    new_filings.append({
                        "accession": acc, "filed_date": filed_date, "fetch": _fetch,
                    })

            # Process oldest-first so logs read naturally
            for filing in reversed(new_filings):
                acc = filing["accession"]
                filed_date = filing["filed_date"]
                try:
                    html, url = filing["fetch"]()
                    update = parse_btc_update(html, acc, filed_date, url)

                    # FALLBACK: scan the prose for sale-disclosure language.
                    # Returns (tier, context). 'strong' = past-tense specific
                    # sale; 'weak' = forward-looking/discussion (e.g. listing
                    # bitcoin sales as a potential funding source).
                    sale_tier, sale_context = scan_for_sale_language(html)

                    # HOLDINGS-DELTA CHECK: if we have a parsed update and a
                    # previously-seen holdings figure, flag any decrease.
                    holdings_decreased_from = None
                    if update is not None and update.aggregate_holdings is not None:
                        prev = state.get("last_holdings")
                        if prev is not None and update.aggregate_holdings < prev:
                            holdings_decreased_from = prev
                            if update.action == "purchase":
                                logging.warning(
                                    f"Holdings decreased {prev} -> "
                                    f"{update.aggregate_holdings} but action "
                                    f"was 'purchase' - overriding to 'sale'."
                                )
                                update.action = "sale"
                        # Persist current holdings for next comparison.
                        state["last_holdings"] = update.aggregate_holdings

                    # Open in browser if requested.
                    should_open = False
                    if args.open:
                        if args.open_only_btc:
                            should_open = update is not None or sale_tier is not None
                        else:
                            should_open = True
                    if should_open:
                        try:
                            webbrowser.open_new_tab(url)
                            logging.info(f"Opened in browser: {url}")
                        except Exception as e:
                            logging.warning(f"Could not open browser: {e}")

                    # Assemble sale-signal alarms. STRONG hits only.
                    sale_alarms = []
                    if update is not None and update.action == "sale":
                        sale_alarms.append("table parsed as SALE")
                    if sale_tier == "strong":
                        sale_alarms.append(f"strong sale language: '{sale_context}'")
                    if holdings_decreased_from is not None:
                        sale_alarms.append(
                            f"holdings decreased {holdings_decreased_from} -> "
                            f"{update.aggregate_holdings}"
                        )

                    # Weak prose hits — note but don't sound the big alarm.
                    weak_note = None
                    if sale_tier == "weak" and not sale_alarms:
                        weak_note = (f"⚠️ Sale-related language found (may be "
                                     f"forward-looking): '{sale_context}'")

                    banner = None
                    if sale_alarms:
                        banner = "🚨 SALE SIGNAL — " + " | ".join(sale_alarms)
                        logging.warning(banner)
                        if not args.no_audible:
                            audible_alert()
                            audible_alert()

                    if update is None and sale_tier is None:
                        logging.info(f"New 8-K {acc} ({filed_date}) — not a BTC update. {url}")
                    elif update is not None:
                        msg = update.pretty()
                        if banner:
                            msg = banner + "\n" + msg
                        elif weak_note:
                            msg = weak_note + "\n" + msg
                        logging.info("BTC update detected:\n" + msg)
                        if not args.no_audible and not sale_alarms:
                            audible_alert()
                        if args.telegram_token:
                            n = telegram_notify_all(args.telegram_token,
                                                    args.telegram_chat_id, state, msg)
                            logging.info(f"Telegram: notified {n} recipient(s).")
                    elif sale_tier == "strong":
                        msg = (f"🚨 Possible BTC sale disclosed in 8-K "
                               f"{acc} ({filed_date}) — no parseable table.\n"
                               f"Context: {sale_context}\nURL: {url}")
                        logging.warning(msg)
                        if args.telegram_token:
                            telegram_notify_all(args.telegram_token,
                                                args.telegram_chat_id, state, msg)
                    elif sale_tier == "weak":
                        logging.info(
                            f"New 8-K {acc} ({filed_date}) mentions bitcoin "
                            f"sale-related language (likely forward-looking): "
                            f"'{sale_context}' — {url}"
                        )

                    # ----------------------------------------------------------------
                    # LLM second-opinion analysis (Groq, Llama 3.3 70B).
                    # Runs on every new BTC update — but ONLY after primary
                    # alerts have already fired. Never blocks the alert path.
                    # If it disagrees with regex, posts a separate alert.
                    # ----------------------------------------------------------------
                    if args.groq_api_key and (update is not None or sale_tier is not None):
                        # Build a regex summary for the LLM to second-guess.
                        if update is not None:
                            regex_summary = (
                                f"action={update.action}, "
                                f"btc_delta={update.btc_delta}, "
                                f"holdings={update.aggregate_holdings}"
                            )
                            if holdings_decreased_from is not None:
                                regex_summary += f", holdings decreased from {holdings_decreased_from}"
                        else:
                            regex_summary = (
                                f"no parseable table, prose-scan tier={sale_tier}"
                            )

                        try:
                            llm_result = analyze_with_llm(
                                html, regex_summary, args.groq_api_key,
                                timeout=args.llm_timeout,
                            )
                        except Exception as e:
                            logging.warning(f"LLM call raised: {e}")
                            llm_result = None

                        if llm_result is not None:
                            llm_says_sale = bool(llm_result.get("sale_detected"))
                            conf = llm_result.get("confidence", "low")
                            reasoning = llm_result.get("reasoning", "")
                            btc_sold = llm_result.get("btc_sold")

                            # Determine regex verdict for comparison.
                            regex_says_sale = bool(sale_alarms)

                            llm_line = (
                                f"🤖 LLM verdict: "
                                f"{'SALE' if llm_says_sale else 'no sale'} "
                                f"(conf={conf})"
                                + (f", btc_sold={btc_sold}" if btc_sold else "")
                                + f" — {reasoning}"
                            )
                            logging.info(llm_line)

                            # Disagreement = alert. Specifically: LLM thinks
                            # sale, regex doesn't. (Reverse case = regex
                            # already alarmed, no need to re-alert.)
                            if llm_says_sale and not regex_says_sale and conf in ("medium", "high"):
                                discrepancy_msg = (
                                    f"⚠️ LLM DISAGREES with regex — flagging possible sale "
                                    f"that regex missed!\n"
                                    f"  Regex said: {regex_summary}\n"
                                    f"  LLM said:   sale_detected=True, conf={conf}, "
                                    f"btc_sold={btc_sold}\n"
                                    f"  LLM reasoning: {reasoning}\n"
                                    f"  URL: {url}"
                                )
                                logging.warning(discrepancy_msg)
                                if not args.no_audible:
                                    audible_alert()
                                if args.telegram_token:
                                    telegram_notify_all(args.telegram_token,
                                                        args.telegram_chat_id,
                                                        state, discrepancy_msg)
                            # If both agree it's a sale → just confirmation,
                            # no extra alert (primary already fired).
                            # If both agree no sale → quiet.
                            # If regex says sale, LLM says no → log it but
                            # trust the primary signal.
                            elif regex_says_sale and not llm_says_sale:
                                logging.info(
                                    f"Note: regex flagged sale but LLM disagrees "
                                    f"(conf={conf}). Trusting regex; check manually."
                                )

                    seen.add(acc)
                    state["seen_accessions"] = sorted(seen)
                    save_state(state)
                except requests.HTTPError as e:
                    logging.warning(f"HTTP error fetching {acc}: {e}")
                    # Per-filing fetch failure shouldn't tank loop-level health.
                except Exception as e:
                    logging.exception(f"Error processing {acc}: {e}")

        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            if status == 429:
                backoff = min(backoff * 2, 30.0)
                logging.warning(f"429 from SEC — backing off to {backoff:.1f}s.")
                health.record_failure(f"HTTP 429 (rate limited)")
            else:
                logging.warning(f"HTTP error: {e}")
                health.record_failure(f"HTTP {status}")
        except requests.ConnectionError as e:
            logging.warning(f"Connection error (internet down?): {e}")
            health.record_failure(f"connection error: {type(e).__name__}")
        except requests.Timeout as e:
            logging.warning(f"Request timeout: {e}")
            health.record_failure("timeout")
        except requests.RequestException as e:
            logging.warning(f"Network error: {e}")
            health.record_failure(f"network: {type(e).__name__}")
        except Exception as e:
            logging.exception(f"Unexpected error: {e}")
            health.record_failure(f"unexpected: {type(e).__name__}")

        health.heartbeat()

        if args.once:
            break

        # Tight sleep to next tick
        elapsed = time.monotonic() - loop_started
        sleep_for = max(0.0, backoff - elapsed)
        if sleep_for > 0:
            # Sleep in small chunks so signals are responsive
            end = time.monotonic() + sleep_for
            while not stop["flag"] and time.monotonic() < end:
                time.sleep(min(0.1, end - time.monotonic()))

    logging.info("Bye.")


if __name__ == "__main__":
    main()
