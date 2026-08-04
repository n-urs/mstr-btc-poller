#!/usr/bin/env python3
"""
SPCX (Space Exploration Technologies Corp) CAPEX poller.

Watches SEC EDGAR (per-CIK submissions feed) for new SPCX 8-K / 10-Q / 10-K
filings, fetches the primary document PLUS exhibits (earnings numbers usually
live in an EX-99 press release, not the 8-K shell), extracts CAPEX
(capital-expenditure) figures, and sends OWNER-ONLY Telegram alerts.

An LLM second opinion (Groq Llama 3.3 70B) evaluates the capex figures
against Street benchmarks supplied by the owner and also watches for any
notion of a Tesla (TSLA) merger/acquisition. The LLM runs AFTER the primary
regex alert has been sent (never delays it) and reports in a separate
message.

Derived from mstr_btc_poller.py (same repo). Single feed (submissions),
single thread, owner-only alerts: this instance deliberately does NOT poll
Telegram getUpdates, so it can never conflict with the MSTR poller's
subscriber discovery if both run.
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
from pathlib import Path
from typing import Optional

import requests

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

CIK = "0001181412"  # Space Exploration Technologies Corp (SPCX)
CIK_INT = int(CIK)
SUBMISSIONS_URL_TMPL = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVE_BASE = "https://www.sec.gov/Archives/edgar/data"

# Forms that run through the pipeline. Everything else (Form 3/4/D, S-8,
# prospectuses, correspondence) is ignored silently.
WATCH_FORMS = ("8-K", "8-K/A", "10-Q", "10-Q/A", "10-K", "10-K/A")

DEFAULT_UA = "SPCX-CAPEX-Poller (set SEC_UA to your-email@example.com)"
DEFAULT_INTERVAL = 0.20  # ~5 req/s; MSTR poller is stopped so budget is free
REQUEST_TIMEOUT = 10

STATE_FILE = Path.home() / ".spcx_capex_poller_state.json"
CONFIG_FILENAME = "config.env"

# Max exhibit documents fetched per filing (primary doc + this many extras).
MAX_EXHIBITS = 4
# LLM input cap. 10-Qs are long and capex tables sit deep in the cash-flow
# statement, so this is much larger than the MSTR poller's cap. Llama 3.3 has
# a 128k-token context; 60k chars ≈ 15-20k tokens.
MAX_LLM_CHARS = 60000


def load_config_env() -> dict:
    for base in (Path(__file__).resolve().parent, Path.cwd()):
        p = base / CONFIG_FILENAME
        if p.exists():
            cfg = {}
            try:
                for line in p.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, _, v = line.partition("=")
                    v = v.strip().strip('"').strip("'")
                    if v:
                        cfg[k.strip()] = v
            except Exception as e:
                logging.warning(f"Could not read {p}: {e}")
            return cfg
    return {}


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            logging.warning("Could not parse state file; starting fresh.")
    return {"seen_accessions": []}


def save_state(state: dict) -> None:
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        logging.warning(f"Could not save state: {e}")


# ----------------------------------------------------------------------------
# HTTP helpers
# ----------------------------------------------------------------------------


def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
    })
    return s


def fetch_submissions(session: requests.Session, cik: str) -> dict:
    r = session.get(SUBMISSIONS_URL_TMPL.format(cik=cik),
                    headers={"Host": "data.sec.gov"}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def iter_recent_forms(submissions: dict, forms=WATCH_FORMS):
    """Yield (accession, filed_date, form, primary_doc) for watched forms,
    newest first."""
    recent = submissions.get("filings", {}).get("recent", {})
    for form, date, acc, prim in zip(recent.get("form", []),
                                     recent.get("filingDate", []),
                                     recent.get("accessionNumber", []),
                                     recent.get("primaryDocument", [])):
        if form in forms:
            yield acc, date, form, prim


def fetch_filing_docs(session: requests.Session, cik_int: int, accession: str,
                      primary_doc: str):
    """Fetch the primary document AND up to MAX_EXHIBITS additional .htm
    documents (EX-99 press releases etc.) for a filing.

    Returns (combined_clean_text, primary_url, doc_names). Earnings numbers
    routinely live in an exhibit while the primary 8-K is a shell — scanning
    only the primary would miss the actual capex figures.
    """
    acc_nodash = accession.replace("-", "")
    base = f"{ARCHIVE_BASE}/{cik_int}/{acc_nodash}"
    primary_url = f"{base}/{primary_doc}"

    docs = []  # (name, text)
    r = session.get(primary_url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    docs.append((primary_doc, clean(r.text)))

    # index.json lists every file in the filing folder.
    try:
        idx = session.get(f"{base}/index.json", timeout=REQUEST_TIMEOUT).json()
        items = idx.get("directory", {}).get("item", [])
        extras = []
        for it in items:
            name = it.get("name", "")
            low = name.lower()
            if not low.endswith((".htm", ".html")):
                continue
            if name == primary_doc or "index" in low:
                continue
            # Skip XBRL viewer fragments (R1.htm, R10.htm ...) — rendering
            # noise that wastes exhibit slots and injects tag names like
            # "SpaceXMember" into the text.
            if re.fullmatch(r"r\d+\.htm[l]?", low):
                continue
            # Prefer exhibit-looking documents first (ex99/ex-99/press).
            prio = 0 if re.search(r"ex[-_]?99|press|release|earnings|letter",
                                  low) else 1
            extras.append((prio, name))
        extras.sort()
        for _, name in extras[:MAX_EXHIBITS]:
            try:
                rr = session.get(f"{base}/{name}", timeout=REQUEST_TIMEOUT)
                rr.raise_for_status()
                docs.append((name, clean(rr.text)))
                time.sleep(0.1)  # be polite
            except Exception as e:
                logging.warning(f"Could not fetch exhibit {name}: {e}")
    except Exception as e:
        logging.warning(f"Could not list filing index for {accession}: {e}")

    combined = "\n\n".join(f"[DOCUMENT: {n}]\n{t}" for n, t in docs)
    return combined, primary_url, [n for n, _ in docs]


# ----------------------------------------------------------------------------
# Text cleaning (same approach as the MSTR poller)
# ----------------------------------------------------------------------------

TAG_RE = re.compile(r"<[^>]+>")
ENTITY_MAP = {"&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">",
              "&#8217;": "'", "&#8220;": '"', "&#8221;": '"', "&#160;": " ",
              "&#8211;": "–", "&#8212;": "—", "&rsquo;": "'", "&ldquo;": '"',
              "&rdquo;": '"', "&ndash;": "–", "&mdash;": "—", "&#36;": "$"}


def clean(html: str) -> str:
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html,
                  flags=re.DOTALL | re.IGNORECASE)
    text = TAG_RE.sub(" ", text)
    for ent, ch in ENTITY_MAP.items():
        text = text.replace(ent, ch)
    text = re.sub(r"&#\d+;", " ", text)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ----------------------------------------------------------------------------
# CAPEX extraction (regex layer)
# ----------------------------------------------------------------------------

CAPEX_TERM_RE = re.compile(
    r"capital\s+expenditures?"
    r"|\bcapex\b"
    r"|purchases?\s+of\s+property\s+and\s+equipment"
    r"|investments?\s+in\s+property,?\s+plant\s+and\s+equipment"
    r"|additions?\s+to\s+property,?\s+plant\s+and\s+equipment",
    re.IGNORECASE,
)
MONEY_RE = re.compile(
    r"\$\s?[\d][\d,.]*\s*(?:billion|million|bn|mm|b\b|m\b)?", re.IGNORECASE)


def scan_for_capex(text: str):
    """Return a list of (snippet, dollar_amounts) windows around CAPEX terms.

    Only windows that contain at least one dollar figure are kept — a bare
    boilerplate mention of 'capital expenditures' with no number is noise.
    Overlapping windows are merged. Capped at 8 snippets.
    """
    spans = []
    for m in CAPEX_TERM_RE.finditer(text):
        start = max(0, m.start() - 300)
        end = min(len(text), m.end() + 300)
        if spans and start <= spans[-1][1]:
            spans[-1] = (spans[-1][0], end)  # merge overlap
        else:
            spans.append((start, end))

    hits = []
    for start, end in spans:
        ctx = text[start:end]
        # Word-boundary trims so alerts read cleanly.
        if start > 0:
            sp = ctx.find(" ")
            if 0 <= sp < 25:
                ctx = ctx[sp + 1:]
        if end < len(text):
            sp = ctx.rfind(" ")
            if sp > len(ctx) - 25:
                ctx = ctx[:sp]
        monies = MONEY_RE.findall(ctx)
        if monies:
            hits.append((ctx.strip(), [m.strip() for m in monies]))
        if len(hits) >= 8:
            break
    return hits


def _amount_to_billions(amount: str):
    """'$14.1 billion' -> 14.1, '$3.3B' -> 3.3, '$535 million' -> 0.535.

    Unitless numbers: SEC financial tables are stated "in millions", so a bare
    '$18,369' in a capex context means $18.369B. Treat unitless values >= 100
    as millions; smaller unitless values are ambiguous -> None."""
    m = re.match(r"\$\s?([\d][\d,.]*)\s*(billion|bn|b|million|mm|m)?\b",
                 amount.strip(), re.IGNORECASE)
    if not m:
        return None
    try:
        val = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    unit = (m.group(2) or "").lower()
    if unit in ("billion", "bn", "b"):
        return val
    if unit in ("million", "mm", "m"):
        return val / 1000.0
    return val / 1000.0 if val >= 100 else None


QTR_RE = re.compile(r"\bQ[1-4]\b|(?:first|second|third|fourth)\s+quarter"
                    r"|three\s+months", re.IGNORECASE)
H2_RE = re.compile(r"second\s+half|\bH2\b|2H\s*20\d\d", re.IGNORECASE)
FY_RE = re.compile(r"full[-\s]?year|fiscal\s+(?:year\s+)?20\d\d|\bFY\s?20\d\d"
                   r"|annual", re.IGNORECASE)
Y2027_RE = re.compile(r"\b2027\b")


def classify_amount(amount: str, context: str, amount_pos: int = 0):
    """Heuristic verdict per figure vs the owner's Street bars.
    Q: LOW<13.2<=NORMAL<16<=HIGH | H2: NORMAL<30<=HIGH
    FY2026: LOW<46<=NORMAL<55<=HIGH | 2027: LOW<87<=NORMAL<100<=HIGH
    Period is guessed from surrounding words; the LLM message remains the
    authority (front-loading and mixed-flash guards live there)."""
    val = _amount_to_billions(amount)
    if val is None:
        return "❔ unclassified"
    # A figure's period almost always lives in ITS OWN sentence — dense
    # earnings prose mentions several periods per paragraph, so first
    # restrict to the sentence containing the amount; fall back to the wider
    # window (nearest token wins) only if that sentence names no period.
    s_start = context.rfind(". ", 0, amount_pos)
    s_start = 0 if s_start < 0 else s_start + 2
    s_end = context.find(". ", amount_pos + len(amount))
    s_end = len(context) if s_end < 0 else s_end + 1

    def _candidates(lo, hi):
        found = []
        for period, rex, low, high in (("2027", Y2027_RE, 87.0, 100.0),
                                       ("H2", H2_RE, None, 30.0),
                                       ("FY 2026", FY_RE, 46.0, 55.0),
                                       ("quarter", QTR_RE, 13.2, 16.0)):
            for mm in rex.finditer(context, lo, hi):
                center = (mm.start() + mm.end()) // 2
                dist = min(abs(center - amount_pos),
                           abs(center - (amount_pos + len(amount))))
                found.append((dist, period, low, high))
        return found

    candidates = _candidates(s_start, s_end) or _candidates(0, len(context))
    if not candidates:
        return "❔ period unclear"
    _, period, low, high = min(candidates)
    if val >= high:
        return f"🔴 HIGH {period} — SHORT SPCX"
    if low is not None and val < low:
        return f"🟢 LOW ({period})"
    return f"⚪ NORMAL ({period})"


def extract_labeled_amounts(text: str):
    """Return [(amount, micro_quote)] for dollar figures that sit DIRECTLY
    next to capex language (capex term within ~90 chars before or ~40 after
    the amount). The micro-quote is the verbatim sentence fragment around the
    number, so each figure is self-explanatory. Excludes revenue/cash figures
    that merely share a paragraph with a capex sentence."""
    out, seen_keys = [], set()
    for m in MONEY_RE.finditer(text):
        # Capex term must come BEFORE the amount ("Capital expenditures were
        # $14.1 billion", "Capex Space $1,174 ..."). Flattened tables put the
        # PREVIOUS row's numbers right before a row label, so accepting
        # amounts that precede the term grabs the wrong row (observed live:
        # Adjusted-EBITDA cells attributed to capex).
        before = text[max(0, m.start() - 120):m.start()]
        if not CAPEX_TERM_RE.search(before):
            continue
        q_start = max(0, m.start() - 70)
        q_end = min(len(text), m.end() + 45)
        quote = text[q_start:q_end]
        if q_start > 0:
            sp = quote.find(" ")
            if 0 <= sp < 25:
                quote = quote[sp + 1:]
        if q_end < len(text):
            sp = quote.rfind(" ")
            if sp > len(quote) - 25:
                quote = quote[:sp]
        amount = m.group(0).strip()
        key = (amount.replace(" ", "").upper(), quote[:30])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        # Wider context for period detection than the display quote.
        ctx_start = max(0, m.start() - 160)
        cls_ctx = text[ctx_start:m.end() + 160]
        out.append((amount, quote.strip(),
                    classify_amount(amount, cls_ctx, m.start() - ctx_start)))
        if len(out) >= 24:
            break
    # Largest figures first — in flattened segment tables the totals ($18.4B)
    # matter far more than per-segment cells, and the display cap is 8.
    out.sort(key=lambda t: -( _amount_to_billions(t[0]) or 0.0))
    return out[:8]


def format_capex_alert(accession, filed_date, form, hits, url, labeled=None) -> str:
    labeled = labeled or []
    n = len(labeled)
    head = (f"{n} capex figure(s) found" if n else
            ("capex language found, no adjacent $ figure" if hits
             else "no capex language found"))
    lines = [f"🛰 SPCX {form} FILED — {head}",
             f"{accession}  |  filed {filed_date}",
             ""]
    for amount, quote, cls in labeled:
        val = _amount_to_billions(amount)
        approx = (f" (≈${val:.1f}B)" if val is not None
                  and not re.search(r"billion|bn|\bb\b", amount, re.I) else "")
        lines.append(f"• {amount}{approx} — {cls}")
        lines.append(f"   “…{quote}…”")
    if not labeled and hits:
        for snip, _ in hits[:3]:
            lines.append(f"• “…{snip[:350]}…”")
    if not labeled and not hits:
        lines.append("See LLM verdict message for the deep read.")
    lines.append("")
    lines.append(url)
    return "\n".join(lines)


# ----------------------------------------------------------------------------
# Telegram (OWNER-ONLY — no getUpdates, no subscribers, no broadcast)
# ----------------------------------------------------------------------------


def telegram_send(token: str, chat_id: str, text: str) -> None:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data={"chat_id": chat_id, "text": text[:4096],
              "disable_web_page_preview": True},
        timeout=15,
    )
    r.raise_for_status()


def notify_owner(token: Optional[str], chat_id: Optional[str], text: str) -> None:
    if not token or not chat_id:
        return
    try:
        telegram_send(token, chat_id, text)
    except Exception as e:
        logging.warning(f"Telegram send failed: {e}")


# ----------------------------------------------------------------------------
# LLM analysis (Groq, Llama 3.3 70B) — owner's decision rule encoded verbatim
# ----------------------------------------------------------------------------

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

LLM_SYSTEM_PROMPT = """You are a precise financial-disclosure analyst reading
SEC filings of Space Exploration Technologies Corp (SPCX, "SpaceX").

You have THREE tasks:

TASK 1 — CAPEX FIGURES: extract every capital-expenditure (capex) figure the
filing states — actuals and guidance/forecast — with its period (e.g. Q2 2026,
Q3, H2 2026, FY 2026, FY 2027). Capex may be phrased as "capital
expenditures", "capex", "purchases of property and equipment", or similar.
ONLY capital-expenditure figures: do NOT include operating cash flow, cash
balances, revenue, or profit as capex figures.

UNITS — CRITICAL: SEC financial tables state amounts "in millions". A bare
table value like "$18,369" or "18,369" means $18,369 million = $18.369
BILLION. ALWAYS normalize every figure to billions and report it in billions
(e.g. "$18.4B"), then compare the NORMALIZED value against the bars: $18,369
million = $18.4B, which EXCEEDS the $16B quarterly bar. A quarterly capex at
or above $16B triggers the verdict wherever it appears, including segment
tables. Never conclude "no signal" without first normalizing table values to
billions.

TASK 2 — CAPEX VERDICT versus Street benchmarks:
Street capex: Q2 consensus about $13.2B; full-year 2026 benchmark roughly
$46B to $49B; 2027 path about $87B.

Answer "CAPEX IS VERY HIGH, SHORT" only if the SpaceX results, guidance, or a
wire reporting them state a capex FIGURE materially above these benchmarks:
any quarter, reported or guided, at $16B or more; SECOND-HALF 2026 capex
guided at $30B or more; full-year 2026 capex or guidance of $55B or more;
2027 capex guidance of $100B or more; or new borrowing or debt facilities of
$25B or more announced to fund investment. For RANGE guidance use the
midpoint: $50B to $60B counts as $55B, yes. Compare yourself: Q2 reported
$14.8B is below the $16B bar, no; FY $48B is the benchmark, no; FY $56B
exceeds $55B, yes; H2 $26B is near the implied path, no; H2 $32B exceeds
$30B, yes. The borrowing bar is INDEPENDENT of capex figures: a new credit
facility, notes offering, or debt raise of $25B or more announced to fund
investment triggers the verdict BY ITSELF, even if every stated capex figure
is below its bar — a new $30B credit facility to fund infrastructure exceeds
$25B, yes.
FRONT-LOADING GUARD: if the same message shows a high quarter but full-year
guidance reaffirmed in line at roughly $46B to $49B, answer no — timing, not
acceleration.
MIXED-FLASH GUARD: if the SAME message also reports revenue or profit beating
its stated estimate by more than about ten percent, answer "MIXED NUMBERS" —
a capex surprise inside a blowout quarter is a mixed signal, skip it.
If neither trigger fires, the verdict is "NO SIGNAL".
The dollar values in the rule above ($13.2B, $16B, $14.8B, $48B, $87B etc.)
are BENCHMARKS AND ILLUSTRATIONS ONLY — never report them as filing figures.
capex_figures must contain ONLY numbers that appear in the provided filing
text.

TASK 3 — TESLA: does the filing contain ANY notion of a merger, acquisition,
combination, or strategic transaction involving Tesla (TSLA)? Any mention of
Tesla in a deal context counts, however tentative.

Respond with ONLY a JSON object, no other text:
  - capex_figures: array of {"period": str, "type": "actual"|"guidance",
    "amount": str}   ([] if none)
  - verdict: "CAPEX IS VERY HIGH, SHORT" | "MIXED NUMBERS" | "NO SIGNAL"
  - tsla_merger_detected: boolean
  - tsla_context: str or null (short quote if detected)
  - reasoning: str (2 sentences max: which bar was compared and why)
"""


def build_llm_input(text: str) -> str:
    """Fit the filing into MAX_LLM_CHARS without ever losing capex content.

    Blind head-truncation cut the segment capex tables out of the 10-Q (they
    sat ~170k chars in) and the LLM concluded 'no signal' on an $18.4B
    quarter. Instead: document head + a wide window around EVERY capex term,
    so the capex tables always reach the model wherever they live."""
    if len(text) <= MAX_LLM_CHARS:
        return text
    head = text[:20000]
    spans = []
    for m in CAPEX_TERM_RE.finditer(text):
        start, end = max(0, m.start() - 1500), min(len(text), m.end() + 2500)
        if spans and start <= spans[-1][1]:
            spans[-1] = (spans[-1][0], end)
        else:
            spans.append((start, end))
    windows = []
    budget = MAX_LLM_CHARS - len(head) - 200
    for start, end in spans:
        chunk = text[start:end]
        if budget - len(chunk) < 0:
            break
        windows.append(chunk)
        budget -= len(chunk)
    return head + "\n[...document truncated; capex-relevant sections follow...]\n" \
        + "\n[...]\n".join(windows)


def analyze_with_llm(text: str, regex_summary: str, api_key: str,
                     timeout: float = 30.0) -> Optional[dict]:
    if not api_key:
        return None
    text = build_llm_input(text)
    user_msg = (f"Regex analysis already found: {regex_summary}\n\n"
                f"Now independently analyze the following filing text:\n\n{text}")
    try:
        r = requests.post(
            GROQ_API_URL,
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json={"model": GROQ_MODEL,
                  "messages": [
                      {"role": "system", "content": LLM_SYSTEM_PROMPT},
                      {"role": "user", "content": user_msg}],
                  "temperature": 0.0,
                  "max_tokens": 700,
                  "response_format": {"type": "json_object"}},
            timeout=timeout,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            return None
        return parsed
    except Exception as e:
        logging.warning(f"LLM call failed: {e}")
        return None


def _llm_figure_emoji(period: str, amount: str) -> str:
    """Map an LLM-reported figure onto the same HIGH/LOW/NORMAL bars as the
    regex layer, using the LLM's own period label."""
    val = _amount_to_billions(str(amount))
    if val is None:
        return "❔"
    p = str(period).upper()
    if "2027" in p:
        low, high = 87.0, 100.0
    elif "H2" in p or "HALF" in p:
        low, high = None, 30.0
    elif "FY" in p or "FULL" in p or "YEAR" in p:
        low, high = 46.0, 55.0
    elif "Q" in p or "QUARTER" in p:
        low, high = 13.2, 16.0
    else:
        return "❔"
    if val >= high:
        return "🔴"
    if low is not None and val < low:
        return "🟢"
    return "⚪"


def format_llm_message(llm: dict) -> str:
    verdict = llm.get("verdict", "NO SIGNAL")
    v_emoji = {"CAPEX IS VERY HIGH, SHORT": "🔴",
               "MIXED NUMBERS": "🟡"}.get(verdict, "⚪")
    lines = [f"💰 LLM CAPEX VERDICT: {v_emoji} {verdict}"]
    figures = llm.get("capex_figures") or []
    if figures:
        lines.append("Figures:")
        for f in figures:
            period, typ, amount = (f.get("period", "?"), f.get("type", "?"),
                                   f.get("amount", "?"))
            lines.append(f"• {_llm_figure_emoji(period, amount)} "
                         f"{period} {typ}: {amount}")
    else:
        lines.append("Figures: none found")
    if llm.get("tsla_merger_detected"):
        lines.append(f"🟢 TSLA MERGER LANGUAGE: {llm.get('tsla_context')}")
    else:
        lines.append("TSLA merger language: none")
    lines.append(f"Reasoning: {llm.get('reasoning', '')}")
    return "\n".join(lines)


# ----------------------------------------------------------------------------
# Audible + health (owner-only alerts)
# ----------------------------------------------------------------------------


def audible_alert() -> None:
    try:
        if sys.platform == "win32":
            import winsound
            for _ in range(3):
                winsound.Beep(880, 200)
                time.sleep(0.1)
        else:
            sys.stdout.write("\a")
            sys.stdout.flush()
    except Exception:
        pass


class HealthMonitor:
    def __init__(self, telegram_token=None, telegram_chat_id=None,
                 fail_threshold=5, heartbeat_secs=300, audible=True):
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self.fail_threshold = fail_threshold
        self.heartbeat_secs = heartbeat_secs
        self.audible = audible
        self.consecutive_failures = 0
        self.alerted_degraded = False
        self.last_success_at = time.monotonic()
        self.last_heartbeat_at = time.monotonic()
        self.last_failure_reason = ""

    def _send(self, msg: str) -> None:
        logging.warning(msg)
        notify_owner(self.telegram_token, self.telegram_chat_id, msg)
        if self.audible:
            audible_alert()

    def record_success(self) -> None:
        self.last_success_at = time.monotonic()
        if self.alerted_degraded:
            self._send("✅ SPCX poller RECOVERED — successfully polling SEC again.")
            self.alerted_degraded = False
        self.consecutive_failures = 0

    def record_failure(self, reason: str) -> None:
        self.consecutive_failures += 1
        self.last_failure_reason = reason
        if (self.consecutive_failures >= self.fail_threshold
                and not self.alerted_degraded):
            secs = int(time.monotonic() - self.last_success_at)
            self._send(f"⚠️ SPCX poller DEGRADED — {self.consecutive_failures} "
                       f"consecutive failures, no successful poll in {secs}s. "
                       f"Last error: {reason}")
            self.alerted_degraded = True

    def heartbeat(self) -> None:
        now = time.monotonic()
        if now - self.last_heartbeat_at >= self.heartbeat_secs:
            secs = int(now - self.last_success_at)
            status = "OK" if self.consecutive_failures == 0 else (
                f"DEGRADED ({self.consecutive_failures} fails, "
                f"last err: {self.last_failure_reason})")
            logging.info(f"♥ heartbeat — status: {status}, "
                         f"{secs}s since last successful poll.")
            self.last_heartbeat_at = now


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------


def main():
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    cfg = load_config_env()

    def cfg_get(key, default=None):
        return os.environ.get(key) or cfg.get(key) or default

    ap = argparse.ArgumentParser(
        description="Poll SEC EDGAR for new SPCX 8-K/10-Q/10-K filings and "
                    "extract CAPEX figures. Owner-only Telegram alerts.")
    ap.add_argument("--user-agent", default=cfg_get("SEC_UA", DEFAULT_UA))
    ap.add_argument("--interval", type=float,
                    default=float(cfg_get("INTERVAL", DEFAULT_INTERVAL)))
    ap.add_argument("--prime", action="store_true",
                    help="Mark all existing watched filings as seen; run once "
                         "before going live.")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--backtest", default=None,
                    help="Comma-separated accession numbers to run through the "
                         "pipeline (no state change).")
    ap.add_argument("--backtest-cik", default=None,
                    help="Use a different CIK for --backtest lookups (e.g. "
                         "1318605 for TSLA) to test extraction on real reports.")
    ap.add_argument("--backtest-latest", default=None,
                    help="Backtest the newest filing(s) of the given form(s), "
                         "e.g. '10-Q' or '10-Q,8-K'. Combines with --backtest-cik.")
    ap.add_argument("--test-telegram", action="store_true")
    ap.add_argument("--telegram-token", default=cfg_get("TELEGRAM_BOT_TOKEN"))
    ap.add_argument("--telegram-chat-id", default=cfg_get("TELEGRAM_CHAT_ID"))
    ap.add_argument("--groq-api-key", default=cfg_get("GROQ_API_KEY"))
    ap.add_argument("--llm-timeout", type=float,
                    default=float(cfg_get("LLM_TIMEOUT", 30)))
    ap.add_argument("--no-audible", action="store_true",
                    default=cfg_get("NO_AUDIBLE", "").lower() in ("1", "true", "yes"))
    ap.add_argument("--health-fail-threshold", type=int,
                    default=int(cfg_get("HEALTH_FAIL_THRESHOLD", 5)))
    ap.add_argument("--heartbeat-secs", type=int,
                    default=int(cfg_get("HEARTBEAT_SECS", 300)))
    ap.add_argument("--log-file", default=cfg_get("LOG_FILE"))
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            *([logging.FileHandler(args.log_file, encoding="utf-8")]
              if args.log_file else []),
        ],
    )

    session = make_session(args.user_agent)
    state = load_state()
    seen = set(state.get("seen_accessions", []))

    def process_filing(acc, filed_date, form, primary, cik_int, notify=True):
        """Fetch (primary + exhibits), scan capex, alert owner, then LLM."""
        text, url, doc_names = fetch_filing_docs(session, cik_int, acc, primary)
        logging.info(f"Fetched {len(doc_names)} doc(s) for {acc}: {doc_names}")

        hits = scan_for_capex(text)
        labeled = extract_labeled_amounts(text)
        msg = format_capex_alert(acc, filed_date, form, hits, url, labeled)
        logging.info("SPCX filing processed:\n" + msg)
        if not args.no_audible:
            audible_alert()
        if notify:
            notify_owner(args.telegram_token, args.telegram_chat_id, msg)

        # LLM second opinion — separate message, never delays the alert above.
        if args.groq_api_key:
            regex_summary = (f"form={form}, capex windows found={len(hits)}, "
                             f"amounts={[m for _, ms in hits for m in ms][:10]}")
            llm = analyze_with_llm(text, regex_summary, args.groq_api_key,
                                   timeout=args.llm_timeout)
            if llm is not None:
                llm_msg = format_llm_message(llm)
                logging.info(llm_msg)
                if notify:
                    notify_owner(args.telegram_token, args.telegram_chat_id,
                                 llm_msg)
                # Escalation beeps for actionable verdicts.
                if (llm.get("verdict") == "CAPEX IS VERY HIGH, SHORT"
                        or llm.get("tsla_merger_detected")):
                    if not args.no_audible:
                        audible_alert()
                        audible_alert()
            else:
                fail = "🤖 LLM call failed — no second opinion for this filing."
                logging.warning(fail)
                if notify:
                    notify_owner(args.telegram_token, args.telegram_chat_id, fail)
        return hits

    # ---- test-telegram (owner-only by design) ----
    if args.test_telegram:
        if not (args.telegram_token and args.telegram_chat_id):
            logging.error("Need TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.")
            sys.exit(1)
        telegram_send(args.telegram_token, args.telegram_chat_id,
                      "🧪 TEST — SPCX capex poller: owner-only delivery works.")
        logging.info("✅ Test alert sent to owner chat only.")
        return

    # ---- backtest ----
    if args.backtest or args.backtest_latest:
        cik = (args.backtest_cik or CIK).lstrip("0")
        cik_padded = cik.zfill(10)
        subs = fetch_submissions(session, cik_padded)
        rows = list(iter_recent_forms(subs))
        targets = []
        if args.backtest:
            wanted = {a.strip() for a in args.backtest.split(",") if a.strip()}
            targets = [r for r in rows if r[0] in wanted]
        if args.backtest_latest:
            for form in [f.strip() for f in args.backtest_latest.split(",")]:
                for r in rows:
                    if r[2] == form or r[2] == form.upper():
                        targets.append(r)
                        break
        if not targets:
            logging.error("No matching filings found for backtest.")
            sys.exit(1)
        logging.info(f"Backtest: {len(targets)} filing(s) from CIK {cik}.")
        for acc, date, form, prim in targets:
            print(f"\n===== BACKTEST {form} {acc} ({date}) =====")
            try:
                process_filing(acc, date, form, prim, int(cik), notify=False)
            except Exception as e:
                logging.exception(f"Backtest error on {acc}: {e}")
            time.sleep(args.interval)
        return

    # ---- prime ----
    if args.prime:
        subs = fetch_submissions(session, CIK)
        n = 0
        for acc, date, form, prim in iter_recent_forms(subs):
            seen.add(acc)
            n += 1
        state["seen_accessions"] = sorted(seen)
        save_state(state)
        logging.info(f"Primed: {n} existing watched filing(s) marked seen "
                     f"({len(seen)} total in state).")
        return

    # ---- live loop ----
    stop = {"flag": False}

    def handle_sig(signum, frame):
        stop["flag"] = True
        logging.info("Shutdown signal received.")
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    health = HealthMonitor(
        telegram_token=args.telegram_token,
        telegram_chat_id=args.telegram_chat_id,
        fail_threshold=args.health_fail_threshold,
        heartbeat_secs=args.heartbeat_secs,
        audible=not args.no_audible,
    )

    logging.info(f"Polling SEC submissions for SPCX (CIK {CIK_INT}) every "
                 f"{args.interval:.2f}s (~{1/args.interval:.1f} req/s), forms "
                 f"{'/'.join(WATCH_FORMS)}. {len(seen)} seen. Owner-only alerts.")

    backoff = args.interval
    while not stop["flag"]:
        tick = time.monotonic()
        try:
            subs = fetch_submissions(session, CIK)
            backoff = args.interval
            health.record_success()
            new = [(acc, d, f, p) for acc, d, f, p in iter_recent_forms(subs)
                   if acc not in seen]
            for acc, date, form, prim in reversed(new):
                try:
                    process_filing(acc, date, form, prim, CIK_INT)
                    seen.add(acc)
                    state["seen_accessions"] = sorted(seen)
                    save_state(state)
                except Exception as e:
                    logging.exception(f"Error processing {acc}: {e}")
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            if status == 429:
                backoff = min(backoff * 2, 30.0)
                logging.warning(f"429 from SEC — backing off to {backoff:.1f}s.")
                health.record_failure("HTTP 429")
            else:
                logging.warning(f"HTTP error: {e}")
                health.record_failure(f"HTTP {status}")
        except requests.Timeout:
            logging.warning("Request timeout.")
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
        elapsed = time.monotonic() - tick
        sleep_for = max(0.0, backoff - elapsed)
        end = time.monotonic() + sleep_for
        while not stop["flag"] and time.monotonic() < end:
            time.sleep(min(0.1, end - time.monotonic()))

    logging.info("Bye.")


if __name__ == "__main__":
    main()
