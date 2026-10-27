#!/usr/bin/env python3
"""
SK hynix DART/KIND filing watcher — ADS/ADR issuance question.

Polls DART's per-company RSS (no API key needed) for new SK hynix
disclosures, fetches the full filing text from DART's viewer, scans it for
ADS/ADR/overseas-listing/new-share language, and asks an LLM ONE question:
does this filing say anything about an ADS (American Depositary Share)
issuance / ADR program change — and if so, what exactly?

Every new filing produces an OWNER-ONLY Telegram alert either way (present
or none). DART mirrors KRX/KIND exchange disclosures for listed companies,
so the single RSS covers both systems.

Derived from spcx_capex_poller.py. Separate state file, no getUpdates, no
subscribers, no broadcast — safe to run beside the MSTR poller (different
host: dart.fss.or.kr, so no SEC rate budget is consumed).
"""

from __future__ import annotations

import argparse
import html as _html
import json
import logging
import os
import re
import signal
import sys
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

import requests

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

DART_CORP_CODE = "00164779"  # SK hynix Inc. (유가 000660) — validated at startup
RSS_URL = "https://dart.fss.or.kr/api/companyRSS.xml?crpCd={corp}"
VIEWER_MAIN = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp}"
VIEWER_PART = "https://dart.fss.or.kr/report/viewer.do"

DEFAULT_INTERVAL = 5.0  # RSS poll period (s). Lower it only around 08:00 KST.
REQUEST_TIMEOUT = 15
STATE_FILE = Path.home() / ".skhynix_dart_poller_state.json"
CONFIG_FILENAME = "config.env"
MAX_LLM_CHARS = 60000
MAX_PARTS = 12  # viewer sections fetched per filing


def load_config_env() -> dict:
    for base in (Path(__file__).resolve().parent, Path.cwd()):
        p = base / CONFIG_FILENAME
        if p.exists():
            cfg = {}
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                v = v.strip().strip('"').strip("'")
                if v:
                    cfg[k.strip()] = v
            return cfg
    return {}


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            logging.warning("Could not parse state file; starting fresh.")
    return {"seen_rcp": []}


def save_state(state: dict) -> None:
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False),
                              encoding="utf-8")
    except Exception as e:
        logging.warning(f"Could not save state: {e}")


# ----------------------------------------------------------------------------
# DART access (key-free)
# ----------------------------------------------------------------------------


def make_session(ua: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": ua, "Accept-Language": "ko,en;q=0.8"})
    return s


def fetch_rss(session, corp: str):
    """Yield (rcp_no, title, pub_utc_iso) newest first."""
    r = session.get(RSS_URL.format(corp=corp), timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    r.encoding = "utf-8"
    out = []
    for item in re.findall(r"<item>(.*?)</item>", r.text, re.S):
        t = re.search(r"<title>(.*?)</title>", item, re.S)
        l = re.search(r"<link>(.*?)</link>", item, re.S)
        d = re.search(r"<pubDate>(.*?)</pubDate>", item, re.S)
        rcp = re.search(r"rcpNo=(\d+)", l.group(1)) if l else None
        if not rcp:
            continue
        title = _html.unescape(t.group(1).strip()) if t else "?"
        title = re.sub(r"^\(유가\)SK하이닉스\s*-\s*", "", title)
        pub = ""
        if d:
            try:
                pub = parsedate_to_datetime(d.group(1).strip()).strftime("%Y-%m-%d %H:%M UTC")
            except Exception:
                pub = d.group(1).strip()
        out.append((rcp.group(1), title, pub))
    return out


def _decode(resp) -> str:
    """DART-native pages are UTF-8; KIND-origin viewer pages are CP949."""
    raw = resp.content
    try:
        txt = raw.decode("utf-8")
        if txt.count("�") < 5:
            return txt
    except UnicodeDecodeError:
        pass
    for enc in ("cp949", "euc-kr"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _strip(html_text: str) -> str:
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html_text, flags=re.S | re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = _html.unescape(t).replace("\xa0", " ")
    return re.sub(r"\s+", " ", t).strip()


def fetch_filing_text(session, rcp: str):
    """Full text of a DART filing via the viewer: main.do exposes the table of
    contents as JS vars (dcmNo/eleId/offset/length per section); viewer.do
    returns each section's HTML."""
    main = session.get(VIEWER_MAIN.format(rcp=rcp), timeout=REQUEST_TIMEOUT)
    main.raise_for_status()
    main_text = _decode(main)
    # Group node vars into sections.
    nodes, cur = [], {}
    for key, val in re.findall(r"node\d+\['(\w+)'\]\s*=\s*\"([^\"]*)\"", main_text):
        if key == "text" and cur:
            nodes.append(cur)
            cur = {}
        cur[key] = val
    if cur:
        nodes.append(cur)
    # KIND/KRX-origin filings (rcpNo ...8xxxxx) expose no section table — the
    # only handle is the dcmNo on the PDF-download button; the whole document
    # is served with eleId=0 & dtd=HTML (CP949-encoded).
    if not nodes:
        pdf = re.search(r"openPdfDownload\('\d+',\s*'(\d+)'\)", main_text)
        if pdf:
            nodes = [{"text": "", "dcmNo": pdf.group(1), "eleId": "0",
                      "offset": "0", "length": "0", "dtd": "HTML"}]
    parts, seen = [], set()
    for n in nodes:
        sig = (n.get("dcmNo"), n.get("eleId"), n.get("offset"))
        if not n.get("dcmNo") or sig in seen:
            continue
        seen.add(sig)
        try:
            v = session.get(VIEWER_PART, params={
                "rcpNo": rcp, "dcmNo": n["dcmNo"], "eleId": n.get("eleId", "0"),
                "offset": n.get("offset", "0"), "length": n.get("length", "0"),
                "dtd": n.get("dtd", "dart3.xsd")}, timeout=REQUEST_TIMEOUT)
            txt = _strip(_decode(v))
            if txt:
                parts.append(f"[{n.get('text', '')}] {txt}")
        except Exception as e:
            logging.warning(f"viewer part failed for {rcp}: {e}")
        if len(parts) >= MAX_PARTS:
            break
        time.sleep(0.15)
    return "\n\n".join(parts)


def validate_corp(session, corp: str) -> None:
    r = session.get(RSS_URL.format(corp=corp), timeout=REQUEST_TIMEOUT)
    r.encoding = "utf-8"
    m = re.search(r"<channel>\s*<title>(.*?)</title>", r.text, re.S)
    name = _html.unescape(m.group(1)) if m else ""
    if "하이닉스" not in name and "hynix" not in name.lower():
        raise SystemExit(f"DART corp code {corp} is not SK hynix (feed title: {name!r})")
    logging.info(f"DART feed validated: {name}")


# ----------------------------------------------------------------------------
# ADS / ADR keyword layer (instant, regex)
# ----------------------------------------------------------------------------

ADS_RE = re.compile(
    r"\bADS\b|\bADSs\b|\bADR\b|\bADRs\b|American\s+Depositary|Depositary\s+(?:Share|Receipt)"
    r"|주식예탁증서|예탁증서|미국예탁|해외증권시장|해외\s*상장|해외증권"
    r"|유상증자|신주\s*발행|신주\s*(?:의\s*)?배정|전환\s*(?:한도|비율)|conversion\s+(?:cap|limit|ratio)"
    r"|뉴욕증권거래소|New\s+York\s+Stock\s+Exchange|NYSE|나스닥|Nasdaq",
    re.IGNORECASE,
)


def scan_ads(text: str):
    hits, seen = [], set()
    for m in ADS_RE.finditer(text):
        term = m.group(0)
        if term.lower() in seen:
            continue
        seen.add(term.lower())
        s = max(0, m.start() - 70)
        e = min(len(text), m.end() + 90)
        hits.append((term, text[s:e].strip()))
        if len(hits) >= 6:
            break
    return hits


def format_filing_alert(rcp, title, pub, hits) -> str:
    lines = [f"🇰🇷 SK hynix DART filing — {title}",
             f"{pub}  |  rcpNo {rcp}", ""]
    if hits:
        lines.append(f"🔎 ADS/ADR-related terms found ({len(hits)}):")
        for term, ctx in hits:
            lines.append(f"• {term}")
            lines.append(f"   “…{ctx}…”")
    else:
        lines.append("No ADS/ADR/overseas-listing/new-share keywords found by regex.")
    lines.append("")
    lines.append(f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp}")
    return "\n".join(lines)


# ----------------------------------------------------------------------------
# Telegram — OWNER ONLY
# ----------------------------------------------------------------------------


def telegram_send(token: str, chat_id: str, text: str) -> None:
    r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      data={"chat_id": chat_id, "text": text[:4096],
                            "disable_web_page_preview": True}, timeout=15)
    r.raise_for_status()


def notify_owner(token, chat_id, text) -> None:
    if not token or not chat_id:
        return
    try:
        telegram_send(token, chat_id, text)
    except Exception as e:
        logging.warning(f"Telegram send failed: {e}")


# ----------------------------------------------------------------------------
# LLM — the one question
# ----------------------------------------------------------------------------

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

LLM_SYSTEM_PROMPT = """You are a bilingual (Korean/English) securities-disclosure analyst.
You will be given the text of an SK hynix (SK하이닉스) regulatory filing from
Korea's DART/KIND system, usually in Korean.

Answer ONE question precisely: does this filing contain ANY information about
an ADS / ADR share issuance or the company's American Depositary Share
program? That includes: a decision or plan to issue new shares for ADSs
(유상증자, 신주 발행, 해외증권시장 주권등 상장 결정), listing or registration of
ADSs on a US exchange (NYSE/Nasdaq, 뉴욕증권거래소), changes to the ADS
conversion cap / depositary conversion limit (전환 한도), the size, price,
timing or use of proceeds of an ADS offering, or a board resolution, inquiry
response, or rumor clarification (풍문또는보도에대한해명) that discusses ADS/ADR
plans — including statements that NO decision has been made.

Rules:
- If present: report exactly what the filing says, in English, quoting key
  figures verbatim (share counts, KRW/USD amounts, dates, ratios, exchange
  names) and distinguishing DECIDED vs UNDER REVIEW vs DENIED/NO PLAN.
- If absent: say so plainly. Do not infer from the filing type.
- Also give a one-line English summary of what the filing is about overall.

Respond with ONLY a JSON object:
  - filing_summary: str (one line, English: what this filing is)
  - ads_info_present: boolean
  - ads_status: "decided" | "under_review" | "denied_or_no_plan" | "mentioned_other" | null
  - ads_summary: str — EXACTLY TWO LINES (separated by "\\n"), English, about the
    ADS/ADR issuance specifically. Line 1: whether the filing contains any
    ADS/ADR issuance information and its status (decided / under review /
    denied / not mentioned). Line 2: if present, the key facts with verbatim
    figures (shares, amounts, ratio, exchange, dates); if absent, state
    explicitly that there is no ADS/ADR, overseas-listing or new-share content
    and what the filing covers instead. Never leave either line empty.
  - ads_details: str or null (English, verbatim figures, up to 4 sentences)
  - confidence: "low" | "medium" | "high"
"""


def build_llm_input(text: str) -> str:
    if len(text) <= MAX_LLM_CHARS:
        return text
    head = text[:15000]
    spans = []
    for m in ADS_RE.finditer(text):
        s, e = max(0, m.start() - 1500), min(len(text), m.end() + 2500)
        if spans and s <= spans[-1][1]:
            spans[-1] = (spans[-1][0], e)
        else:
            spans.append((s, e))
    budget = MAX_LLM_CHARS - len(head) - 200
    windows = []
    for s, e in spans:
        if budget - (e - s) < 0:
            break
        windows.append(text[s:e])
        budget -= (e - s)
    return head + "\n[...truncated; keyword-relevant sections follow...]\n" + "\n[...]\n".join(windows)


def analyze_with_llm(text: str, title: str, api_key: str, model: str,
                     timeout: float = 45.0) -> Optional[dict]:
    if not api_key:
        return None
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": LLM_SYSTEM_PROMPT},
            {"role": "user", "content": f"Filing title: {title}\n\nFiling text:\n\n{build_llm_input(text)}"},
        ],
        "temperature": 0.0,
        "max_tokens": 2000,
        "response_format": {"type": "json_object"},
    }
    if "gpt-oss" in model:
        body["reasoning_effort"] = "low"
    try:
        r = requests.post(GROQ_API_URL,
                          headers={"Authorization": f"Bearer {api_key}",
                                   "Content-Type": "application/json"},
                          json=body, timeout=timeout)
        r.raise_for_status()
        parsed = json.loads(r.json()["choices"][0]["message"]["content"])
        return parsed if isinstance(parsed, dict) else None
    except Exception as e:
        logging.warning(f"LLM call failed: {e}")
        return None


OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"


def analyze_with_openai(text: str, title: str, api_key: str, model: str,
                        reasoning_effort: str = "low", extra_json: str = "",
                        timeout: float = 45.0):
    """Second, independent opinion from OpenAI. Same prompt and JSON schema as
    the Groq pass. Model / effort / extra body fields come from config so the
    exact strings can be corrected without a code change. Returns
    (parsed_dict_or_None, error_string_or_None)."""
    if not api_key:
        return None, "OPENAI_API_KEY not set"
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": LLM_SYSTEM_PROMPT},
            {"role": "user", "content": f"Filing title: {title}\n\nFiling text:\n\n{build_llm_input(text)}"},
        ],
        "response_format": {"type": "json_object"},
    }
    if reasoning_effort:
        body["reasoning_effort"] = reasoning_effort
    if extra_json:
        try:
            body.update(json.loads(extra_json))
        except Exception as e:
            return None, f"OPENAI_EXTRA_JSON is not valid JSON: {e}"
    try:
        r = requests.post(OPENAI_API_URL,
                          headers={"Authorization": f"Bearer {api_key}",
                                   "Content-Type": "application/json"},
                          json=body, timeout=timeout)
        if r.status_code >= 400:
            try:
                detail = r.json().get("error", {}).get("message", r.text[:300])
            except Exception:
                detail = r.text[:300]
            return None, f"HTTP {r.status_code}: {detail}"
        parsed = json.loads(r.json()["choices"][0]["message"]["content"])
        return (parsed if isinstance(parsed, dict) else None), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def format_llm_message(llm: dict, engine: str = "🤖") -> str:
    present = bool(llm.get("ads_info_present"))
    status = llm.get("ads_status") or ""
    icon = {"decided": "🔴", "under_review": "🟠", "denied_or_no_plan": "🟢",
            "mentioned_other": "🟡"}.get(status, "🟢" if not present else "🟡")
    summary = (llm.get("ads_summary") or "").strip()
    two = [l.strip() for l in summary.split("\n") if l.strip()][:2]
    while len(two) < 2:
        two.append(llm.get("ads_details") or llm.get("filing_summary") or "")
    lines = [f"{engine} ADS/ADR issuance: {'PRESENT' if present else 'NONE'} {icon}"
             + (f" ({status.replace('_', ' ')})" if present and status else "")
             + f"  [conf: {llm.get('confidence', '?')}]",
             f"1️⃣ {two[0]}",
             f"2️⃣ {two[1]}"]
    if present and llm.get("ads_details"):
        lines.append(f"Details: {llm['ads_details']}")
    lines.append(f"Filing: {llm.get('filing_summary', '')}")
    return "\n".join(lines)


# ----------------------------------------------------------------------------
# Health (owner-only)
# ----------------------------------------------------------------------------


class HealthMonitor:
    def __init__(self, token, chat_id, fail_threshold=5, heartbeat_secs=300):
        self.token, self.chat_id = token, chat_id
        self.fail_threshold, self.heartbeat_secs = fail_threshold, heartbeat_secs
        self.fails = 0
        self.alerted = False
        self.last_ok = time.monotonic()
        self.last_hb = time.monotonic()
        self.last_reason = ""

    def ok(self):
        self.last_ok = time.monotonic()
        if self.alerted:
            msg = "✅ SK hynix DART poller RECOVERED."
            logging.warning(msg)
            notify_owner(self.token, self.chat_id, msg)
            self.alerted = False
        self.fails = 0

    def fail(self, reason):
        self.fails += 1
        self.last_reason = reason
        if self.fails >= self.fail_threshold and not self.alerted:
            msg = (f"⚠️ SK hynix DART poller DEGRADED — {self.fails} consecutive "
                   f"failures ({reason}), {int(time.monotonic() - self.last_ok)}s "
                   f"since last successful poll.")
            logging.warning(msg)
            notify_owner(self.token, self.chat_id, msg)
            self.alerted = True

    def heartbeat(self):
        now = time.monotonic()
        if now - self.last_hb >= self.heartbeat_secs:
            st = "OK" if self.fails == 0 else f"DEGRADED ({self.fails} fails: {self.last_reason})"
            logging.info(f"♥ heartbeat — {st}, {int(now - self.last_ok)}s since last OK poll.")
            self.last_hb = now


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

    def cfg_get(k, d=None):
        return os.environ.get(k) or cfg.get(k) or d

    ap = argparse.ArgumentParser(description="Watch SK hynix DART filings; ask LLM about ADS issuance. Owner-only alerts.")
    ap.add_argument("--corp-code", default=cfg_get("DART_CORP_CODE", DART_CORP_CODE))
    ap.add_argument("--interval", type=float, default=float(cfg_get("DART_INTERVAL", DEFAULT_INTERVAL)))
    ap.add_argument("--user-agent", default=cfg_get("DART_UA", "Mozilla/5.0 (compatible; filing-watcher) " + cfg_get("SEC_UA", "")))
    ap.add_argument("--prime", action="store_true", help="Mark current RSS items seen; run once before going live.")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--backtest", default=None, help="Comma-separated rcpNo(s) to run through the pipeline (no state change, no Telegram).")
    ap.add_argument("--test-telegram", action="store_true")
    ap.add_argument("--telegram-token", default=cfg_get("TELEGRAM_BOT_TOKEN"))
    ap.add_argument("--telegram-chat-id", default=cfg_get("TELEGRAM_CHAT_ID"))
    ap.add_argument("--groq-api-key", default=cfg_get("GROQ_API_KEY"))
    ap.add_argument("--groq-model", default=cfg_get("GROQ_MODEL", "openai/gpt-oss-120b"))
    ap.add_argument("--llm-timeout", type=float, default=float(cfg_get("LLM_TIMEOUT", 45)))
    ap.add_argument("--openai-api-key", default=cfg_get("OPENAI_API_KEY"))
    ap.add_argument("--openai-model", default=cfg_get("OPENAI_MODEL", "gpt-6.1-sol"))
    ap.add_argument("--openai-reasoning-effort", default=cfg_get("OPENAI_REASONING_EFFORT", "low"))
    ap.add_argument("--openai-extra-json", default=cfg_get("OPENAI_EXTRA_JSON", ""),
                    help='Extra JSON merged into the OpenAI request body, e.g. {"service_tier":"fast"}')
    ap.add_argument("--health-fail-threshold", type=int, default=int(cfg_get("HEALTH_FAIL_THRESHOLD", 5)))
    ap.add_argument("--heartbeat-secs", type=int, default=int(cfg_get("HEARTBEAT_SECS", 300)))
    ap.add_argument("--log-file", default=cfg_get("LOG_FILE"))
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout),
                                  *([logging.FileHandler(args.log_file, encoding="utf-8")] if args.log_file else [])])

    session = make_session(args.user_agent)
    state = load_state()
    seen = set(state.get("seen_rcp", []))

    def process(rcp, title, pub, notify=True):
        text = fetch_filing_text(session, rcp)
        logging.info(f"Fetched {len(text)} chars for {rcp} ({title})")
        hits = scan_ads(text)
        msg1 = format_filing_alert(rcp, title, pub, hits)
        if len(text) < 40:
            msg1 = ("⚠️ FILING TEXT COULD NOT BE FETCHED — open the link and read it "
                    "manually. (LLM skipped: no content to judge.)\n\n" + msg1)
        logging.info("New filing:\n" + msg1)
        if notify:
            notify_owner(args.telegram_token, args.telegram_chat_id, msg1)
        if args.groq_api_key and len(text) >= 40:
            llm = analyze_with_llm(text, title, args.groq_api_key, args.groq_model, args.llm_timeout)
            msg2 = (format_llm_message(llm, f"🤖 {args.groq_model}") if llm
                    else f"🤖 {args.groq_model} call failed — no answer for this filing; read it manually.")
            logging.info(msg2)
            if notify:
                notify_owner(args.telegram_token, args.telegram_chat_id, msg2)
        # Second, independent opinion (OpenAI) — its own separate alert.
        if len(text) >= 40 and (args.openai_api_key or args.openai_model):
            oai, err = analyze_with_openai(text, title, args.openai_api_key,
                                           args.openai_model, args.openai_reasoning_effort,
                                           args.openai_extra_json, args.llm_timeout)
            msg3 = (format_llm_message(oai, f"🧠 {args.openai_model}") if oai
                    else f"🧠 {args.openai_model} unavailable — {err}")
            logging.info(msg3)
            if notify and (oai or args.openai_api_key):
                notify_owner(args.telegram_token, args.telegram_chat_id, msg3)
        return hits

    if args.test_telegram:
        telegram_send(args.telegram_token, args.telegram_chat_id,
                      "🧪 TEST — SK hynix DART watcher: owner-only delivery works.")
        logging.info("✅ test sent to owner only")
        return

    if args.backtest:
        items = {r: (t, p) for r, t, p in fetch_rss(session, args.corp_code)}
        for rcp in [x.strip() for x in args.backtest.split(",") if x.strip()]:
            title, pub = items.get(rcp, ("(not in current RSS window)", ""))
            print(f"\n===== BACKTEST {rcp} =====")
            try:
                process(rcp, title, pub, notify=False)
            except Exception as e:
                logging.exception(f"backtest error {rcp}: {e}")
        return

    validate_corp(session, args.corp_code)

    if args.prime:
        for rcp, _, _ in fetch_rss(session, args.corp_code):
            seen.add(rcp)
        state["seen_rcp"] = sorted(seen)
        save_state(state)
        logging.info(f"Primed: {len(seen)} filing(s) marked seen.")
        return

    stop = {"flag": False}

    def _sig(*_):
        stop["flag"] = True
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    health = HealthMonitor(args.telegram_token, args.telegram_chat_id,
                           args.health_fail_threshold, args.heartbeat_secs)
    logging.info(f"Polling DART RSS for corp {args.corp_code} every {args.interval:.1f}s; "
                 f"{len(seen)} seen; LLM={args.groq_model}; owner-only alerts.")
    backoff = args.interval
    while not stop["flag"]:
        tick = time.monotonic()
        try:
            items = fetch_rss(session, args.corp_code)
            backoff = args.interval
            health.ok()
            for rcp, title, pub in reversed([i for i in items if i[0] not in seen]):
                try:
                    process(rcp, title, pub)
                except Exception as e:
                    logging.exception(f"Error processing {rcp}: {e}")
                    notify_owner(args.telegram_token, args.telegram_chat_id,
                                 f"⚠️ New SK hynix filing {rcp} ({title}) detected but processing "
                                 f"failed: {e}\nhttps://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp}")
                seen.add(rcp)
                state["seen_rcp"] = sorted(seen)
                save_state(state)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            if code in (429, 503):
                backoff = min(backoff * 2, 60.0)
            logging.warning(f"HTTP {code}: {e}")
            health.fail(f"HTTP {code}")
        except requests.RequestException as e:
            logging.warning(f"Network error: {e}")
            health.fail(type(e).__name__)
        except Exception as e:
            logging.exception(f"Unexpected: {e}")
            health.fail(type(e).__name__)
        health.heartbeat()
        if args.once:
            break
        end = time.monotonic() + max(0.0, backoff - (time.monotonic() - tick))
        while not stop["flag"] and time.monotonic() < end:
            time.sleep(min(0.2, end - time.monotonic()))
    logging.info("Bye.")


if __name__ == "__main__":
    main()
