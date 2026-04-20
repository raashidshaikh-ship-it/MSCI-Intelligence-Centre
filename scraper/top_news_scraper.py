"""
Top News Scraper — MSCI Competitor Intelligence (Finnhub + RSS edition)
========================================================================

Purpose
-------
Populate data/top_news.json with recent news about MSCI competitors, using
reliable, structured data sources instead of fragile HTML scraping.

Data sources
------------
1. Finnhub /company-news  (primary) — 60 req/min, 1-year history, JSON
2. Competitor press-release RSS feeds (secondary, covers product launches)

Inputs
------
- config/sources.json        (competitor roster — editable without code changes)
- env FINNHUB_API_KEY        (GitHub Actions secret)

Outputs
-------
- data/top_news.json         (list of articles, newest first)
- data/scraper_status.json   (per-source last run status)

Deps: requests (standard library for everything else)

Usage
-----
    export FINNHUB_API_KEY=xxx
    python scraper/top_news_scraper.py
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests

# ──────────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
CONFIG_FILE = os.path.join(REPO_ROOT, "config", "sources.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "top_news.json")
STATUS_FILE = os.path.join(DATA_DIR, "scraper_status.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/xml, application/xml, */*",
}

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()

# ──────────────────────────────────────────────────────────────────────────
# Categorization + sentiment (rule-based, no external deps)
# ──────────────────────────────────────────────────────────────────────────
CATEGORY_KEYWORDS = {
    "Product Launch": [r"\blaunch(es|ed|ing)?\b", r"\bunveil(s|ed|ing)?\b",
                       r"\bdebut(s|ed)?\b", r"\bintroduc(e|es|ed|ing)\b",
                       r"\brolls? out\b", r"\bnew (index|product|platform|service)\b"],
    "M&A": [r"\bacqui(re|res|red|ring|sition)\b", r"\bmerger\b",
            r"\bagrees? to (buy|purchase|acquire)\b", r"\btakeover\b",
            r"\bto acquire\b", r"\bdivest(s|ed|iture)?\b"],
    "Partnership": [r"\bpartner(s|ed|ship)?\b", r"\bcollaborat(e|es|ed|ion)\b",
                    r"\bjoint venture\b", r"\balliance\b", r"\bteams? up with\b"],
    "Earnings": [r"\bQ[1-4]\b", r"\b(quarterly|annual) (earnings|results)\b",
                 r"\breports? (results|earnings)\b", r"\bfiscal (year|quarter)\b",
                 r"\bguidance\b", r"\bEPS\b"],
    "Regulatory": [r"\bSEC\b", r"\bESMA\b", r"\bregulator(s|y)?\b",
                   r"\bfine(d|s)?\b", r"\binvestigation\b", r"\bprobe\b",
                   r"\bantitrust\b", r"\bcompliance\b"],
    "Leadership": [r"\bappoint(s|ed|ment)?\b", r"\bnames?.{1,40}(CEO|CFO|CTO|president|chair)\b",
                   r"\bsteps? down\b", r"\bresign(s|ed|ation)?\b", r"\bhires?\b"],
    "Strategy": [r"\bstrategy\b", r"\bexpand(s|ed|ing|sion)?\b",
                 r"\brestructur(e|es|ed|ing)\b", r"\brebrand\b"],
}

POSITIVE_WORDS = {
    "surge", "soar", "rally", "gain", "jump", "growth", "record", "beat",
    "outperform", "upgrade", "strong", "rise", "climb", "boost", "exceed",
    "win", "launch", "partnership", "acquire", "innovation", "expand",
    "successful", "momentum", "dividend",
}

NEGATIVE_WORDS = {
    "crash", "plunge", "drop", "fall", "decline", "loss", "slump", "downturn",
    "crisis", "fear", "cut", "layoff", "slash", "inflation", "deficit",
    "bankruptcy", "weak", "miss", "downgrade", "collapse", "volatile", "warn",
    "fine", "lawsuit", "probe", "investigation", "antitrust",
}


def classify_category(text: str) -> str:
    for cat, patterns in CATEGORY_KEYWORDS.items():
        for pat in patterns:
            if re.search(pat, text, flags=re.IGNORECASE):
                return cat
    return "General"


def classify_sentiment(text: str) -> str:
    low = text.lower()
    pos = sum(1 for w in POSITIVE_WORDS if w in low)
    neg = sum(1 for w in NEGATIVE_WORDS if w in low)
    if pos > neg + 1:
        return "Positive"
    if neg > pos + 1:
        return "Negative"
    return "Neutral"


def importance_score(article: dict) -> int:
    score = 20
    text = f"{article.get('title', '')} {article.get('summary', '')}".lower()
    if article.get("competitor", "").lower() in text:
        score += 25
    if article.get("category") in ("M&A", "Product Launch"):
        score += 25
    elif article.get("category") in ("Earnings", "Regulatory"):
        score += 15
    pub = article.get("published")
    if pub:
        try:
            dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - dt).days
            if age_days <= 3:
                score += 20
            elif age_days <= 7:
                score += 10
        except Exception:
            pass
    if any(k in text for k in ("msci", "index", "esg", "benchmark", "etf")):
        score += 15
    return min(100, score)


# ──────────────────────────────────────────────────────────────────────────
# Data-source fetchers
# ──────────────────────────────────────────────────────────────────────────
def fetch_finnhub(ticker: str, competitor: str, segment: str,
                  days_back: int, max_items: int) -> list[dict]:
    """Call Finnhub /company-news. Returns normalized article dicts."""
    if not FINNHUB_API_KEY:
        raise RuntimeError("FINNHUB_API_KEY not set in environment")
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days_back)
    url = (
        "https://finnhub.io/api/v1/company-news"
        f"?symbol={ticker}&from={start.isoformat()}&to={end.isoformat()}"
        f"&token={FINNHUB_API_KEY}"
    )
    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    raw = resp.json()
    if not isinstance(raw, list):
        raise RuntimeError(f"Finnhub returned non-list: {str(raw)[:200]}")

    out = []
    for it in raw[:max_items]:
        title = (it.get("headline") or "").strip()
        url_ = (it.get("url") or "").strip()
        if not title or not url_:
            continue
        published = ""
        if it.get("datetime"):
            try:
                published = datetime.fromtimestamp(
                    int(it["datetime"]), tz=timezone.utc
                ).isoformat()
            except Exception:
                published = ""
        out.append({
            "title": title,
            "url": url_,
            "source": it.get("source") or "Finnhub",
            "summary": (it.get("summary") or "").strip()[:500],
            "published": published,
            "image": it.get("image") or "",
            "competitor": competitor,
            "ticker": ticker,
            "segment": segment,
            "origin": "finnhub",
        })
    return out


def _parse_rss_date(raw: str) -> str:
    if not raw:
        return ""
    for parser in (parsedate_to_datetime,):
        try:
            dt = parser(raw)
            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    # ISO-8601 fallback (e.g. 2026-03-14T09:00:00Z)
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ""


def fetch_rss(feed_url: str, competitor: str, ticker: str, segment: str,
              max_items: int) -> list[dict]:
    """Parse an RSS/Atom feed. Returns normalized article dicts."""
    resp = requests.get(feed_url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    # Strip namespaces so we can use simple tag names
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]

    items = root.findall(".//item") or root.findall(".//entry")
    out = []
    for it in items[:max_items]:
        title_el = it.find("title")
        link_el = it.find("link")
        desc_el = it.find("description") or it.find("summary") or it.find("content")
        date_el = (it.find("pubDate") or it.find("published")
                   or it.find("updated") or it.find("date"))

        title = (title_el.text or "").strip() if title_el is not None else ""
        # Atom <link href="..."/> vs RSS <link>url</link>
        if link_el is not None:
            url_ = link_el.get("href") or (link_el.text or "").strip()
        else:
            url_ = ""
        url_ = url_.strip()
        if not title or not url_:
            continue

        summary_html = desc_el.text or "" if desc_el is not None else ""
        summary = re.sub(r"<[^>]+>", " ", summary_html)
        summary = re.sub(r"\s+", " ", summary).strip()[:500]

        published = _parse_rss_date(date_el.text if date_el is not None else "")

        out.append({
            "title": title,
            "url": url_,
            "source": competitor,  # RSS from the company itself
            "summary": summary,
            "published": published,
            "image": "",
            "competitor": competitor,
            "ticker": ticker,
            "segment": segment,
            "origin": "rss",
        })
    return out


# ──────────────────────────────────────────────────────────────────────────
# Dedupe + enrich
# ──────────────────────────────────────────────────────────────────────────
def article_id(article: dict) -> str:
    key = (article.get("url", "") or article.get("title", "")).strip().lower()
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:16]


def score_to_label(score: int) -> str:
    if score >= 80:
        return "Critical"
    if score >= 60:
        return "High"
    if score >= 40:
        return "Medium"
    return "Low"


def enrich(article: dict) -> dict:
    text = f"{article.get('title', '')} {article.get('summary', '')}"
    article["category"] = classify_category(text)
    article["sentiment"] = classify_sentiment(text)
    score = importance_score(article)
    article["importance_score"] = score
    article["importance"] = score_to_label(score)
    article["impact_level"] = score_to_label(score)  # dashboard reads both fields
    article["id"] = article_id(article)
    return article


def dedupe(articles: list[dict]) -> list[dict]:
    seen: dict[str, dict] = {}
    for a in articles:
        aid = a.get("id") or article_id(a)
        a["id"] = aid
        prev = seen.get(aid)
        # Prefer articles with a published date; otherwise keep the first seen.
        if prev is None:
            seen[aid] = a
        else:
            if not prev.get("published") and a.get("published"):
                seen[aid] = a
    return list(seen.values())


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def main() -> int:
    cfg = load_config()
    settings = cfg.get("settings", {})
    days_back = int(settings.get("finnhub_days_back", 14))
    per_src_cap = int(settings.get("max_articles_per_source", 25))
    total_cap = int(settings.get("total_cap", 200))

    all_articles: list[dict] = []
    statuses: list[dict] = []

    for comp in cfg.get("competitors", []):
        name = comp.get("name") or "Unknown"
        ticker = comp.get("ticker") or ""
        segment = comp.get("segment") or ""

        # 1. Finnhub
        if ticker and FINNHUB_API_KEY:
            src_name = f"Finnhub-{ticker}"
            try:
                items = fetch_finnhub(ticker, name, segment, days_back, per_src_cap)
                all_articles.extend(items)
                statuses.append({"name": src_name, "status": "ok", "items": len(items)})
                print(f"[ok] {src_name}: {len(items)} articles")
            except Exception as e:
                statuses.append({"name": src_name, "status": "error", "error": str(e)[:200], "items": 0})
                print(f"[err] {src_name}: {e}")
            time.sleep(1.1)  # stay well under 60 req/min
        elif ticker and not FINNHUB_API_KEY:
            statuses.append({"name": f"Finnhub-{ticker}", "status": "skipped",
                             "error": "FINNHUB_API_KEY not set", "items": 0})

        # 2. RSS
        for feed in comp.get("rss", []) or []:
            short = feed.split("/")[2] if "://" in feed else feed
            src_name = f"RSS-{short}"
            try:
                items = fetch_rss(feed, name, ticker, segment, per_src_cap)
                all_articles.extend(items)
                statuses.append({"name": src_name, "status": "ok", "items": len(items)})
                print(f"[ok] {src_name}: {len(items)} articles")
            except Exception as e:
                statuses.append({"name": src_name, "status": "error", "error": str(e)[:200], "items": 0})
                print(f"[err] {src_name}: {e}")

    # Enrich + dedupe
    all_articles = [enrich(a) for a in all_articles]
    all_articles = dedupe(all_articles)

    # Sort newest first, then by importance as tiebreaker
    def sort_key(a):
        pub = a.get("published") or ""
        return (pub, a.get("importance", 0))
    all_articles.sort(key=sort_key, reverse=True)
    all_articles = all_articles[:total_cap]

    # Write outputs
    payload = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total": len(all_articles),
        "articles": all_articles,
    }
    write_json(OUTPUT_FILE, payload)

    # Merge status file so other scrapers' entries aren't clobbered
    existing_status = {}
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as f:
                existing_status = json.load(f) or {}
        except Exception:
            existing_status = {}
    sources = {s.get("name"): s for s in (existing_status.get("sources") or [])}
    for s in statuses:
        sources[s["name"]] = s
    write_json(STATUS_FILE, {
        "last_run": datetime.now(timezone.utc).isoformat(),
        "sources": list(sources.values()),
    })

    ok = sum(1 for s in statuses if s["status"] == "ok")
    total_items = sum(s.get("items", 0) for s in statuses)
    print(f"\nDone. {ok}/{len(statuses)} sources ok, "
          f"{total_items} raw items, {len(all_articles)} after dedupe.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
