"""
Brand Intelligence Scraper — MSCI digital identity
===================================================

Tracks the evolution of MSCI's digital identity across six signal
families, written to data/brand_intelligence.json for the dashboard:

  brand_trend[]              — 90-day pageview interest, MSCI vs 4 peers
  top_queries[]              — MSCI-anchored search queries (not raw tokens)
  rising_queries[]           — fastest-growing MSCI queries
  regional_interest[]        — country-level interest
  competitor_comparison[]    — latest-week snapshot

  sentiment_daily[]          — daily mean sentiment per brand (VADER)
  sentiment_summary{}        — 7d/30d sentiment + delta + article count
  share_of_voice[]           — daily SoV% per brand (article-count share)
  share_of_voice_summary{}   — 7d SoV + rank + delta

  autocomplete_suggestions[] — live Google Autocomplete for "MSCI ..."
  market_footprint{}         — $MSCI stock + MSCI-indexed ETF performance

Data-source layering (each layer is independently fault-tolerant):
  L1 Wikipedia Pageviews API  — brand trend, regional interest
  L2 Google News RSS          — SoV, sentiment, queries
  L3 Google Autocomplete      — live demand signals, rising queries
  L4 Yahoo Finance (yfinance) — market footprint
  L5 pytrends (best effort)   — fallback for queries
  L6 Seeded baseline          — keeps dashboard populated if offline

Run:   python scraper/brand_intelligence_scraper.py
Deps:  requests, vaderSentiment, yfinance  (all optional — falls back)
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests

# ──────────────────────────────────────────────────────────────────────────
# Paths + constants
# ──────────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "brand_intelligence.json")

BRANDS = ["MSCI", "S&P Global", "FTSE Russell", "Bloomberg Terminal", "Morningstar"]

WIKI_ARTICLE = {
    "MSCI": "MSCI",
    "S&P Global": "S%26P_Global",
    "FTSE Russell": "FTSE_Russell",
    "Bloomberg Terminal": "Bloomberg_Terminal",
    "Morningstar": "Morningstar,_Inc.",
}

WIKI_REGIONS = [
    ("en.wikipedia", "United States"),
    ("de.wikipedia", "Germany"),
    ("fr.wikipedia", "France"),
    ("es.wikipedia", "Spain"),
    ("ja.wikipedia", "Japan"),
    ("zh.wikipedia", "China"),
    ("pt.wikipedia", "Brazil"),
    ("ko.wikipedia", "South Korea"),
    ("it.wikipedia", "Italy"),
    ("ru.wikipedia", "Russia"),
    ("nl.wikipedia", "Netherlands"),
    ("pl.wikipedia", "Poland"),
]

HEADERS = {
    "User-Agent": (
        "MSCI-Intelligence-Centre/1.0 (https://github.com/raashidshaikh-ship-it/"
        "MSCI-Intelligence-Centre; raashid.shaikh@msci.com)"
    ),
    "Accept": "application/json, text/xml, application/xml, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# Known MSCI product vocabulary — these are the meaningful terms we want
# to surface as "queries". Anything in here, when it co-occurs with "msci",
# is worth showing; anything else is usually noise.
MSCI_PRODUCT_TERMS = {
    # Flagship indexes
    "world", "acwi", "eafe", "emerging markets", "em", "world index",
    "kld", "usa", "esg leaders", "sri", "climate change",
    # Thematic indexes
    "esg", "esg ratings", "esg rating", "esg research", "climate", "climate action",
    "net zero", "net-zero", "sustainable", "sustainability", "transition", "paris aligned",
    "biodiversity", "carbon", "carbon metrics", "low carbon", "ai", "thematic",
    "factor", "factors", "quality", "momentum", "minimum volatility", "min vol",
    "value", "size", "small cap", "mid cap", "large cap",
    # Products & services
    "analytics", "barra", "riskmetrics", "real estate", "private capital",
    "private equity", "private assets", "real assets", "wealth", "wealth solutions",
    "fabric", "indexes", "index", "ratings", "research", "data", "api",
    # Corporate
    "earnings", "revenue", "ceo", "cfo", "henry fernandez", "linda huber",
    "acquisition", "buyback", "dividend", "guidance", "target price",
    # Region-flavoured
    "china a", "japan", "india", "europe", "asia pacific", "frontier",
}

# Publications / news brands / generic words to EXCLUDE from query lists
# (these dominate any raw frequency count of news titles)
PUBLICATION_NOISE = {
    "yahoo", "reuters", "bloomberg", "cnbc", "barrons", "barron", "ft", "wsj",
    "seeking alpha", "marketwatch", "investopedia", "forbes", "zacks", "nasdaq",
    "benzinga", "motley fool", "fool", "etf.com", "insider", "yahoo finance",
    "press release", "globenewswire", "businesswire", "prnewswire", "newswire",
    "com", "finance", "stock", "stocks", "market", "markets", "news", "report",
    "reports", "update", "analysis", "review", "preview", "outlook", "shares",
    "trading", "investment", "investor", "investors", "fund", "funds",
    "today", "week", "month", "quarter", "year", "annual",
    "how", "why", "what", "when", "where", "who", "whose",
    "best", "top", "new", "latest", "next", "last", "first",
    "good", "bad", "high", "low", "up", "down",
    "could", "should", "will", "would", "may", "might", "can", "does",
    "has", "have", "had", "is", "are", "was", "were",
    "developed", "developing", "global", "international", "national",
    "greece", "indonesia", "turkey", "argentina",  # country-of-the-day noise
}

STOPWORDS = {
    "the","and","for","with","from","this","that","have","has","are","was","were",
    "will","would","what","when","where","which","there","their","them","they",
    "you","your","its","it's","into","onto","been","being","over","under","more",
    "most","some","such","than","then","these","those","about","after","before",
    "inc","ltd","llc","plc","co","corp","corporation","group","amp","vs","via",
}

# Autocomplete prefixes to probe
AUTOCOMPLETE_PREFIXES = [
    "MSCI", "MSCI ", "MSCI a", "MSCI b", "MSCI c", "MSCI d", "MSCI e",
    "MSCI f", "MSCI g", "MSCI i", "MSCI k", "MSCI m", "MSCI n", "MSCI p",
    "MSCI r", "MSCI s", "MSCI t", "MSCI w", "MSCI vs",
    "is MSCI", "what is MSCI", "MSCI index", "MSCI world", "MSCI esg",
]

# Market footprint tickers
MSCI_TICKER = "MSCI"
MSCI_ETFS = [
    ("URTH", "iShares MSCI World"),
    ("ACWI", "iShares MSCI ACWI"),
    ("EFA",  "iShares MSCI EAFE"),
    ("EEM",  "iShares MSCI Emerging Markets"),
    ("ESGU", "iShares ESG Aware MSCI USA"),
]

# ──────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────
def log(msg: str, indent: int = 0) -> None:
    print(("  " * indent) + msg, flush=True)


def load_existing(path: str):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_data(data: dict, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log(f"saved → {path}", 1)


def get_json(url: str, timeout: int = 20):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.json()


def get_text(url: str, timeout: int = 20) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


# ──────────────────────────────────────────────────────────────────────────
# L1 — Wikipedia Pageviews
# ──────────────────────────────────────────────────────────────────────────
def fetch_wiki_pageviews(article: str, start: datetime, end: datetime,
                         project: str = "en.wikipedia"):
    s = start.strftime("%Y%m%d"); e = end.strftime("%Y%m%d")
    url = (f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
           f"{project}/all-access/all-agents/{article}/daily/{s}/{e}")
    data = get_json(url)
    return [(f"{it['timestamp'][0:4]}-{it['timestamp'][4:6]}-{it['timestamp'][6:8]}",
             int(it.get("views", 0)))
            for it in data.get("items", []) if len(it.get("timestamp", "")) >= 8]


def build_brand_trend_from_wiki(days: int = 90):
    end = datetime.utcnow().date() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    end_dt = datetime.combine(end, datetime.min.time())
    start_dt = datetime.combine(start, datetime.min.time())

    per_brand_raw = {}
    any_success = False
    for brand in BRANDS:
        article = WIKI_ARTICLE.get(brand)
        try:
            rows = fetch_wiki_pageviews(article, start_dt, end_dt)
            per_brand_raw[brand] = {d: v for d, v in rows}
            log(f"wiki {brand}: {len(rows)} days, max {max([v for _,v in rows] or [0])}", 2)
            any_success = True
            time.sleep(0.3)
        except Exception as ex:
            log(f"wiki {brand}: FAIL ({str(ex)[:80]})", 2)
            per_brand_raw[brand] = {}

    if not any_success:
        return [], "wikipedia_unreachable"

    all_values = [v for s in per_brand_raw.values() for v in s.values()]
    global_max = max(all_values) if all_values else 1
    if global_max <= 0:
        global_max = 1

    all_dates = sorted({d for s in per_brand_raw.values() for d in s})
    trend = []
    for d in all_dates:
        row = {"date": d}
        for brand in BRANDS:
            raw = per_brand_raw.get(brand, {}).get(d, 0)
            row[brand] = int(round((raw / global_max) * 100))
        trend.append(row)
    return trend, "wikipedia_pageviews"


def build_regional_from_wiki(days: int = 30):
    end = datetime.utcnow().date() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    end_dt = datetime.combine(end, datetime.min.time())
    start_dt = datetime.combine(start, datetime.min.time())

    totals = []
    for project, country in WIKI_REGIONS:
        try:
            rows = fetch_wiki_pageviews("MSCI", start_dt, end_dt, project=project)
            total = sum(v for _, v in rows)
            if total > 0:
                totals.append((country, total))
            time.sleep(0.3)
        except Exception:
            pass
    if not totals:
        return [], "wikipedia_unreachable"
    totals.sort(key=lambda x: x[1], reverse=True)
    top = totals[:12]
    max_v = max(v for _, v in top) if top else 1
    return [{"country": c, "interest": int(round((v / max_v) * 100))} for c, v in top], "wikipedia_pageviews"


# ──────────────────────────────────────────────────────────────────────────
# L2 — Google News RSS (articles → SoV, sentiment, queries)
# ──────────────────────────────────────────────────────────────────────────
def fetch_news_rss(query: str, window: str = "90d") -> list[dict]:
    q = urllib.parse.quote(f"{query} when:{window}")
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    try:
        xml = get_text(url, timeout=20)
    except Exception as ex:
        log(f"news {query} failed: {str(ex)[:80]}", 1)
        return []
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return []
    items = []
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        pub = it.findtext("pubDate") or ""
        desc = (it.findtext("description") or "").strip()
        src_el = it.find("source")
        src = src_el.text.strip() if src_el is not None and src_el.text else ""
        try:
            dt = parsedate_to_datetime(pub).astimezone(timezone.utc)
        except Exception:
            dt = None
        # Strip the publication suffix that Google News appends to titles
        # e.g. "MSCI launches climate index - Reuters"
        clean_title = re.sub(r"\s[\-–]\s[A-Z][\w\. &]+$", "", title).strip()
        items.append({
            "title": clean_title,
            "raw_title": title,
            "pub_date": dt,
            "source": src,
            "description": desc,
        })
    return items


def extract_msci_queries(articles: list[dict]) -> tuple[list[dict], list[dict]]:
    """MSCI-anchored n-gram extraction.

    Strategy: only emit phrases that either (a) start with "msci" and are
    followed by a non-stopword, or (b) match a known MSCI product term.
    This produces "msci world", "msci climate", "msci esg ratings" rather
    than "yahoo", "reuters", "com".
    """
    if not articles:
        return [], []

    now = datetime.now(timezone.utc)
    recent_cutoff = now - timedelta(days=30)

    def clean_tokens(title: str) -> list[str]:
        t = title.lower()
        t = re.sub(r"[^\w\s&-]", " ", t)
        toks = [w.strip("-&") for w in t.split() if w.strip("-&")]
        return toks

    total_counts: Counter = Counter()
    recent_counts: Counter = Counter()
    total_articles = 0
    recent_articles = 0

    for a in articles:
        toks = clean_tokens(a["title"])
        phrases = set()

        # 1. MSCI-anchored bigrams: "msci X"
        for i, t in enumerate(toks):
            if t == "msci" and i + 1 < len(toks):
                nxt = toks[i + 1]
                if (nxt not in STOPWORDS
                        and nxt not in PUBLICATION_NOISE
                        and len(nxt) >= 3
                        and not nxt.isdigit()):
                    phrases.add(f"msci {nxt}")
                    # trigram if next-next token extends meaningfully
                    if i + 2 < len(toks):
                        nn = toks[i + 2]
                        if (nn not in STOPWORDS
                                and nn not in PUBLICATION_NOISE
                                and len(nn) >= 3
                                and not nn.isdigit()):
                            phrases.add(f"msci {nxt} {nn}")

        # 2. Product-term matches even without "msci" prefix
        title_joined = " ".join(toks)
        for term in MSCI_PRODUCT_TERMS:
            if " " in term:
                # multi-word product term
                if term in title_joined:
                    phrases.add(f"msci {term}" if not term.startswith("msci") else term)
            else:
                if term in toks and term not in PUBLICATION_NOISE:
                    # only interesting if article actually mentions MSCI
                    if "msci" in toks:
                        phrases.add(f"msci {term}")

        total_articles += 1
        is_recent = a["pub_date"] and a["pub_date"] >= recent_cutoff
        if is_recent:
            recent_articles += 1
        for p in phrases:
            total_counts[p] += 1
            if is_recent:
                recent_counts[p] += 1

    if not total_counts:
        return [], []

    # Top queries: by total frequency, capped at 10, normalised to 0..100
    most_common = total_counts.most_common(15)
    top_max = most_common[0][1] if most_common else 1
    top_queries = [
        {"query": q, "volume_index": int(round((c / top_max) * 100))}
        for q, c in most_common[:10]
    ]

    # Rising: compare recent-window rate vs prior-window rate
    prior_articles = max(1, total_articles - recent_articles)
    recent_articles_safe = max(1, recent_articles)
    rising_scores = []
    for q, tot in total_counts.items():
        rec = recent_counts.get(q, 0)
        if tot < 2:
            continue
        rec_rate = rec / recent_articles_safe
        prior = tot - rec
        prior_rate = prior / prior_articles
        if prior_rate == 0 and rec_rate > 0:
            rising_scores.append((q, float("inf"), rec))
        elif prior_rate > 0:
            growth = (rec_rate / prior_rate) - 1.0
            if growth > 0.15:
                rising_scores.append((q, growth, rec))
    rising_scores.sort(key=lambda x: (x[1] if x[1] != float("inf") else 10.0, x[2]), reverse=True)
    rising_queries = []
    for q, g, _ in rising_scores[:10]:
        label = "Breakout" if g == float("inf") or g > 4.0 else f"+{int(round(g * 100))}%"
        rising_queries.append({"query": q, "growth": label})

    return top_queries, rising_queries


def build_share_of_voice(articles_per_brand, days=90):
    """Daily % share of article volume per brand."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days - 1)

    per_day = defaultdict(lambda: Counter())
    for brand, arts in articles_per_brand.items():
        for a in arts:
            if a["pub_date"] is None:
                continue
            d = a["pub_date"].date()
            if start <= d <= end:
                per_day[d.isoformat()][brand] += 1

    rows = []
    for i in range(days):
        d = (start + timedelta(days=i)).isoformat()
        counts = per_day.get(d, Counter())
        total = sum(counts.values()) or 1
        row = {"date": d}
        for b in BRANDS:
            row[b] = round(counts.get(b, 0) / total * 100, 1)
        rows.append(row)
    return rows


def sov_summary(share_of_voice):
    """7-day SoV average per brand + delta vs prior 7 days + rank."""
    if not share_of_voice:
        return {}
    last_7 = share_of_voice[-7:]
    prior_7 = share_of_voice[-14:-7] if len(share_of_voice) >= 14 else share_of_voice[:7]
    summary = {}
    for b in BRANDS:
        avg = sum(r.get(b, 0) for r in last_7) / max(1, len(last_7))
        prior = sum(r.get(b, 0) for r in prior_7) / max(1, len(prior_7))
        summary[b] = {
            "sov_7d_pct": round(avg, 1),
            "sov_prior_7d_pct": round(prior, 1),
            "delta_pct_points": round(avg - prior, 1),
        }
    ranked = sorted(summary.items(), key=lambda kv: kv[1]["sov_7d_pct"], reverse=True)
    for i, (b, _) in enumerate(ranked):
        summary[b]["rank"] = i + 1
    return summary


# ──────────────────────────────────────────────────────────────────────────
# Sentiment — VADER
# ──────────────────────────────────────────────────────────────────────────
def compute_sentiment_daily(articles_per_brand, days=90):
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    except ImportError:
        log("vaderSentiment not installed — skipping sentiment", 1)
        return [], {}
    sia = SentimentIntensityAnalyzer()
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days - 1)

    # {date: {brand: [scores]}}
    bucket = defaultdict(lambda: defaultdict(list))
    for brand, arts in articles_per_brand.items():
        for a in arts:
            if a["pub_date"] is None:
                continue
            d = a["pub_date"].date()
            if start <= d <= end:
                text = (a["title"] + ". " + a.get("description", "")).strip(". ")
                if not text:
                    continue
                score = sia.polarity_scores(text)["compound"]
                bucket[d.isoformat()][brand].append(score)

    rows = []
    for i in range(days):
        d = (start + timedelta(days=i)).isoformat()
        row = {"date": d}
        for b in BRANDS:
            scores = bucket.get(d, {}).get(b, [])
            row[b] = round(sum(scores) / len(scores), 3) if scores else None
            row[f"{b}__n"] = len(scores)
        rows.append(row)

    # Summary: 7-day avg, 30-day avg, delta
    summary = {}
    for b in BRANDS:
        def avg_window(window):
            vals = [r[b] for r in window if r.get(b) is not None]
            return round(sum(vals) / len(vals), 3) if vals else None
        last_7 = rows[-7:]
        last_30 = rows[-30:]
        prior_30 = rows[-60:-30] if len(rows) >= 60 else rows[:30]
        n_7 = sum(r.get(f"{b}__n", 0) for r in last_7)
        n_30 = sum(r.get(f"{b}__n", 0) for r in last_30)
        a7 = avg_window(last_7); a30 = avg_window(last_30); ap30 = avg_window(prior_30)
        summary[b] = {
            "sentiment_7d": a7,
            "sentiment_30d": a30,
            "sentiment_prior_30d": ap30,
            "delta_30d_vs_prior": round((a30 - ap30), 3) if (a30 is not None and ap30 is not None) else None,
            "articles_7d": n_7,
            "articles_30d": n_30,
            "label_7d": sentiment_label(a7),
        }
    return rows, summary


def sentiment_label(score):
    if score is None:
        return "neutral"
    if score >= 0.25: return "very positive"
    if score >= 0.05: return "positive"
    if score <= -0.25: return "very negative"
    if score <= -0.05: return "negative"
    return "neutral"


# ──────────────────────────────────────────────────────────────────────────
# L3 — Google Autocomplete
# ──────────────────────────────────────────────────────────────────────────
def fetch_autocomplete(prefix: str) -> list[str]:
    url = f"http://suggestqueries.google.com/complete/search?client=firefox&q={urllib.parse.quote(prefix)}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and len(data) > 1 and isinstance(data[1], list):
            return [s for s in data[1] if isinstance(s, str)]
    except Exception:
        return []
    return []


def build_autocomplete(existing_suggestions: list[dict]) -> list[dict]:
    seen_before = {s["suggestion"].lower(): s for s in (existing_suggestions or [])}
    today = datetime.utcnow().date().isoformat()
    results = []
    collected = set()
    for prefix in AUTOCOMPLETE_PREFIXES:
        sugs = fetch_autocomplete(prefix)
        for s in sugs:
            key = s.lower()
            if key in collected:
                continue
            collected.add(key)
            prev = seen_before.get(key)
            first_seen = prev["first_seen"] if prev else today
            results.append({
                "suggestion": s,
                "prefix": prefix,
                "first_seen": first_seen,
                "is_new": prev is None,
            })
        time.sleep(0.25)
    # Filter to MSCI-relevant
    filtered = [
        r for r in results
        if "msci" in r["suggestion"].lower()
        and not any(p in r["suggestion"].lower()
                    for p in ["login", "sign in", "password"])
    ]
    # Sort: new first, then alphabetical
    filtered.sort(key=lambda r: (not r["is_new"], r["suggestion"].lower()))
    return filtered[:40]


# ──────────────────────────────────────────────────────────────────────────
# L4 — Market Footprint (yfinance)
# ──────────────────────────────────────────────────────────────────────────
def build_market_footprint():
    try:
        import yfinance as yf
    except ImportError:
        log("yfinance not installed — skipping market footprint", 1)
        return {}
    try:
        t = yf.Ticker(MSCI_TICKER)
        hist = t.history(period="90d", interval="1d", auto_adjust=True)
        if hist.empty:
            log("yfinance: empty MSCI history", 1)
            return {}
        close = hist["Close"]; vol = hist["Volume"]
        price = round(float(close.iloc[-1]), 2)
        def pct_change(n):
            if len(close) <= n: return None
            prev = float(close.iloc[-n - 1]); last = float(close.iloc[-1])
            return round((last - prev) / prev * 100, 2) if prev else None
        price_history = [
            {"date": d.strftime("%Y-%m-%d"), "close": round(float(v), 2)}
            for d, v in close.items()
        ]
        # Info (market cap, PE etc.) — resilient to missing fields
        info = {}
        try:
            raw = t.info or {}
            info = {
                "market_cap": raw.get("marketCap"),
                "pe": raw.get("trailingPE"),
                "dividend_yield": raw.get("dividendYield"),
                "52w_high": raw.get("fiftyTwoWeekHigh"),
                "52w_low": raw.get("fiftyTwoWeekLow"),
            }
        except Exception:
            pass

        etfs = []
        for ticker, name in MSCI_ETFS:
            try:
                et = yf.Ticker(ticker).history(period="90d", interval="1d", auto_adjust=True)
                if et.empty:
                    continue
                ec = et["Close"]
                e_price = round(float(ec.iloc[-1]), 2)
                def etf_pct(n):
                    if len(ec) <= n: return None
                    prev = float(ec.iloc[-n - 1]); last = float(ec.iloc[-1])
                    return round((last - prev) / prev * 100, 2) if prev else None
                etfs.append({
                    "ticker": ticker,
                    "name": name,
                    "price": e_price,
                    "change_1d_pct": etf_pct(1),
                    "change_7d_pct": etf_pct(5),
                    "change_30d_pct": etf_pct(21),
                    "change_90d_pct": etf_pct(min(63, len(ec) - 1)),
                })
                time.sleep(0.3)
            except Exception as ex:
                log(f"yfinance {ticker}: {str(ex)[:60]}", 2)

        return {
            "ticker": MSCI_TICKER,
            "price": price,
            "change_1d_pct": pct_change(1),
            "change_7d_pct": pct_change(5),
            "change_30d_pct": pct_change(21),
            "change_90d_pct": pct_change(min(63, len(close) - 1)),
            "volume_7d_avg": int(round(vol.tail(7).mean())) if len(vol) >= 7 else None,
            "info": info,
            "price_history": price_history,
            "etfs": etfs,
        }
    except Exception as ex:
        log(f"yfinance failed: {str(ex)[:80]}", 1)
        return {}


# ──────────────────────────────────────────────────────────────────────────
# Seeded baseline (L6)
# ──────────────────────────────────────────────────────────────────────────
def seed_baseline() -> dict:
    import random
    random.seed(42)
    end = datetime.utcnow().date() - timedelta(days=1)
    trend = []
    base = {"MSCI": 38, "S&P Global": 72, "FTSE Russell": 12,
            "Bloomberg Terminal": 58, "Morningstar": 33}
    for i in range(90):
        d = (end - timedelta(days=89 - i)).isoformat()
        row = {"date": d}
        for b, bv in base.items():
            day = (end - timedelta(days=89 - i)).weekday()
            weekly = 1.0 if day < 5 else 0.6
            jitter = random.uniform(-0.2, 0.2)
            row[b] = max(1, int(round(bv * weekly * (1 + jitter))))
        trend.append(row)
    top_queries = [
        {"query": "msci world", "volume_index": 100},
        {"query": "msci acwi", "volume_index": 82},
        {"query": "msci esg ratings", "volume_index": 71},
        {"query": "msci climate", "volume_index": 64},
        {"query": "msci emerging markets", "volume_index": 58},
        {"query": "msci eafe", "volume_index": 47},
        {"query": "msci private capital", "volume_index": 38},
        {"query": "msci analytics", "volume_index": 31},
        {"query": "msci barra", "volume_index": 24},
        {"query": "msci real estate", "volume_index": 19},
    ]
    rising_queries = [
        {"query": "msci climate action", "growth": "Breakout"},
        {"query": "msci private capital", "growth": "+240%"},
        {"query": "msci carbon metrics", "growth": "+180%"},
        {"query": "msci transition finance", "growth": "+150%"},
        {"query": "msci ai indexes", "growth": "+95%"},
        {"query": "msci biodiversity", "growth": "+70%"},
        {"query": "msci fabric", "growth": "+55%"},
    ]
    regional_interest = [
        {"country": "United States", "interest": 100},
        {"country": "United Kingdom", "interest": 74},
        {"country": "Switzerland", "interest": 61},
        {"country": "Germany", "interest": 58},
        {"country": "Singapore", "interest": 49},
        {"country": "Hong Kong", "interest": 46},
        {"country": "Japan", "interest": 42},
        {"country": "France", "interest": 38},
        {"country": "Netherlands", "interest": 33},
        {"country": "Canada", "interest": 30},
        {"country": "Australia", "interest": 27},
        {"country": "India", "interest": 22},
    ]
    latest = trend[-1] if trend else {}
    competitor_comparison = sorted(
        [{"brand": b, "interest": latest.get(b, 0)} for b in BRANDS],
        key=lambda x: x["interest"], reverse=True)

    # ── Seed sentiment_daily + summary so the Sentiment Pulse widget renders ──
    # Real values come from VADER on Google News headlines when the network
    # is available. Seed values are modest (near-neutral, slightly positive
    # for MSCI) so a live scraper run produces larger, clearly-different
    # numbers — no risk of baseline masking a real move.
    sentiment_daily = []
    sentiment_totals = {b: {"sum": 0.0, "n": 0} for b in BRANDS}
    sentiment_totals_30 = {b: {"sum": 0.0, "n": 0} for b in BRANDS}
    sentiment_totals_prior = {b: {"sum": 0.0, "n": 0} for b in BRANDS}
    base_sent = {"MSCI": 0.12, "S&P Global": 0.08, "FTSE Russell": 0.05,
                 "Bloomberg Terminal": 0.10, "Morningstar": 0.07}
    for i in range(90):
        d = (end - timedelta(days=89 - i)).isoformat()
        row = {"date": d}
        for b in BRANDS:
            score = round(base_sent[b] + random.uniform(-0.08, 0.08), 3)
            row[b] = score
            # Last 7 days
            if i >= 83:
                sentiment_totals[b]["sum"] += score
                sentiment_totals[b]["n"] += 1
            # Last 30 days
            if i >= 60:
                sentiment_totals_30[b]["sum"] += score
                sentiment_totals_30[b]["n"] += 1
            # Prior 30 days (days 30-59 back)
            if 30 <= i < 60:
                sentiment_totals_prior[b]["sum"] += score
                sentiment_totals_prior[b]["n"] += 1
        sentiment_daily.append(row)

    def _label(v):
        if v is None:
            return "n/a"
        return "positive" if v >= 0.05 else ("negative" if v <= -0.05 else "neutral")

    sentiment_summary = {}
    for b in BRANDS:
        n7 = sentiment_totals[b]["n"] or 1
        n30 = sentiment_totals_30[b]["n"] or 1
        n_prior = sentiment_totals_prior[b]["n"] or 1
        s7 = round(sentiment_totals[b]["sum"] / n7, 3)
        s30 = round(sentiment_totals_30[b]["sum"] / n30, 3)
        s_prior = sentiment_totals_prior[b]["sum"] / n_prior
        sentiment_summary[b] = {
            "sentiment_7d": s7,
            "sentiment_30d": s30,
            "delta_30d_vs_prior": round(s30 - s_prior, 3),
            "label_7d": _label(s7),
            "articles_7d": random.randint(8, 18),
            "articles_30d": random.randint(35, 70),
        }

    # ── Seed share_of_voice + summary ──
    # SoV is news-volume share. Real values come from Google News article
    # counts per brand. Seed values reflect typical article-count distribution
    # across these five brands.
    sov_weights = {"MSCI": 0.22, "S&P Global": 0.31, "FTSE Russell": 0.08,
                   "Bloomberg Terminal": 0.26, "Morningstar": 0.13}
    share_of_voice = []
    sov_7d_totals = {b: 0.0 for b in BRANDS}
    sov_prior_7d_totals = {b: 0.0 for b in BRANDS}
    for i in range(90):
        d = (end - timedelta(days=89 - i)).isoformat()
        row = {"date": d}
        raw = {}
        for b in BRANDS:
            raw[b] = max(0.01, sov_weights[b] + random.uniform(-0.04, 0.04))
        tot = sum(raw.values())
        for b in BRANDS:
            pct = round((raw[b] / tot) * 100, 2)
            row[b] = pct
            if i >= 83:
                sov_7d_totals[b] += pct
            if 76 <= i < 83:
                sov_prior_7d_totals[b] += pct
        share_of_voice.append(row)

    share_of_voice_summary = {}
    for b in BRANDS:
        sov_7d = round(sov_7d_totals[b] / 7, 2)
        sov_prior = sov_prior_7d_totals[b] / 7
        share_of_voice_summary[b] = {
            "sov_7d_pct": sov_7d,
            "delta_pct_points": round(sov_7d - sov_prior, 2),
            "rank": 0,  # filled after sort
        }
    # Assign ranks
    for rank, b in enumerate(
        sorted(share_of_voice_summary, key=lambda x: share_of_voice_summary[x]["sov_7d_pct"], reverse=True), start=1
    ):
        share_of_voice_summary[b]["rank"] = rank

    return {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "data_source": "seed_baseline",
        "brand_trend": trend,
        "top_queries": top_queries,
        "rising_queries": rising_queries,
        "regional_interest": regional_interest,
        "competitor_comparison": competitor_comparison,
        "sentiment_daily": sentiment_daily,
        "sentiment_summary": sentiment_summary,
        "share_of_voice": share_of_voice,
        "share_of_voice_summary": share_of_voice_summary,
        "market_footprint": {},
    }


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 72)
    print(f"  MSCI BRAND INTELLIGENCE SCRAPER — {datetime.now():%Y-%m-%d %H:%M}")
    print("=" * 72)

    existing = load_existing(OUTPUT_FILE) or {}

    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "data_source": "unknown",
        "sources_attempted": [],
        "brand_trend": [],
        "top_queries": [],
        "rising_queries": [],
        "regional_interest": [],
        "competitor_comparison": [],
        "sentiment_daily": [],
        "sentiment_summary": {},
        "share_of_voice": [],
        "share_of_voice_summary": {},
        "autocomplete_suggestions": [],
        "market_footprint": {},
    }

    # ── L1 Wikipedia ─────────────────────────────────────────────
    log("L1: Wikipedia Pageviews", 0)
    try:
        trend, src = build_brand_trend_from_wiki(days=90)
        output["sources_attempted"].append(f"wiki_trend:{src}")
        if trend:
            output["brand_trend"] = trend
            log(f"brand_trend: {len(trend)} days", 1)
    except Exception as ex:
        log(f"wiki trend failed: {ex}", 1)
    try:
        regions, src = build_regional_from_wiki(days=30)
        output["sources_attempted"].append(f"wiki_regions:{src}")
        if regions:
            output["regional_interest"] = regions
            log(f"regional_interest: {len(regions)}", 1)
    except Exception as ex:
        log(f"wiki regions failed: {ex}", 1)

    # ── L2 News RSS → queries, SoV, sentiment ─────────────────────
    log("L2: Google News RSS", 0)
    articles_per_brand = {}
    for brand in BRANDS:
        arts = fetch_news_rss(brand, window="90d")
        articles_per_brand[brand] = arts
        log(f"news {brand}: {len(arts)}", 1)
        time.sleep(0.5)

    msci_arts = articles_per_brand.get("MSCI", [])
    if msci_arts:
        top_q, rising_q = extract_msci_queries(msci_arts)
        if top_q:
            output["top_queries"] = top_q
            log(f"top_queries: {len(top_q)}", 1)
            output["sources_attempted"].append("news_queries:ok")
        if rising_q:
            output["rising_queries"] = rising_q
            log(f"rising_queries: {len(rising_q)}", 1)

    if any(articles_per_brand.values()):
        sov = build_share_of_voice(articles_per_brand, days=90)
        output["share_of_voice"] = sov
        output["share_of_voice_summary"] = sov_summary(sov)
        log(f"share_of_voice: {len(sov)} days", 1)
        output["sources_attempted"].append("share_of_voice:ok")

        sent_daily, sent_summary = compute_sentiment_daily(articles_per_brand, days=90)
        if sent_daily:
            output["sentiment_daily"] = sent_daily
            output["sentiment_summary"] = sent_summary
            log(f"sentiment_daily: {len(sent_daily)} days", 1)
            output["sources_attempted"].append("sentiment_vader:ok")

    # ── L3 Google Autocomplete ────────────────────────────────────
    log("L3: Google Autocomplete", 0)
    try:
        autocomplete = build_autocomplete(existing.get("autocomplete_suggestions") or [])
        output["autocomplete_suggestions"] = autocomplete
        log(f"autocomplete: {len(autocomplete)} suggestions ({sum(1 for a in autocomplete if a['is_new'])} new)", 1)
        output["sources_attempted"].append("autocomplete:ok")
    except Exception as ex:
        log(f"autocomplete failed: {ex}", 1)

    # If rising queries weren't produced from news, derive from autocomplete NEW items
    if not output["rising_queries"] and output["autocomplete_suggestions"]:
        new_items = [a for a in output["autocomplete_suggestions"] if a["is_new"]]
        if new_items:
            output["rising_queries"] = [
                {"query": a["suggestion"], "growth": "New"}
                for a in new_items[:10]
            ]
            log(f"rising_queries from autocomplete: {len(output['rising_queries'])}", 1)

    # ── L4 Market Footprint ───────────────────────────────────────
    log("L4: Market Footprint (yfinance)", 0)
    mf = build_market_footprint()
    if mf:
        output["market_footprint"] = mf
        log(f"market_footprint: {mf.get('ticker')} @ ${mf.get('price')} "
            f"({mf.get('change_1d_pct')}% 1d / {mf.get('change_30d_pct')}% 30d)", 1)
        output["sources_attempted"].append("yfinance:ok")

    # ── L5 pytrends (fallback only) ───────────────────────────────
    if not output["brand_trend"] or not output["top_queries"]:
        log("L5: pytrends (fallback)", 0)
        try:
            from pytrends.request import TrendReq
            pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 20), retries=1, backoff_factor=0.5)
            if not output["brand_trend"]:
                pytrends.build_payload(BRANDS, cat=0, timeframe="today 3-m", geo="")
                df = pytrends.interest_over_time()
                if not df.empty:
                    rows = []
                    for date, row in df.iterrows():
                        entry = {"date": date.strftime("%Y-%m-%d")}
                        for b in BRANDS:
                            if b in row: entry[b] = int(row[b])
                        rows.append(entry)
                    output["brand_trend"] = rows
                    output["sources_attempted"].append("pytrends_trend:ok")
        except Exception as ex:
            log(f"pytrends failed: {str(ex)[:80]}", 1)

    # ── Competitor snapshot ───────────────────────────────────────
    if output["brand_trend"]:
        latest = output["brand_trend"][-1]
        output["competitor_comparison"] = sorted(
            [{"brand": b, "interest": latest.get(b, 0)} for b in BRANDS],
            key=lambda x: x["interest"], reverse=True)

    # ── Data-source label ─────────────────────────────────────────
    tags = []
    if output["brand_trend"] and any("wiki_trend:wikipedia_pageviews" in s
                                      for s in output["sources_attempted"]):
        tags.append("wikipedia")
    if output["top_queries"] and any("news_queries:ok" in s
                                      for s in output["sources_attempted"]):
        tags.append("news")
    if output["sentiment_daily"]:
        tags.append("vader")
    if output["share_of_voice"]:
        tags.append("sov")
    if output["autocomplete_suggestions"]:
        tags.append("autocomplete")
    if output["market_footprint"]:
        tags.append("yfinance")
    if tags:
        output["data_source"] = "+".join(tags)
    else:
        log("All network sources failed — using seeded baseline", 0)
        base = seed_baseline()
        for k in ("brand_trend", "top_queries", "rising_queries",
                  "regional_interest", "competitor_comparison",
                  "sentiment_daily", "sentiment_summary",
                  "share_of_voice", "share_of_voice_summary"):
            if not output.get(k):
                output[k] = base[k]
        output["data_source"] = "seed_baseline"

    # Back-fill missing sections from seed to guarantee dashboard renders.
    # Sentiment + SoV + market_footprint are seeded too so the widgets never
    # show blank placeholders — live values replace seed values the moment
    # the scraper successfully hits Google News RSS / yfinance.
    base = None
    for key in ("brand_trend", "top_queries", "rising_queries",
                "regional_interest", "competitor_comparison",
                "sentiment_daily", "sentiment_summary",
                "share_of_voice", "share_of_voice_summary"):
        # dicts are "missing" when empty {} too, not just None
        val = output.get(key)
        is_empty = val in (None, [], {})
        if is_empty:
            if base is None: base = seed_baseline()
            output[key] = base[key]
            output["sources_attempted"].append(f"{key}:seeded")

    save_data(output, OUTPUT_FILE)

    print()
    print(f"  data_source       : {output['data_source']}")
    print(f"  brand_trend       : {len(output['brand_trend'])} days")
    print(f"  top_queries       : {len(output['top_queries'])}")
    print(f"  rising_queries    : {len(output['rising_queries'])}")
    print(f"  regional_interest : {len(output['regional_interest'])}")
    print(f"  competitors       : {len(output['competitor_comparison'])}")
    print(f"  sentiment_daily   : {len(output['sentiment_daily'])} days")
    print(f"  share_of_voice    : {len(output['share_of_voice'])} days")
    print(f"  autocomplete      : {len(output['autocomplete_suggestions'])} suggestions")
    print(f"  market_footprint  : {'yes' if output['market_footprint'] else 'no'}")
    print(f"  sources_attempted : {output['sources_attempted']}")
    print("=" * 72)


if __name__ == "__main__":
    main()
