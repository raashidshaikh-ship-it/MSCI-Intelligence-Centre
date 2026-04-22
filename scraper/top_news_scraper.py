"""
Top News Scraper — MSCI Competitor Intelligence (Finnhub + RSS edition)
========================================================================

Purpose
-------
Populate data/top_news.json with recent news about MSCI competitors, using
reliable, structured data sources instead of fragile HTML scraping.

Data sources
------------
1. Finnhub /company-news       (primary) — 60 req/min, 1-year history, JSON
2. Competitor press-release RSS feeds (secondary — product launches, direct PR)
3. Premium paywalled news      (tertiary) — WSJ, FT, Bloomberg, Barron's,
   MarketWatch, Reuters. Finnhub doesn't index these. Surfaced via two public
   RSS channels:
     3a. Google News RSS with site: filters (per-competitor targeting)
     3b. WSJ / MarketWatch section feeds (broad ingest, keyword-filtered to
         competitor-relevant items only)
4. Manual seeds                (data/manual_news_seeds.json) — analyst-curated
   articles for op-eds, internal press, or anything the above miss.

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
import urllib.parse
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
SEEDS_FILE = os.path.join(DATA_DIR, "manual_news_seeds.json")

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
# Ordered: the FIRST matching category wins, so put the most specific
# (and most valuable) buckets first and let fuzzier ones fall through.
# Taxonomy mirrors the front-end CATEGORIES array in index.html — keep
# the two in sync.
CATEGORY_KEYWORDS = {
    "M&A": [r"\bacqui(re|res|red|ring|sition)\b", r"\bmerger\b",
            r"\bagrees? to (buy|purchase|acquire)\b", r"\btakeover\b",
            r"\bto acquire\b", r"\bdivest(s|ed|iture)?\b",
            r"\bspin[- ]?off\b", r"\bstake\b.{0,25}\b(sold|bought|acquired)\b"],
    "Partnership": [r"\bpartner(s|ed|ship)?\b", r"\bcollaborat(e|es|ed|ion)\b",
                    r"\bjoint venture\b", r"\balliance\b", r"\bteams? up with\b",
                    r"\bmemorandum of understanding\b", r"\bMOU\b"],
    "ESG / Sustainability": [r"\bESG\b", r"\bsustainab(le|ility)\b",
                             r"\bimpact invest(ing|ment|or)?\b",
                             r"\bnet[- ]zero\b", r"\bgreen (bond|finance|fund)\b",
                             r"\bstewardship\b", r"\bbiodivers(ity|e)\b",
                             r"\btransition (finance|plan)\b"],
    "Climate / Physical Risk": [r"\bclimate\b", r"\bphysical risk\b",
                                r"\bdecarbon(ize|ise|ization)\b",
                                r"\bemissions\b", r"\bscope ?[123]\b",
                                r"\bTCFD\b", r"\bTNFD\b", r"\bCSRD\b"],
    "Indices / Benchmarks": [r"\b(new )?index\b", r"\bbenchmark\b",
                             r"\blaunches? .{0,40} index\b",
                             r"\bindex (family|series|rebalanc(e|ing))\b",
                             r"\brebalanc(e|ed|ing)\b", r"\bfactor (index|tilt)\b"],
    "ETF / Fund Flows": [r"\bETF\b", r"\binflows?\b", r"\boutflows?\b",
                         r"\bfund (launch|flow|inflow|outflow)\b",
                         r"\bassets under management\b", r"\bAUM\b"],
    "Private Assets / Alternatives": [r"\bprivate (equity|credit|markets|assets)\b",
                                      r"\balternatives\b", r"\bsecondaries\b",
                                      r"\binfrastructure fund\b", r"\bhedge fund\b",
                                      r"\bventure capital\b", r"\bLP/GP\b"],
    "Fixed Income / Credit": [r"\bfixed income\b", r"\bbond(s)?\b", r"\bcredit\b",
                              r"\byield curve\b", r"\bcoupon\b", r"\bsovereign\b",
                              r"\brating (upgrade|downgrade)\b"],
    "Risk / Analytics": [r"\brisk (model|analytics|management|engine)\b",
                         r"\bstress test\b", r"\bVaR\b", r"\bscenario analysis\b",
                         r"\bfactor model\b", r"\bexposure analytics\b"],
    "Data / Market Data": [r"\bmarket data\b", r"\bdata feed\b",
                           r"\bdata platform\b", r"\breference data\b",
                           r"\bdata product\b", r"\bAPI\b.{0,20}\bdata\b"],
    "Technology / AI": [r"\bAI\b", r"\bartificial intelligence\b",
                        r"\bmachine learning\b", r"\bgenerative AI\b",
                        r"\bLLM\b", r"\bcopilot\b", r"\bautomation\b",
                        r"\bcloud (migration|platform)\b", r"\bcopilot\b"],
    "Cybersecurity / Outage": [r"\bcyber(attack|security|incident)?\b",
                               r"\bdata breach\b", r"\bransomware\b",
                               r"\boutage\b", r"\bdowntime\b",
                               r"\bservice disruption\b"],
    "Regulatory": [r"\bSEC\b", r"\bESMA\b", r"\bregulator(s|y)?\b",
                   r"\bfine(d|s)?\b", r"\bpenalt(y|ies)\b", r"\bprobe\b",
                   r"\bantitrust\b", r"\bcompliance\b", r"\bCFTC\b",
                   r"\bFCA\b", r"\brule (change|proposal)\b"],
    "Legal / Litigation": [r"\blawsuit\b", r"\blitigation\b", r"\bsued?\b",
                           r"\bsettle(ment|s|d)?\b", r"\bcourt\b",
                           r"\bjudg(e|ment)\b", r"\bplea(d|ded)?\b"],
    "Earnings": [r"\bQ[1-4]\b", r"\b(quarterly|annual) (earnings|results)\b",
                 r"\breports? (results|earnings)\b", r"\bfiscal (year|quarter)\b",
                 r"\bguidance\b", r"\bEPS\b", r"\brevenue beat\b",
                 r"\bprofit (rose|fell|up|down)\b"],
    "Client Wins / Market Share": [r"\bselects?\b.{0,40}\b(as|for)\b",
                                   r"\bwins? (mandate|contract|deal)\b",
                                   r"\bmarket share\b", r"\brenews?\b.{0,40}\bcontract\b",
                                   r"\badopts?\b.{0,40}\b(index|platform|service)\b"],
    "Leadership": [r"\bappoint(s|ed|ment)?\b", r"\bnames?.{1,40}(CEO|CFO|CTO|COO|president|chair)\b",
                   r"\bsteps? down\b", r"\bresign(s|ed|ation)?\b",
                   r"\bhires?\b", r"\bpromot(e|ed|ion)\b",
                   r"\bsuccession\b", r"\bdeparts?\b"],
    "Product Launch": [r"\blaunch(es|ed|ing)?\b", r"\bunveil(s|ed|ing)?\b",
                       r"\bdebut(s|ed)?\b", r"\bintroduc(e|es|ed|ing)\b",
                       r"\brolls? out\b", r"\bnew (index|product|platform|service)\b",
                       r"\bgoes? live\b", r"\bgeneral availability\b"],
    "Macro / Markets": [r"\binflation\b", r"\bdeflation\b", r"\brate (cut|hike|decision)\b",
                       r"\b(fed|ECB|BoE|BoJ)\b", r"\brecession\b",
                       r"\bGDP\b", r"\bgeopolitical\b", r"\bsanctions?\b"],
    "Strategy": [r"\bstrategy\b", r"\bstrategic (plan|priorit(y|ies)|review)\b",
                 r"\bexpand(s|ed|ing|sion)?\b",
                 r"\brestructur(e|es|ed|ing)\b", r"\brebrand\b",
                 r"\blong[- ]term (plan|target)\b"],
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


# ──────────────────────────────────────────────────────────────────────────
# MSCI canonical taxonomy — Product Line + Client Segment
# Source: msci.com "Our Solutions" + "Who We Serve" pages. Keep this list in
# sync with PRODUCT_LINES / SEGMENTS in index.html.
# ──────────────────────────────────────────────────────────────────────────
PRODUCT_LINE_KEYWORDS = {
    "Index":           [r"\bindex(es|ing)?\b", r"\bbenchmark", r"\bETF\b",
                        r"\bfactor\b", r"\brebalanc", r"\bindex licensing\b"],
    "Analytics":       [r"\banalytics?\b", r"\brisk model", r"\bfactor model",
                        r"\bbarra\b", r"\brisk(metrics)?\b", r"\bscenario\b",
                        r"\bstress test", r"\bVaR\b", r"\bperformance attribution"],
    "Sustainability":  [r"\bESG\b", r"\bsustainab", r"\bstewardship",
                        r"\bimpact invest", r"\btransition (finance|plan)",
                        r"\bSFDR\b", r"\bCSRD\b"],
    "Climate":         [r"\bclimate\b", r"\bnet[- ]zero\b", r"\bdecarbon",
                        r"\bemissions?\b", r"\bscope ?[123]\b", r"\bTCFD\b",
                        r"\bTNFD\b", r"\bphysical risk", r"\btransition risk"],
    "Private Capital": [r"\bprivate (equity|credit|markets|capital|assets)\b",
                        r"\balternatives?\b", r"\bsecondaries\b",
                        r"\bventure capital\b", r"\bLP/GP\b", r"\bhedge fund\b",
                        r"\bBurgiss\b"],
    "Real Assets":     [r"\breal estate\b", r"\bREIT\b", r"\binfrastructure\b",
                        r"\bproperty (market|investor)", r"\bMSCI[- ]IPD\b",
                        r"\breal asset"],
    "Wealth":          [r"\bwealth (management|manager|advisor|platform)\b",
                        r"\bfinancial advisor", r"\bSMA\b",
                        r"\bmodel portfolio", r"\bdirect indexing\b"],
}

SEGMENT_KEYWORDS = {
    "Asset Managers":          [r"\basset manager", r"\bfund manager",
                                r"\bmutual fund\b", r"\bBlackRock\b",
                                r"\bVanguard\b", r"\bState Street\b",
                                r"\bFidelity\b", r"\bInvesco\b",
                                r"\bSchwab\b", r"\bAmundi\b"],
    "Asset Owners":            [r"\basset owner", r"\bpension (fund|plan)",
                                r"\bsovereign wealth", r"\bendowment\b",
                                r"\bfoundation\b", r"\bCalPERS\b",
                                r"\bCPP(IB)?\b", r"\bGIC\b", r"\bNorges\b",
                                r"\bADIA\b"],
    "Hedge Funds":             [r"\bhedge fund", r"\bmulti[- ]strategy\b",
                                r"\bCitadel\b", r"\bMillennium\b",
                                r"\bBridgewater\b", r"\bPoint72\b",
                                r"\bElliott\b", r"\bBalyasny\b"],
    "Wealth Managers":         [r"\bwealth manager", r"\bprivate bank",
                                r"\bfinancial advisor", r"\bRIA\b",
                                r"\bMorgan Stanley Wealth", r"\bUBS\b",
                                r"\bRaymond James\b", r"\bEdward Jones\b"],
    "Banks":                   [r"\bbank\b", r"\bJPMorgan\b",
                                r"\bGoldman( Sachs)?\b", r"\bCitigroup\b",
                                r"\bDeutsche Bank\b", r"\bBarclays\b",
                                r"\bHSBC\b", r"\bBNP Paribas\b",
                                r"\bWells Fargo\b"],
    "Insurance":               [r"\binsurance\b", r"\binsurer\b",
                                r"\bAllianz\b", r"\bAXA\b", r"\bMetLife\b",
                                r"\bPrudential\b", r"\bAIG\b",
                                r"\bMunich Re\b", r"\bSwiss Re\b",
                                r"\breinsur"],
    "Corporates":              [r"\bcorporat(e|ion)\b", r"\bissuer\b",
                                r"\bCFO\b", r"\btreasur(y|er)\b",
                                r"\bcompany disclosure"],
    "Private Market Sponsors": [r"\bprivate equity (firm|sponsor|fund)",
                                r"\bKKR\b", r"\bBlackstone\b",
                                r"\bApollo( Global)?\b", r"\bCarlyle\b",
                                r"\bBain Capital\b", r"\bBrookfield\b",
                                r"\bGP stake", r"\bgeneral partner"],
}


def classify_product_line(text: str) -> str:
    for pl, patterns in PRODUCT_LINE_KEYWORDS.items():
        for pat in patterns:
            if re.search(pat, text, flags=re.IGNORECASE):
                return pl
    return "Non-core"


def classify_segments(text: str) -> list[str]:
    hits: list[str] = []
    for seg, patterns in SEGMENT_KEYWORDS.items():
        for pat in patterns:
            if re.search(pat, text, flags=re.IGNORECASE):
                hits.append(seg)
                break
    return hits or ["Non-core"]


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
    cat = article.get("category")
    if cat in ("M&A", "Product Launch", "Client Wins / Market Share"):
        score += 25
    elif cat in ("Earnings", "Regulatory", "Legal / Litigation",
                 "Indices / Benchmarks", "ESG / Sustainability",
                 "Cybersecurity / Outage"):
        score += 15
    elif cat in ("Technology / AI", "Risk / Analytics",
                 "Private Assets / Alternatives", "ETF / Fund Flows"):
        score += 10
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
# Deterministic narrative enrichment — gives readers a 20-sec "read"
# without needing an LLM
# ──────────────────────────────────────────────────────────────────────────
_CATEGORY_WHY = {
    "M&A":                          "Deal activity reshapes the {segment} competitive landscape.",
    "Product Launch":               "New {segment} offering — potential pressure on MSCI's roadmap.",
    "Partnership":                  "Strategic alliance may alter distribution or data access in {segment}.",
    "Earnings":                     "Financial print signals competitor health and client-spend implications.",
    "Regulatory":                   "Regulatory development affecting {segment} market structure.",
    "Legal / Litigation":           "Litigation risk — may constrain {competitor}'s near-term posture.",
    "Leadership":                   "Key personnel change may shift strategic direction.",
    "Strategy":                     "Strategic move by {competitor} — watch for follow-through.",
    "Technology / AI":              "Tech / AI move — potential differentiation versus MSCI analytics.",
    "ESG / Sustainability":         "ESG product or policy move — direct overlap with MSCI ESG franchise.",
    "Climate / Physical Risk":      "Climate / physical-risk development — relevant to MSCI Climate Lab.",
    "Indices / Benchmarks":         "Benchmark move — watch for licensing / AUM impact on index franchise.",
    "ETF / Fund Flows":             "ETF / fund-flow signal — read-through for index licensing revenue.",
    "Private Assets / Alternatives":"Private-markets move — adjacent to MSCI's Burgiss/PrivatePulse stack.",
    "Fixed Income / Credit":        "Credit / fixed-income development — read across to credit analytics demand.",
    "Risk / Analytics":             "Risk-analytics news — direct competitive adjacency to RiskMetrics / BarraOne.",
    "Data / Market Data":           "Market-data move — pricing or distribution implications for data vendors.",
    "Client Wins / Market Share":   "Mandate / win signal — possible displacement risk in {segment}.",
    "Cybersecurity / Outage":       "Operational / security incident — reputational and client-trust exposure.",
    "Macro / Markets":              "Macro backdrop — shapes client risk appetite and spend cycle.",
    "General":                      "News mention involving {competitor} in {segment}.",
}


def derive_signals(article: dict) -> list[str]:
    """Structured tags — compact chips rendered next to the headline."""
    tags: list[str] = []
    text = f"{article.get('title', '')} {article.get('summary', '')}".lower()
    cat = article.get("category", "General")

    if "msci" in text:
        tags.append("Mentions MSCI")
    if any(k in text for k in ("index", "benchmark", "etf")):
        tags.append("Index / ETF")
    if any(k in text for k in ("esg", "sustainab", "climate", "net zero")):
        tags.append("ESG / Climate")
    if cat not in ("General", "Strategy"):
        tags.append(cat)

    pub = article.get("published")
    if pub:
        try:
            dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
            age_days = (datetime.now(timezone.utc) - dt).days
            if age_days <= 1:
                tags.append("Breaking (<24h)")
            elif age_days <= 3:
                tags.append("Recent (<3d)")
        except Exception:
            pass

    if article.get("importance_score", 0) >= 75:
        tags.append("High priority")

    # Dedupe while preserving order
    seen = set()
    out = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def derive_why_it_matters(article: dict) -> str:
    """One-sentence so-what, deterministic and safe for compliance."""
    cat = article.get("category", "General")
    segment = article.get("segment") or "the sector"
    competitor = article.get("competitor") or "the company"
    base = _CATEGORY_WHY.get(cat, _CATEGORY_WHY["General"]).format(
        segment=segment, competitor=competitor,
    )
    text = f"{article.get('title','')} {article.get('summary','')}".lower()
    extras = []
    if "msci" in text:
        extras.append("Article specifically references MSCI.")
    if any(k in text for k in ("index", "benchmark")) and cat != "Product Launch":
        extras.append("Index/benchmark angle present.")
    if extras:
        return (base + " " + " ".join(extras)).strip()
    return base


def derive_key_points(summary: str, max_points: int = 3) -> list[str]:
    """Naive sentence splitter — gives 2-3 tight bullets for a quick read."""
    if not summary:
        return []
    sentences = re.split(r"(?<=[.!?])\s+", summary.strip())
    points = [s.strip() for s in sentences if len(s.split()) >= 6]
    return points[:max_points]


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
            "related": (it.get("related") or "").strip(),
            "finnhub_category": (it.get("category") or "").strip(),
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

        # Best-effort image extraction from common RSS/Atom patterns
        image = ""
        thumb_el = it.find("thumbnail")
        if thumb_el is not None:
            image = thumb_el.get("url") or (thumb_el.text or "").strip()
        if not image:
            content_el = it.find("content")
            if content_el is not None and content_el.get("url"):
                image = content_el.get("url")
        if not image:
            enc_el = it.find("enclosure")
            if enc_el is not None and (enc_el.get("type") or "").startswith("image/"):
                image = enc_el.get("url") or ""

        out.append({
            "title": title,
            "url": url_,
            "source": competitor,  # RSS from the company itself
            "summary": summary,
            "published": published,
            "image": image,
            "competitor": competitor,
            "ticker": ticker,
            "segment": segment,
            "origin": "rss",
            "related": "",
            "finnhub_category": "",
        })
    return out


# ──────────────────────────────────────────────────────────────────────────
# Premium news (paywalled publishers) — WSJ, FT, Barron's, Bloomberg, Reuters
# Finnhub /company-news doesn't index these. We surface them via two public
# RSS channels that require no login:
#   (1) Google News RSS with site: filters — per-competitor targeting
#   (2) WSJ / MarketWatch section feeds — broad, then keyword-filtered
# Both return headline + dek + published date, which is what the dashboard
# classifier and Why-It-Matters engine need. Full-text stays behind the
# paywall (user clicks through to read).
# ──────────────────────────────────────────────────────────────────────────

# Keyword → (canonical competitor name, ticker, default segment). Used to
# re-attribute articles pulled from section feeds that don't carry a ticker.
# Order matters: we match the first key found in the text, so put the most
# specific (multi-word) keys before single-word aliases.
HIGH_SIGNAL_BRANDS: list[tuple[str, tuple[str, str, str]]] = [
    ("aladdin wealth",   ("BlackRock",            "BLK",     "Asset Managers")),
    ("aladdin copilot",  ("BlackRock",            "BLK",     "Asset Managers")),
    ("aladdin",          ("BlackRock",            "BLK",     "Asset Managers")),
    ("blackrock",        ("BlackRock",            "BLK",     "Asset Managers")),
    ("state street",     ("State Street",         "STT",     "Asset Managers")),
    ("charles river",    ("State Street",         "STT",     "Asset Managers")),
    ("s&p global",       ("S&P Global",           "SPGI",    "Indices / Ratings / Data")),
    ("s&p dow jones",    ("S&P Global",           "SPGI",    "Indices / Ratings / Data")),
    ("ftse russell",     ("LSEG (FTSE Russell)",  "LSEG.L",  "Indices / Benchmarking / Data")),
    ("lseg",             ("LSEG (FTSE Russell)",  "LSEG.L",  "Indices / Benchmarking / Data")),
    ("london stock exchange", ("LSEG (FTSE Russell)", "LSEG.L", "Indices / Benchmarking / Data")),
    ("morningstar",      ("Morningstar",          "MORN",    "Research / ESG / Ratings")),
    ("bloomberg terminal", ("Bloomberg",          "",        "Data / Analytics")),
    ("bloomberg lp",     ("Bloomberg",            "",        "Data / Analytics")),
    ("factset",          ("FactSet",              "FDS",     "Analytics / Data Platform")),
    ("moody's",          ("Moody's",              "MCO",     "Ratings / Risk Analytics")),
    ("moodys",           ("Moody's",              "MCO",     "Ratings / Risk Analytics")),
    ("nasdaq",           ("Nasdaq",               "NDAQ",    "Exchanges / Index Licensing")),
    ("intercontinental exchange", ("Intercontinental Exchange", "ICE", "Exchanges / Data / Indices")),
    ("cme group",        ("CME Group",            "CME",     "Derivatives / Indices")),
    ("preqin",           ("BlackRock",            "BLK",     "Private Market Sponsors")),
    ("burgiss",          ("MSCI (industry mention)", "MSCI", "Private Market Sponsors")),
    ("riskmetrics",      ("MSCI (industry mention)", "MSCI", "Risk / Analytics")),
    ("barraone",         ("MSCI (industry mention)", "MSCI", "Risk / Analytics")),
    ("barra",            ("MSCI (industry mention)", "MSCI", "Risk / Analytics")),
    ("msci",             ("MSCI (industry mention)", "MSCI", "Indices")),
]


def _google_news_search_url(query: str) -> str:
    """Build a Google News RSS search URL for the given query."""
    return (
        "https://news.google.com/rss/search?"
        f"q={urllib.parse.quote(query)}&hl=en-US&gl=US&ceid=US:en"
    )


def _clean_google_news_title(title: str) -> tuple[str, str]:
    """Google News appends ' - Publisher' to every title. Split it off and
    return (clean_title, publisher_suffix)."""
    m = re.search(r"^(.*?)\s+[-–—]\s+([^-–—]+)$", title or "")
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return (title or "").strip(), ""


def _match_brand(text: str) -> tuple[str, str, str] | None:
    """Return (competitor, ticker, segment) for the first high-signal brand
    whose keyword appears in text. Used to re-attribute section-feed items."""
    low = (text or "").lower()
    for kw, meta in HIGH_SIGNAL_BRANDS:
        if kw in low:
            return meta
    return None


def fetch_premium_news_for_competitor(
    competitor: str, ticker: str, segment: str,
    sites: list[str], max_items: int,
) -> list[dict]:
    """Query Google News RSS filtered to paywalled publishers.

    Returns articles from WSJ, FT, Bloomberg, Barron's, MarketWatch, Reuters
    that mention the competitor by name. Google News provides headline + dek
    + published date for free even when the target article is paywalled —
    enough to feed our classifier and show an analyst-useful row in the
    dashboard. Clicking through opens the paywalled article in a new tab.
    """
    if not sites:
        return []
    site_filter = " OR ".join(f"site:{d}" for d in sites)
    # Quote the competitor name so Google treats it as a phrase
    query = f'"{competitor}" ({site_filter})'
    url = _google_news_search_url(query)

    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]

    out = []
    for it in (root.findall(".//item") or [])[:max_items]:
        title_el = it.find("title")
        link_el = it.find("link")
        desc_el = it.find("description") or it.find("summary")
        date_el = it.find("pubDate") or it.find("published")
        source_el = it.find("source")

        raw_title = (title_el.text or "").strip() if title_el is not None else ""
        url_ = (link_el.text or "").strip() if link_el is not None else ""
        if not raw_title or not url_:
            continue

        title, publisher_from_title = _clean_google_news_title(raw_title)
        source_name = publisher_from_title
        if source_el is not None and (source_el.text or "").strip():
            source_name = source_el.text.strip()

        summary_html = desc_el.text or "" if desc_el is not None else ""
        summary = re.sub(r"<[^>]+>", " ", summary_html)
        summary = re.sub(r"\s+", " ", summary).strip()[:500]

        published = _parse_rss_date(date_el.text if date_el is not None else "")

        out.append({
            "title": title,
            "url": url_,
            "source": source_name or "Google News",
            "summary": summary,
            "published": published,
            "image": "",
            "competitor": competitor,
            "ticker": ticker,
            "segment": segment,
            "origin": "premium_google_news",
            "related": ticker or "",
            "finnhub_category": "",
        })
    return out


def fetch_section_feed_filtered(
    feed_url: str, feed_name: str, max_items: int,
) -> list[dict]:
    """Fetch a WSJ/MarketWatch section feed and keep only items whose headline
    or summary mentions a known competitor, MSCI product, or industry-relevant
    brand. Catches editorial/CIO-Journal coverage (e.g. 'Inside BlackRock's AI
    Transformation') that isn't surfaced by per-company queries.
    """
    resp = requests.get(feed_url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]

    out = []
    for it in (root.findall(".//item") or [])[:max_items]:
        title_el = it.find("title")
        link_el = it.find("link")
        desc_el = it.find("description") or it.find("summary")
        date_el = it.find("pubDate") or it.find("published")

        title = (title_el.text or "").strip() if title_el is not None else ""
        # Atom <link href=...> vs RSS <link>url</link>
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

        haystack = f"{title} {summary}"
        match = _match_brand(haystack)
        if not match:
            continue  # Not competitor-relevant — drop.

        competitor_name, ticker, segment = match
        published = _parse_rss_date(date_el.text if date_el is not None else "")

        out.append({
            "title": title,
            "url": url_,
            "source": feed_name,
            "summary": summary,
            "published": published,
            "image": "",
            "competitor": competitor_name,
            "ticker": ticker,
            "segment": segment,
            "origin": "premium_section_feed",
            "related": ticker or "",
            "finnhub_category": "",
        })
    return out


# ──────────────────────────────────────────────────────────────────────────
# Dedupe + enrich
# ──────────────────────────────────────────────────────────────────────────
def _normalize_title_for_dedupe(title: str) -> str:
    """Strip Google News publisher suffixes and punctuation so the same story
    hashes to the same ID whether it came from Finnhub, Google News redirect,
    WSJ section feed, or a manual seed.
    """
    t = (title or "").strip()
    # Drop trailing " - Publisher" that Google News appends to every headline
    t = re.sub(r"\s+[-–—]\s+[^-–—]+$", "", t).strip()
    t = t.lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def article_id(article: dict) -> str:
    # Prefer title-based dedupe when we have a substantive title. Same story
    # reaches us via multiple URLs (direct publisher + Google News redirect +
    # syndicated copy); titles stay stable across them.
    title_norm = _normalize_title_for_dedupe(article.get("title", ""))
    if len(title_norm) >= 30:
        key = title_norm
    else:
        key = (article.get("url", "") or title_norm).strip().lower()
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
    # MSCI canonical taxonomy — derived from title+summary so front-end
    # can skip client-side re-classification when these are already populated.
    article["product_line"] = classify_product_line(text)
    article["segments"] = classify_segments(text)
    article["non_core"] = (
        article["product_line"] == "Non-core"
        and article["segments"] == ["Non-core"]
    )
    score = importance_score(article)
    article["importance_score"] = score
    article["importance"] = score_to_label(score)
    article["impact_level"] = score_to_label(score)  # dashboard reads both fields
    article["id"] = article_id(article)
    # Narrative enrichment (for the 20-sec read)
    article["signals"] = derive_signals(article)
    article["why_it_matters"] = derive_why_it_matters(article)
    article["key_points"] = derive_key_points(article.get("summary", ""))
    # Approximate reading time (180 wpm baseline)
    words = len((article.get("summary") or "").split())
    article["reading_time"] = f"{max(1, round(words / 180))} min read" if words else "<1 min read"
    return article


def _richness_score(a: dict) -> int:
    """Higher = richer article. Used to pick the best copy when the same
    story arrives from multiple sources."""
    score = 0
    if a.get("published"):
        score += 4
    if a.get("image"):
        score += 3
    url = (a.get("url") or "").lower()
    # Direct publisher URL beats a Google News redirect every time —
    # the redirect URL expires and shows a Google chrome on hover.
    if url and "news.google.com" not in url:
        score += 3
    if len(a.get("summary") or "") > 150:
        score += 2
    elif len(a.get("summary") or "") > 60:
        score += 1
    # Manual seeds carry analyst context — slight tiebreak preference.
    if a.get("origin") == "manual":
        score += 2
    return score


def dedupe(articles: list[dict]) -> list[dict]:
    seen: dict[str, dict] = {}
    for a in articles:
        aid = a.get("id") or article_id(a)
        a["id"] = aid
        prev = seen.get(aid)
        if prev is None:
            seen[aid] = a
        else:
            # Keep the richest copy. Merge "related" tickers so we don't lose
            # Finnhub's cross-references when the richer copy came from elsewhere.
            winner = a if _richness_score(a) > _richness_score(prev) else prev
            loser = prev if winner is a else a
            merged_related = ",".join(sorted({
                t.strip() for t in (
                    (winner.get("related") or "").split(",")
                    + (loser.get("related") or "").split(",")
                ) if t.strip()
            }))
            if merged_related:
                winner["related"] = merged_related
            seen[aid] = winner
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


def load_manual_seeds() -> list[dict]:
    """Load analyst-curated articles from data/manual_news_seeds.json.

    These are articles Finnhub won't pick up — op-eds, trade-press coverage,
    direct-from-competitor press releases, internal flags. They flow through
    the same enrich() + dedupe() pipeline so they show up in the dashboard
    with proper taxonomy tags. Safe to call even if the seeds file is missing
    or malformed — returns [] and the scraper continues.
    """
    if not os.path.exists(SEEDS_FILE):
        return []
    try:
        with open(SEEDS_FILE, "r", encoding="utf-8") as f:
            payload = json.load(f) or {}
        seeds = payload.get("articles") or []
        out = []
        for s in seeds:
            # Require the minimum fields the downstream pipeline expects.
            if not (s.get("title") and s.get("url")):
                continue
            # Fill in defaults so enrich() doesn't choke.
            out.append({
                "title": s.get("title", "").strip(),
                "url": s.get("url", "").strip(),
                "source": s.get("source") or "Manual",
                "summary": (s.get("summary") or "").strip()[:500],
                "published": s.get("published") or "",
                "image": s.get("image") or "",
                "competitor": s.get("competitor") or "Unknown",
                "ticker": s.get("ticker") or "",
                "segment": s.get("segment") or "",
                "origin": s.get("origin") or "manual",
                "related": s.get("related") or "",
                "finnhub_category": s.get("finnhub_category") or "",
            })
        return out
    except Exception as e:
        print(f"[warn] Could not load manual seeds: {e}")
        return []


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

    # ──────────────────────────────────────────────────────────────────
    # 3. Premium news (paywalled publishers Finnhub misses) — WSJ, FT,
    # Bloomberg, Barron's, MarketWatch, Reuters. Gated behind the
    # premium_news.enabled flag in config/sources.json.
    # ──────────────────────────────────────────────────────────────────
    premium_cfg = cfg.get("premium_news") or {}
    if premium_cfg.get("enabled"):
        sites = premium_cfg.get("sites") or []
        per_comp_cap = int(premium_cfg.get("per_competitor_cap", 5))

        # 3a. Per-competitor Google News site-filtered queries
        for comp in cfg.get("competitors", []):
            name = comp.get("name") or "Unknown"
            ticker = comp.get("ticker") or ""
            segment = comp.get("segment") or ""
            src_name = f"Premium-{name}"
            try:
                items = fetch_premium_news_for_competitor(
                    name, ticker, segment, sites, per_comp_cap,
                )
                all_articles.extend(items)
                statuses.append({"name": src_name, "status": "ok", "items": len(items)})
                print(f"[ok] {src_name}: {len(items)} articles")
            except Exception as e:
                statuses.append({"name": src_name, "status": "error",
                                 "error": str(e)[:200], "items": 0})
                print(f"[err] {src_name}: {e}")
            time.sleep(0.6)  # Google News is lenient but be polite

        # 3b. WSJ / MarketWatch section feeds — broad ingest, then
        # keyword-filter to competitor-relevant items only.
        section_cap = int(premium_cfg.get("section_feed_max_items", 40))
        for feed in premium_cfg.get("section_feeds", []) or []:
            feed_url = feed.get("url") or ""
            feed_name = feed.get("name") or feed_url
            if not feed_url:
                continue
            src_name = f"Section-{feed_name}"
            try:
                items = fetch_section_feed_filtered(feed_url, feed_name, section_cap)
                all_articles.extend(items)
                statuses.append({"name": src_name, "status": "ok", "items": len(items)})
                print(f"[ok] {src_name}: {len(items)} articles (after relevance filter)")
            except Exception as e:
                statuses.append({"name": src_name, "status": "error",
                                 "error": str(e)[:200], "items": 0})
                print(f"[err] {src_name}: {e}")

    # Merge analyst-curated seeds (WSJ op-eds, direct press releases, etc.)
    # before enrichment so they flow through the same classifier and appear
    # in the dashboard alongside Finnhub/RSS results.
    manual = load_manual_seeds()
    if manual:
        all_articles.extend(manual)
        statuses.append({"name": "Manual-seeds", "status": "ok", "items": len(manual)})
        print(f"[ok] Manual-seeds: {len(manual)} articles")

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
