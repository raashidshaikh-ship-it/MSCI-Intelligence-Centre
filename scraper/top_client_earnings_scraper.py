# Top Client Earnings Scraper
# ===============================================================
# Tracks quarterly earnings of MSCI's top 12 CLIENTS (firms that
# purchase MSCI indexes, analytics, ESG, and private-markets data).
#
# Client roster (from MSCI Client Insights & Analytics Q4'25 deck):
#   Traditional Asset & Wealth Managers (7)
#     BlackRock, State Street, Aberdeen, Amundi,
#     Goldman Sachs, J.P. Morgan, UBS
#   Private Equity & Alternative Managers (5)
#     KKR, Blackstone, Carlyle, Apollo, Brookfield
#
# Pulls from: company IR press releases, Google News RSS, earnings
# call keyword mining, SEC EDGAR facts (US-listed only). Optional
# MSCI run-rate / client-revenue fields remain null until authored
# source is provided.
#
# Writes to: data/top_client_earnings.json
# Run:       python scraper/top_client_earnings_scraper.py
# Deps:      pip install requests beautifulsoup4 lxml python-dateutil

import json, os, re, time, hashlib
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

try:
    from dateutil import parser as dateparser
except ImportError:
    dateparser = None

# ─── Paths ───────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "top_client_earnings.json")

# ─── HTTP headers ────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
SEC_USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "MSCI Competitor Intel raashid.shaikh@msci.com",
)

# ─── Client roster ───────────────────────────────────────────────
# Each client: name, ticker, category, CIK (if US-listed), IR URL,
# Google News query, home country, AUM bucket labels.
CLIENTS = {
    # ── Traditional Asset & Wealth Managers ──
    "BlackRock": {
        "name": "BlackRock", "ticker": "BLK", "cik": "0001364742",
        "category": "Traditional", "country": "US",
        "ir_url": "https://ir.blackrock.com/news-and-events/press-releases",
        "news_query": "BlackRock earnings AUM quarterly results",
        "aum_buckets": ["equity", "fixed_income", "multi_asset", "alternatives", "cash_other"],
    },
    "State Street": {
        "name": "State Street", "ticker": "STT", "cik": "0000093751",
        "category": "Traditional", "country": "US",
        "ir_url": "https://investors.statestreet.com/investors/news-releases/default.aspx",
        "news_query": "State Street earnings AUC AUM quarterly results",
        "aum_buckets": ["equity", "fixed_income", "multi_asset", "alternatives", "cash_other"],
    },
    "Aberdeen": {
        "name": "Aberdeen", "ticker": "ABDN.L", "cik": None,
        "category": "Traditional", "country": "UK",
        "ir_url": "https://www.aberdeeninvestments.com/en-gb/investor/results-reports-and-presentations",
        "news_query": "Aberdeen abrdn AUMA net flows half year results",
        "aum_buckets": ["investments", "adviser", "ii_wealth", "insurance_partners", "other"],
    },
    "Amundi": {
        "name": "Amundi", "ticker": "AMUN.PA", "cik": None,
        "category": "Traditional", "country": "France",
        "ir_url": "https://about.amundi.com/Investor-relations/Results",
        "news_query": "Amundi earnings AUM net inflows quarterly",
        "aum_buckets": ["equity", "bonds", "multi_asset", "treasury", "alternatives"],
    },
    "Goldman Sachs": {
        "name": "Goldman Sachs", "ticker": "GS", "cik": "0000886982",
        "category": "Traditional", "country": "US",
        "ir_url": "https://www.goldmansachs.com/investor-relations/financials/current/press-releases/",
        "news_query": "Goldman Sachs earnings AUS AWM quarterly results",
        "aum_buckets": ["equity", "fixed_income", "alternatives", "liquidity", "multi_asset"],
    },
    "J.P. Morgan": {
        "name": "J.P. Morgan", "ticker": "JPM", "cik": "0000019617",
        "category": "Traditional", "country": "US",
        "ir_url": "https://www.jpmorganchase.com/ir/news",
        "news_query": "JPMorgan Chase earnings AUM AWM quarterly results",
        "aum_buckets": ["equity", "fixed_income", "multi_asset", "alternatives", "liquidity"],
    },
    "UBS": {
        "name": "UBS", "ticker": "UBS", "cik": "0001114446",
        "category": "Traditional", "country": "Switzerland",
        "ir_url": "https://www.ubs.com/global/en/investor-relations/financial-information/quarterly-reporting.html",
        "news_query": "UBS earnings invested assets GWM asset management quarterly",
        "aum_buckets": ["equity", "fixed_income", "multi_asset", "alternatives", "money_market"],
    },

    # ── Private Equity & Alternative Managers ──
    "KKR": {
        "name": "KKR", "ticker": "KKR", "cik": "0001404912",
        "category": "PE/Alternative", "country": "US",
        "ir_url": "https://ir.kkr.com/events-presentations/press-releases/",
        "news_query": "KKR earnings FPAUM fee related earnings quarterly",
        "aum_buckets": ["private_equity", "real_assets", "credit_liquid", "insurance", "other"],
    },
    "Blackstone": {
        "name": "Blackstone", "ticker": "BX", "cik": "0001393818",
        "category": "PE/Alternative", "country": "US",
        "ir_url": "https://ir.blackstone.com/news-events/press-releases",
        "news_query": "Blackstone earnings AUM distributable earnings quarterly",
        "aum_buckets": ["real_estate", "private_equity", "credit_insurance", "multi_asset", "other"],
    },
    "Carlyle": {
        "name": "Carlyle", "ticker": "CG", "cik": "0001527166",
        "category": "PE/Alternative", "country": "US",
        "ir_url": "https://ir.carlyle.com/press-releases/",
        "news_query": "Carlyle earnings FRE fee related earnings quarterly",
        "aum_buckets": ["global_private_equity", "global_credit", "alpinvest", "other", "fee_earning"],
    },
    "Apollo": {
        "name": "Apollo", "ticker": "APO", "cik": "0001858681",
        "category": "PE/Alternative", "country": "US",
        "ir_url": "https://ir.apollo.com/news-events/press-releases",
        "news_query": "Apollo Global Management earnings AUM FRE quarterly",
        "aum_buckets": ["credit", "equity", "insurance_athene", "global_wealth", "other"],
    },
    "Brookfield": {
        "name": "Brookfield", "ticker": "BAM", "cik": "0001001250",
        "category": "PE/Alternative", "country": "Canada",
        "ir_url": "https://bam.brookfield.com/investors/news-presentations",
        "news_query": "Brookfield Asset Management earnings FBC FRE quarterly",
        "aum_buckets": ["infrastructure", "renewable_power", "real_estate", "private_equity", "credit"],
    },
}

# ─── Fund family roster (Morningstar Direct / SEC N-CSR universe) ─
FUND_FAMILIES = [
    "Vanguard", "iShares", "Fidelity", "Capital Group", "SPDR State Street",
    "Invesco", "JPMorgan", "T. Rowe Price", "Dimensional", "Franklin Templeton",
]

# ─── Opinion-mining keyword vocabularies ─────────────────────────
INITIATIVE_KEYWORDS = [
    "launch", "launched", "new product", "new index", "acquired", "acquisition",
    "partnership", "platform", "initiative", "expand", "investing in",
    "rolling out", "going live", "unveiled", "introduced",
]
COST_KEYWORDS = [
    "operating expense", "compensation", "cost discipline", "efficiency",
    "headcount", "restructuring", "integration cost", "severance", "buyback",
    "dividend", "margin pressure", "expense ratio",
]
STRATEGIC_KEYWORDS = [
    "outlook", "target", "medium-term", "long-term", "ambition", "strategy",
    "priority", "priorities", "double", "growth", "2028", "2030",
    "private markets", "private assets", "tokenization", "AI", "digital",
]

# ─── Insight-tag classifier (keyword-based) ──────────────────────
# Maps catalog tags to trigger phrases used in earnings-call text
# and press releases. Scraper applies these to build `insight_tags`
# on each quarter block. The front-end reads the same tag IDs from
# `insight_tag_catalog` at the JSON root.
INSIGHT_TAG_RULES = {
    "ai_growth":        ["ai infrastructure", "ai infra", "ai partnership", "ai-led", "ai use cases",
                          "data centers", "onegs", "ai operating model"],
    "private_credit":   ["private credit", "direct lending", "k-abf", "asset-based credit",
                          "ABS", "asset-backed finance"],
    "etf_expansion":    ["ishares", "etf inflows", "etf net inflows", "active etf", "etf aum"],
    "tokenization":     ["tokenization", "tokenized", "digital asset platform", "kinexys", "stablecoin"],
    "wealth_channel":   ["401(k)", "retirement", "lifepath", "private wealth", "model portfolios",
                          "wealth platform", "target-date"],
    "insurance_scale":  ["insurance general account", "athene", "reinsurance", "annuities",
                          "insurance partners", "bws"],
    "alts_fundraising": ["fundraising", "record year", "capital raised", "inflows", "fpaum", "fbc"],
    "margin_expansion": ["margin expanded", "record margin", "margin +", "fre margin", "pre-tax margin"],
    "revenue_decline":  ["revenue decline", "revenue decreased", "revenue down", "flat revenue"],
    "cost_pressure":    ["operating expense +", "expenses +", "integration cost", "compensation +",
                          "cost pressure", "margin pressure"],
    "integration":      ["integration", "cs integration", "m&a", "acquired", "acquisition complete"],
    "record_year":      ["record year", "record fy", "all-time high", "highest since", "record revenue"],
    "retention_risk":   ["retention", "outflow", "switched", "replaced msci", "competitor won"],
    "run_rate_growth":  ["run rate", "subscription growth", "recurring revenue"],
}


def classify_insight_tags(text):
    """Scan a block of text; return sorted list of matching catalog tag IDs."""
    if not text:
        return []
    low = text.lower()
    hits = set()
    for tag_id, phrases in INSIGHT_TAG_RULES.items():
        for p in phrases:
            if p in low:
                hits.add(tag_id)
                break
    return sorted(hits)


# ─── Segment-level AUM keyword extractor ─────────────────────────
# Maps a scraper-side canonical segment name to the keyword triggers
# that typically appear before a money value in earnings commentary.
SEGMENT_RULES_TRADITIONAL = {
    "equity":       ["equity aum", "equities aum", "equity assets"],
    "fixed_income": ["fixed income aum", "bonds aum", "fixed-income"],
    "alternatives": ["alternatives aum", "alts aum", "alternative assets"],
    "multi_asset":  ["multi-asset aum", "multi asset aum", "balanced aum"],
    "cash_other":   ["cash aum", "liquidity aum", "money market aum"],
}
SEGMENT_RULES_PE = {
    "real_estate":      ["real estate aum", "real estate assets"],
    "private_equity":   ["private equity aum", "pe aum"],
    "credit_insurance": ["credit aum", "insurance aum", "credit & insurance"],
    "multi_asset":      ["multi-asset aum", "balanced aum"],
    "other":            ["other aum", "multi-strategy aum"],
}


def extract_segments(text, category):
    """Return dict of segment_name -> usd value by scanning text."""
    if not text:
        return {}
    rules = SEGMENT_RULES_TRADITIONAL if category == "Traditional" else SEGMENT_RULES_PE
    out = {}
    for seg, keywords in rules.items():
        raw, usd = find_metric(text, keywords)
        if usd is not None:
            out[seg] = usd
    return out

# ─── Target quarters (explicit 3-quarter window) ─────────────────
# Q3 2025 (Jul–Sep), Q4 2025 (Oct–Dec), Q1 2026 (Jan–Mar) per the
# "Top Clients Earnings Summary Q4 2025" intelligence framework. The
# newest entry (Q4 2025 today) is always treated as the current_quarter.
TARGET_QUARTERS = ["Q3 2025", "Q4 2025", "Q1 2026"]
CURRENT_QUARTER = "Q4 2025"

def target_quarters(n=None):
    """Return the fixed 3-quarter target window."""
    return list(TARGET_QUARTERS)


# ─── Persistence helpers ─────────────────────────────────────────
def load_existing(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_data(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Saved to {path}")


def make_id(text):
    return hashlib.md5((text or "")[:120].lower().encode()).hexdigest()[:12]


# ─── Quarter inference ───────────────────────────────────────────
QUARTER_RE = re.compile(r"Q([1-4])\s*('?\d{2,4}|20\d{2})", re.IGNORECASE)
FQ_RE = re.compile(r"(first|second|third|fourth)\s+quarter\s+(20\d{2})", re.IGNORECASE)
FY_RE = re.compile(r"(full year|FY)\s*(20\d{2})", re.IGNORECASE)

def infer_quarter(text):
    if not text:
        return None
    m = QUARTER_RE.search(text)
    if m:
        q = int(m.group(1))
        y = m.group(2).lstrip("'")
        if len(y) == 2:
            y = "20" + y
        return f"Q{q} {y}"
    m = FQ_RE.search(text)
    if m:
        name = m.group(1).lower()
        q = {"first": 1, "second": 2, "third": 3, "fourth": 4}[name]
        return f"Q{q} {m.group(2)}"
    return None


# ─── Money parsers ───────────────────────────────────────────────
MONEY_RE = re.compile(
    r"\$\s?([\d,]+\.?\d*)\s?(trillion|billion|million|tn|bn|mn|T|B|M)\b",
    re.IGNORECASE,
)
PCT_RE = re.compile(r"([+\-]?\d+\.?\d*)\s?%")


def parse_money_to_usd(raw, unit):
    try:
        n = float(raw.replace(",", ""))
    except ValueError:
        return None
    u = unit.lower()
    if u in ("t", "tn", "trillion"):
        return n * 1_000_000_000_000
    if u in ("b", "bn", "billion"):
        return n * 1_000_000_000
    if u in ("m", "mn", "million"):
        return n * 1_000_000
    return n


def find_metric(text, keywords, context_chars=120):
    """Given a block of text and trigger keywords, find a money value
    that appears near any of the keywords. Returns the first hit's
    (raw_string, usd_value) or (None, None)."""
    if not text:
        return None, None
    lower = text.lower()
    for kw in keywords:
        idx = lower.find(kw)
        if idx < 0:
            continue
        window = text[max(0, idx - context_chars): idx + context_chars]
        m = MONEY_RE.search(window)
        if m:
            usd = parse_money_to_usd(m.group(1), m.group(2))
            if usd:
                return m.group(0), usd
    return None, None


# ─── Google News RSS ─────────────────────────────────────────────
def fetch_news_rss(query, days=120, max_items=25):
    q = requests.utils.quote(query)
    url = (
        f"https://news.google.com/rss/search?"
        f"q={q}+when:{days}d&hl=en-US&gl=US&ceid=US:en"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, verify=False)
        soup = BeautifulSoup(r.content, "xml")
        items = []
        for it in soup.find_all("item")[:max_items]:
            title = it.title.text.strip() if it.title else ""
            link = it.link.text.strip() if it.link else ""
            pub = it.pubDate.text.strip() if it.pubDate else ""
            source = it.source.text.strip() if it.source else ""
            items.append({
                "title": title, "url": link, "source": source, "published": pub,
            })
        return items
    except Exception as e:
        print(f"     [!] News RSS failed ({query[:40]}): {e}")
        return []


# ─── IR page scraper ─────────────────────────────────────────────
def fetch_ir_releases(client, max_items=8):
    url = client.get("ir_url")
    if not url:
        return []
    try:
        r = requests.get(url, headers=HEADERS, timeout=20, verify=False)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        items = []
        for a in soup.find_all("a", href=True)[:200]:
            text = a.get_text(" ", strip=True)
            if not text or len(text) < 25:
                continue
            href = a["href"]
            low = text.lower()
            if not any(k in low for k in (
                "earnings", "results", "quarter", "annual", "press release",
                "half year", "interim", "full year",
            )):
                continue
            if href.startswith("/"):
                from urllib.parse import urljoin
                href = urljoin(url, href)
            items.append({"title": text[:180], "url": href, "source": client["name"] + " IR", "published": ""})
            if len(items) >= max_items:
                break
        return items
    except Exception as e:
        print(f"     [!] IR fetch failed ({client['name']}): {e}")
        return []


# ─── Article body fetch ──────────────────────────────────────────
def fetch_article_text(url, max_chars=6000):
    if not url:
        return ""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, verify=False, allow_redirects=True)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        ps = soup.find_all("p")
        txt = " ".join(p.get_text().strip() for p in ps if len(p.get_text().strip()) > 30)
        return re.sub(r"\s+", " ", txt)[:max_chars]
    except Exception:
        return ""


# ─── Metric extraction per article ───────────────────────────────
FIN_METRIC_KEYWORDS = {
    "revenue":          ["total revenue", "net revenue", "revenues of", "total revenues"],
    "operating_income": ["operating income", "operating profit", "pre-tax income", "adj. pre-tax income"],
    "net_income":       ["net income", "net earnings", "net profit"],
    "aum":              ["assets under management", "aum of", "aum ", "invested assets", "fee-bearing capital", "auma"],
    "inflows":          ["net inflows", "net new money", "net flows", "gross inflows", "capital raised"],
    "eps":              ["diluted eps", "earnings per share", "eps of"],
}


def extract_financials(text):
    """Pull a best-effort set of financial numbers from one article body."""
    out = {}
    for metric, kws in FIN_METRIC_KEYWORDS.items():
        raw, usd = find_metric(text, kws)
        if usd is not None:
            out[metric] = {"raw": raw, "value_usd": usd}
    # EPS special (may be a plain number, not $X billion)
    if "eps" not in out:
        m = re.search(r"(diluted\s+eps|earnings\s+per\s+share)\D{0,30}\$?(\d+\.\d{2})", text, re.IGNORECASE)
        if m:
            out["eps"] = {"raw": m.group(0), "value_usd": float(m.group(2))}
    return out


def mine_themes(text, client_name):
    """Return a short list of themes found in a text block."""
    if not text:
        return {"initiatives": [], "cost_pressures": [], "strategic_insights": []}
    sentences = re.split(r"(?<=[.!?])\s+", text)
    trimmed = [s.strip() for s in sentences if 30 < len(s.strip()) < 280]

    def collect(keywords, limit=3):
        hits = []
        seen = set()
        for s in trimmed:
            low = s.lower()
            if any(k in low for k in keywords):
                key = s[:80]
                if key in seen:
                    continue
                seen.add(key)
                hits.append(s)
                if len(hits) >= limit:
                    break
        return hits

    return {
        "initiatives":        collect(INITIATIVE_KEYWORDS),
        "cost_pressures":     collect(COST_KEYWORDS),
        "strategic_insights": collect(STRATEGIC_KEYWORDS),
    }


# ─── Empty per-quarter scaffold (schema v4.1) ────────────────────
def empty_quarter():
    return {
        "financials": {
            "revenue_usd": None, "revenue_yoy_pct": None,
            "operating_income_usd": None, "operating_margin_pct": None,
            "net_income_usd": None, "eps_usd": None,
            "aum_usd": None, "aum_yoy_growth_pct": None,
            "organic_base_fee_growth_pct": None,
            "net_inflows_q_usd": None, "net_inflows_fy_usd": None,
        },
        "aum_breakdown": {},                 # e.g. {"equity": 4500000000000, ...}
        "segments": {},                      # e.g. {"equity_aum_usd": ..., "fixed_income_aum_usd": ...}
        "msci_relationship": {
            "run_rate_usd": None,
            "run_rate_yoy_growth_pct": None,
            "client_revenue_usd": None,
            "client_revenue_yoy_growth_pct": None,
        },
        "insight_tags": [],                  # catalog tag IDs: ["ai_growth", "private_credit", ...]
        "strategic_highlights": [],          # bullet highlights for drilldown
        "themes": {
            "key_initiatives": [],
            "cost_pressures": [],
            "strategic_insights": [],
        },
        "top_trends": [],                    # headline bullets from deep-dive pages
        "sources": [],                       # [{metric, type, url, accessed}]
        "notes": [],
    }


def empty_client_block(client):
    return {
        "name": client["name"],
        "ticker": client["ticker"],
        "cik": client.get("cik"),
        "category": client["category"],
        "country": client["country"],
        "ir_url": client.get("ir_url"),
        "aum_buckets": client.get("aum_buckets", []),
        "quarters": {},     # keyed by "Qx yyyy"
    }


# ─── Merge rules ─────────────────────────────────────────────────
def merge_field(existing, incoming, source_tag, sources_list, metric_name, url=None):
    """Only overwrite nulls. Record provenance for each new fill."""
    if incoming is None:
        return existing
    if existing is None:
        sources_list.append({
            "metric": metric_name,
            "type": source_tag,
            "url": url or "",
            "accessed": datetime.utcnow().isoformat() + "Z",
        })
        return incoming
    return existing


# ─── Main pipeline ───────────────────────────────────────────────
def main():
    print("=" * 70)
    print("  MSCI TOP CLIENT EARNINGS SCRAPER  (12-client roster)")
    print(f"  {datetime.now().strftime('%A, %B %d, %Y %I:%M %p')}")
    print("=" * 70)

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    quarters = target_quarters()
    current_q = CURRENT_QUARTER
    print(f"  Target quarters: {quarters}   (current: {current_q})\n")

    existing = load_existing(OUTPUT_FILE) or {}

    # Root payload — v4.1 preserves seeded macro_themes, insight_tag_catalog, VoC
    out = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "schema_version": "4.1",
        "current_quarter": current_q,
        "quarters": quarters,
        "client_categories": {
            "Traditional": [k for k, c in CLIENTS.items() if c["category"] == "Traditional"],
            "PE/Alternative": [k for k, c in CLIENTS.items() if c["category"] == "PE/Alternative"],
        },
        # Preserve catalog + curated Q4'25 seed sections; scraper won't overwrite
        "insight_tag_catalog": existing.get("insight_tag_catalog", []),
        "executive_summary": existing.get("executive_summary", {
            "period": current_q,
            "title": "",
            "macro_themes": [],
            "themes": [],     # back-compat
        }),
        "fund_family_flows": existing.get("fund_family_flows", {
            "period": current_q,
            "source": "Morningstar Direct Asset Flows",
            "note": "",
            "families": [
                {"family": fam, "active_usd": None, "passive_usd": None,
                 "jan_2026_usd": None, "ytd_usd": None, "ttm_usd": None,
                 "total_assets_usd": None}
                for fam in FUND_FAMILIES
            ],
        }),
        "opinion_mining": existing.get("opinion_mining", {}),
        "aum_summary": existing.get("aum_summary", {
            "period": current_q,
            "note": "",
            "traditional": [],
            "pe_alternative": [],
        }),
        "voc_analysis": existing.get("voc_analysis", {
            "total_entries": None,
            "period": current_q,
            "source": "",
            "headline": "",
            "clients": [],
            "qualitative_insights": {"traditional": {}, "pe_alternative": {}},
        }),
        "clients": {},
    }

    # Merge existing client scaffolds; ensure every client has every quarter
    existing_clients = (existing.get("clients") or {})
    for key, client in CLIENTS.items():
        block = existing_clients.get(key) or empty_client_block(client)
        # Keep metadata authoritative from the code
        for k in ("name", "ticker", "cik", "category", "country", "ir_url", "aum_buckets"):
            block[k] = empty_client_block(client)[k]
        if "quarters" not in block or not isinstance(block["quarters"], dict):
            block["quarters"] = {}
        for q in quarters:
            if q not in block["quarters"]:
                block["quarters"][q] = empty_quarter()
        out["clients"][key] = block

    # Opinion mining scaffold (one row per client)
    om = out["opinion_mining"] or {}
    for key in CLIENTS:
        if key not in om:
            om[key] = {"initiatives": [], "cost_pressures": [], "strategic_insights": []}
    out["opinion_mining"] = om

    # ── Per-client scraping loop ─────────────────────────────────
    for key, client in CLIENTS.items():
        print(f"\n  [{client['category']}] {client['name']} ({client['ticker']})")
        block = out["clients"][key]

        # 1. IR releases
        ir_items = fetch_ir_releases(client)
        print(f"     IR releases: {len(ir_items)}")

        # 2. News RSS
        news_items = fetch_news_rss(client["news_query"])
        print(f"     News RSS:    {len(news_items)}")

        # 3. Merge article set (cap to keep runtime reasonable)
        articles = ir_items + news_items

        # 4. Process each article: infer quarter, extract metrics, mine themes
        for art in articles[:12]:
            title = art.get("title", "")
            url = art.get("url", "")
            q = infer_quarter(title)
            if not q:
                # Try to infer from publish date
                pub = art.get("published", "")
                if pub and dateparser:
                    try:
                        d = dateparser.parse(pub)
                        cq = (d.month - 1) // 3 + 1
                        q = f"Q{cq} {d.year}"
                    except Exception:
                        q = None
            if q not in block["quarters"]:
                continue
            body = fetch_article_text(url)
            if not body:
                continue
            fins = extract_financials(body)
            themes = mine_themes(body, client["name"])
            # v4.1: tag classification + segment extraction from the same body
            tags_new = classify_insight_tags(title + "\n" + body)
            seg_new  = extract_segments(body, client["category"])

            qb = block["quarters"][q]

            # Merge financials (only fills nulls)
            fmap = {
                "revenue":          "revenue_usd",
                "operating_income": "operating_income_usd",
                "net_income":       "net_income_usd",
                "aum":              "aum_usd",
                "inflows":          "net_inflows_q_usd",
                "eps":              "eps_usd",
            }
            src_tag = "IR release" if "IR" in art.get("source", "") else "News RSS"
            for metric_src, field in fmap.items():
                got = fins.get(metric_src)
                if got:
                    qb["financials"][field] = merge_field(
                        qb["financials"][field], got["value_usd"],
                        src_tag, qb["sources"], field, url,
                    )

            # Append themes (dedupe)
            for bucket_src, bucket_dst in [
                ("initiatives", "key_initiatives"),
                ("cost_pressures", "cost_pressures"),
                ("strategic_insights", "strategic_insights"),
            ]:
                existing_set = set(qb["themes"][bucket_dst])
                for s in themes.get(bucket_src, []):
                    if s not in existing_set and len(qb["themes"][bucket_dst]) < 5:
                        qb["themes"][bucket_dst].append(s)
                        existing_set.add(s)

            # Merge insight_tags (union, keep sorted, cap 10)
            existing_tags = set(qb.get("insight_tags") or [])
            for t in tags_new:
                existing_tags.add(t)
            qb["insight_tags"] = sorted(existing_tags)[:10]

            # Merge segments — only fill nulls; normalize keys to *_aum_usd
            seg_block = qb.get("segments") or {}
            for seg_name, usd_val in seg_new.items():
                col = f"{seg_name}_aum_usd"
                if seg_block.get(col) in (None, 0):
                    seg_block[col] = usd_val
            qb["segments"] = seg_block

            # Record top-trend headline pointer (current quarter only)
            if q == current_q and title and len(qb["top_trends"]) < 8:
                qb["top_trends"].append({
                    "headline": title[:180],
                    "url": url,
                    "source": art.get("source", ""),
                })

            time.sleep(0.5)

        # 5. Mirror latest-quarter themes into opinion_mining table
        latest = block["quarters"].get(current_q, {})
        t = latest.get("themes", {})
        om_entry = om.get(key, {})
        if t.get("key_initiatives") and not om_entry.get("initiatives"):
            om_entry["initiatives"] = t["key_initiatives"][:3]
        if t.get("cost_pressures") and not om_entry.get("cost_pressures"):
            om_entry["cost_pressures"] = t["cost_pressures"][:3]
        if t.get("strategic_insights") and not om_entry.get("strategic_insights"):
            om_entry["strategic_insights"] = t["strategic_insights"][:3]

        time.sleep(0.8)

    # ── Build AUM summary rows for the current quarter ───────────
    # IMPORTANT: this section MERGES into existing aum_summary rather than
    # overwriting it. Manually-curated PDF-sourced rows are preserved;
    # scraper-derived values only fill nulls.
    existing_aum = existing.get("aum_summary", {}) or {}
    existing_trad = {r.get("client"): r for r in (existing_aum.get("traditional") or [])}
    existing_pe   = {r.get("client"): r for r in (existing_aum.get("pe_alternative") or [])}

    def _merge_aum_row(existing_row, scraped_row):
        """Return a row where existing non-null values win; scraped values only fill nulls."""
        if not existing_row:
            return scraped_row
        merged = dict(existing_row)
        for k, v in scraped_row.items():
            if merged.get(k) in (None, "", "N/A") and v not in (None, "", "N/A"):
                merged[k] = v
        return merged

    # Segment columns we preserve on each AUM summary row
    TRAD_SEG_COLS = ["equity_aum_usd", "fixed_income_aum_usd",
                     "alternatives_aum_usd", "multi_asset_aum_usd",
                     "other_aum_usd"]
    PE_SEG_COLS   = ["real_estate_aum_usd", "private_equity_aum_usd",
                     "credit_insurance_aum_usd", "multi_asset_aum_usd",
                     "other_aum_usd"]

    trad_rows, pe_rows = [], []
    for key, block in out["clients"].items():
        q = block["quarters"].get(current_q, {}) or {}
        fins = q.get("financials", {}) or {}
        rel  = q.get("msci_relationship", {}) or {}
        segs = q.get("segments", {}) or {}
        is_trad = block["category"] == "Traditional"
        seg_cols = TRAD_SEG_COLS if is_trad else PE_SEG_COLS

        scraped = {
            "client": block["name"],
            "ticker": block["ticker"],
            "aum_usd": fins.get("aum_usd"),
            "aum_yoy_growth_pct": fins.get("aum_yoy_growth_pct"),
            "organic_base_fee_growth_pct": fins.get("organic_base_fee_growth_pct"),
            "net_inflows_q_usd": fins.get("net_inflows_q_usd"),
            "msci_run_rate_usd": rel.get("run_rate_usd"),
            "msci_run_rate_yoy_pct": rel.get("run_rate_yoy_growth_pct"),
            "msci_client_revenue_usd": rel.get("client_revenue_usd"),
            "msci_client_revenue_yoy_pct": rel.get("client_revenue_yoy_growth_pct"),
        }
        # carry segment columns over from the per-quarter block so the
        # AUM summary mirrors the drill-down table
        for col in seg_cols:
            scraped[col] = segs.get(col)

        if is_trad:
            trad_rows.append(_merge_aum_row(existing_trad.get(block["name"]), scraped))
        else:
            pe_rows.append(_merge_aum_row(existing_pe.get(block["name"]), scraped))

    # Preserve any hand-curated rows (e.g. Fidelity, non-public firms) that
    # aren't in CLIENTS but still appear in the existing AUM summary.
    scraped_trad_names = {r["client"] for r in trad_rows}
    scraped_pe_names   = {r["client"] for r in pe_rows}
    for name, row in existing_trad.items():
        if name not in scraped_trad_names:
            trad_rows.append(row)
    for name, row in existing_pe.items():
        if name not in scraped_pe_names:
            pe_rows.append(row)

    out["aum_summary"] = {
        "period": existing_aum.get("period") or current_q,
        "note": existing_aum.get("note", ""),
        "traditional": trad_rows,
        "pe_alternative": pe_rows,
    }

    # ── Save ─────────────────────────────────────────────────────
    save_data(out, OUTPUT_FILE)
    print("\n  ----- Summary -----")
    print(f"  Clients tracked:    {len(out['clients'])}")
    print(f"  Quarters covered:   {quarters}")
    print(f"  Fund families:      {len(FUND_FAMILIES)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
