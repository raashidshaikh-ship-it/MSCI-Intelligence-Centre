# Top Client Earnings Scraper — v4.2 (public-data only)
# ===============================================================
# Tracks quarterly earnings of MSCI's top 12 CLIENTS across a
# fixed 3-quarter window: Q3 2025 · Q4 2025 · Q1 2026.
#
# ── STRICT RULES ────────────────────────────────────────────────
#   1. NO hardcoded numerical values. All numbers come from public
#      sources (IR pages, SEC EDGAR, Google News).
#   2. If a number cannot be sourced publicly, the field stays null.
#   3. Per-quarter pipeline: we query specifically for each quarter
#      so data never leaks between Q3 2025, Q4 2025, and Q1 2026.
#   4. A duplicate-across-quarters validator flags any client whose
#      same value repeats across all three quarters.
#
# Client roster (MSCI Client Insights & Analytics):
#   Traditional Asset & Wealth Managers (7)
#     BlackRock, State Street, Aberdeen, Amundi,
#     Goldman Sachs, J.P. Morgan, UBS
#   Private Equity & Alternative Managers (5)
#     KKR, Blackstone, Carlyle, Apollo, Brookfield
#
# Pipeline:
#   For each client × target_quarter:
#     (a) Google News RSS query scoped to that quarter
#     (b) IR press-release scrape (filtered by quarter keywords)
#     (c) Fetch article body → extract financials + tags + segments
#     (d) Store under clients[<client>].quarters[<quarter>]
#   After all clients processed:
#     (e) Build aum_summary rows from each client's current_quarter
#     (f) Duplicate-across-quarters validator
#
# Writes to: data/top_client_earnings.json
# Run:       python scraper/top_client_earnings_scraper.py
# Deps:      pip install -r scraper/requirements.txt

import json, os, re, time, hashlib
from datetime import datetime

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

# ─── HTTP ────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
SEC_USER_AGENT = os.environ.get(
    "SEC_USER_AGENT",
    "MSCI Competitor Intel raashid.shaikh@msci.com",
)

# ─── Target quarter window (fixed) ───────────────────────────────
# Q1 → Jan–Mar · Q2 → Apr–Jun · Q3 → Jul–Sep · Q4 → Oct–Dec
TARGET_QUARTERS = ["Q3 2025", "Q4 2025", "Q1 2026"]
CURRENT_QUARTER = "Q4 2025"

# Map ("Q<n>", <year>) → [(month_start, month_end)] for date→quarter inference.
_Q_MONTHS = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}

# Synonyms used when querying news for a specific quarter.
QUARTER_SYNONYMS = {
    "Q3 2025": ['"Q3 2025"', '"third quarter 2025"', '"3Q25"', '"3Q 2025"'],
    "Q4 2025": ['"Q4 2025"', '"fourth quarter 2025"', '"4Q25"', '"4Q 2025"', '"full year 2025"', '"FY 2025"'],
    "Q1 2026": ['"Q1 2026"', '"first quarter 2026"', '"1Q26"', '"1Q 2026"'],
}


def target_quarters():
    return list(TARGET_QUARTERS)


# ─── Client roster (public tickers + IR URLs only) ───────────────
CLIENTS = {
    "BlackRock": {
        "name": "BlackRock", "ticker": "BLK", "cik": "0001364742",
        "category": "Traditional", "country": "US",
        "ir_url": "https://ir.blackrock.com/news-and-events/press-releases",
        "news_query_base": "BlackRock earnings AUM",
        "aum_buckets": ["equity", "fixed_income", "multi_asset", "alternatives", "cash_other"],
    },
    "State Street": {
        "name": "State Street", "ticker": "STT", "cik": "0000093751",
        "category": "Traditional", "country": "US",
        "ir_url": "https://investors.statestreet.com/investors/news-releases/default.aspx",
        "news_query_base": "State Street earnings AUC AUM",
        "aum_buckets": ["equity", "fixed_income", "multi_asset", "alternatives", "cash_other"],
    },
    "Aberdeen": {
        "name": "Aberdeen", "ticker": "ABDN.L", "cik": None,
        "category": "Traditional", "country": "UK",
        "ir_url": "https://www.aberdeeninvestments.com/en-gb/investor/results-reports-and-presentations",
        "news_query_base": "Aberdeen abrdn AUMA net flows",
        "aum_buckets": ["investments", "adviser", "ii_wealth", "insurance_partners", "other"],
    },
    "Amundi": {
        "name": "Amundi", "ticker": "AMUN.PA", "cik": None,
        "category": "Traditional", "country": "France",
        "ir_url": "https://about.amundi.com/Investor-relations/Results",
        "news_query_base": "Amundi earnings AUM net inflows",
        "aum_buckets": ["equity", "bonds", "multi_asset", "treasury", "alternatives"],
    },
    "Goldman Sachs": {
        "name": "Goldman Sachs", "ticker": "GS", "cik": "0000886982",
        "category": "Traditional", "country": "US",
        "ir_url": "https://www.goldmansachs.com/investor-relations/financials/current/press-releases/",
        "news_query_base": "Goldman Sachs earnings AUS AWM",
        "aum_buckets": ["equity", "fixed_income", "alternatives", "liquidity", "multi_asset"],
    },
    "J.P. Morgan": {
        "name": "J.P. Morgan", "ticker": "JPM", "cik": "0000019617",
        "category": "Traditional", "country": "US",
        "ir_url": "https://www.jpmorganchase.com/ir/news",
        "news_query_base": "JPMorgan Chase earnings AUM AWM",
        "aum_buckets": ["equity", "fixed_income", "multi_asset", "alternatives", "liquidity"],
    },
    "UBS": {
        "name": "UBS", "ticker": "UBS", "cik": "0001114446",
        "category": "Traditional", "country": "Switzerland",
        "ir_url": "https://www.ubs.com/global/en/investor-relations/financial-information/quarterly-reporting.html",
        "news_query_base": "UBS earnings invested assets GWM asset management",
        "aum_buckets": ["equity", "fixed_income", "multi_asset", "alternatives", "money_market"],
    },

    "KKR": {
        "name": "KKR", "ticker": "KKR", "cik": "0001404912",
        "category": "PE/Alternative", "country": "US",
        "ir_url": "https://ir.kkr.com/events-presentations/press-releases/",
        "news_query_base": "KKR earnings FPAUM fee related",
        "aum_buckets": ["private_equity", "real_assets", "credit_liquid", "insurance", "other"],
    },
    "Blackstone": {
        "name": "Blackstone", "ticker": "BX", "cik": "0001393818",
        "category": "PE/Alternative", "country": "US",
        "ir_url": "https://ir.blackstone.com/news-events/press-releases",
        "news_query_base": "Blackstone earnings AUM distributable",
        "aum_buckets": ["real_estate", "private_equity", "credit_insurance", "multi_asset", "other"],
    },
    "Carlyle": {
        "name": "Carlyle", "ticker": "CG", "cik": "0001527166",
        "category": "PE/Alternative", "country": "US",
        "ir_url": "https://ir.carlyle.com/press-releases/",
        "news_query_base": "Carlyle earnings FRE fee related",
        "aum_buckets": ["global_private_equity", "global_credit", "alpinvest", "other", "fee_earning"],
    },
    "Apollo": {
        "name": "Apollo", "ticker": "APO", "cik": "0001858681",
        "category": "PE/Alternative", "country": "US",
        "ir_url": "https://ir.apollo.com/news-events/press-releases",
        "news_query_base": "Apollo Global Management earnings AUM FRE",
        "aum_buckets": ["credit", "equity", "insurance_athene", "global_wealth", "other"],
    },
    "Brookfield": {
        "name": "Brookfield", "ticker": "BAM", "cik": "0001001250",
        "category": "PE/Alternative", "country": "Canada",
        "ir_url": "https://bam.brookfield.com/investors/news-presentations",
        "news_query_base": "Brookfield Asset Management earnings FBC FRE",
        "aum_buckets": ["infrastructure", "renewable_power", "real_estate", "private_equity", "credit"],
    },
}

FUND_FAMILIES = [
    "Vanguard", "iShares", "Fidelity", "Capital Group", "SPDR State Street",
    "Invesco", "JPMorgan", "T. Rowe Price", "Dimensional", "Franklin Templeton",
]

# ─── Keyword vocabularies ────────────────────────────────────────
INITIATIVE_KEYWORDS = [
    "launch", "launched", "new product", "new index", "acquired", "acquisition",
    "partnership", "platform", "initiative", "expand", "investing in",
    "rolling out", "going live", "unveiled", "introduced",
]
COST_KEYWORDS = [
    "operating expense", "compensation", "cost discipline", "efficiency",
    "headcount", "restructuring", "integration cost", "severance",
    "margin pressure", "expense ratio",
]
STRATEGIC_KEYWORDS = [
    "outlook", "target", "medium-term", "long-term", "ambition", "strategy",
    "priority", "priorities", "double", "growth", "2028", "2030",
    "private markets", "private assets", "tokenization", "AI", "digital",
]

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
                          "insurance partners"],
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


def classify_insight_tags(text):
    if not text:
        return []
    low = text.lower()
    hits = set()
    for tag_id, phrases in INSIGHT_TAG_RULES.items():
        for p in phrases:
            if p.lower() in low:
                hits.add(tag_id)
                break
    return sorted(hits)


def extract_segments(text, category):
    if not text:
        return {}
    rules = SEGMENT_RULES_TRADITIONAL if category == "Traditional" else SEGMENT_RULES_PE
    out = {}
    for seg, keywords in rules.items():
        raw, usd = find_metric(text, keywords)
        if usd is not None:
            out[f"{seg}_aum_usd"] = usd
    return out


# ─── Persistence ─────────────────────────────────────────────────
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
FQ_RE     = re.compile(r"(first|second|third|fourth)[-\s]+quarter[-\s]+(20\d{2})", re.IGNORECASE)
NQ_RE     = re.compile(r"([1-4])Q[-\s]?(20\d{2}|\d{2})", re.IGNORECASE)
FY_RE     = re.compile(r"(full[-\s]+year|FY)[-\s]*(20\d{2})", re.IGNORECASE)


def infer_quarter_from_text(text):
    """Derive Qx YYYY from text. Returns a normalized 'Qn YYYY' or None."""
    if not text:
        return None

    def _norm(q, y):
        y = str(y).lstrip("'")
        if len(y) == 2:
            y = "20" + y
        return f"Q{int(q)} {y}"

    m = QUARTER_RE.search(text)
    if m:
        return _norm(m.group(1), m.group(2))
    m = NQ_RE.search(text)
    if m:
        return _norm(m.group(1), m.group(2))
    m = FQ_RE.search(text)
    if m:
        name = m.group(1).lower()
        q = {"first": 1, "second": 2, "third": 3, "fourth": 4}[name]
        return _norm(q, m.group(2))
    m = FY_RE.search(text)
    if m:
        # FY <year> typically released as Q4 of that year.
        return _norm(4, m.group(2))
    return None


def infer_quarter_from_date(pub_str):
    """Fallback — map a publication date to the quarter it reports on.
    Earnings for Q<n> YYYY are typically published 2–6 weeks after the
    quarter closes. We map back one quarter from publish-date:
       Jan–Feb  → Q4 of prior year
       Mar–Apr  → Q1 of same year
       May–Jul  → Q1 or Q2 of same year (tiebreak: use publish month)
    Simple approach: subtract ~45 days from publish date and bucket.
    """
    if not pub_str or not dateparser:
        return None
    try:
        d = dateparser.parse(pub_str)
    except Exception:
        return None
    if d is None:
        return None
    # Shift back ~45 days to approximate the quarter being reported on.
    from datetime import timedelta
    d2 = d - timedelta(days=45)
    q = (d2.month - 1) // 3 + 1
    return f"Q{q} {d2.year}"


def quarter_in_window(q):
    return q in TARGET_QUARTERS


# ─── Money parsers ───────────────────────────────────────────────
MONEY_RE = re.compile(
    r"\$?\s?([\d]{1,4}(?:[,.]\d{3})*(?:\.\d+)?)\s?(trillion|billion|million|tn|bn|mn|T\b|B\b|M\b)\b",
    re.IGNORECASE,
)
PCT_RE = re.compile(r"([+\-]?\d+\.?\d*)\s?%")


def parse_money_to_usd(raw, unit):
    try:
        n = float(str(raw).replace(",", ""))
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


def find_metric(text, keywords, context_chars=140):
    """Find a money value near any of the keywords. Returns first hit."""
    if not text:
        return None, None
    lower = text.lower()
    for kw in keywords:
        idx = lower.find(kw.lower())
        if idx < 0:
            continue
        window = text[max(0, idx - context_chars): idx + context_chars]
        m = MONEY_RE.search(window)
        if m:
            usd = parse_money_to_usd(m.group(1), m.group(2))
            if usd:
                return m.group(0), usd
    return None, None


def find_pct(text, keywords, context_chars=120):
    if not text:
        return None
    lower = text.lower()
    for kw in keywords:
        idx = lower.find(kw.lower())
        if idx < 0:
            continue
        window = text[max(0, idx - context_chars): idx + context_chars]
        m = PCT_RE.search(window)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                continue
    return None


# ─── Google News RSS (scoped to a specific quarter) ──────────────
def fetch_news_rss_for_quarter(client_name, base_query, quarter, days=180, max_items=12):
    """Build a quarter-scoped Google News query using synonyms. Each call
    returns items likely to reference `quarter`."""
    synonyms = QUARTER_SYNONYMS.get(quarter, [])
    synonyms_clause = "(" + " OR ".join(synonyms) + ")" if synonyms else ""
    query = f"{client_name} {base_query} {synonyms_clause}".strip()
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
                "quarter_hint": quarter,
            })
        return items
    except Exception as e:
        print(f"     [!] News RSS failed ({client_name}/{quarter}): {e}")
        return []


# ─── IR page scraper ─────────────────────────────────────────────
def fetch_ir_releases(client, max_items=15):
    url = client.get("ir_url")
    if not url:
        return []
    try:
        r = requests.get(url, headers=HEADERS, timeout=20, verify=False)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        items = []
        for a in soup.find_all("a", href=True)[:300]:
            text = a.get_text(" ", strip=True)
            if not text or len(text) < 25:
                continue
            href = a["href"]
            low = text.lower()
            if not any(k in low for k in (
                "earnings", "results", "quarter", "annual", "press release",
                "half year", "interim", "full year", "1q", "2q", "3q", "4q",
                "q1", "q2", "q3", "q4",
            )):
                continue
            if href.startswith("/"):
                from urllib.parse import urljoin
                href = urljoin(url, href)
            items.append({"title": text[:200], "url": href,
                          "source": client["name"] + " IR", "published": ""})
            if len(items) >= max_items:
                break
        return items
    except Exception as e:
        print(f"     [!] IR fetch failed ({client['name']}): {e}")
        return []


# ─── Article body fetch ──────────────────────────────────────────
def fetch_article_text(url, max_chars=8000):
    if not url:
        return ""
    try:
        r = requests.get(url, headers=HEADERS, timeout=18, verify=False, allow_redirects=True)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        ps = soup.find_all(["p", "li"])
        txt = " ".join(p.get_text().strip() for p in ps if len(p.get_text().strip()) > 25)
        return re.sub(r"\s+", " ", txt)[:max_chars]
    except Exception:
        return ""


# ─── Financial extraction ────────────────────────────────────────
FIN_METRIC_KEYWORDS = {
    "revenue": [
        "total revenue", "total revenues", "net revenue", "net revenues",
        "revenues of", "revenue of", "revenues were", "revenue was",
        "gaap revenue",
    ],
    "operating_income": [
        "operating income", "operating profit", "pre-tax income",
        "adj. pre-tax income", "adjusted pre-tax income",
    ],
    "net_income": [
        "net income", "net earnings", "net profit", "net income attributable",
    ],
    "aum": [
        "assets under management", "aum of", "aum was", "aum were",
        "invested assets", "fee-bearing capital", "auma",
        "fee-earning aum", "feaum",
    ],
    "inflows": [
        "net inflows", "net new money", "net flows", "gross inflows",
        "capital raised", "long-term net flows",
    ],
    "eps": [
        "diluted eps", "earnings per share", "eps of",
        "diluted earnings per share",
    ],
}


def extract_financials(text):
    """Pull a best-effort set of financial numbers from one article body."""
    out = {}
    for metric, kws in FIN_METRIC_KEYWORDS.items():
        raw, usd = find_metric(text, kws)
        if usd is not None:
            out[metric] = {"raw": raw, "value_usd": usd}
    # EPS special — may be a plain decimal not a $X billion string
    if "eps" not in out:
        m = re.search(
            r"(diluted\s+eps|earnings\s+per\s+share)\D{0,40}\$?(\d+\.\d{2})",
            text, re.IGNORECASE,
        )
        if m:
            out["eps"] = {"raw": m.group(0), "value_usd": float(m.group(2))}
    # y/y growth percent for AUM
    aum_yoy = find_pct(text, ["aum grew", "aum increased", "aum up",
                               "assets under management grew", "aum was up"])
    if aum_yoy is not None:
        out["aum_yoy_growth_pct"] = {"raw": f"{aum_yoy}%", "value_usd": aum_yoy}
    rev_yoy = find_pct(text, ["revenue grew", "revenue increased", "revenue up",
                               "revenues grew", "revenues increased", "revenue was up"])
    if rev_yoy is not None:
        out["revenue_yoy_pct"] = {"raw": f"{rev_yoy}%", "value_usd": rev_yoy}
    return out


def mine_themes(text, client_name):
    if not text:
        return {"initiatives": [], "cost_pressures": [], "strategic_insights": []}
    sentences = re.split(r"(?<=[.!?])\s+", text)
    trimmed = [s.strip() for s in sentences if 30 < len(s.strip()) < 280]

    def collect(keywords, limit=3):
        hits, seen = [], set()
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


# ─── Scaffolds ───────────────────────────────────────────────────
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
        "aum_breakdown": {},
        "segments": {},
        "msci_relationship": {
            "run_rate_usd": None,
            "run_rate_yoy_growth_pct": None,
            "client_revenue_usd": None,
            "client_revenue_yoy_growth_pct": None,
        },
        "insight_tags": [],
        "strategic_highlights": [],
        "themes": {"key_initiatives": [], "cost_pressures": [], "strategic_insights": []},
        "top_trends": [],
        "sources": [],
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
        "quarters": {},
    }


# ─── Merge rule: only fills nulls, records provenance ────────────
def merge_field(existing, incoming, source_tag, sources_list, metric_name, url=None):
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


# ─── Duplicate-across-quarters validator ────────────────────────
def validate_no_duplication(clients_obj, quarters):
    """Flag any client whose same revenue / AUM value appears across all
    three quarters. That's a strong signal of a bug (or PDF leak)."""
    warnings = []
    for key, block in clients_obj.items():
        qmap = block.get("quarters") or {}
        for metric in ("revenue_usd", "aum_usd", "net_inflows_q_usd"):
            vals = []
            for q in quarters:
                v = (qmap.get(q) or {}).get("financials", {}).get(metric)
                if v is not None:
                    vals.append((q, v))
            if len(vals) >= 2 and len({v for _, v in vals}) == 1:
                warnings.append(
                    f"  [VALIDATION] {block.get('name', key)} — {metric} is identical "
                    f"across {[q for q, _ in vals]}. Check source mapping."
                )
    return warnings


# ─── Main pipeline ───────────────────────────────────────────────
def main():
    print("=" * 72)
    print("  MSCI TOP CLIENT EARNINGS SCRAPER  v4.2 (public-data only)")
    print(f"  {datetime.now().strftime('%A, %B %d, %Y %I:%M %p')}")
    print("=" * 72)

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    quarters = target_quarters()
    current_q = CURRENT_QUARTER
    print(f"  Target quarters: {quarters}   (current: {current_q})\n")

    existing = load_existing(OUTPUT_FILE) or {}

    # Root payload — we keep taxonomy only (catalog, families, empty VoC),
    # NOT any previously-seeded values. No PDF data survives in this version.
    out = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "schema_version": "4.2",
        "current_quarter": current_q,
        "quarters": quarters,
        "client_categories": {
            "Traditional": [k for k, c in CLIENTS.items() if c["category"] == "Traditional"],
            "PE/Alternative": [k for k, c in CLIENTS.items() if c["category"] == "PE/Alternative"],
        },
        "insight_tag_catalog": existing.get("insight_tag_catalog", []),
        "executive_summary": {
            "period": current_q,
            "title": "",
            "macro_themes": [],
            "themes": [],
        },
        "fund_family_flows": existing.get("fund_family_flows", {
            "period": current_q,
            "source": "Morningstar Direct Asset Flows (scraper-populated)",
            "note": "",
            "families": [
                {"family": fam, "active_usd": None, "passive_usd": None,
                 "jan_2026_usd": None, "ytd_usd": None, "ttm_usd": None,
                 "total_assets_usd": None}
                for fam in FUND_FAMILIES
            ],
        }),
        "opinion_mining": {},
        "aum_summary": {
            "period": current_q,
            "note": "Populated by scraper per current quarter. Values remain null until a public filing is parsed.",
            "traditional": [],
            "pe_alternative": [],
        },
        "voc_analysis": {
            "total_entries": None,
            "period": current_q,
            "source": "",
            "headline": "",
            "clients": [],
            "qualitative_insights": {"traditional": {}, "pe_alternative": {}},
        },
        "clients": {},
    }

    # Build per-client scaffold with an empty quarter block for each target.
    for key, client in CLIENTS.items():
        block = empty_client_block(client)
        for q in quarters:
            block["quarters"][q] = empty_quarter()
        out["clients"][key] = block
        out["opinion_mining"][key] = {
            "initiatives": [], "cost_pressures": [], "strategic_insights": [],
        }

    # ── Per-client × per-quarter scraping loop ──
    for key, client in CLIENTS.items():
        print(f"\n  [{client['category']}] {client['name']} ({client['ticker']})")
        block = out["clients"][key]

        # IR releases (one call per client — we quarter-assign below)
        ir_items = fetch_ir_releases(client)
        print(f"     IR releases found: {len(ir_items)}")

        # News per-quarter
        news_by_q = {}
        for q in quarters:
            items = fetch_news_rss_for_quarter(client["name"], client["news_query_base"], q)
            news_by_q[q] = items
            print(f"     News RSS  {q}: {len(items)} items")

        # Combine: IR items get quarter-inferred on the fly; news items are
        # already pre-grouped by quarter_hint.
        all_items = []
        for it in ir_items:
            all_items.append({**it, "quarter_hint": None})
        for q, items in news_by_q.items():
            all_items.extend(items)

        # Process each article once; assign to at most one quarter
        processed_urls = set()
        for art in all_items[:30]:
            url = art.get("url", "")
            if url in processed_urls:
                continue
            processed_urls.add(url)

            title = art.get("title", "")
            # Quarter resolution — preference order: explicit title → hint → date
            q = infer_quarter_from_text(title)
            if not q and art.get("quarter_hint"):
                q = art["quarter_hint"]
            if not q:
                q = infer_quarter_from_date(art.get("published", ""))
            if not q or not quarter_in_window(q):
                continue

            body = fetch_article_text(url)
            # Title+body re-check to harden quarter mapping (prevents cross-quarter leak)
            body_q = infer_quarter_from_text(title + " " + body[:500])
            if body_q and quarter_in_window(body_q):
                q = body_q
            if not quarter_in_window(q):
                continue
            if not body:
                continue

            fins = extract_financials(body)
            themes = mine_themes(body, client["name"])
            tags_new = classify_insight_tags(title + "\n" + body)
            seg_new  = extract_segments(body, client["category"])

            qb = block["quarters"][q]
            src_tag = "IR release" if "IR" in art.get("source", "") else "News RSS"

            fmap = {
                "revenue":          "revenue_usd",
                "operating_income": "operating_income_usd",
                "net_income":       "net_income_usd",
                "aum":              "aum_usd",
                "inflows":          "net_inflows_q_usd",
                "eps":              "eps_usd",
                "aum_yoy_growth_pct": "aum_yoy_growth_pct",
                "revenue_yoy_pct":    "revenue_yoy_pct",
            }
            filled = []
            for metric_src, field in fmap.items():
                got = fins.get(metric_src)
                if got:
                    before = qb["financials"].get(field)
                    qb["financials"][field] = merge_field(
                        before, got["value_usd"],
                        src_tag, qb["sources"], field, url,
                    )
                    if before is None and qb["financials"][field] is not None:
                        filled.append(f"{field}={got['value_usd']}")

            # Themes (dedupe)
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

            # Tags union
            existing_tags = set(qb.get("insight_tags") or [])
            for t in tags_new:
                existing_tags.add(t)
            qb["insight_tags"] = sorted(existing_tags)[:10]

            # Segments (only fill nulls)
            seg_block = qb.get("segments") or {}
            for col, v in seg_new.items():
                if seg_block.get(col) in (None, 0):
                    seg_block[col] = v
            qb["segments"] = seg_block

            # Top-trend pointer (current quarter only)
            if q == current_q and title and len(qb["top_trends"]) < 8:
                qb["top_trends"].append({
                    "headline": title[:200],
                    "url": url,
                    "source": art.get("source", ""),
                })

            # DEBUG log per successful extraction
            print(f"     ⟶ {q}: extracted {{{', '.join(filled) or 'no new fields'}}} "
                  f"tags={tags_new} segs={list(seg_new.keys())}")
            time.sleep(0.5)

        # Mirror latest-quarter themes into opinion_mining
        latest = block["quarters"].get(current_q, {})
        t = latest.get("themes", {})
        om_entry = out["opinion_mining"][key]
        if t.get("key_initiatives"):   om_entry["initiatives"]        = t["key_initiatives"][:3]
        if t.get("cost_pressures"):    om_entry["cost_pressures"]     = t["cost_pressures"][:3]
        if t.get("strategic_insights"):om_entry["strategic_insights"] = t["strategic_insights"][:3]

        time.sleep(0.8)

    # ── Build AUM summary rows for the current quarter ──
    TRAD_SEG_COLS = ["equity_aum_usd", "fixed_income_aum_usd",
                     "alternatives_aum_usd", "multi_asset_aum_usd", "other_aum_usd"]
    PE_SEG_COLS   = ["real_estate_aum_usd", "private_equity_aum_usd",
                     "credit_insurance_aum_usd", "multi_asset_aum_usd", "other_aum_usd"]

    trad_rows, pe_rows = [], []
    for key, block in out["clients"].items():
        q = block["quarters"].get(current_q, {}) or {}
        fins = q.get("financials", {}) or {}
        rel  = q.get("msci_relationship", {}) or {}
        segs = q.get("segments", {}) or {}
        is_trad = block["category"] == "Traditional"
        seg_cols = TRAD_SEG_COLS if is_trad else PE_SEG_COLS

        row = {
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
        for col in seg_cols:
            row[col] = segs.get(col)

        (trad_rows if is_trad else pe_rows).append(row)

    out["aum_summary"]["traditional"]   = trad_rows
    out["aum_summary"]["pe_alternative"] = pe_rows

    # ── Validation: same value across quarters would be suspicious ──
    warnings = validate_no_duplication(out["clients"], quarters)
    if warnings:
        print("\n  Validation warnings:")
        for w in warnings:
            print(w)
    else:
        print("\n  Validation: ✓ No duplicate financials detected across quarters.")

    # ── Save ──
    save_data(out, OUTPUT_FILE)
    print("\n  ----- Summary -----")
    print(f"  Clients tracked:  {len(out['clients'])}")
    print(f"  Quarters covered: {quarters}")
    print(f"  Fund families:    {len(FUND_FAMILIES)}")
    filled_count = 0
    for k, b in out["clients"].items():
        for q in quarters:
            fs = (b["quarters"].get(q) or {}).get("financials") or {}
            if any(v is not None for v in fs.values()):
                filled_count += 1
    print(f"  Client-quarters with ≥1 financial: {filled_count}/{len(out['clients'])*len(quarters)}")
    print("=" * 72)


if __name__ == "__main__":
    main()
