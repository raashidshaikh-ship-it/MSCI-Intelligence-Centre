# Top Client Earnings Scraper — Quarterly Financials
# ==========================================================
# Pulls quarterly financial data for MSCI + 5 tier-1 competitors.
#
# Sources (in order of reliability):
#   1. SEC EDGAR XBRL  (data.sec.gov)  — revenue / op income / net income
#   2. Company IR pages                — AUM, run-rate, retention (MSCI)
#   3. Google News RSS                 — fallback numeric extraction
#   4. Morningstar public pages        — attempt (often limited)
#
# Output: data/top_client_earnings.json
#
# Design rules (from task spec):
#   - Never hardcode values. If a metric can't be scraped, store null.
#   - Every metric carries a 'sources' array so you can trace provenance.
#   - Schema supports multi-quarter arrays; easy to extend with new quarters.
#   - Idempotent: safe to re-run. Keeps previously-scraped values unless
#     a newer/better source overrides them.
#
# Run:   python scraper/top_client_earnings_scraper.py
# Deps:  requests, beautifulsoup4, lxml  (same as other scrapers)
# ==========================================================

import json
import os
import re
import time
import sys
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup


# ── PATHS ──────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "top_client_earnings.json")


# ── HTTP CONFIG ────────────────────────────────────────────
# SEC EDGAR requires a descriptive User-Agent with contact email.
# If the env var SEC_USER_AGENT is set in GH Actions secrets, use it;
# otherwise fall back to a generic identifier.
SEC_UA = os.environ.get(
    "SEC_USER_AGENT",
    "MSCI-Intelligence-Dashboard contact@example.com",
)
SEC_HEADERS = {"User-Agent": SEC_UA, "Accept-Encoding": "gzip, deflate"}

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ── TARGET COMPANIES ───────────────────────────────────────
# Only MSCI + 5 tier-1 competitors (all publicly listed except LSEG which
# reports semi-annual). CIK is a 10-digit zero-padded SEC identifier.
COMPANIES = {
    "MSCI": {
        "name": "MSCI Inc.",
        "ticker": "MSCI",
        "cik": "0001408198",
        "is_msci": True,
        "segment": "Indices / Analytics / ESG & Climate",
        "ir_pages": [
            "https://ir.msci.com/press-releases",
            "https://ir.msci.com/financial-information/quarterly-results",
        ],
    },
    "S&P Global": {
        "name": "S&P Global Inc.",
        "ticker": "SPGI",
        "cik": "0000064040",
        "is_msci": False,
        "segment": "Indices / Ratings / Data",
        "ir_pages": ["https://investor.spglobal.com/news-releases"],
    },
    "Moody's": {
        "name": "Moody's Corporation",
        "ticker": "MCO",
        "cik": "0001059556",
        "is_msci": False,
        "segment": "Ratings / Analytics",
        "ir_pages": ["https://ir.moodys.com/news-and-financials/press-releases"],
    },
    "Morningstar": {
        "name": "Morningstar, Inc.",
        "ticker": "MORN",
        "cik": "0001289419",
        "is_msci": False,
        "segment": "Research / ESG / Fund Data",
        "ir_pages": ["https://shareholders.morningstar.com/investor-relations/financials/"],
    },
    "FactSet": {
        "name": "FactSet Research Systems Inc.",
        "ticker": "FDS",
        "cik": "0001013237",
        "is_msci": False,
        "segment": "Analytics / Data Platform",
        "ir_pages": ["https://investor.factset.com/financial-information/quarterly-results"],
    },
    "LSEG": {
        "name": "London Stock Exchange Group plc",
        "ticker": "LSE.L",
        "cik": None,  # Not SEC-registered
        "is_msci": False,
        "segment": "Data / Indices / Trading (FTSE Russell, Refinitiv)",
        "ir_pages": [
            "https://www.lseg.com/en/investor-relations/financial-reports",
            "https://www.lseg.com/en/investor-relations/results-centre",
        ],
    },
}


# ── XBRL TAGS TO PULL ──────────────────────────────────────
# We try multiple tags because companies use different us-gaap concepts.
# First hit wins.
XBRL_REVENUE_TAGS = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueNet",
]
XBRL_OPINCOME_TAGS = ["OperatingIncomeLoss"]
XBRL_NETINCOME_TAGS = ["NetIncomeLoss", "ProfitLoss"]


# ── QUARTER UTILITIES ──────────────────────────────────────
def current_quarter_label(d: datetime) -> str:
    q = (d.month - 1) // 3 + 1
    return f"Q{q} {d.year}"


def target_quarters(n: int = 4) -> list:
    """
    Return the last `n` completed quarter labels in chronological order.
    We skip the in-progress quarter because companies haven't reported yet.
    Also skip the just-ended quarter if we're still within ~6 weeks of
    quarter-end (many companies haven't released results yet).
    """
    today = datetime.utcnow()
    q = (today.month - 1) // 3 + 1
    y = today.year
    # Start from the previous quarter (in-progress one isn't reported).
    q -= 1
    if q == 0:
        q = 4
        y -= 1
    # If we're in the first 6 weeks after quarter-end, some companies may
    # not have reported yet — still include that quarter (nulls will fill in).
    out = []
    for _ in range(n):
        out.append(f"Q{q} {y}")
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    out.reverse()
    return out


def period_to_quarter(end_date_str: str, form: str = "") -> str:
    """
    Map an XBRL period end date to a quarter label.
    10-K periods land in Q4; 10-Q periods land in their fiscal quarter.
    """
    try:
        d = datetime.strptime(end_date_str, "%Y-%m-%d")
    except Exception:
        return None
    q = (d.month - 1) // 3 + 1
    return f"Q{q} {d.year}"


def quarter_sort_key(q: str) -> tuple:
    # "Q2 2025" -> (2025, 2)
    try:
        parts = q.split()
        return (int(parts[1]), int(parts[0].lstrip("Q")))
    except Exception:
        return (0, 0)


# ── SEC EDGAR PULLERS ──────────────────────────────────────
def fetch_sec_concept(cik: str, tag: str, taxonomy: str = "us-gaap") -> dict:
    """Pull one XBRL concept for a company. Returns {} if not found."""
    url = (
        f"https://data.sec.gov/api/xbrl/companyconcept/"
        f"CIK{cik}/{taxonomy}/{tag}.json"
    )
    try:
        r = requests.get(url, headers=SEC_HEADERS, timeout=20)
        if r.status_code != 200:
            return {}
        return r.json()
    except Exception as e:
        print(f"     [!] SEC concept fetch failed ({tag}): {e}")
        return {}


def extract_quarterly_values(concept_json: dict, want_quarters: list) -> dict:
    """
    From a SEC companyconcept response, pick USD values whose period end
    maps to any quarter in `want_quarters`. Prefers 10-Q forms; falls back
    to 10-K for Q4. Returns {"Q2 2025": {"value":..., "source":...}}.
    """
    out = {}
    if not concept_json:
        return out
    units = concept_json.get("units", {})
    usd = units.get("USD", []) or units.get("USD/shares", [])
    if not usd:
        return out

    # Group candidate filings by quarter; prefer 10-Q over 10-K, newest filed wins
    candidates = {}
    for entry in usd:
        end = entry.get("end")
        form = entry.get("form", "")
        # Only accept ~3-month durations for quarterly numbers.
        start = entry.get("start")
        if start and end:
            try:
                sd = datetime.strptime(start, "%Y-%m-%d")
                ed = datetime.strptime(end, "%Y-%m-%d")
                days = (ed - sd).days
                # Quarterly = ~90 days; reject YTD or annual sums
                if not (75 <= days <= 100):
                    continue
            except Exception:
                continue
        else:
            continue

        q = period_to_quarter(end, form)
        if q not in want_quarters:
            continue
        val = entry.get("val")
        if val is None:
            continue

        rank = 0 if form.startswith("10-Q") else (1 if form.startswith("10-K") else 2)
        filed = entry.get("filed", "")
        existing = candidates.get(q)
        if existing is None or (rank, existing["filed"]) > (rank, filed):
            # Prefer lower rank (10-Q), then more recently filed
            if existing is None or rank < existing["rank"] or (
                rank == existing["rank"] and filed > existing["filed"]
            ):
                candidates[q] = {
                    "value": float(val),
                    "filed": filed,
                    "form": form,
                    "end": end,
                    "accn": entry.get("accn", ""),
                    "rank": rank,
                }

    for q, c in candidates.items():
        accn_clean = c["accn"].replace("-", "")
        source_url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&"
            f"CIK={concept_json.get('cik')}&type={c['form']}&dateb=&owner=include&count=40"
        )
        out[q] = {
            "value": c["value"],
            "source": {
                "type": "sec_edgar",
                "form": c["form"],
                "period_end": c["end"],
                "filed": c["filed"],
                "url": source_url,
            },
        }
    return out


def pull_sec_metric(cik: str, tag_list: list, want_quarters: list) -> dict:
    """Try each tag until one returns data. Return combined per-quarter dict."""
    for tag in tag_list:
        data = fetch_sec_concept(cik, tag)
        quarterly = extract_quarterly_values(data, want_quarters)
        if quarterly:
            return quarterly
        time.sleep(0.2)  # be polite to SEC
    return {}


# ── MSCI-SPECIFIC METRICS (AUM / RUN RATE) ─────────────────
# These are disclosed in MSCI press releases, not XBRL. We pull the latest
# earnings press release from the IR page and regex the numbers.

RUN_RATE_PATTERNS = [
    r"[Tt]otal\s+run\s+rate.{0,40}?\$?([\d,]+\.?\d*)\s*(million|billion|m|bn|b)",
    r"run\s+rate.{0,40}?\$?([\d,]+\.?\d*)\s*(million|billion|m|bn|b)",
]
AUM_PATTERNS = [
    r"AUM.{0,60}?\$?([\d,]+\.?\d*)\s*(trillion|billion|tn|bn|t|b)",
    r"assets\s+under\s+management.{0,60}?\$?([\d,]+\.?\d*)\s*(trillion|billion|tn|bn|t|b)",
    r"\$?([\d,]+\.?\d*)\s*(trillion|billion|tn|bn|t|b)\s+in\s+(?:assets|AUM)",
]
RETENTION_PATTERNS = [
    r"(?:Retention\s+Rate|retention)\s+(?:was|of)\s+([\d.]+)\s*%",
    r"Retention\s+Rate.{0,20}?([\d.]+)\s*%",
]
ORGANIC_GROWTH_PATTERNS = [
    r"organic\s+(?:subscription\s+)?(?:run[-\s]?rate\s+)?growth\s+(?:was|of)\s+([\d.]+)\s*%",
]


def _scale(value_str: str, unit: str) -> float:
    unit = (unit or "").lower()
    try:
        v = float(value_str.replace(",", ""))
    except Exception:
        return None
    if unit in ("trillion", "tn", "t"):
        return v * 1_000_000_000_000
    if unit in ("billion", "bn", "b"):
        return v * 1_000_000_000
    if unit in ("million", "mn", "m"):
        return v * 1_000_000
    return v


def extract_number(text: str, patterns: list):
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            groups = m.groups()
            if len(groups) == 2:
                return _scale(groups[0], groups[1])
            if len(groups) == 1:
                try:
                    return float(groups[0].replace(",", ""))
                except Exception:
                    continue
    return None


def fetch_ir_text(url: str) -> tuple:
    """Return (plain_text, effective_url) or ("", url)."""
    try:
        r = requests.get(
            url, headers=BROWSER_HEADERS, timeout=20,
            allow_redirects=True, verify=False,
        )
        if r.status_code != 200:
            return "", url
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        text = " ".join(p.get_text(" ").strip() for p in soup.find_all(["p", "li", "td"]))
        return re.sub(r"\s+", " ", text)[:12000], r.url
    except Exception as e:
        print(f"     [!] IR fetch failed for {url}: {e}")
        return "", url


def find_quarterly_press_release_links(index_url: str, max_links: int = 6) -> list:
    """Return up to N links on the IR index page that look like Q-reporting PRs."""
    try:
        r = requests.get(
            index_url, headers=BROWSER_HEADERS, timeout=20,
            allow_redirects=True, verify=False,
        )
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        out = []
        for a in soup.find_all("a", href=True):
            txt = (a.get_text() or "").strip()
            href = a["href"]
            if not txt:
                continue
            if re.search(r"\b(Q[1-4]|first[-\s]quarter|second[-\s]quarter|third[-\s]quarter|fourth[-\s]quarter|full[-\s]year|annual results|interim results)\b", txt, re.I):
                if href.startswith("/"):
                    from urllib.parse import urljoin
                    href = urljoin(index_url, href)
                out.append((txt[:120], href))
            if len(out) >= max_links:
                break
        return out
    except Exception as e:
        print(f"     [!] IR index fetch failed for {index_url}: {e}")
        return []


def parse_ir_release_for_metrics(text: str) -> dict:
    """Extract a handful of finance metrics from press release prose."""
    metrics = {}
    rr = extract_number(text, RUN_RATE_PATTERNS)
    if rr:
        metrics["run_rate_usd"] = rr
    aum = extract_number(text, AUM_PATTERNS)
    if aum:
        metrics["aum_benchmarked_usd"] = aum
    ret = extract_number(text, RETENTION_PATTERNS)
    if ret:
        metrics["retention_rate_pct"] = ret
    og = extract_number(text, ORGANIC_GROWTH_PATTERNS)
    if og:
        metrics["organic_growth_pct"] = og
    return metrics


def press_release_quarter_hint(text: str) -> str:
    """Infer which quarter a press release is reporting on."""
    m = re.search(r"(?:first|Q1)[\s-]*quarter[\s-]*(\d{4})", text, re.I)
    if m:
        return f"Q1 {m.group(1)}"
    m = re.search(r"(?:second|Q2)[\s-]*quarter[\s-]*(\d{4})", text, re.I)
    if m:
        return f"Q2 {m.group(1)}"
    m = re.search(r"(?:third|Q3)[\s-]*quarter[\s-]*(\d{4})", text, re.I)
    if m:
        return f"Q3 {m.group(1)}"
    m = re.search(r"(?:fourth|Q4)[\s-]*quarter[\s-]*(\d{4})", text, re.I)
    if m:
        return f"Q4 {m.group(1)}"
    m = re.search(r"full[\s-]*year[\s-]*(\d{4})", text, re.I)
    if m:
        return f"Q4 {m.group(1)}"
    return None


def scrape_ir_press_releases(company_name: str, company_info: dict, want_quarters: list) -> dict:
    """For each IR index page, find press releases and extract metrics per quarter."""
    results = {q: {} for q in want_quarters}
    for idx_url in company_info.get("ir_pages", []):
        print(f"     IR index: {idx_url}")
        links = find_quarterly_press_release_links(idx_url)
        for (label, href) in links:
            text, eff_url = fetch_ir_text(href)
            if not text:
                continue
            quarter = press_release_quarter_hint(label) or press_release_quarter_hint(text)
            if quarter not in want_quarters:
                continue
            metrics = parse_ir_release_for_metrics(text)
            if metrics:
                print(f"       -> {quarter}: {list(metrics.keys())}")
                for k, v in metrics.items():
                    results[quarter][k] = {
                        "value": v,
                        "source": {"type": "ir_press_release", "url": eff_url, "label": label},
                    }
            time.sleep(0.5)
    return results


# ── NEWS-BASED FALLBACK EXTRACTOR ──────────────────────────
def fetch_google_news(query: str, max_items: int = 5) -> list:
    q = requests.utils.quote(query)
    url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
    try:
        r = requests.get(url, headers=BROWSER_HEADERS, timeout=15, verify=False)
        soup = BeautifulSoup(r.content, "xml")
        items = []
        for it in soup.find_all("item")[:max_items]:
            items.append({
                "title": (it.title.text or "").strip() if it.title else "",
                "link": (it.link.text or "").strip() if it.link else "",
                "source": (it.source.text or "").strip() if it.source else "",
                "published": (it.pubDate.text or "").strip() if it.pubDate else "",
            })
        return items
    except Exception as e:
        print(f"     [!] Google News failed: {e}")
        return []


def news_based_revenue_for_quarter(company: str, quarter: str) -> dict:
    """Extract a revenue figure for {company, quarter} from news headlines."""
    q_name, year = quarter.split()
    queries = [
        f"{company} {q_name} {year} earnings revenue",
        f"{company} {quarter} results",
    ]
    for q in queries:
        items = fetch_google_news(q, 3)
        for it in items:
            title = it["title"]
            # Look for "$NN.N million" or "$NN.N billion" patterns
            m = re.search(
                r"revenue(?:s)?\s+(?:of|rose|grew|increased|climbed|reached|was|totaled|totaling|came in at)?\s*\$?([\d.,]+)\s*(million|billion|bn|m)",
                title, re.IGNORECASE
            )
            if m:
                val = _scale(m.group(1), m.group(2))
                if val:
                    return {
                        "value": val,
                        "source": {
                            "type": "news_headline",
                            "url": it.get("link", ""),
                            "headline": title,
                            "publisher": it.get("source", ""),
                        },
                    }
    return {}


# ── MERGE & PERSIST ────────────────────────────────────────
METRIC_SCHEMA = [
    "revenue_usd",
    "operating_income_usd",
    "net_income_usd",
    "yoy_revenue_growth_pct",
    "aum_benchmarked_usd",
    "run_rate_usd",
    "subscription_run_rate_usd",
    "asset_based_run_rate_usd",
    "retention_rate_pct",
    "organic_growth_pct",
    "net_inflows_usd",
    "client_count",
    "client_revenue_usd",
]


def empty_quarter_record() -> dict:
    rec = {k: None for k in METRIC_SCHEMA}
    rec["sources"] = []
    rec["notes"] = []
    return rec


def merge_metric(record: dict, metric_key: str, new_value: dict):
    """
    Attach a metric value to a quarter record. Preserves existing non-null
    values that came from higher-trust sources (SEC > IR > News > Morningstar).
    """
    if not new_value:
        return
    trust_rank = {
        "sec_edgar": 0,
        "ir_press_release": 1,
        "news_headline": 2,
        "morningstar": 3,
    }
    new_trust = trust_rank.get(new_value["source"]["type"], 9)
    existing_val = record.get(metric_key)
    existing_src = None
    for s in record.get("sources", []):
        if s.get("metric") == metric_key:
            existing_src = s.get("type")
    existing_trust = trust_rank.get(existing_src, 9) if existing_src else 9

    if existing_val is None or new_trust < existing_trust:
        record[metric_key] = new_value["value"]
        # Replace existing source for this metric
        record["sources"] = [s for s in record.get("sources", []) if s.get("metric") != metric_key]
        src_entry = dict(new_value["source"])
        src_entry["metric"] = metric_key
        record["sources"].append(src_entry)


def compute_yoy_growth(company_record: dict, quarters: list):
    """Fill in yoy_revenue_growth_pct where we have this quarter + 4 quarters prior."""
    all_q = sorted(quarters, key=quarter_sort_key)
    for i, q in enumerate(all_q):
        rec = company_record["quarters"].get(q)
        if not rec:
            continue
        if rec.get("yoy_revenue_growth_pct") is not None:
            continue  # already computed or scraped
        this_rev = rec.get("revenue_usd")
        if this_rev is None:
            continue
        # Find year-ago quarter
        try:
            qnum = int(q.split()[0].lstrip("Q"))
            year = int(q.split()[1])
        except Exception:
            continue
        prior_label = f"Q{qnum} {year - 1}"
        prior = company_record["quarters"].get(prior_label)
        if prior and prior.get("revenue_usd"):
            growth = (this_rev - prior["revenue_usd"]) / prior["revenue_usd"] * 100
            rec["yoy_revenue_growth_pct"] = round(growth, 2)
            rec["notes"].append(f"YoY growth computed from {prior_label} revenue.")


def load_existing() -> dict:
    if not os.path.exists(OUTPUT_FILE):
        return None
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_output(data: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    print(f"  Saved to {OUTPUT_FILE}")


# ── MAIN ───────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("  MSCI QUARTERLY FINANCIALS SCRAPER")
    print(f"  {datetime.now().strftime('%A, %B %d, %Y %I:%M %p')}")
    print("=" * 70)

    quarters = target_quarters(4)
    print(f"  Target quarters: {', '.join(quarters)}")

    # Start from previous output if it exists, to preserve already-scraped values.
    existing = load_existing()
    if existing and existing.get("companies"):
        output = existing
        output["last_updated"] = datetime.utcnow().isoformat() + "Z"
        # Ensure newest quarter is in the list
        existing_qs = set(output.get("quarters", []))
        for q in quarters:
            if q not in existing_qs:
                output["quarters"].append(q)
        output["quarters"] = sorted(set(output["quarters"]), key=quarter_sort_key)[-4:]
    else:
        output = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "schema_version": "3.0",
            "quarters": quarters,
            "current_quarter": quarters[-1],
            "companies": {},
            "sources_used": [
                "SEC EDGAR XBRL (data.sec.gov)",
                "Company investor relations pages",
                "Google News RSS",
            ],
        }

    output["current_quarter"] = quarters[-1]

    # Initialize company records with empty quarter slots
    for comp_name, info in COMPANIES.items():
        if comp_name not in output["companies"]:
            output["companies"][comp_name] = {
                "name": info["name"],
                "ticker": info["ticker"],
                "cik": info["cik"],
                "segment": info["segment"],
                "is_msci": info.get("is_msci", False),
                "quarters": {},
            }
        cr = output["companies"][comp_name]
        cr["name"] = info["name"]
        cr["ticker"] = info["ticker"]
        cr["segment"] = info["segment"]
        cr["is_msci"] = info.get("is_msci", False)
        for q in quarters:
            if q not in cr["quarters"]:
                cr["quarters"][q] = empty_quarter_record()

    # ── Phase 1: SEC EDGAR XBRL ──
    print("\n" + "-" * 70)
    print("  Phase 1: SEC EDGAR XBRL")
    print("-" * 70)
    for comp_name, info in COMPANIES.items():
        cik = info.get("cik")
        if not cik:
            print(f"\n  {comp_name}: no CIK (non-US listing); skipping EDGAR.")
            continue
        print(f"\n  {comp_name} (CIK {cik}):")
        rev = pull_sec_metric(cik, XBRL_REVENUE_TAGS, quarters)
        op = pull_sec_metric(cik, XBRL_OPINCOME_TAGS, quarters)
        ni = pull_sec_metric(cik, XBRL_NETINCOME_TAGS, quarters)
        hit = 0
        for q in quarters:
            rec = output["companies"][comp_name]["quarters"][q]
            if q in rev:
                merge_metric(rec, "revenue_usd", rev[q]); hit += 1
            if q in op:
                merge_metric(rec, "operating_income_usd", op[q])
            if q in ni:
                merge_metric(rec, "net_income_usd", ni[q])
        print(f"     -> revenue hits: {sum(1 for q in quarters if q in rev)}/{len(quarters)}")
        print(f"     -> op income hits: {sum(1 for q in quarters if q in op)}/{len(quarters)}")
        time.sleep(0.3)

    # ── Phase 2: IR press releases (MSCI-specific metrics) ──
    print("\n" + "-" * 70)
    print("  Phase 2: IR press releases (AUM, run rate, retention)")
    print("-" * 70)
    for comp_name, info in COMPANIES.items():
        print(f"\n  {comp_name}:")
        try:
            ir_metrics = scrape_ir_press_releases(comp_name, info, quarters)
        except Exception as e:
            print(f"     [!] IR scrape failed: {e}")
            continue
        for q, metrics in ir_metrics.items():
            rec = output["companies"][comp_name]["quarters"][q]
            for k, v in metrics.items():
                merge_metric(rec, k, v)

    # ── Phase 3: News-based revenue fallback ──
    print("\n" + "-" * 70)
    print("  Phase 3: News-based fallback for missing revenue")
    print("-" * 70)
    for comp_name, info in COMPANIES.items():
        for q in quarters:
            rec = output["companies"][comp_name]["quarters"][q]
            if rec.get("revenue_usd") is None:
                print(f"  {comp_name} {q}: trying news fallback...")
                res = news_based_revenue_for_quarter(info["name"], q)
                if res:
                    merge_metric(rec, "revenue_usd", res)
                    print(f"     -> found ${res['value']/1e6:.1f}M")
                time.sleep(0.6)

    # ── Phase 4: Derive YoY growth ──
    print("\n" + "-" * 70)
    print("  Phase 4: Deriving YoY growth where possible")
    print("-" * 70)
    for comp_name in COMPANIES:
        compute_yoy_growth(output["companies"][comp_name], output["quarters"])

    # ── Phase 5: Summary + save ──
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    for comp_name, cr in output["companies"].items():
        filled = 0
        total = 0
        for q in quarters:
            rec = cr["quarters"].get(q, {})
            for k in METRIC_SCHEMA:
                total += 1
                if rec.get(k) is not None:
                    filled += 1
        pct = round(filled / total * 100) if total else 0
        print(f"  {comp_name:15s}  {filled}/{total} metric-cells filled ({pct}%)")

    save_output(output)
    print("=" * 70)


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}")
        sys.exit(1)
