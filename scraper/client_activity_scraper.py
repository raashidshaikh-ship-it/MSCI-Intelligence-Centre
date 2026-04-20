# Client Activity Scraper — MSCI Intelligence
# =============================================
# Tracks top 30+ MSCI institutional clients via Google News RSS.
# Enriches with opportunity type, deal value, competitor risk.
# Writes to: data/client_activity.json
# Run: python scraper/client_activity_scraper.py
# Deps: pip install requests beautifulsoup4 lxml

import requests, json, os, re, time, hashlib
from bs4 import BeautifulSoup
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "client_activity.json")

# ── TOP MSCI INSTITUTIONAL CLIENTS ──
CLIENTS = {
    "BlackRock": {"type": "Asset Manager", "queries": ["BlackRock ETF MSCI", "BlackRock iShares index mandate"], "tier": 1},
    "Vanguard": {"type": "Asset Manager", "queries": ["Vanguard index fund benchmark", "Vanguard MSCI ETF"], "tier": 1},
    "State Street": {"type": "Asset Manager", "queries": ["State Street SPDR ETF index", "State Street Global Advisors mandate"], "tier": 1},
    "Norges Bank": {"type": "Sovereign Wealth", "queries": ["Norges Bank Investment Management benchmark", "Norway sovereign fund index"], "tier": 1},
    "GIC": {"type": "Sovereign Wealth", "queries": ["GIC Singapore sovereign fund investment"], "tier": 1},
    "ADIA": {"type": "Sovereign Wealth", "queries": ["Abu Dhabi Investment Authority ADIA allocation"], "tier": 1},
    "CalPERS": {"type": "Pension Fund", "queries": ["CalPERS benchmark index allocation"], "tier": 1},
    "CalSTRS": {"type": "Pension Fund", "queries": ["CalSTRS investment benchmark"], "tier": 2},
    "CPPIB": {"type": "Pension Fund", "queries": ["Canada Pension Plan Investment Board benchmark"], "tier": 1},
    "GPIF": {"type": "Pension Fund", "queries": ["Japan GPIF pension fund index ESG"], "tier": 1},
    "Amundi": {"type": "Asset Manager", "queries": ["Amundi ETF index MSCI benchmark"], "tier": 2},
    "DWS": {"type": "Asset Manager", "queries": ["DWS Xtrackers ETF MSCI index"], "tier": 2},
    "Invesco": {"type": "Asset Manager", "queries": ["Invesco ETF index fund mandate"], "tier": 2},
    "Fidelity": {"type": "Asset Manager", "queries": ["Fidelity index fund benchmark"], "tier": 2},
    "JP Morgan Asset": {"type": "Asset Manager", "queries": ["JP Morgan Asset Management ETF index"], "tier": 1},
    "Goldman Sachs AM": {"type": "Asset Manager", "queries": ["Goldman Sachs Asset Management index fund"], "tier": 2},
    "UBS Asset": {"type": "Asset Manager", "queries": ["UBS Asset Management ETF index"], "tier": 2},
    "BNP Paribas AM": {"type": "Asset Manager", "queries": ["BNP Paribas Asset Management index"], "tier": 2},
    "Schroders": {"type": "Asset Manager", "queries": ["Schroders investment ESG benchmark"], "tier": 2},
    "APG": {"type": "Pension Fund", "queries": ["APG Netherlands pension fund index"], "tier": 2},
    "Ontario Teachers": {"type": "Pension Fund", "queries": ["Ontario Teachers Pension Plan investment"], "tier": 2},
    "PFA Pension": {"type": "Pension Fund", "queries": ["PFA Denmark pension allocation"], "tier": 2},
    "AustralianSuper": {"type": "Pension Fund", "queries": ["AustralianSuper investment benchmark index"], "tier": 2},
    "PIMCO": {"type": "Asset Manager", "queries": ["PIMCO fixed income index allocation"], "tier": 2},
    "Wellington": {"type": "Asset Manager", "queries": ["Wellington Management investment mandate"], "tier": 2},
    "T. Rowe Price": {"type": "Asset Manager", "queries": ["T Rowe Price index benchmark fund"], "tier": 2},
    "Northern Trust": {"type": "Asset Manager", "queries": ["Northern Trust index fund passive"], "tier": 2},
    "Legal & General": {"type": "Asset Manager", "queries": ["Legal General LGIM index fund MSCI"], "tier": 1},
    "Robeco": {"type": "Asset Manager", "queries": ["Robeco ESG sustainable index"], "tier": 2},
    "Nuveen": {"type": "Asset Manager", "queries": ["Nuveen TIAA investment index benchmark"], "tier": 2},
}

# ── ACTIVITY KEYWORDS ──
ACTIVITY_KEYWORDS = {
    "Mandate Award": ["mandate", "awarded", "selected", "appointed", "chosen", "RFP", "contract"],
    "Fund Launch": ["launch", "launches", "new fund", "new ETF", "new product", "debut", "introduces"],
    "Allocation Shift": ["allocation", "reallocate", "shift", "pivot", "increase exposure", "reduce exposure", "overweight", "underweight"],
    "Benchmark Change": ["benchmark", "index switch", "migrate", "transition", "rebalance", "tracking"],
    "ESG Initiative": ["ESG", "sustainability", "climate", "net zero", "carbon", "responsible investing", "impact"],
    "Partnership": ["partnership", "collaboration", "joint venture", "strategic alliance", "agreement"],
    "M&A / Expansion": ["acquisition", "acquire", "merge", "expand", "growth", "new market", "enter"],
    "Technology": ["AI", "technology", "platform", "digital", "automation", "data", "analytics"],
}

MSCI_PRODUCT_KEYWORDS = {
    "MSCI World": ["msci world", "msci acwi"],
    "MSCI EM": ["msci emerging", "msci em ", "emerging markets index"],
    "MSCI ESG": ["msci esg", "esg rating", "sustainability rating"],
    "MSCI Climate": ["msci climate", "climate index", "net zero index", "carbon"],
    "MSCI Factor": ["factor index", "factor model", "barra", "minimum volatility", "quality factor"],
    "MSCI Private Assets": ["private asset", "private equity index", "private credit"],
    "MSCI Analytics": ["portfolio analytics", "risk model", "riskmetrics"],
    "MSCI Custom Index": ["custom index", "direct indexing", "bespoke index"],
}

COMPETITOR_NAMES = ["S&P Global", "FTSE Russell", "Bloomberg", "Morningstar", "FactSet",
                    "Moody's", "Refinitiv", "ISS", "Qontigo", "Verisk", "ICE"]

RELEVANCE_KEYWORDS = ["index", "ETF", "benchmark", "mandate", "allocation", "fund", "investment",
                      "portfolio", "ESG", "climate", "passive", "active", "asset", "equity",
                      "fixed income", "emerging", "sustainable", "factor", "risk"]


def make_id(text):
    return hashlib.md5(text.lower()[:80].encode()).hexdigest()[:12]


def is_relevant(title, text=""):
    combined = (title + " " + (text or "")[:1000]).lower()
    return sum(1 for kw in RELEVANCE_KEYWORDS if kw in combined) >= 2


def detect_activity_type(title, text=""):
    combined = (title + " " + (text or "")[:1500]).lower()
    scores = {}
    for act_type, keywords in ACTIVITY_KEYWORDS.items():
        count = sum(1 for kw in keywords if kw.lower() in combined)
        if count > 0:
            scores[act_type] = count
    if not scores:
        return "General Activity"
    return max(scores, key=scores.get)


def detect_msci_link(title, text=""):
    combined = (title + " " + (text or "")[:2000]).lower()
    if "msci" in combined:
        for product, keywords in MSCI_PRODUCT_KEYWORDS.items():
            if any(kw in combined for kw in keywords):
                return True, product
        return True, "MSCI (general)"
    return False, "N/A"


def detect_opportunity(client_info, activity_type, msci_linked, title, text=""):
    combined = (title + " " + (text or "")).lower()
    if msci_linked:
        if any(w in combined for w in ["expand", "increase", "new", "additional", "launch"]):
            return "Upsell", "Client expanding MSCI usage — pitch complementary products"
        return "Defend", "Existing MSCI relationship — monitor for renewal signals"
    if any(w in combined for w in ["RFP", "mandate", "benchmark selection", "shortlist"]):
        return "New Client", "Active mandate search — opportunity for MSCI proposal"
    if any(w in combined for w in ["ESG", "climate", "sustainability"]):
        return "Cross-sell", "ESG/Climate interest — pitch MSCI ESG Ratings or Climate indexes"
    if any(w in combined for w in ["private", "alternative", "real estate"]):
        return "Cross-sell", "Private assets interest — pitch MSCI Private Asset Solutions"
    return "Monitor", "Track for future opportunity signals"


def detect_competitor_risk(title, text=""):
    combined = (title + " " + (text or "")[:2000]).lower()
    risks = []
    for comp in COMPETITOR_NAMES:
        if comp.lower() in combined:
            risks.append(comp)
    return ", ".join(risks) if risks else "None detected"


def extract_deal_value(title, text=""):
    combined = title + " " + (text or "")
    patterns = [
        r'\$[\d,.]+\s*(?:billion|bn|B)\b', r'\$[\d,.]+\s*(?:million|mn|M)\b',
        r'\$[\d,.]+\s*(?:trillion|tn|T)\b', r'€[\d,.]+\s*(?:billion|bn|B)\b',
        r'£[\d,.]+\s*(?:billion|bn|B)\b', r'AUM\s+(?:of\s+)?\$[\d,.]+',
        r'\$[\d,.]+\s*(?:AUM|assets)',
    ]
    for p in patterns:
        match = re.search(p, combined, re.IGNORECASE)
        if match:
            return match.group(0)
    return "N/A"


def scrape_client_news(client_name, queries, max_per_query=3):
    articles = []
    for query in queries:
        encoded_q = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded_q}+when:7d&hl=en-US&gl=US&ceid=US:en"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15, verify=False)
            soup = BeautifulSoup(r.content, "xml")
            for item in soup.find_all("item")[:max_per_query]:
                title = item.title.text.strip() if item.title else ""
                source = item.source.text.strip() if item.source else ""
                link = item.link.text.strip() if item.link else ""
                pub_date = item.pubDate.text.strip() if item.pubDate else ""
                if title:
                    articles.append({
                        "title": title, "source": source, "url": link,
                        "published": pub_date, "client": client_name,
                    })
        except Exception as e:
            print(f"     [!] Query '{query}' failed: {e}")
        time.sleep(0.5)
    return articles


def fetch_text(url):
    if not url:
        return ""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, verify=False, allow_redirects=True)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        paragraphs = soup.find_all("p")
        text = " ".join(p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 40)
        return re.sub(r"\s+", " ", text).strip()[:5000]
    except:
        return ""


def detect_region(title, text=""):
    combined = (title + " " + (text or "")[:2000]).lower()
    regions = {
        "US": ["u.s.", "united states", "america", "new york", "wall street"],
        "Europe": ["europe", "eu ", "uk ", "london", "germany", "france"],
        "Asia-Pacific": ["asia", "china", "japan", "india", "singapore", "hong kong", "apac"],
        "Middle East": ["middle east", "saudi", "uae", "dubai", "qatar"],
        "Global": ["global", "worldwide", "international"],
    }
    for region, keywords in regions.items():
        if any(kw in combined for kw in keywords):
            return region
    return "Global"


def score_priority(client_info, activity_type, msci_linked, opp_type):
    score = 0
    if client_info.get("tier") == 1:
        score += 2
    if msci_linked:
        score += 2
    if activity_type in ("Mandate Award", "Benchmark Change"):
        score += 2
    if opp_type in ("New Client", "Upsell"):
        score += 1
    if activity_type == "ESG Initiative":
        score += 1
    if score >= 5:
        return "Critical"
    elif score >= 3:
        return "High"
    elif score >= 1:
        return "Medium"
    return "Low"


def load_existing(path):
    if not os.path.exists(path):
        return {"last_updated": None, "activities": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"last_updated": None, "activities": []}


def save_data(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Saved to {path}")


def main():
    print("=" * 65)
    print("  MSCI CLIENT ACTIVITY SCRAPER")
    print(f"  {datetime.now().strftime('%A, %B %d, %Y %I:%M %p')}")
    print("=" * 65)
    print(f"  Tracking {len(CLIENTS)} clients | Last 7 days")

    existing = load_existing(OUTPUT_FILE)
    existing_titles = {a.get("title", "").lower()[:60] for a in existing.get("activities", [])}

    all_articles = []
    for client_name, info in CLIENTS.items():
        print(f"\n  Scanning {client_name} ({info['type']})...")
        results = scrape_client_news(client_name, info["queries"])
        for r in results:
            r["client_type"] = info["type"]
        print(f"  -> {len(results)} articles")
        all_articles.extend(results)

    # Deduplicate
    seen = set()
    unique = []
    for a in all_articles:
        key = a["title"].lower()[:60]
        if key not in seen:
            seen.add(key)
            unique.append(a)
    unique = [a for a in unique if a["title"].lower()[:60] not in existing_titles]
    print(f"\n  Unique new articles: {len(unique)}")

    if not unique:
        existing["last_updated"] = datetime.utcnow().isoformat() + "Z"
        save_data(existing, OUTPUT_FILE)
        print("  No new client activity found.")
        return

    # Enrich
    enriched = []
    for i, a in enumerate(unique):
        print(f"  [{i+1}/{len(unique)}] [{a['client']}] {a['title'][:55]}...")
        text = fetch_text(a["url"])

        if not is_relevant(a["title"], text):
            print(f"     SKIP (not relevant)")
            continue

        a["id"] = make_id(a["title"])
        activity_type = detect_activity_type(a["title"], text)
        msci_linked, msci_product = detect_msci_link(a["title"], text)
        opp_type, opp_signal = detect_opportunity(
            CLIENTS.get(a["client"], {}), activity_type, msci_linked, a["title"], text
        )

        a["activity_type"] = activity_type
        a["msci_linked"] = msci_linked
        a["msci_product"] = msci_product
        a["deal_value"] = extract_deal_value(a["title"], text)
        a["opportunity_type"] = opp_type
        a["opportunity_signal"] = opp_signal
        a["competitor_risk"] = detect_competitor_risk(a["title"], text)
        a["region"] = detect_region(a["title"], text)
        a["priority"] = score_priority(
            CLIENTS.get(a["client"], {}), activity_type, msci_linked, opp_type
        )
        a["scraped_at"] = datetime.utcnow().isoformat() + "Z"

        print(f"     {activity_type} | MSCI: {msci_linked} | {opp_type} | {a['priority']}")
        enriched.append(a)
        time.sleep(1)

    all_stored = enriched + existing.get("activities", [])
    all_stored = all_stored[:300]

    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "total_activities": len(all_stored),
        "clients_tracked": len(CLIENTS),
        "activities": all_stored
    }
    save_data(output, OUTPUT_FILE)
    print(f"\n  {len(enriched)} new activities added. Total: {len(all_stored)}")
    print("=" * 65)


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()
