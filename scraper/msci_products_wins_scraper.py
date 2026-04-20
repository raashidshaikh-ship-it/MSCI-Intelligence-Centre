# MSCI Products & Wins Scraper
# ================================================
# Tracks MSCI's own activity: product launches, acquisitions,
# partnerships, index milestones, and confirmed client wins.
# Writes to: data/msci_products_wins.json
# Run: python scraper/msci_products_wins_scraper.py
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
OUTPUT_FILE = os.path.join(DATA_DIR, "msci_products_wins.json")

# ── SEARCH QUERIES (Google News RSS) ──
PRODUCT_QUERIES = [
    "MSCI new index launch",
    "MSCI product launch 2026",
    "MSCI IndexAI Insights",
    "MSCI analytics platform new",
    "MSCI ESG ratings update",
    "MSCI climate index new",
    "MSCI private assets solution",
    "MSCI custom index solution",
    "MSCI factor model update",
    "MSCI acquisition",
    "MSCI partnership agreement",
    "MSCI technology AI innovation",
]

WIN_QUERIES = [
    "MSCI ETF agreement",
    "MSCI benchmark mandate",
    "MSCI client win",
    "MSCI index licensing deal",
    "MSCI iShares agreement",
    "MSCI ETF launch tracking",
    "MSCI benchmark selected",
    "MSCI NYSE options listing",
]

MILESTONE_QUERIES = [
    "MSCI index review rebalance",
    "MSCI reclassification consultation",
    "MSCI earnings revenue quarterly",
    "MSCI AUM assets benchmarked",
    "MSCI Greece developed market",
]

# ── CLASSIFICATION ──
PRODUCT_LINES = {
    "Index Solutions": ["index", "indices", "benchmark", "rebalance", "custom index", "direct indexing"],
    "ESG & Climate": ["esg", "sustainability", "climate", "carbon", "net zero", "green", "sfdr"],
    "Analytics & Risk": ["analytics", "risk model", "barra", "factor", "portfolio", "riskmetrics"],
    "Private Assets": ["private asset", "private equity", "private credit", "real estate", "secondaries"],
    "Technology & AI": ["ai", "artificial intelligence", "indexai", "machine learning", "platform", "api"],
    "Data & Content": ["data", "content", "pricing", "valuation", "reference data"],
}

LAUNCH_TYPES = {
    "New Launch": ["launch", "launches", "launched", "unveil", "introduces", "debuts", "new product", "new index"],
    "Acquisition": ["acquire", "acquisition", "acquires", "acquired", "buyout", "buys", "purchase"],
    "Enhancement": ["enhance", "update", "upgrade", "expand", "extends", "improve", "adds"],
    "Partnership": ["partnership", "agreement", "collaboration", "alliance", "deal", "joint"],
    "Consultation": ["consultation", "proposal", "reclassification", "review", "consultation feedback"],
    "Milestone": ["record", "milestone", "AUM", "earnings", "revenue", "quarterly results"],
}

SECTION_TYPES = {
    "Product": ["launch", "product", "platform", "solution", "tool", "index new", "introduces", "unveil"],
    "Win": ["ETF agreement", "mandate", "client win", "licensing", "benchmark selected", "iShares", "options listing"],
    "Milestone": ["earnings", "revenue", "quarterly", "AUM", "rebalance", "review", "reclassification"],
}


def make_id(text):
    return hashlib.md5(text.lower()[:80].encode()).hexdigest()[:12]


def classify_product_line(title, text=""):
    combined = (title + " " + (text or "")[:1500]).lower()
    scores = {}
    for line, keywords in PRODUCT_LINES.items():
        count = sum(1 for kw in keywords if kw in combined)
        if count:
            scores[line] = count
    if not scores:
        return "General"
    return max(scores, key=scores.get)


def classify_launch_type(title, text=""):
    combined = (title + " " + (text or "")[:1500]).lower()
    for lt, keywords in LAUNCH_TYPES.items():
        if any(kw in combined for kw in keywords):
            return lt
    return "Announcement"


def classify_section(title, text=""):
    combined = (title + " " + (text or "")[:1000]).lower()
    for section, keywords in SECTION_TYPES.items():
        if any(kw in combined for kw in keywords):
            return section
    return "Product"


def extract_competitive_edge(title, text=""):
    combined = (title + " " + (text or "")).lower()
    edges = []
    if "first" in combined or "only" in combined:
        edges.append("First-mover / Only provider")
    if "ai" in combined or "artificial intelligence" in combined:
        edges.append("AI-powered innovation")
    if "real-time" in combined or "real time" in combined:
        edges.append("Real-time capability")
    if "integration" in combined or "seamless" in combined:
        edges.append("Integrated platform approach")
    if "proprietary" in combined or "unique data" in combined:
        edges.append("Proprietary data advantage")
    return "; ".join(edges[:3]) if edges else "N/A"


def extract_deal_info(title, text=""):
    combined = title + " " + (text or "")
    value_patterns = [
        r'\$[\d,.]+\s*(?:billion|bn|B)\b', r'\$[\d,.]+\s*(?:million|mn|M)\b',
        r'\$[\d,.]+\s*(?:trillion|tn|T)\b',
    ]
    value = "N/A"
    for p in value_patterns:
        match = re.search(p, combined, re.IGNORECASE)
        if match:
            value = match.group(0)
            break
    term_patterns = [r'through\s+(\d{4})', r'until\s+(\d{4})', r'(\d+)[\-\s]year']
    term = "N/A"
    for p in term_patterns:
        match = re.search(p, combined, re.IGNORECASE)
        if match:
            term = match.group(0)
            break
    return value, term


def scrape_queries(queries, max_per_query=3):
    articles = []
    for query in queries:
        encoded_q = requests.utils.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded_q}+when:30d&hl=en-US&gl=US&ceid=US:en"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15, verify=False)
            soup = BeautifulSoup(r.content, "xml")
            for item in soup.find_all("item")[:max_per_query]:
                title = item.title.text.strip() if item.title else ""
                source = item.source.text.strip() if item.source else ""
                link = item.link.text.strip() if item.link else ""
                pub_date = item.pubDate.text.strip() if item.pubDate else ""
                if title and "msci" in title.lower():
                    articles.append({
                        "title": title, "source": source,
                        "url": link, "published": pub_date,
                    })
        except Exception as e:
            print(f"     [!] '{query}' failed: {e}")
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


def generate_summary(title, text):
    if not text or len(text) < 100:
        return "Details not available."
    sentences = re.split(r"(?<=[.!?])\s+", text)
    skip = ["cookie", "subscribe", "sign up", "newsletter", "privacy", "copyright"]
    clean = [s.strip() for s in sentences if 30 < len(s.strip()) < 400 and not any(sk in s.lower() for sk in skip)]
    summary = " ".join(clean[:4])
    return summary[:600] + "..." if len(summary) > 600 else summary if summary else "Details not available."


def load_existing(path):
    if not os.path.exists(path):
        return {"last_updated": None, "products": [], "wins": [], "milestones": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"last_updated": None, "products": [], "wins": [], "milestones": []}


def save_data(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Saved to {path}")


def main():
    print("=" * 65)
    print("  MSCI PRODUCTS & WINS SCRAPER")
    print(f"  {datetime.now().strftime('%A, %B %d, %Y %I:%M %p')}")
    print("=" * 65)

    existing = load_existing(OUTPUT_FILE)
    all_existing_titles = set()
    for section in ["products", "wins", "milestones"]:
        for a in existing.get(section, []):
            all_existing_titles.add(a.get("title", "").lower()[:60])

    # Scrape all query groups
    print("\n  Scraping product launches...")
    prod_raw = scrape_queries(PRODUCT_QUERIES)
    print(f"  -> {len(prod_raw)} articles")

    print("  Scraping client wins...")
    win_raw = scrape_queries(WIN_QUERIES)
    print(f"  -> {len(win_raw)} articles")

    print("  Scraping milestones...")
    mile_raw = scrape_queries(MILESTONE_QUERIES)
    print(f"  -> {len(mile_raw)} articles")

    all_raw = prod_raw + win_raw + mile_raw

    # Deduplicate
    seen = set()
    unique = []
    for a in all_raw:
        key = a["title"].lower()[:60]
        if key not in seen and key not in all_existing_titles:
            seen.add(key)
            unique.append(a)

    print(f"\n  Unique new articles: {len(unique)}")
    if not unique:
        existing["last_updated"] = datetime.utcnow().isoformat() + "Z"
        save_data(existing, OUTPUT_FILE)
        return

    # Enrich and classify
    new_products, new_wins, new_milestones = [], [], []
    for i, a in enumerate(unique):
        print(f"  [{i+1}/{len(unique)}] {a['title'][:60]}...")
        text = fetch_text(a["url"])
        section = classify_section(a["title"], text)

        a["id"] = make_id(a["title"])
        a["product_line"] = classify_product_line(a["title"], text)
        a["launch_type"] = classify_launch_type(a["title"], text)
        a["section"] = section
        a["summary"] = generate_summary(a["title"], text)
        a["competitive_edge"] = extract_competitive_edge(a["title"], text)
        a["scraped_at"] = datetime.utcnow().isoformat() + "Z"

        if section == "Win":
            value, term = extract_deal_info(a["title"], text)
            a["deal_value"] = value
            a["deal_term"] = term
            new_wins.append(a)
        elif section == "Milestone":
            new_milestones.append(a)
        else:
            new_products.append(a)

        print(f"     {section} | {a['product_line']} | {a['launch_type']}")
        time.sleep(1)

    # Merge
    all_products = new_products + existing.get("products", [])
    all_wins = new_wins + existing.get("wins", [])
    all_milestones = new_milestones + existing.get("milestones", [])

    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "total_items": len(all_products) + len(all_wins) + len(all_milestones),
        "products": all_products[:200],
        "wins": all_wins[:100],
        "milestones": all_milestones[:100],
    }
    save_data(output, OUTPUT_FILE)
    print(f"\n  Products: {len(new_products)} new, {len(all_products)} total")
    print(f"  Wins: {len(new_wins)} new, {len(all_wins)} total")
    print(f"  Milestones: {len(new_milestones)} new, {len(all_milestones)} total")
    print("=" * 65)


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()
