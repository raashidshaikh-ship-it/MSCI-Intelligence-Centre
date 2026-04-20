# Top News Scraper — MSCI Competitor Intelligence
# ====================================================================
# Outputs to: data/top_news.json (relative to repo root)
# Appends new articles each run (no duplicates).
# Run:  python scraper/top_news_scraper.py
# Deps: pip install requests beautifulsoup4 lxml
#
# v2 CHANGES:
#   - Added sector_impact (array of affected sectors with direction + magnitude)
#   - Added stock_impact (array of affected tickers with direction)
#   - Added what_to_watch (forward-looking signal for each article)
#   - Timer changed to 8 hours in scrape.yml (update separately)

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import sys
import re
import time
import json
import hashlib

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Output path — relative to repo root (works in GitHub Actions & locally)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "top_news.json")


# ──────────────────────────────────────────────
# MSCI COMPETITORS
# ──────────────────────────────────────────────
COMPETITORS = {
    "S&P Global": {
        "queries": ["S&P Global financial", "S&P Global indices", "S&P Dow Jones Indices"],
        "segment": "Indices / Ratings / Data",
        "ticker": "SPGI",
    },
    "FTSE Russell": {
        "queries": ["FTSE Russell index", "FTSE Russell LSEG"],
        "segment": "Indices / Benchmarking",
        "ticker": "LSEG",
    },
    "Bloomberg": {
        "queries": ["Bloomberg LP financial", "Bloomberg terminal data", "Bloomberg indices"],
        "segment": "Data / Analytics / Indices",
        "ticker": None,  # Private company
    },
    "Morningstar": {
        "queries": ["Morningstar financial", "Morningstar Sustainalytics ESG", "Morningstar ratings"],
        "segment": "Research / ESG / Ratings",
        "ticker": "MORN",
    },
    "FactSet": {
        "queries": ["FactSet financial data", "FactSet analytics"],
        "segment": "Analytics / Data Platform",
        "ticker": "FDS",
    },
    "Moody's": {
        "queries": ["Moody's analytics", "Moody's ratings financial"],
        "segment": "Ratings / Risk Analytics",
        "ticker": "MCO",
    },
    "London Stock Exchange Group": {
        "queries": ["LSEG financial data", "London Stock Exchange Group"],
        "segment": "Data / Indices / Trading",
        "ticker": "LSEG",
    },
    "Refinitiv": {
        "queries": ["Refinitiv LSEG data"],
        "segment": "Data / Analytics",
        "ticker": "LSEG",
    },
    "ISS": {
        "queries": ["ISS governance ESG proxy", "Institutional Shareholder Services"],
        "segment": "ESG / Governance / Proxy",
        "ticker": None,  # Private (owned by Deutsche Börse)
    },
    "Qontigo": {
        "queries": ["Qontigo STOXX DAX analytics"],
        "segment": "Indices / Risk Models",
        "ticker": "DB1",  # Deutsche Börse parent
    },
    "Verisk": {
        "queries": ["Verisk analytics financial"],
        "segment": "Risk Analytics / Data",
        "ticker": "VRSK",
    },
    "Intercontinental Exchange": {
        "queries": ["ICE Intercontinental Exchange data indices"],
        "segment": "Exchanges / Data / Indices",
        "ticker": "ICE",
    },
}


# ──────────────────────────────────────────────
# SENTIMENT LEXICON (finance-tuned)
# ──────────────────────────────────────────────
POSITIVE_WORDS = {
    "surge", "surges", "surging", "soar", "soars", "soaring", "rally", "rallies", "rallying",
    "gain", "gains", "gained", "jump", "jumps", "jumped", "boom", "booming", "profit", "profits",
    "profitable", "growth", "growing", "grows", "grew", "recover", "recovery", "recovering",
    "upbeat", "optimism", "optimistic", "bullish", "record high", "beat", "beats", "beating",
    "outperform", "outperforms", "upgrade", "upgraded", "strong", "stronger", "strongest",
    "rise", "rises", "rising", "rose", "climb", "climbs", "climbing", "advance", "advances",
    "positive", "improve", "improves", "improved", "improvement", "boost", "boosted", "boosts",
    "exceed", "exceeds", "exceeded", "win", "wins", "winning", "success", "successful",
    "accelerate", "accelerates", "momentum", "breakout", "upside", "dividend", "expansion",
    "launch", "launches", "launched", "partnership", "acquire", "acquisition", "innovation",
}

NEGATIVE_WORDS = {
    "crash", "crashes", "crashing", "plunge", "plunges", "plunging", "drop", "drops", "dropped",
    "fall", "falls", "falling", "fell", "decline", "declines", "declining", "loss", "losses",
    "lose", "losing", "lost", "slump", "slumps", "recession", "downturn", "bearish", "crisis",
    "fear", "fears", "worried", "worry", "concern", "concerns", "risk", "risks", "risky",
    "cut", "cuts", "cutting", "layoff", "layoffs", "slash", "slashes", "tariff", "tariffs",
    "sanction", "sanctions", "inflation", "deficit", "debt", "default", "bankrupt", "bankruptcy",
    "weak", "weaker", "weakest", "miss", "misses", "missed", "downgrade", "downgraded",
    "collapse", "collapses", "volatile", "volatility", "sell-off", "selloff", "warning",
    "warns", "warned", "threat", "threatens", "uncertainty", "negative", "tumble", "tumbles",
    "shrink", "shrinks", "contraction", "stagnation", "shutdown", "fraud", "scandal", "probe",
    "fine", "fined", "penalty", "lawsuit", "sued", "antitrust", "investigation", "regulatory",
}


# ──────────────────────────────────────────────
# CATEGORY KEYWORDS (competitor-focused)
# ──────────────────────────────────────────────
CATEGORY_MAP = {
    "Product Launch": ["launch", "launches", "launched", "unveil", "unveils", "introduce", "introduces",
                       "new product", "new platform", "new tool", "new index", "new solution",
                       "release", "releases", "rollout", "debut", "expand offering"],
    "M&A / Partnerships": ["merger", "acquisition", "acquire", "acquires", "acquired", "deal", "partnership",
                           "partner", "partners", "joint venture", "alliance", "strategic", "collaboration",
                           "buyout", "takeover", "combine", "spinoff", "divest"],
    "Earnings / Financials": ["earnings", "revenue", "profit", "quarterly", "annual", "fiscal", "q1", "q2",
                              "q3", "q4", "eps", "guidance", "forecast", "outlook", "results",
                              "financial results", "beat estimates", "miss estimates", "margin"],
    "ESG / Sustainability": ["esg", "sustainability", "sustainable", "climate", "carbon", "net zero",
                             "green bond", "responsible investing", "social", "governance", "dei",
                             "emissions", "environmental", "sustainalytics", "impact investing"],
    "Indices / Benchmarks": ["index", "indices", "benchmark", "rebalance", "reconstitution", "inclusion",
                             "exclusion", "weighting", "methodology", "etf", "passive", "tracker",
                             "s&p 500", "ftse", "stoxx", "russell"],
    "Technology / AI": ["ai", "artificial intelligence", "machine learning", "technology", "tech",
                        "platform", "cloud", "data analytics", "automation", "api", "digital",
                        "saas", "fintech", "generative ai", "llm", "model"],
    "Regulation / Compliance": ["regulation", "regulatory", "compliance", "sec", "esma", "mifid",
                                "sfdr", "taxonomy", "reporting requirements", "disclosure",
                                "antitrust", "fine", "penalty", "investigation"],
    "Leadership / Talent": ["ceo", "cfo", "appoint", "appointed", "hire", "hired", "resign", "resigned",
                            "departure", "successor", "leadership", "executive", "board", "chairman",
                            "layoff", "layoffs", "restructuring", "headcount"],
    "Market Data / Analytics": ["data", "analytics", "risk model", "factor model", "portfolio analytics",
                                "market data", "pricing", "valuation", "reference data", "real-time",
                                "fixed income", "credit", "derivatives"],
    "Client Wins / Market Share": ["client", "clients", "win", "wins", "mandate", "adoption", "switch",
                                   "migrate", "migration", "market share", "competitive", "displaced"],
}


# ──────────────────────────────────────────────
# REGION KEYWORDS
# ──────────────────────────────────────────────
REGION_MAP = {
    "US": ["u.s.", "us ", "united states", "america", "american", "wall street", "nyse", "nasdaq",
           "sec ", "washington", "new york", "california", "silicon valley"],
    "Europe": ["europe", "european", "eu ", "uk ", "britain", "london", "ftse", "germany",
               "france", "ecb", "esma", "stoxx", "dax", "paris", "frankfurt", "amsterdam"],
    "Asia-Pacific": ["asia", "china", "chinese", "japan", "japanese", "india", "indian",
                     "hong kong", "singapore", "korea", "taiwan", "australia", "apac",
                     "asia-pacific", "tokyo", "shanghai", "mumbai"],
    "Middle East": ["middle east", "saudi", "uae", "dubai", "qatar", "abu dhabi"],
    "Global": ["global", "worldwide", "international", "cross-border"],
}


# ──────────────────────────────────────────────
# RELEVANCE KEYWORDS
# ──────────────────────────────────────────────
RELEVANCE_KEYWORDS = [
    "index", "indices", "data", "analytics", "esg", "rating", "ratings", "benchmark",
    "portfolio", "risk", "factor", "etf", "financial", "asset", "investment", "investor",
    "fund", "equity", "fixed income", "credit", "derivatives", "valuation", "pricing",
    "platform", "product", "launch", "earnings", "revenue", "acquisition", "partnership",
    "client", "governance", "sustainability", "climate", "carbon", "compliance",
    "regulation", "technology", "ai ", "machine learning", "market data", "research",
    "quarterly", "annual", "profit", "ceo", "leadership", "restructur",
]


# ──────────────────────────────────────────────
# IMPORTANCE & IMPACT SCORING
# ──────────────────────────────────────────────
TIER_1_COMPETITORS = {"S&P Global", "Bloomberg", "FTSE Russell", "Morningstar", "Moody's"}
HIGH_IMPACT_CATEGORIES = {"M&A / Partnerships", "Technology / AI", "Client Wins / Market Share"}
CRITICAL_CATEGORIES = {"M&A / Partnerships"}


def score_importance(article):
    """Score importance: Critical / High / Medium / Low."""
    cat = article.get("category", "")
    comp = article.get("competitor", "")
    sentiment = article.get("sentiment", "Neutral")
    confidence = article.get("confidence", 0.5)

    score = 0
    if comp in TIER_1_COMPETITORS:
        score += 2
    if any(hc in cat for hc in HIGH_IMPACT_CATEGORIES):
        score += 2
    if any(cc in cat for cc in CRITICAL_CATEGORIES):
        score += 1
    if sentiment == "Negative" and confidence >= 0.7:
        score += 1
    if sentiment == "Positive" and confidence >= 0.7:
        score += 1

    if score >= 5:
        return "Critical"
    elif score >= 3:
        return "High"
    elif score >= 1:
        return "Medium"
    return "Low"


def score_impact(article):
    """Score MSCI business impact: High / Medium / Low."""
    cat = article.get("category", "")
    comp = article.get("competitor", "")

    score = 0
    if comp in TIER_1_COMPETITORS:
        score += 1
    if any(hc in cat for hc in HIGH_IMPACT_CATEGORIES):
        score += 1
    if "ESG" in cat or "Climate" in cat:
        score += 1
    if "Indices" in cat or "Benchmark" in cat:
        score += 1

    if score >= 3:
        return "High"
    elif score >= 1:
        return "Medium"
    return "Low"


# ──────────────────────────────────────────────
# NEW v2: SECTOR IMPACT ENGINE
# ──────────────────────────────────────────────
# Maps categories to affected MSCI sectors with direction logic

CATEGORY_SECTOR_MAP = {
    "ESG / Sustainability": [
        {"sector": "ESG & Climate", "msci_product": "ESG Ratings, Climate Solutions"},
        {"sector": "Sustainable Investing", "msci_product": "ESG Indexes"},
    ],
    "Indices / Benchmarks": [
        {"sector": "Index & Benchmarks", "msci_product": "MSCI Indexes, Custom Indexes"},
        {"sector": "Passive Investing / ETFs", "msci_product": "Licensed ETF Benchmarks"},
    ],
    "Technology / AI": [
        {"sector": "Technology & Platforms", "msci_product": "Analytics Platform, Data Products"},
    ],
    "M&A / Partnerships": [
        {"sector": "Market Structure", "msci_product": "Competitive positioning across segments"},
    ],
    "Earnings / Financials": [
        {"sector": "Financial Data Industry", "msci_product": "Broad competitive landscape"},
    ],
    "Market Data / Analytics": [
        {"sector": "Data & Analytics", "msci_product": "Barra Models, Portfolio Analytics"},
        {"sector": "Risk Management", "msci_product": "Risk Models, Factor Models"},
    ],
    "Regulation / Compliance": [
        {"sector": "Regulatory & Compliance", "msci_product": "Reporting Solutions, SFDR"},
    ],
    "Leadership / Talent": [
        {"sector": "Talent & Strategy", "msci_product": "Competitive talent landscape"},
    ],
    "Client Wins / Market Share": [
        {"sector": "Client Mandates", "msci_product": "Institutional client base"},
    ],
    "Product Launch": [
        {"sector": "Product Innovation", "msci_product": "Competitive product portfolio"},
    ],
}


def generate_sector_impact(article):
    """Generate sector_impact array based on category, competitor, and sentiment."""
    cat = article.get("category", "General")
    comp = article.get("competitor", "")
    sentiment = article.get("sentiment", "Neutral")
    importance = article.get("importance", "Medium")
    impact_level = article.get("impact_level", "Medium")

    sectors = []
    matched = False

    # Match against each category in the article's category string (may contain " / ")
    for cat_key, sector_list in CATEGORY_SECTOR_MAP.items():
        if cat_key in cat:
            matched = True
            for s in sector_list:
                # Determine direction: competitor strength = negative for MSCI
                if sentiment == "Positive":
                    direction = "negative"  # Competitor getting stronger = bad for MSCI
                elif sentiment == "Negative":
                    direction = "positive"  # Competitor struggling = opportunity for MSCI
                else:
                    direction = "neutral"

                # Override: if it's an earnings report, direction depends on context
                if "Earnings" in cat_key:
                    direction = "neutral"

                # Magnitude from impact_level
                magnitude = impact_level  # High / Medium / Low

                # Build note
                note = f"{comp} activity in {s['sector'].lower()} may affect {s['msci_product']}"

                sectors.append({
                    "sector": s["sector"],
                    "direction": direction,
                    "magnitude": magnitude,
                    "note": note,
                })

    if not matched:
        sectors.append({
            "sector": "General Market",
            "direction": "neutral",
            "magnitude": "Low",
            "note": f"Monitoring {comp} activity for potential MSCI relevance",
        })

    return sectors


# ──────────────────────────────────────────────
# NEW v2: STOCK IMPACT ENGINE
# ──────────────────────────────────────────────
# Generates predicted stock-level impact for the competitor + MSCI

def generate_stock_impact(article):
    """Generate stock_impact array for competitor ticker + MSCI."""
    comp = article.get("competitor", "")
    sentiment = article.get("sentiment", "Neutral")
    cat = article.get("category", "General")
    impact_level = article.get("impact_level", "Medium")

    stocks = []

    # Competitor's own ticker
    comp_info = COMPETITORS.get(comp, {})
    comp_ticker = comp_info.get("ticker")

    if comp_ticker:
        if sentiment == "Positive":
            comp_dir = "positive"
            comp_note = f"Positive development strengthens {comp}'s market position"
        elif sentiment == "Negative":
            comp_dir = "negative"
            comp_note = f"Negative signal may weigh on {comp}'s outlook"
        else:
            comp_dir = "neutral"
            comp_note = f"Mixed signals from {comp}; market impact unclear"

        stocks.append({
            "ticker": comp_ticker,
            "direction": comp_dir,
            "note": comp_note,
        })

    # MSCI impact (always include)
    if impact_level == "High":
        if sentiment == "Positive":
            msci_dir = "negative"
            msci_note = f"Competitive pressure from {comp}'s gains may affect MSCI positioning"
        elif sentiment == "Negative":
            msci_dir = "positive"
            msci_note = f"{comp}'s weakness creates potential opportunity for MSCI"
        else:
            msci_dir = "neutral"
            msci_note = f"Monitor {comp} developments for impact on MSCI competitive landscape"
    elif impact_level == "Medium":
        msci_dir = "neutral"
        msci_note = f"Indirect competitive signal from {comp}; limited near-term MSCI impact"
    else:
        msci_dir = "neutral"
        msci_note = f"Minimal direct competitive overlap with MSCI"

    stocks.append({
        "ticker": "MSCI",
        "direction": msci_dir,
        "note": msci_note,
    })

    # Add related tickers for M&A or partnership stories
    if "M&A" in cat or "Partnership" in cat:
        # Add LSEG if FTSE Russell involved, SPGI if S&P involved, etc.
        related = {
            "S&P Global": "LSEG", "FTSE Russell": "SPGI", "Bloomberg": "SPGI",
            "Morningstar": "SPGI", "Moody's": "SPGI", "FactSet": "VRSK",
        }
        related_ticker = related.get(comp)
        if related_ticker and related_ticker != comp_ticker:
            stocks.append({
                "ticker": related_ticker,
                "direction": "neutral",
                "note": f"May accelerate competitive response from {related_ticker}",
            })

    return stocks


# ──────────────────────────────────────────────
# NEW v2: WHAT TO WATCH ENGINE
# ──────────────────────────────────────────────
# Generates a forward-looking signal for each article

def generate_what_to_watch(article):
    """Generate a forward-looking 'what to watch next' signal."""
    comp = article.get("competitor", "")
    cat = article.get("category", "General")
    sentiment = article.get("sentiment", "Neutral")
    importance = article.get("importance", "Medium")

    signals = []

    if "M&A" in cat:
        signals.append(f"Watch for regulatory approvals and integration timeline announcements from {comp}.")
        signals.append("Monitor whether this triggers similar M&A moves from other index/data providers.")
    elif "Technology" in cat or "AI" in cat:
        signals.append(f"Track {comp}'s product launch timeline and early client adoption signals.")
        signals.append("Watch for MSCI's response in its own AI/technology roadmap.")
    elif "ESG" in cat or "Sustainability" in cat:
        signals.append(f"Monitor institutional adoption of {comp}'s ESG framework vs MSCI ESG Ratings.")
        signals.append("Watch for regulatory endorsement or standard-setting implications.")
    elif "Indices" in cat or "Benchmark" in cat:
        signals.append(f"Track ETF issuer responses and potential benchmark switches from MSCI to {comp}.")
        signals.append("Monitor AUM flows in competing index-linked products.")
    elif "Earnings" in cat:
        signals.append(f"Watch {comp}'s forward guidance and segment-level growth trends.")
        if sentiment == "Positive":
            signals.append("Strong results may attract talent and increase R&D spend in competing products.")
        else:
            signals.append("Weak results may lead to cost cuts or strategic pivots worth monitoring.")
    elif "Client" in cat:
        signals.append(f"Watch for follow-on mandate announcements and competitive displacement patterns.")
        signals.append("Monitor MSCI's own mandate renewal pipeline for at-risk relationships.")
    elif "Leadership" in cat:
        signals.append(f"Track the new leadership's strategic priorities and any product direction shifts at {comp}.")
    elif "Regulation" in cat:
        signals.append("Monitor regulatory timeline and compliance requirements across the industry.")
        signals.append(f"Watch how {comp}'s response positions them vs MSCI in regulated markets.")
    elif "Product Launch" in cat:
        signals.append(f"Track market uptake and client feedback on {comp}'s new offering.")
        signals.append("Watch for competitive response from MSCI or other providers.")
    else:
        signals.append(f"Continue monitoring {comp} for follow-up developments.")

    # Add importance-based urgency signal
    if importance in ("Critical", "High"):
        signals.append("Flag for QBR discussion and executive briefing.")

    return " ".join(signals[:2])  # Cap at 2 sentences to keep it crisp


# ──────────────────────────────────────────────
# MSCI IMPACT TEXT (existing, unchanged)
# ──────────────────────────────────────────────
def generate_msci_impact_text(article):
    """Generate a brief MSCI impact assessment from available metadata."""
    comp = article.get("competitor", "Unknown")
    cat = article.get("category", "General")
    segment = article.get("segment", "")
    sentiment = article.get("sentiment", "Neutral")
    title = article.get("title", "")

    parts = []

    if "M&A" in cat or "acquisition" in title.lower():
        parts.append(f"{comp}'s M&A activity signals strategic expansion that may overlap with MSCI's product lines.")
    elif "Technology" in cat or "AI" in cat:
        parts.append(f"{comp}'s technology investment raises the competitive bar for MSCI's analytics and data platform capabilities.")
    elif "ESG" in cat or "Sustainability" in cat:
        parts.append(f"{comp}'s ESG initiative directly intersects with MSCI's sustainability and climate franchise.")
    elif "Indices" in cat or "Benchmark" in cat:
        parts.append(f"{comp}'s index development competes with MSCI's core benchmarking business.")
    elif "Earnings" in cat:
        if sentiment == "Positive":
            parts.append(f"{comp}'s strong financial performance signals robust competitive positioning and potential for increased investment in competing products.")
        else:
            parts.append(f"{comp}'s financial results may signal shifting market dynamics relevant to MSCI's competitive landscape.")
    elif "Client" in cat:
        parts.append(f"{comp}'s client wins indicate potential market share shifts that MSCI should monitor.")
    elif "Leadership" in cat:
        parts.append(f"Leadership changes at {comp} may signal strategic pivots relevant to MSCI's competitive positioning.")
    else:
        parts.append(f"Activity from {comp} ({segment}) warrants monitoring for potential impact on MSCI's market position.")

    parts.append("Validate with domain experts before strategic action.")
    return " ".join(parts)


# ──────────────────────────────────────────────
# CORE FUNCTIONS (from original scraper)
# ──────────────────────────────────────────────
def is_relevant(title, text=""):
    combined = (title + " " + (text or "")[:1000]).lower()
    matches = sum(1 for kw in RELEVANCE_KEYWORDS if kw in combined)
    return matches >= 2


def scrape_competitor_news(competitor_name, queries, max_per_query=5):
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
                if title and competitor_name.lower().split()[0] in title.lower() or any(
                    q.lower().split()[0] in title.lower() for q in queries
                ):
                    articles.append({
                        "title": title,
                        "source": source,
                        "url": link,
                        "published": pub_date,
                        "competitor": competitor_name,
                    })
        except Exception as e:
            print(f"     [!] Query '{query}' failed: {e}")
        time.sleep(0.5)
    return articles


def fetch_article_text_and_meta(url):
    result = {"text": "", "author": "", "meta_keywords": ""}
    if not url:
        return result
    try:
        r = requests.get(url, headers=HEADERS, timeout=20, verify=False, allow_redirects=True)
        if r.status_code != 200:
            return result
        soup = BeautifulSoup(r.text, "html.parser")

        author = ""
        for meta_name in ["author", "article:author", "og:article:author", "dc.creator", "byl"]:
            tag = soup.find("meta", attrs={"name": meta_name}) or soup.find("meta", attrs={"property": meta_name})
            if tag and tag.get("content"):
                author = tag["content"].strip()
                break
        if not author:
            author_tag = soup.find(attrs={"rel": "author"}) or soup.find(attrs={"class": re.compile(r"author|byline|by-line", re.I)})
            if author_tag:
                author = author_tag.get_text().strip()
        if author.lower().startswith("by "):
            author = author[3:].strip()
        result["author"] = author[:100]

        kw_tag = soup.find("meta", attrs={"name": "keywords"}) or soup.find("meta", attrs={"name": "news_keywords"})
        if kw_tag and kw_tag.get("content"):
            result["meta_keywords"] = kw_tag["content"].strip()[:200]

        for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside", "form", "iframe", "noscript"]):
            tag.decompose()

        selectors = [
            "article", "[class*='article-body']", "[class*='ArticleBody']",
            "[class*='story-body']", "[class*='post-content']", "[class*='entry-content']",
            "[class*='article-content']", "[class*='ArticleContent']", "[class*='RenderBody']",
            "[class*='caas-body']", "[itemprop='articleBody']", "[data-testid='article-body']",
            "main", "[role='main']",
        ]
        text = ""
        for sel in selectors:
            found = soup.select(sel)
            if found:
                paragraphs = found[0].find_all("p")
                text = " ".join(p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 40)
                if len(text) > 200:
                    break
        if len(text) < 200:
            paragraphs = soup.find_all("p")
            text = " ".join(p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 40)

        text = re.sub(r"\s+", " ", text).strip()
        result["text"] = text[:5000]

    except Exception as e:
        print(f"     [!] Could not fetch article: {e}")
    return result


def generate_summary(title, article_text):
    if not article_text or len(article_text) < 100:
        return "Summary not available — article could not be accessed."
    sentences = re.split(r"(?<=[.!?])\s+", article_text)
    skip_patterns = [
        "cookie", "subscribe", "sign up", "newsletter", "click here", "read more",
        "advertisement", "sponsored", "privacy policy", "terms of service",
        "all rights reserved", "copyright", "follow us", "share this", "login",
    ]
    clean = []
    for s in sentences:
        s = s.strip()
        if len(s) < 30 or len(s) > 400:
            continue
        if any(skip in s.lower() for skip in skip_patterns):
            continue
        if title.lower()[:40] in s.lower():
            continue
        clean.append(s)
    if not clean:
        return "Summary not available — article content could not be parsed."
    summary = " ".join(clean[:5])
    if len(summary) > 800:
        summary = summary[:797] + "..."
    return summary


def analyze_sentiment(title, article_text):
    combined = (title + " " + (article_text or "")).lower()
    words = set(re.findall(r"[a-z\-&]+", combined))
    pos_count = len(words & POSITIVE_WORDS)
    neg_count = len(words & NEGATIVE_WORDS)
    total = pos_count + neg_count
    if total == 0:
        return "Neutral", 0.5
    score = pos_count / total
    if score >= 0.65:
        label = "Positive"
    elif score <= 0.35:
        label = "Negative"
    else:
        label = "Mixed"
    confidence = round(abs(score - 0.5) * 2, 2)
    return label, confidence


def detect_category(title, article_text, meta_keywords=""):
    combined = (title + " " + (article_text or "")[:1500] + " " + (meta_keywords or "")).lower()
    scores = {}
    for category, keywords in CATEGORY_MAP.items():
        count = sum(1 for kw in keywords if kw in combined)
        if count > 0:
            scores[category] = count
    if not scores:
        return "General"
    sorted_cats = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top = sorted_cats[0][0]
    if len(sorted_cats) > 1 and sorted_cats[1][1] >= sorted_cats[0][1] * 0.7:
        return f"{top} / {sorted_cats[1][0]}"
    return top


def detect_region(title, article_text):
    combined = (title + " " + (article_text or "")[:2000]).lower()
    scores = {}
    for region, keywords in REGION_MAP.items():
        count = sum(1 for kw in keywords if kw in combined)
        if count > 0:
            scores[region] = count
    if not scores:
        return "Global"
    sorted_r = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top = sorted_r[0][0]
    if len(sorted_r) > 1 and sorted_r[1][1] >= sorted_r[0][1] * 0.6:
        return f"{top} / {sorted_r[1][0]}"
    return top


def calc_word_count(text):
    return len(text.split()) if text else 0


def calc_reading_time(wc):
    if wc == 0:
        return "N/A"
    return f"{max(1, round(wc / 230))} min"


def deduplicate(articles):
    seen = set()
    unique = []
    for a in articles:
        key = a["title"].lower()[:60]
        if key not in seen:
            seen.add(key)
            unique.append(a)
    return unique


def make_id(title):
    """Generate a stable unique ID from headline text."""
    return hashlib.md5(title.lower()[:60].encode()).hexdigest()[:12]


# ──────────────────────────────────────────────
# JSON I/O
# ──────────────────────────────────────────────
def load_existing_data(path):
    if not os.path.exists(path):
        return {"last_updated": None, "articles": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"last_updated": None, "articles": []}


def save_data(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Data saved to {path}")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  MSCI COMPETITOR INTELLIGENCE SCRAPER v2 (JSON)")
    print(f"  {datetime.now().strftime('%A, %B %d, %Y %I:%M %p')}")
    print("=" * 65)
    print(f"  Tracking {len(COMPETITORS)} competitors | Last 7 days")

    existing_data = load_existing_data(OUTPUT_FILE)
    existing_ids = {a.get("id") for a in existing_data.get("articles", [])}
    existing_headlines = {a.get("title", "").lower()[:60] for a in existing_data.get("articles", [])}

    # ── Phase 1: Scrape headlines ──
    all_articles = []
    for comp_name, comp_info in COMPETITORS.items():
        print(f"\n  Scraping {comp_name} ({comp_info['segment']})...")
        results = scrape_competitor_news(comp_name, comp_info["queries"])
        for r in results:
            r["segment"] = comp_info["segment"]
        print(f"  -> {len(results)} articles found")
        all_articles.extend(results)

    unique = deduplicate(all_articles)
    print(f"\n  Total unique articles: {len(unique)}")

    if not unique:
        print("\n  No articles retrieved. Check your internet connection.")
        sys.exit(1)

    new_articles = [a for a in unique if a["title"].lower()[:60] not in existing_headlines]
    print(f"  Already saved: {len(unique) - len(new_articles)}  |  New: {len(new_articles)}")

    if not new_articles:
        print("\n  No new articles to add — file is up to date.")
        existing_data["last_updated"] = datetime.utcnow().isoformat() + "Z"
        save_data(existing_data, OUTPUT_FILE)
        print("=" * 65)
        return

    # ── Phase 2: Fetch & Enrich ──
    print("\n" + "-" * 65)
    print("  FETCHING & ENRICHING ARTICLES...")
    print("-" * 65)

    relevant_articles = []
    for i, a in enumerate(new_articles):
        print(f"\n  [{i+1}/{len(new_articles)}] [{a['competitor']}] {a['title'][:55]}...")

        meta = fetch_article_text_and_meta(a["url"])
        article_text = meta["text"]

        if not is_relevant(a["title"], article_text):
            print(f"     SKIP (not relevant to financial data/index industry)")
            continue

        a["id"] = make_id(a["title"])
        a["author"] = meta["author"] or "N/A"
        a["summary"] = generate_summary(a["title"], article_text)
        a["category"] = detect_category(a["title"], article_text, meta["meta_keywords"])
        a["region"] = detect_region(a["title"], article_text)

        sentiment, confidence = analyze_sentiment(a["title"], article_text)
        a["sentiment"] = sentiment
        a["confidence"] = confidence

        wc = calc_word_count(article_text)
        a["word_count"] = wc
        a["reading_time"] = calc_reading_time(wc)
        a["scraped_at"] = datetime.utcnow().isoformat() + "Z"

        # Scoring (must come before impact generation)
        a["importance"] = score_importance(a)
        a["impact_level"] = score_impact(a)

        # Impact analysis (v1 — existing)
        a["msci_impact"] = generate_msci_impact_text(a)

        # ── NEW v2: Sector impact, Stock impact, What to watch ──
        a["sector_impact"] = generate_sector_impact(a)
        a["stock_impact"] = generate_stock_impact(a)
        a["what_to_watch"] = generate_what_to_watch(a)

        status = "OK" if article_text else "NO TEXT"
        sectors_str = ", ".join(s["sector"] for s in a["sector_impact"][:2])
        tickers_str = ", ".join(s["ticker"] for s in a["stock_impact"][:3])
        print(f"     {status} | {a['category']} | {a['sentiment']} ({confidence})")
        print(f"     Importance: {a['importance']} | Impact: {a['impact_level']}")
        print(f"     Sectors: {sectors_str} | Tickers: {tickers_str}")

        relevant_articles.append(a)
        time.sleep(1)

    if not relevant_articles:
        print("\n  No relevant competitor articles found in this run.")
        existing_data["last_updated"] = datetime.utcnow().isoformat() + "Z"
        save_data(existing_data, OUTPUT_FILE)
        print("=" * 65)
        return

    # ── Phase 3: Merge & Save ──
    all_stored = relevant_articles + existing_data.get("articles", [])
    all_stored = all_stored[:500]  # Cap at 500 articles

    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "total_articles": len(all_stored),
        "competitors_tracked": len(COMPETITORS),
        "schema_version": "2.0",
        "articles": all_stored
    }

    save_data(output, OUTPUT_FILE)

    # ── Phase 4: Summary ──
    print("\n" + "=" * 65)
    print("  COMPETITOR INTELLIGENCE SUMMARY")
    print("=" * 65)

    by_comp = {}
    for a in relevant_articles:
        by_comp.setdefault(a["competitor"], []).append(a)

    for comp, arts in sorted(by_comp.items()):
        print(f"\n  {comp} ({len(arts)} articles):")
        for a in arts[:3]:
            print(f"    - {a['title'][:70]}")
            print(f"      {a['category']} | {a['sentiment']} | {a['importance']}")

    print(f"\n  {len(relevant_articles)} new articles added.")
    print(f"  Total in database: {len(all_stored)}")
    print(f"  Output: {OUTPUT_FILE}")
    print("=" * 65)


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    main()