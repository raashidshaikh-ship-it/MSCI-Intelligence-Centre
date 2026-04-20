# Brand Intelligence Scraper (Google Trends via pytrends)
# ==============================================================
# Tracks MSCI brand search interest, competitor comparison,
# top/rising related queries, and regional interest.
# Writes to: data/brand_intelligence.json
# Run: python scraper/brand_intelligence_scraper.py
# Deps: pip install pytrends

import json, os, time
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(REPO_ROOT, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "brand_intelligence.json")

# Brands to compare
BRANDS = ["MSCI", "S&P Global", "FTSE Russell", "Bloomberg Terminal", "Morningstar"]

def load_existing(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return None

def save_data(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Saved to {path}")

def main():
    print("=" * 65)
    print("  MSCI BRAND INTELLIGENCE SCRAPER (Google Trends)")
    print(f"  {datetime.now().strftime('%A, %B %d, %Y %I:%M %p')}")
    print("=" * 65)

    existing = load_existing(OUTPUT_FILE)

    try:
        from pytrends.request import TrendReq
    except ImportError:
        print("  [!] pytrends not installed. Run: pip install pytrends")
        if existing:
            print("  Keeping previous data.")
        return

    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "brand_trend": [],
        "top_queries": [],
        "rising_queries": [],
        "regional_interest": [],
        "competitor_comparison": [],
    }

    try:
        pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25), retries=2, backoff_factor=0.5)

        # 1. Brand trend (MSCI vs competitors, last 90 days)
        print("\n  Fetching brand trend comparison (90 days)...")
        pytrends.build_payload(BRANDS, cat=0, timeframe="today 3-m", geo="", gprop="")
        trend_df = pytrends.interest_over_time()

        if not trend_df.empty:
            trend_data = []
            for date, row in trend_df.iterrows():
                entry = {"date": date.strftime("%Y-%m-%d")}
                for brand in BRANDS:
                    if brand in row:
                        entry[brand] = int(row[brand])
                trend_data.append(entry)
            output["brand_trend"] = trend_data
            print(f"  -> {len(trend_data)} data points")
        else:
            print("  -> No trend data returned")

        time.sleep(2)

        # 2. Related queries for "MSCI"
        print("  Fetching related queries for MSCI...")
        pytrends.build_payload(["MSCI"], cat=0, timeframe="today 3-m", geo="", gprop="")
        related = pytrends.related_queries()

        if "MSCI" in related and related["MSCI"]["top"] is not None:
            top_df = related["MSCI"]["top"]
            output["top_queries"] = [
                {"query": row["query"], "volume_index": int(row["value"])}
                for _, row in top_df.head(10).iterrows()
            ]
            print(f"  -> {len(output['top_queries'])} top queries")

        if "MSCI" in related and related["MSCI"]["rising"] is not None:
            rising_df = related["MSCI"]["rising"]
            output["rising_queries"] = [
                {"query": row["query"], "growth": str(row["value"]) + "%" if isinstance(row["value"], int) else str(row["value"])}
                for _, row in rising_df.head(10).iterrows()
            ]
            print(f"  -> {len(output['rising_queries'])} rising queries")

        time.sleep(2)

        # 3. Regional interest for "MSCI"
        print("  Fetching regional interest...")
        pytrends.build_payload(["MSCI"], cat=0, timeframe="today 3-m", geo="", gprop="")
        region_df = pytrends.interest_by_region(resolution="COUNTRY", inc_low_vol=False, inc_geo_code=False)

        if not region_df.empty:
            region_df = region_df[region_df["MSCI"] > 0].sort_values("MSCI", ascending=False)
            output["regional_interest"] = [
                {"country": idx, "interest": int(row["MSCI"])}
                for idx, row in region_df.head(15).iterrows()
            ]
            print(f"  -> {len(output['regional_interest'])} countries")

        time.sleep(2)

        # 4. Competitor head-to-head (latest week snapshot)
        print("  Computing competitor comparison snapshot...")
        if output["brand_trend"]:
            latest = output["brand_trend"][-1]
            comp_list = []
            for brand in BRANDS:
                if brand in latest:
                    comp_list.append({"brand": brand, "interest": latest[brand]})
            comp_list.sort(key=lambda x: x["interest"], reverse=True)
            output["competitor_comparison"] = comp_list

        save_data(output, OUTPUT_FILE)
        print(f"\n  Brand intelligence updated successfully.")

    except Exception as e:
        print(f"\n  [!] Error fetching trends: {e}")
        if existing:
            print("  Keeping previous data.")
            existing["last_updated"] = datetime.utcnow().isoformat() + "Z"
            save_data(existing, OUTPUT_FILE)
        else:
            # Save empty structure
            save_data(output, OUTPUT_FILE)

    print("=" * 65)


if __name__ == "__main__":
    main()
