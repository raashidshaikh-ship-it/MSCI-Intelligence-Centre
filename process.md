# Top Client Earnings — Pre-Deploy Test Process

**Purpose.** Before pushing any change to the `Top Client Earnings` section of the
MSCI Intelligence Centre, Claude must run the full pipeline locally, verify the
JSON output, and verify the dashboard renders correctly. Only after all three
gates pass does Claude ask the user to push.

No PDF-seeded values. No hardcoded numbers. If the scraper can't find a value,
the field stays `null` and the dashboard shows `—`. This is intentional and
compliant.

---

## Pipeline overview

```
┌────────────────────────┐    ┌───────────────────────────┐    ┌──────────────────┐
│  Public sources:       │    │  Scraper                  │    │  JSON            │
│  · Company IR pages    │───▶│  top_client_earnings_     │───▶│  data/top_       │
│  · SEC EDGAR (via CIK) │    │  scraper.py               │    │  client_         │
│  · Google News RSS     │    │  (per-client × per-qtr)   │    │  earnings.json   │
└────────────────────────┘    └───────────────────────────┘    └──────────────────┘
                                                                         │
                                                                         ▼
                                                            ┌──────────────────────┐
                                                            │  Frontend            │
                                                            │  index.html          │
                                                            │  renderEarnings()    │
                                                            └──────────────────────┘
```

Three quarters tracked in a rolling window: **Q3 2025**, **Q4 2025**, **Q1 2026**.
Current quarter is set by `CURRENT_QUARTER` in `scraper/top_client_earnings_scraper.py`.

12 clients in scope (the Top Client Earnings roster):

| Category | Clients |
|---|---|
| Traditional (7) | BlackRock, State Street, Aberdeen, Amundi, Goldman Sachs, J.P. Morgan, UBS |
| PE / Alternative (5) | KKR, Blackstone, Carlyle, Apollo, Brookfield |

---

## Self-test workflow (what Claude runs before asking for a push)

### Gate 1 — Syntax & scaffold sanity

```bash
python3 -m py_compile scraper/top_client_earnings_scraper.py
python3 -c "import json; d=json.load(open('data/top_client_earnings.json')); \
    assert d['schema_version']=='4.2'; \
    assert d['quarters']==['Q3 2025','Q4 2025','Q1 2026']; \
    print('✓ scaffold sanity')"
```

**Pass criteria.** Python compiles cleanly. JSON has schema v4.2 and the 3
target quarters.

### Gate 2 — Offline pipeline validation (mock mode)

The sandbox (and many CI runs) cannot reach Google News / IR pages / SEC. To
prove the pipeline still works end-to-end we ship a deterministic `--mock`
mode that feeds 3 synthetic quarter-differentiated articles per test client
and exercises every extraction path.

```bash
python3 scraper/top_client_earnings_scraper.py --mock
```

**Pass criteria.**
- Script exits 0.
- `Validation: ✓ No duplicate financials detected across quarters.`
- Summary reports ≥ 15 / 36 client-quarters with financials (5 mock clients × 3 quarters).
- Per-client debug lines show distinct `revenue_usd`, `aum_usd`, `net_inflows_q_usd`
  across Q3 2025 / Q4 2025 / Q1 2026 (no cross-quarter leak).

### Gate 3 — JSON ↔ frontend field audit

Run the scaffold audit to confirm every field `renderEarnings()` reads is
present in the generated JSON.

```bash
python3 - <<'EOF'
import json, re
d = json.load(open('data/top_client_earnings.json'))

# Paths the frontend reads, extracted from index.html
paths = [
    "schema_version", "current_quarter", "quarters",
    "client_categories.Traditional", "client_categories.PE/Alternative",
    "insight_tag_catalog", "executive_summary.period",
    "fund_family_flows.families", "aum_summary.traditional",
    "aum_summary.pe_alternative", "voc_analysis.clients",
]
for p in paths:
    cur = d
    for s in p.split("."):
        cur = cur[s] if isinstance(cur, dict) and s in cur else "__MISSING__"
    status = "✗" if cur == "__MISSING__" else "✓"
    print(f"  {status} {p}")
EOF
```

Also check per-client quarter paths after a mock run:

```bash
python3 - <<'EOF'
import json
d = json.load(open('data/top_client_earnings.json'))
bk = d['clients'].get('BlackRock', {}).get('quarters', {})
for q in d['quarters']:
    f = (bk.get(q) or {}).get('financials', {}) or {}
    print(f"{q}: revenue={f.get('revenue_usd')}, aum={f.get('aum_usd')}, inflows={f.get('net_inflows_q_usd')}")
EOF
```

**Pass criteria.** Every path prints `✓`. The three quarters have three
different `(revenue, aum, inflows)` triples for BlackRock.

### Gate 4 — Local dashboard render

```bash
python3 -m http.server 8000
# open http://localhost:8000 in the browser, click "Top Client Earnings"
```

**Pass criteria.** In DevTools console (F12):
1. The following debug group appears:
   ```
   🎯 renderEarnings() — quarter binding debug
      state.quarterEarnings: Q4 2025
      QUARTERLY_DATA.current_quarter: Q4 2025
      → resolved currentQ: Q4 2025
      clients loaded: 12 [BlackRock, State Street, ...]
      Q3 2025: X/12 clients have data [...]
      Q4 2025: X/12 clients have data [...]
      Q1 2026: X/12 clients have data [...]
   ```
2. `📊 Deep-dive → BlackRock · Q4 2025` log shows the full `qData` object.
3. Clicking the quarter pills (Q3 2025 / Q4 2025 / Q1 2026) re-fires
   `renderEarnings()` and the console shows the new `currentQ`.
4. Quarters with zero data render the `⏳ No data for {Q}` notice; quarters
   with data render populated metric cards, segments, tags, and trends.

### Gate 5 — Live run (user executes locally)

Only after Gates 1–4 pass does Claude ask the user to run the real scraper:

```bash
python3 scraper/top_client_earnings_scraper.py
```

**Pass criteria.**
- At least one client has `≥1` financial populated for at least one quarter.
- `Validation: ✓ No duplicate financials detected across quarters.`
- No crashes (proxy 403s from the sandbox are expected; real local runs
  should succeed on Google News / Yahoo / company IR pages).

If Gate 5 passes, Claude asks the user to commit the updated
`data/top_client_earnings.json` and push.

---

## Known pipeline behaviors

| Behavior | Why |
|---|---|
| Fields stay `null` when not found | Strict rule: no fabrication. Dashboard shows `—`. |
| Sandbox returns 0 articles per client | Outbound HTTPS is blocked in the Anthropic sandbox; use `--mock` instead. |
| `msci_relationship.*` often null | MSCI-specific run-rate rarely appears in client public filings; this is expected. |
| `organic_base_fee_growth_pct`, `net_inflows_fy_usd` often null | These are reported only by some firms (BlackRock, Amundi). |
| Duplicate-quarter validator rarely fires | It fires only if the SAME value appears across multiple quarters — a strong signal of parsing leak. |

---

## Field inventory — scraper writes ↔ frontend reads

These paths are the contract between the two components. If a bug appears
on the dashboard, check whether the scraper writes the expected path.

| JSON path | Scraper writes? | Frontend reads? |
|---|---|---|
| `schema_version` | ✓ root | ✓ header |
| `quarters` | ✓ root | ✓ quarter toggle |
| `current_quarter` | ✓ root | ✓ default selection |
| `client_categories.*` | ✓ derived from `CLIENTS` | ✓ tab ordering |
| `insight_tag_catalog` | preserved | ✓ chip colors & labels |
| `fund_family_flows.families[*]` | scaffold only (null values) | ✓ Fund Family Flows table |
| `aum_summary.traditional[*]` | ✓ built from current quarter | ✓ Master table |
| `aum_summary.pe_alternative[*]` | ✓ built from current quarter | ✓ Master table |
| `clients.<K>.quarters.<Q>.financials.revenue_usd` | ✓ news/IR extraction | ✓ FIN_METRICS + drill panel |
| `clients.<K>.quarters.<Q>.financials.operating_income_usd` | ✓ news/IR extraction | ✓ drill panel |
| `clients.<K>.quarters.<Q>.financials.operating_margin_pct` | ✓ derived (op_inc / revenue) | ✓ drill panel |
| `clients.<K>.quarters.<Q>.financials.net_income_usd` | ✓ news/IR extraction | ✓ FIN_METRICS |
| `clients.<K>.quarters.<Q>.financials.eps_usd` | ✓ special regex | ✓ drill panel |
| `clients.<K>.quarters.<Q>.financials.aum_usd` | ✓ news/IR extraction | ✓ FIN_METRICS + trends |
| `clients.<K>.quarters.<Q>.financials.aum_yoy_growth_pct` | ✓ percent extraction | ✓ master table |
| `clients.<K>.quarters.<Q>.financials.net_inflows_q_usd` | ✓ news/IR extraction | ✓ master table |
| `clients.<K>.quarters.<Q>.financials.net_inflows_fy_usd` | scaffold only | ✓ FIN_METRICS |
| `clients.<K>.quarters.<Q>.financials.organic_base_fee_growth_pct` | scaffold only | ✓ FIN_METRICS |
| `clients.<K>.quarters.<Q>.financials.revenue_yoy_pct` | ✓ percent extraction | ✓ drill panel |
| `clients.<K>.quarters.<Q>.segments.*_aum_usd` | ✓ segment extraction | ✓ master table columns |
| `clients.<K>.quarters.<Q>.msci_relationship.run_rate_usd` | scaffold only | ✓ master table |
| `clients.<K>.quarters.<Q>.insight_tags` | ✓ keyword classifier | ✓ chips + filter |
| `clients.<K>.quarters.<Q>.strategic_highlights` | ✓ mined from themes | ✓ drill panel |
| `clients.<K>.quarters.<Q>.themes.key_initiatives` | ✓ mine_themes() | ✓ deep-dive |
| `clients.<K>.quarters.<Q>.themes.cost_pressures` | ✓ mine_themes() | ✓ deep-dive |
| `clients.<K>.quarters.<Q>.themes.strategic_insights` | ✓ mine_themes() | ✓ deep-dive |
| `clients.<K>.quarters.<Q>.top_trends` | ✓ per-quarter headlines | ✓ deep-dive |
| `clients.<K>.quarters.<Q>.sources` | ✓ provenance log | ✓ sources panel |

---

## Change log

- **v4.2 (2026-04-22).** Removed all PDF-seeded values; all data now from
  public sources. Added `--mock` mode, per-quarter Google News queries with
  synonyms, quarter inference regexes + date fallback, sentence-bounded
  metric extraction, earliest-position keyword resolution,
  cross-quarter-duplication validator, strategic_highlights mining from
  themes, per-quarter top_trends (not just current quarter), derived
  `operating_margin_pct`, and `other_aum_usd` key alignment between scraper
  and frontend.
- **v4.1.** Added segments and insight tags (deprecated — values were
  PDF-seeded).
- **v4.0.** Initial per-quarter dashboard.
