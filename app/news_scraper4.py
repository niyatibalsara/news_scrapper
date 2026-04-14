"""
news_scraper.py
---------------
IMPORTANT: Uses raw mysql.connector ONLY. Never import Flask helpers here.
Safe to run inside background threads.

How category filtering works
─────────────────────────────
• Each category has 100+ built-in keywords.
• A headline is scanned first (fast, no network).
  - If target is 'all'        → keep if ANY keyword from ANY category matches.
  - If target is specific cats → keep only if at least one keyword from
                                 those specific categories matches.
• Only then is the full article body fetched.
• Final category label is assigned by whichever category scores highest.
"""

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout
from datetime import date
from urllib.parse import urljoin, urlparse

import mysql.connector
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

LISTING_TIMEOUT = 10
ARTICLE_TIMEOUT = 6
MAX_WORKERS     = 4
TASK_TIMEOUT    = 15


# ════════════════════════════════════════════════════════════════
#  BUILT-IN CATEGORY KEYWORD SETS  (100+ per category)
# ════════════════════════════════════════════════════════════════

CATEGORY_KEYWORDS = {
    "agricultural": [
        "agriculture", "agricultural", "farming", "farmer", "farmers", "farm",
        "crop", "crops", "harvest", "harvesting", "cultivation", "cultivate",
        "paddy", "rice", "wheat", "maize", "corn", "sugarcane", "cotton",
        "soybean", "soya", "groundnut", "sunflower", "mustard", "rapeseed",
        "pulse", "pulses", "dal", "lentil", "lentils", "chickpea", "gram",
        "mung", "moong", "urad", "tur", "arhar", "pigeon pea",
        "potato", "onion", "tomato", "brinjal", "cabbage", "cauliflower",
        "carrot", "spinach", "peas", "garlic", "ginger", "turmeric",
        "mango", "banana", "apple", "orange", "grapes", "papaya", "guava",
        "watermelon", "pomegranate", "litchi", "pineapple", "coconut",
        "fertilizer", "fertiliser", "fertilizers", "fertilisers",
        "urea", "DAP", "potash", "pesticide", "insecticide",
        "herbicide", "fungicide", "irrigation", "drip irrigation",
        "sprinkler", "seeds", "hybrid seeds", "kisan", "agri",
        "livestock", "dairy", "milk", "cattle", "poultry", "fisheries",
        "aquaculture", "horticulture", "floriculture", "sericulture",
        "mandi", "MSP", "minimum support price", "agri market", "APMC",
        "food grain", "foodgrain", "grain storage", "warehouse", "cold storage",
        "agri export", "crop insurance", "PM-Kisan", "kisan credit",
        "rural", "rabi", "kharif", "zaid", "soil", "soil health",
        "organic farming", "natural farming", "agritech",
        "drought", "crop damage", "crop loss", "farm produce",
        "vegetable price", "fruit price", "agri subsidy",
        "seed company", "crop yield", "food security", "food inflation",
        "agri policy", "farm loan", "farm bill", "farm income",
    ],

    "weather": [
        "weather", "climate", "temperature", "rainfall", "rain", "rains",
        "monsoon", "pre-monsoon", "post-monsoon", "southwest monsoon",
        "northeast monsoon", "cyclone", "storm", "thunderstorm", "lightning",
        "hail", "hailstorm", "fog", "smog", "mist", "humidity",
        "heat wave", "heatwave", "cold wave", "coldwave", "frost",
        "snow", "snowfall", "blizzard", "avalanche", "sleet",
        "drought", "flood", "flooding", "inundation", "waterlogging",
        "landslide", "cloudburst", "tornado", "whirlwind", "dust storm",
        "IMD", "India Meteorological Department", "forecast",
        "weather forecast", "weather alert", "yellow alert",
        "orange alert", "red alert", "rainfall deficit", "rainfall excess",
        "maximum temperature", "minimum temperature", "mercury",
        "humidity level", "dew point", "wind speed", "wind direction",
        "atmospheric pressure", "visibility",
        "El Nino", "La Nina", "ENSO", "IOD", "Indian Ocean Dipole",
        "global warming", "climate change", "greenhouse gas",
        "carbon emission", "sea level rise", "glacier", "glacial melt",
        "polar vortex", "heat index", "wet bulb", "UV index",
        "air quality", "AQI", "pollution", "smog alert",
        "monsoon arrival", "monsoon withdrawal", "monsoon onset",
        "western disturbance", "trough", "low pressure", "depression",
        "cyclonic storm", "severe cyclone", "extremely severe",
        "sunny", "cloudy", "partly cloudy", "overcast", "rainy",
        "heavy rain", "light rain", "moderate rain", "heat",
        "cold", "winter", "summer", "spring",
    ],

    "financial": [
        "stock market", "share market", "equity", "BSE", "NSE", "Sensex",
        "Nifty", "index", "trading", "bull market", "bear market",
        "rally", "sell-off", "correction", "volatility",
        "stock", "shares", "equity market", "IPO", "listing",
        "mutual fund", "NAV", "SIP", "ELSS", "dividend",
        "economy", "economic", "GDP", "GNP", "inflation", "deflation",
        "CPI", "WPI", "fiscal", "fiscal deficit", "current account",
        "trade deficit", "balance of payments", "foreign exchange",
        "forex", "rupee", "dollar", "currency", "exchange rate",
        "RBI", "Reserve Bank", "monetary policy", "repo rate",
        "reverse repo", "CRR", "SLR", "liquidity",
        "bank", "banking", "credit", "loan", "NPA", "bad loan",
        "interest rate", "EMI", "home loan", "auto loan",
        "insurance", "SEBI", "IRDAI", "PFRDA", "regulator",
        "budget", "union budget", "tax", "GST", "income tax",
        "direct tax", "indirect tax", "revenue", "expenditure",
        "disinvestment", "privatisation", "FDI", "FPI", "FII",
        "gold price", "silver price", "commodity market",
        "MCX", "NCDEX", "futures", "options", "derivative",
        "profit", "loss", "earnings", "quarterly results",
        "annual results", "merger", "acquisition", "takeover",
        "startup", "unicorn", "funding", "venture capital",
        "market cap", "valuation", "fintech", "cryptocurrency",
        "bitcoin", "blockchain", "digital currency",
    ],

    "energy": [
        "oil", "crude oil", "Brent crude", "WTI", "petroleum",
        "natural gas", "LNG", "LPG", "CNG", "pipeline",
        "OPEC", "oil production", "oil refinery", "refining",
        "petrol", "diesel", "fuel price", "fuel", "gasoline",
        "ONGC", "IOC", "BPCL", "HPCL",
        "electricity", "power", "power plant", "thermal power",
        "coal power", "gas-based power", "nuclear power", "reactor",
        "hydropower", "hydro", "dam", "turbine", "generator",
        "grid", "power grid", "transmission", "distribution",
        "power cut", "power outage", "load shedding", "blackout",
        "energy", "energy sector", "power sector",
        "NTPC", "NHPC", "PGCIL",
        "solar", "solar energy", "solar power", "solar panel",
        "photovoltaic", "wind energy", "wind power", "wind farm",
        "windmill", "renewable", "renewables",
        "green energy", "clean energy", "sustainable energy",
        "biofuel", "ethanol", "biomass", "biogas", "green hydrogen",
        "battery storage", "energy storage", "EV", "electric vehicle",
        "EV charging", "MNRE", "IREDA",
        "carbon credit", "carbon market", "emission trading",
        "energy transition", "net zero", "decarbonisation",
        "coal", "coal mine", "Coal India", "coal block",
        "electricity tariff", "power tariff", "CERC",
    ],
}

# Flat set of all keywords for "all" mode
_ALL_KW_SET = {kw.lower() for kws in CATEGORY_KEYWORDS.values() for kw in kws}


# ════════════════════════════════════════════════════════════════
#  TEXT UTILITIES
# ════════════════════════════════════════════════════════════════

def clean_text(text):
    return re.sub(r"\s+", " ", text or "").strip()


def _has_match(text: str, keywords: list) -> bool:
    """Fast check: does text contain at least one keyword from the list?"""
    t = text.lower()
    for kw in keywords:
        # Use simple substring match for speed; whole-word for short words
        if len(kw) <= 4:
            if re.search(rf"\b{re.escape(kw.lower())}\b", t):
                return True
        else:
            if kw.lower() in t:
                return True
    return False


def _count_matches(text: str, keywords: list) -> int:
    """Count distinct keyword hits in text."""
    t = text.lower()
    count = 0
    for kw in keywords:
        if len(kw) <= 4:
            if re.search(rf"\b{re.escape(kw.lower())}\b", t):
                count += 1
        else:
            if kw.lower() in t:
                count += 1
    return count


def _get_matched_kws(text: str, keywords: list) -> list:
    """Return list of matched keywords."""
    t = text.lower()
    out = []
    for kw in keywords:
        if len(kw) <= 4:
            if re.search(rf"\b{re.escape(kw.lower())}\b", t):
                out.append(kw)
        else:
            if kw.lower() in t:
                out.append(kw)
    return out


def passes_category_filter(text: str, target_categories: list) -> bool:
    """
    Return True if this text should be kept given the selected categories.
    - 'all' in target_categories → keep if any keyword from any category matches.
    - specific categories        → keep if any keyword from those categories matches.
    """
    if "all" in target_categories:
        return _has_match(text, list(_ALL_KW_SET))

    for cat in target_categories:
        kws = CATEGORY_KEYWORDS.get(cat, [])
        if kws and _has_match(text, kws):
            return True
    return False


def detect_best_category(text: str) -> str:
    """
    Score each category by keyword hit count; return the highest-scoring one.
    Falls back to 'general'.
    """
    best_cat   = "general"
    best_score = 0
    for cat, kws in CATEGORY_KEYWORDS.items():
        score = _count_matches(text, kws)
        if score > best_score:
            best_score = score
            best_cat   = cat
    return best_cat


def get_matched_terms(text: str, target_categories: list) -> list:
    """Return matched keywords for the given target categories."""
    if "all" in target_categories:
        pool = list(_ALL_KW_SET)
    else:
        pool = []
        for cat in target_categories:
            pool.extend(CATEGORY_KEYWORDS.get(cat, []))
    return list(dict.fromkeys(_get_matched_kws(text, pool)))


# ════════════════════════════════════════════════════════════════
#  RAW DB HELPERS
# ════════════════════════════════════════════════════════════════

def _open(db_config):
    return mysql.connector.connect(**db_config)


def _fetch_col(db_config, table, col):
    conn = _open(db_config)
    cur  = conn.cursor(dictionary=True)
    cur.execute(f"SELECT {col} FROM `{table}` ORDER BY sr_no ASC")
    rows = cur.fetchall()
    cur.close(); conn.close()
    vals = []
    for r in rows:
        raw = clean_text(r.get(col))
        if raw:
            vals.extend(x.strip() for x in raw.split(",") if x.strip())
    return list(dict.fromkeys(vals))


def _fetch_news_types(db_config):  return _fetch_col(db_config, "news",      "news_type")
def _fetch_commodities(db_config): return _fetch_col(db_config, "commodity", "commodity")
def _fetch_keywords(db_config):    return _fetch_col(db_config, "keywords",  "keyword")


def _fetch_site_configs(db_config):
    conn = _open(db_config)
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT websites FROM websites ORDER BY sr_no ASC")
    rows = cur.fetchall()
    cur.close(); conn.close()

    configs = []
    for row in rows:
        raw = clean_text(row.get("websites"))
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            if raw.lower().startswith(("http://", "https://")):
                obj = {
                    "name": urlparse(raw).netloc or raw,
                    "url": raw,
                    "listing_selector": "a[href]",
                    "article_selectors": ["p"],
                    "news_type": "general",
                }
            else:
                print(f"[SCRAPER] Skipping invalid config: {raw[:60]}")
                continue

        url = clean_text(obj.get("url"))
        ls  = clean_text(obj.get("listing_selector"))
        ars = obj.get("article_selectors", [])
        if not url or not ls or not isinstance(ars, list) or not ars:
            continue
        configs.append({
            "name":              clean_text(obj.get("name")) or urlparse(url).netloc,
            "url":               url,
            "listing_selector":  ls,
            "article_selectors": ars,
            "news_type":         clean_text(obj.get("news_type")) or "general",
        })
    return configs


def _is_duplicate(news_url, db_config):
    conn = _open(db_config)
    cur  = conn.cursor()
    cur.execute(
        "SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1",
        (news_url,)
    )
    found = cur.fetchone() is not None
    cur.close(); conn.close()
    return found


def _is_published_duplicate(news_url, db_config):
    conn = _open(db_config)
    cur  = conn.cursor()
    cur.execute(
        "SELECT id FROM published_news WHERE news_url=%s LIMIT 1",
        (news_url,)
    )
    found = cur.fetchone() is not None
    cur.close(); conn.close()
    return found


def _insert_non_published(headline, news_text, news_url, news_type,
                           matched_terms, db_config):
    """Insert into non_published_news (manual mode)."""
    if not headline or not news_url:
        return False, "missing"
    try:
        conn = _open(db_config)
        cur  = conn.cursor()
        cur.execute(
            "SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1",
            (news_url,)
        )
        if cur.fetchone():
            cur.close(); conn.close()
            return False, "duplicate"
        cur.execute(
            "INSERT INTO non_published_news "
            "(news_date,news_type,news_headline,news_text,news_url,keywords,published) "
            "VALUES (%s,%s,%s,%s,%s,%s,0)",
            (date.today(), news_type, headline, news_text,
             news_url, ", ".join(matched_terms))
        )
        conn.commit(); cur.close(); conn.close()
        return True, "ok"
    except Exception as e:
        return False, f"db:{e}"


def _insert_published(headline, news_text, news_url, news_type,
                      matched_terms, db_config):
    """Insert directly into published_news (auto mode). Returns (pub_id, reason)."""
    from datetime import datetime as dt
    if not headline or not news_url:
        return None, "missing"
    try:
        conn = _open(db_config)
        cur  = conn.cursor()
        # Duplicate checks in both tables
        cur.execute(
            "SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1",
            (news_url,)
        )
        if cur.fetchone():
            cur.close(); conn.close()
            return None, "duplicate"
        cur.execute(
            "SELECT id FROM published_news WHERE news_url=%s LIMIT 1",
            (news_url,)
        )
        if cur.fetchone():
            cur.close(); conn.close()
            return None, "duplicate"
        cur.execute(
            "INSERT INTO published_news "
            "(news_date, news_type, news_headline, news_text, "
            " news_url, keywords, published_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,NOW())",
            (date.today(), news_type, headline, news_text,
             news_url, ", ".join(matched_terms))
        )
        conn.commit()
        pub_id = cur.lastrowid
        cur.close(); conn.close()
        return pub_id, "ok"
    except Exception as e:
        return None, f"db:{e}"


# ════════════════════════════════════════════════════════════════
#  HTTP HELPERS
# ════════════════════════════════════════════════════════════════

def _fetch_html(url, timeout):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def _scrape_listing(site):
    html = _fetch_html(site["url"], LISTING_TIMEOUT)
    soup = BeautifulSoup(html, "html.parser")
    out, seen, host = [], set(), urlparse(site["url"]).netloc.lower()
    for a in soup.select(site["listing_selector"]):
        headline = clean_text(a.get_text(" ", strip=True))
        href     = clean_text(a.get("href"))
        if not headline or len(headline) < 25:
            continue
        if not href or href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        full = urljoin(site["url"], href)
        p    = urlparse(full)
        if not p.scheme.startswith("http"):
            continue
        if host not in p.netloc.lower() and p.netloc.lower() not in host:
            continue
        if full in seen:
            continue
        seen.add(full)
        out.append({"headline": headline, "url": full})
    return out


def _extract_text(url, selectors):
    try:
        html = _fetch_html(url, ARTICLE_TIMEOUT)
    except Exception as e:
        print(f"[SCRAPER] Article fetch failed {url}: {e}")
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for sel in selectors:
        paras = soup.select(sel)
        if not paras:
            continue
        parts, seen_t = [], set()
        for p in paras:
            t = clean_text(p.get_text(" ", strip=True))
            if t and len(t) > 30 and t not in seen_t:
                seen_t.add(t); parts.append(t)
        if parts:
            return "\n".join(parts)
    return ""


# ════════════════════════════════════════════════════════════════
#  PER-ARTICLE PROCESSING
# ════════════════════════════════════════════════════════════════

def _process(article, site, target_categories, publish_mode, db_config):
    headline = clean_text(article.get("headline"))
    url      = clean_text(article.get("url"))
    if not headline or not url:
        return False, "missing", None

    # ── Step 1: fast headline filter (no network) ──────────────
    if not passes_category_filter(headline, target_categories):
        return False, "headline_no_match", None

    # ── Step 2: duplicate check ────────────────────────────────
    if _is_duplicate(url, db_config):
        return False, "duplicate", None
    if publish_mode == "auto" and _is_published_duplicate(url, db_config):
        return False, "duplicate", None

    # ── Step 3: fetch full body ────────────────────────────────
    body      = _extract_text(url, site["article_selectors"])
    full_text = f"{headline} {body}"

    # ── Step 4: full-text filter ───────────────────────────────
    if not passes_category_filter(full_text, target_categories):
        return False, "no_match", None

    # ── Step 5: assign category and keywords ───────────────────
    detected_cat  = detect_best_category(full_text)
    matched_terms = get_matched_terms(full_text, target_categories)
    # Use detected category if it's in our known set, else site default
    news_type = (
        detected_cat
        if detected_cat in CATEGORY_KEYWORDS
        else site.get("news_type", "general")
    )

    # ── Step 6: insert ─────────────────────────────────────────
    if publish_mode == "auto":
        pub_id, reason = _insert_published(
            headline, body, url, news_type, matched_terms, db_config
        )
        return (pub_id is not None), reason, pub_id
    else:
        ok, reason = _insert_non_published(
            headline, body, url, news_type, matched_terms, db_config
        )
        return ok, reason, None


def _save_json(instance_path, site_name, articles):
    folder = os.path.join(instance_path, "downloaded_json")
    os.makedirs(folder, exist_ok=True)
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", site_name)
    path = os.path.join(folder, f"{safe}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    return path


# ════════════════════════════════════════════════════════════════
#  PUBLIC ENTRY POINT
# ════════════════════════════════════════════════════════════════

def run_news_scraper(db_config: dict, instance_path: str,
                     target_categories: list = None,
                     publish_mode: str = "manual") -> dict:
    """
    Fully self-contained scraper.

    Parameters
    ----------
    db_config         : mysql.connector kwargs
    instance_path     : Flask instance_path (for JSON dumps)
    target_categories : categories to keep, e.g. ['agricultural', 'weather']
                        None / ['all'] = keep anything that matches any keyword
    publish_mode      : 'manual' → non_published_news
                        'auto'   → published_news (immediately visible)
    """
    if not target_categories:
        target_categories = ["all"]
    target_categories = [c.strip().lower() for c in target_categories]

    print(f"[SCRAPER] Starting — cats={target_categories} mode={publish_mode}")

    sites = _fetch_site_configs(db_config)
    if not sites:
        return {
            "success": False, "inserted": 0, "skipped": 0,
            "failed_sites": 0, "total": 0,
            "message": "No website configs found in the websites table.",
        }

    total = inserted = skipped = failed = 0
    auto_pub_ids  = []   # pub_ids for auto mode (used for PDF+email)
    auto_articles = []   # article dicts for auto mode
    files         = []

    for site in sites:
        print(f"[SCRAPER] Listing: {site['url']}")
        try:
            articles = _scrape_listing(site)
        except Exception as e:
            print(f"[SCRAPER] Listing failed {site['url']}: {e}")
            failed += 1
            continue

        total += len(articles)
        print(f"[SCRAPER] {site['name']}: {len(articles)} links found")

        try:
            files.append(_save_json(instance_path, site.get("name", "site"), articles))
        except Exception as e:
            print(f"[SCRAPER] JSON save error: {e}")

        if not articles:
            continue

        n = min(MAX_WORKERS, len(articles))
        with ThreadPoolExecutor(max_workers=n) as ex:
            fmap = {
                ex.submit(
                    _process, a, site, target_categories, publish_mode, db_config
                ): a
                for a in articles
            }
            for fut in as_completed(fmap):
                a = fmap[fut]
                try:
                    ok, reason, pub_id = fut.result(timeout=TASK_TIMEOUT)
                    if ok:
                        inserted += 1
                        print(f"[SCRAPER] ✓ Inserted ({publish_mode}): {a['headline'][:70]}")
                        if publish_mode == "auto" and pub_id:
                            auto_pub_ids.append(pub_id)
                            auto_articles.append(a)
                    else:
                        skipped += 1
                        if reason not in ("duplicate", "headline_no_match", "no_match"):
                            print(f"[SCRAPER] Skip({reason}): {a['headline'][:70]}")
                except FutureTimeout:
                    skipped += 1
                    print(f"[SCRAPER] Timeout: {a.get('headline','')[:70]}")
                except Exception as e:
                    skipped += 1
                    print(f"[SCRAPER] Error: {a.get('headline','')[:70]} — {e}")

    msg = (
        f"Done. Scraped:{total} Inserted:{inserted} "
        f"Skipped:{skipped} Failed sites:{failed} "
        f"Categories:{target_categories} Mode:{publish_mode}"
    )
    print(f"[SCRAPER] {msg}")

    return {
        "success":      True,
        "inserted":     inserted,
        "skipped":      skipped,
        "failed_sites": failed,
        "total":        total,
        "files":        files,
        "auto_pub_ids":  auto_pub_ids,
        "auto_articles": auto_articles,
        "message":      msg,
    }
