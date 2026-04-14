"""
news_scraper.py
---------------
IMPORTANT: This module uses raw mysql.connector ONLY.
It must NEVER import or call Flask's get_db(), g, or current_app.
Designed to run safely inside background threads.

Category keyword matching:
  - Each category has 100+ built-in keywords.
  - Headlines are scanned first (fast). If matched, article body is fetched.
  - Category is assigned based on whichever category's keywords match most.
  - If 'all' is in target_categories, all scraped articles are kept regardless of category.
  - Otherwise only articles whose detected category is in target_categories are kept.
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
        # crops & produce
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
        # inputs
        "fertilizer", "fertilisers", "urea", "DAP", "potash", "pesticide",
        "insecticide", "herbicide", "fungicide", "irrigation", "drip irrigation",
        "sprinkler", "seeds", "hybrid seeds", "GM seed", "kisan", "agri",
        # livestock
        "livestock", "dairy", "milk", "cattle", "poultry", "fisheries",
        "aquaculture", "horticulture", "floriculture", "sericulture",
        # market & policy
        "mandi", "MSP", "minimum support price", "agri market", "APMC",
        "food grain", "foodgrain", "grain storage", "warehouse", "cold storage",
        "agri export", "crop insurance", "PM-Kisan", "kisan credit",
        "rural", "rabi", "kharif", "zaid", "soil", "soil health",
        "organic farming", "natural farming", "agri technology", "agritech",
        "drought", "flood relief", "crop damage", "crop loss",
    ],

    "weather": [
        # basic conditions
        "weather", "climate", "temperature", "rainfall", "rain", "rains",
        "monsoon", "pre-monsoon", "post-monsoon", "southwest monsoon",
        "northeast monsoon", "cyclone", "storm", "thunderstorm", "lightning",
        "hail", "hailstorm", "fog", "smog", "mist", "humidity",
        "heat wave", "heatwave", "cold wave", "coldwave", "frost",
        "snow", "snowfall", "blizzard", "avalanche", "ice", "sleet",
        "drought", "flood", "flooding", "inundation", "waterlogging",
        "landslide", "cloudburst", "tornado", "whirlwind", "dust storm",
        # measurements & forecasts
        "IMD", "India Meteorological Department", "forecast", "weather forecast",
        "weather alert", "yellow alert", "orange alert", "red alert",
        "rainfall deficit", "rainfall excess", "normal rainfall",
        "maximum temperature", "minimum temperature", "mercury",
        "humidity level", "dew point", "wind speed", "wind direction",
        "atmospheric pressure", "barometric", "visibility",
        # climate events
        "El Nino", "La Nina", "ENSO", "IOD", "Indian Ocean Dipole",
        "global warming", "climate change", "greenhouse gas", "carbon emission",
        "sea level rise", "glacier", "glacial melt", "polar vortex",
        "heat index", "wet bulb", "UV index", "air quality",
        "AQI", "pollution", "smog alert",
        # regional
        "monsoon arrival", "monsoon withdrawal", "monsoon onset",
        "western disturbance", "trough", "low pressure", "depression",
        "cyclonic storm", "severe cyclone", "extremely severe",
    ],

    "financial": [
        # markets
        "stock market", "share market", "equity", "BSE", "NSE", "Sensex",
        "Nifty", "index", "trading", "bull market", "bear market",
        "rally", "sell-off", "correction", "volatility",
        "stock", "shares", "equity market", "IPO", "listing",
        "mutual fund", "NAV", "SIP", "ELSS", "dividend",
        # economy
        "economy", "economic", "GDP", "GNP", "inflation", "deflation",
        "CPI", "WPI", "fiscal", "fiscal deficit", "current account",
        "trade deficit", "balance of payments", "foreign exchange",
        "forex", "rupee", "dollar", "currency", "exchange rate",
        "RBI", "Reserve Bank", "monetary policy", "repo rate",
        "reverse repo", "CRR", "SLR", "liquidity",
        # banking & finance
        "bank", "banking", "credit", "loan", "NPA", "bad loan",
        "interest rate", "EMI", "home loan", "auto loan",
        "insurance", "SEBI", "IRDAI", "PFRDA", "regulator",
        "budget", "union budget", "tax", "GST", "income tax",
        "direct tax", "indirect tax", "revenue", "expenditure",
        "disinvestment", "privatisation", "FDI", "FPI", "FII",
        # commodities finance
        "gold price", "silver price", "crude oil price", "commodity market",
        "MCX", "NCDEX", "futures", "options", "derivative",
        # corporate
        "profit", "loss", "revenue", "earnings", "quarterly results",
        "annual results", "merger", "acquisition", "takeover",
        "startup", "unicorn", "funding", "venture capital", "PE fund",
    ],

    "energy": [
        # oil & gas
        "oil", "crude oil", "Brent crude", "WTI", "petroleum",
        "natural gas", "LNG", "LPG", "CNG", "pipeline",
        "OPEC", "oil production", "oil refinery", "refining",
        "petrol", "diesel", "fuel price", "fuel", "gasoline",
        "ONGC", "IOC", "BPCL", "HPCL", "Reliance Industries",
        "oil ministry", "petroleum ministry",
        # electricity & power
        "electricity", "power", "power plant", "thermal power",
        "coal power", "gas-based power", "nuclear power", "reactor",
        "hydropower", "hydro", "dam", "turbine", "generator",
        "grid", "power grid", "transmission", "distribution",
        "power cut", "power outage", "load shedding", "blackout",
        "energy", "energy sector", "power sector", "energy ministry",
        "NTPC", "NHPC", "PGCIL", "Power Finance", "REC",
        # renewables
        "solar", "solar energy", "solar power", "solar panel",
        "photovoltaic", "wind energy", "wind power", "wind farm",
        "windmill", "turbine", "renewable", "renewables",
        "green energy", "clean energy", "sustainable energy",
        "biofuel", "ethanol", "biomass", "biogas", "green hydrogen",
        "battery storage", "energy storage", "EV", "electric vehicle",
        "EV charging", "MNRE", "IREDA",
        # policy & markets
        "carbon credit", "carbon market", "emission trading",
        "energy transition", "net zero", "decarbonisation",
        "coal", "coal mine", "coal India", "coal block",
        "electricity tariff", "power tariff", "CERC", "SERC",
    ],
}

# Flat union for "all" category — any keyword from any category matches
ALL_KEYWORDS = list({kw for kws in CATEGORY_KEYWORDS.values() for kw in kws})


# ════════════════════════════════════════════════════════════════
#  TEXT UTILITIES
# ════════════════════════════════════════════════════════════════

def clean_text(text):
    return re.sub(r"\s+", " ", text or "").strip()


def _word_match(text, keywords):
    """Return list of keywords found in text (whole-word, case-insensitive)."""
    text = (text or "").lower()
    return [kw for kw in keywords if re.search(
        rf"(?<![a-z]){re.escape(kw.lower())}(?![a-z])", text
    )]


def detect_category(text, db_news_types=None, db_commodities=None):
    """
    Return the best-matching category name for the given text, or 'general'.
    Scores each category by number of keyword hits; picks the highest.
    Falls back to DB news_types / commodities for legacy compatibility.
    """
    text_lower = (text or "").lower()
    scores = {}
    for cat, kws in CATEGORY_KEYWORDS.items():
        hits = sum(
            1 for kw in kws
            if re.search(rf"(?<![a-z]){re.escape(kw.lower())}(?![a-z])", text_lower)
        )
        if hits:
            scores[cat] = hits

    if scores:
        return max(scores, key=scores.get)

    # Legacy fallback: DB-supplied news_types and commodities
    if db_commodities:
        m = _word_match(text, db_commodities)
        if m:
            return "agricultural"  # commodities are mostly agricultural
    if db_news_types:
        m = _word_match(text, db_news_types)
        if m:
            return m[0].lower()

    return "general"


def matches_target_categories(text, target_categories):
    """
    Return True if the article's detected category is in target_categories.
    If target_categories contains 'all', always return True.
    """
    if "all" in target_categories:
        # Still require at least one keyword hit from any category
        return bool(_word_match(text, ALL_KEYWORDS))
    detected = detect_category(text)
    return detected in target_categories


def get_matched_keywords(text, target_categories):
    """Return matched keywords relevant to the target categories."""
    if "all" in target_categories:
        pools = ALL_KEYWORDS
    else:
        pools = []
        for cat in target_categories:
            pools.extend(CATEGORY_KEYWORDS.get(cat, []))
    return list(dict.fromkeys(_word_match(text, pools)))


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


def _fetch_keywords(db_config):    return _fetch_col(db_config, "keywords",  "keyword")
def _fetch_commodities(db_config): return _fetch_col(db_config, "commodity", "commodity")
def _fetch_news_types(db_config):  return _fetch_col(db_config, "news",      "news_type")


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


def _load_settings(db_config):
    """Load user_settings row 1 as a plain dict."""
    try:
        conn = _open(db_config)
        cur  = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM user_settings WHERE id=1")
        row = cur.fetchone() or {}
        cur.close(); conn.close()
        return row
    except Exception as e:
        print(f"[SCRAPER] Could not load settings: {e}")
        return {}


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


def _is_duplicate_published(news_url, db_config):
    """Also check published_news to avoid re-publishing."""
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
    """Insert directly into published_news (auto-publish mode)."""
    from datetime import datetime
    if not headline or not news_url:
        return None, "missing"
    try:
        conn = _open(db_config)
        cur  = conn.cursor()
        # Check both tables
        cur.execute(
            "SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1",
            (news_url,)
        )
        if cur.fetchone():
            cur.close(); conn.close()
            return None, "duplicate_nonpub"
        cur.execute(
            "SELECT id FROM published_news WHERE news_url=%s LIMIT 1",
            (news_url,)
        )
        if cur.fetchone():
            cur.close(); conn.close()
            return None, "duplicate_pub"
        cur.execute(
            "INSERT INTO published_news "
            "(news_date,news_type,news_headline,news_text,news_url,"
            "keywords,published_at) "
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
        parts, seen = [], set()
        for p in paras:
            t = clean_text(p.get_text(" ", strip=True))
            if t and len(t) > 30 and t not in seen:
                seen.add(t); parts.append(t)
        if parts:
            return "\n".join(parts)
    return ""


# ════════════════════════════════════════════════════════════════
#  PER-ARTICLE PROCESSING
# ════════════════════════════════════════════════════════════════

def _process(article, site, target_categories, db_news_types,
             db_commodities, publish_mode, db_config):
    """
    Process one article:
      1. Fast headline keyword pre-filter.
      2. Duplicate check.
      3. Fetch body and run full category match.
      4. Insert into correct table based on publish_mode.
    Returns (ok: bool, reason: str, pub_id: int|None)
    """
    headline = clean_text(article.get("headline"))
    url      = clean_text(article.get("url"))
    if not headline or not url:
        return False, "missing", None

    # Fast pre-filter on headline only
    if not matches_target_categories(headline, target_categories):
        return False, "headline_no_match", None

    # Duplicate check
    if _is_duplicate(url, db_config):
        return False, "duplicate", None
    if publish_mode == "auto" and _is_duplicate_published(url, db_config):
        return False, "duplicate_pub", None

    # Fetch full body
    body     = _extract_text(url, site["article_selectors"])
    full_text = f"{headline} {body}"

    # Full category check on combined text
    if not matches_target_categories(full_text, target_categories):
        return False, "no_match", None

    # Detect category and matched keywords
    detected_cat  = detect_category(full_text, db_news_types, db_commodities)
    matched_terms = get_matched_keywords(full_text, target_categories)
    news_type     = detected_cat if detected_cat != "general" else (
        site.get("news_type", "general")
    )

    if publish_mode == "auto":
        pub_id, reason = _insert_published(
            headline, body, url, news_type, matched_terms, db_config
        )
        ok = pub_id is not None
        return ok, reason, pub_id
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
    Fully self-contained scraper. Receives plain Python values only.

    Parameters
    ----------
    db_config          : mysql.connector connection kwargs
    instance_path      : Flask instance_path (for JSON dumps)
    target_categories  : list of category strings to keep, e.g. ['agricultural','weather']
                         Pass ['all'] or None to keep everything (still keyword-filtered).
    publish_mode       : 'manual'  → insert into non_published_news
                         'auto'    → insert directly into published_news
    """
    if not target_categories:
        target_categories = ["all"]
    target_categories = [c.strip().lower() for c in target_categories]

    # Load DB-supplied lists (supplements built-in keywords)
    db_keywords   = _fetch_keywords(db_config)
    db_commodities = _fetch_commodities(db_config)
    db_news_types  = _fetch_news_types(db_config)
    sites          = _fetch_site_configs(db_config)

    if not sites:
        return {
            "success": False, "inserted": 0, "skipped": 0,
            "failed_sites": 0, "total": 0,
            "message": "No website configs found in the websites table.",
        }

    total = inserted = skipped = failed = 0
    auto_pub_ids = []   # collect pub_ids when mode=auto, for PDF+email trigger
    auto_articles = []  # matching articles for auto mode
    files = []

    for site in sites:
        print(f"[SCRAPER] Scraping listing: {site['url']}")
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
                    _process, a, site, target_categories,
                    db_news_types, db_commodities, publish_mode, db_config
                ): a
                for a in articles
            }
            for fut in as_completed(fmap):
                a = fmap[fut]
                try:
                    ok, reason, pub_id = fut.result(timeout=TASK_TIMEOUT)
                    if ok:
                        inserted += 1
                        print(f"[SCRAPER] Inserted ({publish_mode}): {a['headline'][:60]}")
                        if publish_mode == "auto" and pub_id:
                            auto_pub_ids.append(pub_id)
                            auto_articles.append(a)
                    else:
                        skipped += 1
                        if reason not in ("duplicate", "duplicate_pub",
                                         "duplicate_nonpub", "headline_no_match",
                                         "no_match"):
                            print(f"[SCRAPER] Skip({reason}): {a['headline'][:60]}")
                except FutureTimeout:
                    skipped += 1
                    print(f"[SCRAPER] Timeout: {a['headline'][:60]}")
                except Exception as e:
                    skipped += 1
                    print(f"[SCRAPER] Error: {a.get('headline','')[:60]} — {e}")

    return {
        "success":       True,
        "inserted":      inserted,
        "skipped":       skipped,
        "failed_sites":  failed,
        "total":         total,
        "files":         files,
        "auto_pub_ids":  auto_pub_ids,    # non-empty only when publish_mode=auto
        "auto_articles": auto_articles,   # matching article dicts for PDF+email
        "message": (
            f"Done. Extracted:{total} Inserted:{inserted} "
            f"Skipped:{skipped} Failed sites:{failed}"
        ),
    }
