# """
# news_scraper.py
# ---------------
# IMPORTANT: Uses raw mysql.connector ONLY. Never import Flask helpers here.
# Safe to run inside background threads.

# How it works
# ────────────
# 1. USER KEYWORDS (from `keywords` table in DB)
#    These are what the user wants news ABOUT.
#    e.g. "wheat, MSP, fertilizer, dal, rice"
#    An article is only scraped if its headline or body contains
#    at least one of these user keywords.
#    This is the PRIMARY FILTER — nothing passes without a match here.

# 2. SELECTED CATEGORIES (from user_settings.content_categories)
#    e.g. ['agricultural', 'weather']
#    After a user-keyword match, the article is ALSO checked against
#    the selected category's built-in keyword set. This ensures that
#    even if a user added "stock" as a keyword, if they only selected
#    "agricultural" category, finance articles won't sneak through.
#    Set to ['all'] to skip category filtering entirely.

# 3. CATEGORY_KEYWORDS (built-in, 100+ per category)
#    Used ONLY to:
#    a) enforce category filtering (step 2 above)
#    b) label/classify the article after it passes the filter
#    NOT used as the primary scrape filter.
# """

# import json
# import os
# import re
# from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout
# from datetime import date
# from urllib.parse import urljoin, urlparse

# import mysql.connector
# import requests
# from bs4 import BeautifulSoup

# HEADERS = {
#     "User-Agent": (
#         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) "
#         "Chrome/124.0.0.0 Safari/537.36"
#     )
# }

# LISTING_TIMEOUT = 10
# ARTICLE_TIMEOUT = 6
# MAX_WORKERS     = 4
# TASK_TIMEOUT    = 15


# # ════════════════════════════════════════════════════════════════
# #  BUILT-IN CATEGORY KEYWORD SETS
# #  Used ONLY for: (a) category-level filtering, (b) labeling
# #  NOT the primary scrape filter — that is the user's keywords.
# # ════════════════════════════════════════════════════════════════

# CATEGORY_KEYWORDS = {
#     "agricultural": [
#         "agriculture", "agricultural", "farming", "farmer", "farmers", "farm",
#         "crop", "crops", "harvest", "harvesting", "cultivation", "cultivate",
#         "paddy", "rice", "wheat", "maize", "corn", "sugarcane", "cotton",
#         "soybean", "soya", "groundnut", "sunflower", "mustard", "rapeseed",
#         "pulse", "pulses", "dal", "lentil", "lentils", "chickpea", "gram",
#         "mung", "moong", "urad", "tur", "arhar", "pigeon pea",
#         "potato", "onion", "tomato", "brinjal", "cabbage", "cauliflower",
#         "carrot", "spinach", "peas", "garlic", "ginger", "turmeric",
#         "mango", "banana", "apple", "orange", "grapes", "papaya", "guava",
#         "watermelon", "pomegranate", "litchi", "pineapple", "coconut",
#         "fertilizer", "fertiliser", "fertilizers", "fertilisers",
#         "urea", "DAP", "potash", "pesticide", "insecticide",
#         "herbicide", "fungicide", "irrigation", "drip irrigation",
#         "sprinkler", "seeds", "hybrid seeds", "kisan", "agri",
#         "livestock", "dairy", "milk", "cattle", "poultry", "fisheries",
#         "aquaculture", "horticulture", "floriculture", "sericulture",
#         "mandi", "MSP", "minimum support price", "agri market", "APMC",
#         "food grain", "foodgrain", "grain storage", "warehouse", "cold storage",
#         "agri export", "crop insurance", "PM-Kisan", "kisan credit",
#         "rural", "rabi", "kharif", "zaid", "soil", "soil health",
#         "organic farming", "natural farming", "agritech",
#         "drought", "crop damage", "crop loss", "farm produce",
#         "vegetable price", "fruit price", "agri subsidy",
#         "seed company", "crop yield", "food security", "food inflation",
#         "agri policy", "farm loan", "farm bill", "farm income",
#     ],

#     "weather": [
#         "weather", "climate", "temperature", "rainfall", "rain", "rains",
#         "monsoon", "pre-monsoon", "post-monsoon", "southwest monsoon",
#         "northeast monsoon", "cyclone", "storm", "thunderstorm", "lightning",
#         "hail", "hailstorm", "fog", "smog", "mist", "humidity",
#         "heat wave", "heatwave", "cold wave", "coldwave", "frost",
#         "snow", "snowfall", "blizzard", "avalanche", "sleet",
#         "drought", "flood", "flooding", "inundation", "waterlogging",
#         "landslide", "cloudburst", "tornado", "whirlwind", "dust storm",
#         "IMD", "India Meteorological Department", "forecast",
#         "weather forecast", "weather alert", "yellow alert",
#         "orange alert", "red alert", "rainfall deficit", "rainfall excess",
#         "maximum temperature", "minimum temperature", "mercury",
#         "humidity level", "dew point", "wind speed", "wind direction",
#         "atmospheric pressure", "visibility",
#         "El Nino", "La Nina", "ENSO", "IOD", "Indian Ocean Dipole",
#         "global warming", "climate change", "greenhouse gas",
#         "carbon emission", "sea level rise", "glacier", "glacial melt",
#         "polar vortex", "heat index", "wet bulb", "UV index",
#         "air quality", "AQI", "pollution", "smog alert",
#         "monsoon arrival", "monsoon withdrawal", "monsoon onset",
#         "western disturbance", "trough", "low pressure", "depression",
#         "cyclonic storm", "severe cyclone", "extremely severe",
#         "sunny", "cloudy", "partly cloudy", "overcast", "rainy",
#         "heavy rain", "light rain", "moderate rain", "heat", "cold",
#         "winter", "summer",
#     ],

#     "financial": [
#         "stock market", "share market", "equity", "BSE", "NSE", "Sensex",
#         "Nifty", "index", "trading", "bull market", "bear market",
#         "rally", "sell-off", "correction", "volatility",
#         "stock", "shares", "equity market", "IPO", "listing",
#         "mutual fund", "NAV", "SIP", "ELSS", "dividend",
#         "economy", "economic", "GDP", "GNP", "inflation", "deflation",
#         "CPI", "WPI", "fiscal", "fiscal deficit", "current account",
#         "trade deficit", "balance of payments", "foreign exchange",
#         "forex", "rupee", "dollar", "currency", "exchange rate",
#         "RBI", "Reserve Bank", "monetary policy", "repo rate",
#         "reverse repo", "CRR", "SLR", "liquidity",
#         "bank", "banking", "credit", "loan", "NPA", "bad loan",
#         "interest rate", "EMI", "home loan", "auto loan",
#         "insurance", "SEBI", "IRDAI", "PFRDA", "regulator",
#         "budget", "union budget", "tax", "GST", "income tax",
#         "direct tax", "indirect tax", "revenue", "expenditure",
#         "disinvestment", "privatisation", "FDI", "FPI", "FII",
#         "gold price", "silver price", "commodity market",
#         "MCX", "NCDEX", "futures", "options", "derivative",
#         "profit", "loss", "earnings", "quarterly results",
#         "annual results", "merger", "acquisition", "takeover",
#         "startup", "unicorn", "funding", "venture capital",
#         "market cap", "valuation", "fintech", "cryptocurrency",
#         "bitcoin", "blockchain", "digital currency",
#     ],

#     "energy": [
#         "oil", "crude oil", "Brent crude", "WTI", "petroleum",
#         "natural gas", "LNG", "LPG", "CNG", "pipeline",
#         "OPEC", "oil production", "oil refinery", "refining",
#         "petrol", "diesel", "fuel price", "fuel", "gasoline",
#         "ONGC", "IOC", "BPCL", "HPCL",
#         "electricity", "power", "power plant", "thermal power",
#         "coal power", "gas-based power", "nuclear power", "reactor",
#         "hydropower", "hydro", "dam", "turbine", "generator",
#         "grid", "power grid", "transmission", "distribution",
#         "power cut", "power outage", "load shedding", "blackout",
#         "energy sector", "power sector",
#         "NTPC", "NHPC", "PGCIL",
#         "solar", "solar energy", "solar power", "solar panel",
#         "photovoltaic", "wind energy", "wind power", "wind farm",
#         "windmill", "renewable", "renewables",
#         "green energy", "clean energy", "sustainable energy",
#         "biofuel", "ethanol", "biomass", "biogas", "green hydrogen",
#         "battery storage", "energy storage", "EV", "electric vehicle",
#         "EV charging", "MNRE", "IREDA",
#         "carbon credit", "carbon market", "emission trading",
#         "energy transition", "net zero", "decarbonisation",
#         "coal", "coal mine", "Coal India", "coal block",
#         "electricity tariff", "power tariff", "CERC",
#     ],
# }


# # ════════════════════════════════════════════════════════════════
# #  TEXT UTILITIES
# # ════════════════════════════════════════════════════════════════

# def clean_text(text):
#     return re.sub(r"\s+", " ", text or "").strip()


# def _kw_match(text: str, keyword: str) -> bool:
#     """Case-insensitive keyword match. Short words use word boundaries."""
#     t = text.lower()
#     k = keyword.lower()
#     if len(k) <= 4:
#         return bool(re.search(rf"\b{re.escape(k)}\b", t))
#     return k in t


# def _has_any_match(text: str, keywords: list) -> bool:
#     """Return True if text contains at least one keyword from the list."""
#     for kw in keywords:
#         if _kw_match(text, kw):
#             return True
#     return False


# def _get_all_matches(text: str, keywords: list) -> list:
#     """Return all keywords from the list that appear in text."""
#     return [kw for kw in keywords if _kw_match(text, kw)]


# def _count_matches(text: str, keywords: list) -> int:
#     return sum(1 for kw in keywords if _kw_match(text, kw))


# def detect_best_category(text: str) -> str:
#     """
#     Label the article by scoring each category's built-in keywords.
#     Returns the category with the most keyword hits, or 'general'.
#     This is ONLY for labeling — not for filtering.
#     """
#     best_cat, best_score = "general", 0
#     for cat, kws in CATEGORY_KEYWORDS.items():
#         score = _count_matches(text, kws)
#         if score > best_score:
#             best_score = score
#             best_cat   = cat
#     return best_cat


# # ════════════════════════════════════════════════════════════════
# #  RAW DB HELPERS
# # ════════════════════════════════════════════════════════════════

# def _open(db_config):
#     return mysql.connector.connect(**db_config)


# def _fetch_col(db_config, table, col):
#     conn = _open(db_config)
#     cur  = conn.cursor(dictionary=True)
#     cur.execute(f"SELECT {col} FROM `{table}` ORDER BY sr_no ASC")
#     rows = cur.fetchall()
#     cur.close(); conn.close()
#     vals = []
#     for r in rows:
#         raw = clean_text(r.get(col))
#         if raw:
#             vals.extend(x.strip() for x in raw.split(",") if x.strip())
#     return list(dict.fromkeys(vals))


# def _fetch_user_keywords(db_config) -> list:
#     """
#     Fetch keywords from the `keywords` table.
#     These are the user's search terms — the PRIMARY scrape filter.
#     """
#     return _fetch_col(db_config, "keywords", "keyword")


# def _fetch_site_configs(db_config):
#     conn = _open(db_config)
#     cur  = conn.cursor(dictionary=True)
#     cur.execute("SELECT websites FROM websites ORDER BY sr_no ASC")
#     rows = cur.fetchall()
#     cur.close(); conn.close()

#     configs = []
#     for row in rows:
#         raw = clean_text(row.get("websites"))
#         if not raw:
#             continue
#         try:
#             obj = json.loads(raw)
#         except Exception:
#             if raw.lower().startswith(("http://", "https://")):
#                 obj = {
#                     "name": urlparse(raw).netloc or raw,
#                     "url": raw,
#                     "listing_selector": "a[href]",
#                     "article_selectors": ["p"],
#                     "news_type": "general",
#                 }
#             else:
#                 print(f"[SCRAPER] Skipping invalid config: {raw[:60]}")
#                 continue

#         url = clean_text(obj.get("url"))
#         ls  = clean_text(obj.get("listing_selector"))
#         ars = obj.get("article_selectors", [])
#         if not url or not ls or not isinstance(ars, list) or not ars:
#             continue
#         configs.append({
#             "name":              clean_text(obj.get("name")) or urlparse(url).netloc,
#             "url":               url,
#             "listing_selector":  ls,
#             "article_selectors": ars,
#             "news_type":         clean_text(obj.get("news_type")) or "general",
#         })
#     return configs


# def _is_duplicate(news_url, db_config):
#     conn = _open(db_config)
#     cur  = conn.cursor()
#     cur.execute(
#         "SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1",
#         (news_url,)
#     )
#     found = cur.fetchone() is not None
#     cur.close(); conn.close()
#     return found


# def _is_published_duplicate(news_url, db_config):
#     conn = _open(db_config)
#     cur  = conn.cursor()
#     cur.execute(
#         "SELECT id FROM published_news WHERE news_url=%s LIMIT 1",
#         (news_url,)
#     )
#     found = cur.fetchone() is not None
#     cur.close(); conn.close()
#     return found


# def _insert_non_published(headline, news_text, news_url, news_type,
#                            matched_kws, db_config):
#     if not headline or not news_url:
#         return False, "missing"
#     try:
#         conn = _open(db_config)
#         cur  = conn.cursor()
#         cur.execute(
#             "SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1",
#             (news_url,)
#         )
#         if cur.fetchone():
#             cur.close(); conn.close()
#             return False, "duplicate"
#         cur.execute(
#             "INSERT INTO non_published_news "
#             "(news_date, news_type, news_headline, news_text, "
#             " news_url, keywords, published) "
#             "VALUES (%s,%s,%s,%s,%s,%s,0)",
#             (date.today(), news_type, headline, news_text,
#              news_url, ", ".join(matched_kws))
#         )
#         conn.commit(); cur.close(); conn.close()
#         return True, "ok"
#     except Exception as e:
#         return False, f"db:{e}"


# def _insert_published(headline, news_text, news_url, news_type,
#                       matched_kws, db_config):
#     """Insert directly into published_news (auto mode)."""
#     if not headline or not news_url:
#         return None, "missing"
#     try:
#         conn = _open(db_config)
#         cur  = conn.cursor()
#         cur.execute(
#             "SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1",
#             (news_url,)
#         )
#         if cur.fetchone():
#             cur.close(); conn.close()
#             return None, "duplicate"
#         cur.execute(
#             "SELECT id FROM published_news WHERE news_url=%s LIMIT 1",
#             (news_url,)
#         )
#         if cur.fetchone():
#             cur.close(); conn.close()
#             return None, "duplicate"
#         cur.execute(
#             "INSERT INTO published_news "
#             "(news_date, news_type, news_headline, news_text, "
#             " news_url, keywords, published_at) "
#             "VALUES (%s,%s,%s,%s,%s,%s,NOW())",
#             (date.today(), news_type, headline, news_text,
#              news_url, ", ".join(matched_kws))
#         )
#         conn.commit()
#         pub_id = cur.lastrowid
#         cur.close(); conn.close()
#         return pub_id, "ok"
#     except Exception as e:
#         return None, f"db:{e}"


# # ════════════════════════════════════════════════════════════════
# #  HTTP HELPERS
# # ════════════════════════════════════════════════════════════════

# def _fetch_html(url, timeout):
#     r = requests.get(url, headers=HEADERS, timeout=timeout)
#     r.raise_for_status()
#     return r.text


# def _scrape_listing(site):
#     html = _fetch_html(site["url"], LISTING_TIMEOUT)
#     soup = BeautifulSoup(html, "html.parser")
#     out, seen, host = [], set(), urlparse(site["url"]).netloc.lower()
#     for a in soup.select(site["listing_selector"]):
#         headline = clean_text(a.get_text(" ", strip=True))
#         href     = clean_text(a.get("href"))
#         if not headline or len(headline) < 25:
#             continue
#         if not href or href.startswith("#") or href.lower().startswith("javascript:"):
#             continue
#         full = urljoin(site["url"], href)
#         p    = urlparse(full)
#         if not p.scheme.startswith("http"):
#             continue
#         if host not in p.netloc.lower() and p.netloc.lower() not in host:
#             continue
#         if full in seen:
#             continue
#         seen.add(full)
#         out.append({"headline": headline, "url": full})
#     return out


# def _extract_text(url, selectors):
#     try:
#         html = _fetch_html(url, ARTICLE_TIMEOUT)
#     except Exception as e:
#         print(f"[SCRAPER] Article fetch failed {url}: {e}")
#         return ""
#     soup = BeautifulSoup(html, "html.parser")
#     for sel in selectors:
#         paras = soup.select(sel)
#         if not paras:
#             continue
#         parts, seen_t = [], set()
#         for p in paras:
#             t = clean_text(p.get_text(" ", strip=True))
#             if t and len(t) > 30 and t not in seen_t:
#                 seen_t.add(t); parts.append(t)
#         if parts:
#             return "\n".join(parts)
#     return ""


# # ════════════════════════════════════════════════════════════════
# #  PER-ARTICLE PROCESSING
# # ════════════════════════════════════════════════════════════════

# def _process(article, site, user_keywords, target_categories,
#              publish_mode, db_config):
#     """
#     Filter, categorize, and insert one article.

#     Filtering logic:
#     ─────────────────
#     Step 1 — Category headline pre-filter (fast, no network)
#              Uses the BUILT-IN category keywords (100+ per category) to
#              decide if the headline is even worth fetching.
#              - If target_categories == ['all']: check all categories combined.
#              - If specific cats selected: check only those categories.
#              This avoids fetching bodies of completely unrelated articles.
#              NOTE: We do NOT use user keywords here — they are too narrow
#              and would reject articles before we even read them.

#     Step 2 — Duplicate check (before expensive network fetch)

#     Step 3 — Fetch full article body

#     Step 4 — User keyword match on FULL TEXT (headline + body)
#              The user's keywords from the `keywords` table are checked
#              against the complete article text.
#              - If user has keywords: article must match at least one.
#              - If user has NO keywords: accept all articles that passed
#                the category filter (step 1).

#     Step 5 — Label with best-matching category

#     Step 6 — Insert
#     """
#     headline = clean_text(article.get("headline"))
#     url      = clean_text(article.get("url"))
#     if not headline or not url:
#         return False, "missing", None

#     # ── Step 1: category pre-filter on headline (fast, no fetch) ─
#     # Build the pool of category keywords to check against
#     if "all" in target_categories:
#         # Union of all category keyword sets
#         cat_pool = [kw for kws in CATEGORY_KEYWORDS.values() for kw in kws]
#     else:
#         cat_pool = []
#         for cat in target_categories:
#             cat_pool.extend(CATEGORY_KEYWORDS.get(cat, []))

#     if cat_pool and not _has_any_match(headline, cat_pool):
#         return False, "headline_no_match", None

#     # ── Step 2: duplicate check ────────────────────────────────
#     if _is_duplicate(url, db_config):
#         return False, "duplicate", None
#     if publish_mode == "auto" and _is_published_duplicate(url, db_config):
#         return False, "duplicate", None

#     # ── Step 3: fetch full article body ───────────────────────
#     body      = _extract_text(url, site["article_selectors"])
#     full_text = f"{headline} {body}"

#     # ── Step 4: user keyword match on full text ────────────────
#     if user_keywords:
#         matched_user_kws = _get_all_matches(full_text, user_keywords)
#         if not matched_user_kws:
#             return False, "no_user_kw_match", None
#     else:
#         # No user keywords configured — accept all that passed cat filter
#         matched_user_kws = []

#     # ── Step 5: label with best category ──────────────────────
#     detected_cat = detect_best_category(full_text)
#     news_type    = (
#         detected_cat if detected_cat in CATEGORY_KEYWORDS
#         else site.get("news_type", "general")
#     )

#     # Store matched user keywords (or fall back to category label)
#     store_kws = matched_user_kws if matched_user_kws else [news_type]

#     # ── Step 6: insert ─────────────────────────────────────────
#     if publish_mode == "auto":
#         pub_id, reason = _insert_published(
#             headline, body, url, news_type, store_kws, db_config
#         )
#         return (pub_id is not None), reason, pub_id
#     else:
#         ok, reason = _insert_non_published(
#             headline, body, url, news_type, store_kws, db_config
#         )
#         return ok, reason, None


# def _save_json(instance_path, site_name, articles):
#     folder = os.path.join(instance_path, "downloaded_json")
#     os.makedirs(folder, exist_ok=True)
#     safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", site_name)
#     path = os.path.join(folder, f"{safe}.json")
#     with open(path, "w", encoding="utf-8") as f:
#         json.dump(articles, f, ensure_ascii=False, indent=2)
#     return path


# # ════════════════════════════════════════════════════════════════
# #  PUBLIC ENTRY POINT
# # ════════════════════════════════════════════════════════════════

# def run_news_scraper(db_config: dict, instance_path: str,
#                      target_categories: list = None,
#                      publish_mode: str = "manual") -> dict:
#     """
#     Parameters
#     ----------
#     db_config         : mysql.connector kwargs
#     instance_path     : Flask instance_path (for JSON dumps)
#     target_categories : user's selected categories, e.g. ['agricultural']
#                         ['all'] or None = accept any category
#     publish_mode      : 'manual' → non_published_news
#                         'auto'   → published_news directly
#     """
#     if not target_categories:
#         target_categories = ["all"]
#     target_categories = [c.strip().lower() for c in target_categories]

#     # Load user's keywords — the PRIMARY scrape filter
#     user_keywords = _fetch_user_keywords(db_config)
#     if not user_keywords:
#         print("[SCRAPER] WARNING: No user keywords found in `keywords` table. "
#               "All articles will be accepted (no keyword filter).")

#     print(f"[SCRAPER] Starting — cats={target_categories} "
#           f"mode={publish_mode} user_keywords={len(user_keywords)}")

#     sites = _fetch_site_configs(db_config)
#     if not sites:
#         return {
#             "success": False, "inserted": 0, "skipped": 0,
#             "failed_sites": 0, "total": 0,
#             "message": "No website configs found in the websites table.",
#         }

#     total = inserted = skipped = failed = 0
#     auto_pub_ids  = []
#     auto_articles = []
#     files         = []
#     skip_reasons  = {}   # reason → count, for debugging

#     for site in sites:
#         print(f"[SCRAPER] Listing: {site['url']}")
#         try:
#             articles = _scrape_listing(site)
#         except Exception as e:
#             print(f"[SCRAPER] Listing failed {site['url']}: {e}")
#             failed += 1
#             continue

#         total += len(articles)
#         print(f"[SCRAPER] {site['name']}: {len(articles)} links found")

#         try:
#             files.append(_save_json(instance_path, site.get("name", "site"), articles))
#         except Exception as e:
#             print(f"[SCRAPER] JSON save error: {e}")

#         if not articles:
#             continue

#         n = min(MAX_WORKERS, len(articles))
#         with ThreadPoolExecutor(max_workers=n) as ex:
#             fmap = {
#                 ex.submit(
#                     _process, a, site,
#                     user_keywords, target_categories,
#                     publish_mode, db_config
#                 ): a
#                 for a in articles
#             }
#             for fut in as_completed(fmap):
#                 a = fmap[fut]
#                 try:
#                     ok, reason, pub_id = fut.result(timeout=TASK_TIMEOUT)
#                     if ok:
#                         inserted += 1
#                         print(f"[SCRAPER] ✓ Inserted ({publish_mode}): {a['headline'][:70]}")
#                         if publish_mode == "auto" and pub_id:
#                             auto_pub_ids.append(pub_id)
#                             auto_articles.append(a)
#                     else:
#                         skipped += 1
#                         skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
#                         if reason not in (
#                             "duplicate", "headline_no_match",
#                             "no_match", "no_user_kw_match", "category_mismatch"
#                         ):
#                             print(f"[SCRAPER] Skip({reason}): {a['headline'][:70]}")
#                 except FutureTimeout:
#                     skipped += 1
#                     print(f"[SCRAPER] Timeout: {a.get('headline','')[:70]}")
#                 except Exception as e:
#                     skipped += 1
#                     print(f"[SCRAPER] Error: {a.get('headline','')[:70]} — {e}")

#     msg = (
#         f"Done. Scraped:{total} Inserted:{inserted} "
#         f"Skipped:{skipped} Failed sites:{failed} "
#         f"Categories:{target_categories} Mode:{publish_mode}"
#     )
#     print(f"[SCRAPER] {msg}")
#     if skip_reasons:
#         print(f"[SCRAPER] Skip breakdown: {skip_reasons}")

#     return {
#         "success":       True,
#         "inserted":      inserted,
#         "skipped":       skipped,
#         "failed_sites":  failed,
#         "total":         total,
#         "files":         files,
#         "auto_pub_ids":  auto_pub_ids,
#         "auto_articles": auto_articles,
#         "message":       msg,
#     }
"""
news_scraper.py
---------------
IMPORTANT: Uses raw mysql.connector ONLY. Never import Flask helpers here.
Safe to run inside background threads.

Website configs in DB can be stored as:
  1. Plain URL string: "https://www.ndtv.com/latest"
     → scraper auto-detects if it's RSS or HTML and picks selectors
  2. JSON config string: {"name":..., "url":..., "listing_selector":..., ...}
     → full control over selectors

RSS feeds are auto-detected and parsed as XML.
Plain HTML pages fall back to common article link selectors.
"""

import json
import os
import re
import xml.etree.ElementTree as ET
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
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}

LISTING_TIMEOUT = 15
ARTICLE_TIMEOUT = 10
MAX_WORKERS     = 4
TASK_TIMEOUT    = 20

# ── Known RSS feeds for common news sites ─────────────────────
# When a plain URL is added, we check if a better RSS version exists
KNOWN_RSS_MAP = {
    "ndtv.com":                     "https://feeds.feedburner.com/ndtvnews-top-stories",
    "timesofindia.indiatimes.com":  "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
    "thehindu.com":                 "https://www.thehindu.com/feeder/default.rss",
    "economictimes.indiatimes.com": "https://economictimes.indiatimes.com/rssfeedsdefault.cms",
    "hindustantimes.com":           "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
    "indianexpress.com":            "https://indianexpress.com/feed/",
    "livemint.com":                 "https://www.livemint.com/rss/news",
    "businessstandard.com":         "https://www.business-standard.com/rss/home_page_top_stories.rss",
    "financialexpress.com":         "https://www.financialexpress.com/feed/",
    "moneycontrol.com":             "https://www.moneycontrol.com/rss/latestnews.xml",
    "zeenews.india.com":            "https://zeenews.india.com/rss/india-national-news.xml",
    "news18.com":                   "https://www.news18.com/rss/india.xml",
    "theprint.in":                  "https://theprint.in/feed/",
    "thewire.in":                   "https://thewire.in/feed",
    "scroll.in":                    "https://scroll.in/feed",
    "mausam.imd.gov.in":            None,  # No RSS, use HTML scraping
}

# ── Article body selectors per domain ─────────────────────────
DOMAIN_ARTICLE_SELECTORS = {
    "ndtv.com":                     [".ins_storybody p", "div.article__text p", "p"],
    "timesofindia.indiatimes.com":  ["div.Normal p", "div._s30J p", "div.ga-headlines p", "p"],
    "thehindu.com":                 ["div.articlebodycontent p", "[itemprop='articleBody'] p", "p"],
    "economictimes.indiatimes.com": ["div.artText p", "div.article-body p", "p"],
    "hindustantimes.com":           ["div.storyDetail p", "div.detail p", "p"],
    "indianexpress.com":            ["div.full-details p", "div.story_details p", "p"],
    "livemint.com":                 ["div.mainArea p", "div.contentSec p", "p"],
    "businessstandard.com":         ["div.storycontent p", "span.p-content p", "p"],
    "financialexpress.com":         ["div.pcl-content p", "div.ie-first-para p", "p"],
    "moneycontrol.com":             ["div.arti-flow p", "div#article-main p", "p"],
}

# ════════════════════════════════════════════════════════════════
#  BUILT-IN CATEGORY KEYWORD SETS
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
        "fertilizer", "fertiliser", "urea", "DAP", "potash",
        "pesticide", "insecticide", "herbicide", "fungicide",
        "irrigation", "drip irrigation", "sprinkler", "seeds", "hybrid seeds",
        "kisan", "agri", "livestock", "dairy", "milk", "cattle",
        "poultry", "fisheries", "aquaculture", "horticulture",
        "floriculture", "sericulture", "mandi", "MSP",
        "minimum support price", "agri market", "APMC",
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
        "heavy rain", "light rain", "moderate rain", "heat", "cold",
        "winter", "summer",
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
        "energy sector", "power sector", "NTPC", "NHPC", "PGCIL",
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


# ════════════════════════════════════════════════════════════════
#  TEXT UTILITIES
# ════════════════════════════════════════════════════════════════

def clean_text(text):
    return re.sub(r"\s+", " ", text or "").strip()


def _kw_match(text: str, keyword: str) -> bool:
    t = text.lower()
    k = keyword.lower()
    if len(k) <= 4:
        return bool(re.search(rf"\b{re.escape(k)}\b", t))
    return k in t


def _has_any_match(text: str, keywords: list) -> bool:
    return any(_kw_match(text, kw) for kw in keywords)


def _get_all_matches(text: str, keywords: list) -> list:
    return [kw for kw in keywords if _kw_match(text, kw)]


def _count_matches(text: str, keywords: list) -> int:
    return sum(1 for kw in keywords if _kw_match(text, kw))


def detect_best_category(text: str) -> str:
    best_cat, best_score = "general", 0
    for cat, kws in CATEGORY_KEYWORDS.items():
        score = _count_matches(text, kws)
        if score > best_score:
            best_score = score
            best_cat   = cat
    return best_cat


# ════════════════════════════════════════════════════════════════
#  RAW DB HELPERS
# ════════════════════════════════════════════════════════════════

def _open(db_config):
    return mysql.connector.connect(**db_config)


def _fetch_user_keywords(db_config) -> list:
    conn = _open(db_config)
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT keyword FROM keywords ORDER BY sr_no ASC")
    rows = cur.fetchall()
    cur.close(); conn.close()
    vals = []
    for r in rows:
        raw = clean_text(r.get("keyword"))
        if raw:
            vals.extend(x.strip() for x in raw.split(",") if x.strip())
    return list(dict.fromkeys(vals))


def _fetch_site_configs(db_config):
    """
    Read websites table. Each row can be:
      - A plain URL string  → auto-resolve to RSS or HTML config
      - A JSON config string → use as-is
    """
    conn = _open(db_config)
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT websites FROM websites ORDER BY sr_no ASC")
    rows = cur.fetchall()
    cur.close(); conn.close()

    configs = []
    for row in rows:
        raw = clean_text(row.get("websites", ""))
        if not raw:
            continue

        # ── Try JSON first ─────────────────────────────────────
        if raw.strip().startswith("{"):
            try:
                obj = json.loads(raw)
                url = clean_text(obj.get("url", ""))
                if not url:
                    continue
                domain = urlparse(url).netloc.lower().replace("www.", "")
                configs.append({
                    "name":              clean_text(obj.get("name")) or domain,
                    "url":               url,
                    "listing_selector":  clean_text(obj.get("listing_selector", "a[href]")),
                    "article_selectors": obj.get("article_selectors", ["p"]),
                    "news_type":         clean_text(obj.get("news_type", "general")),
                    "is_rss":            obj.get("is_rss", False),
                })
                continue
            except Exception as e:
                print(f"[SCRAPER] JSON parse error for: {raw[:60]} — {e}")
                continue

        # ── Plain URL ──────────────────────────────────────────
        if raw.lower().startswith(("http://", "https://")):
            url    = raw
            domain = urlparse(url).netloc.lower().replace("www.", "")

            # Check if we have a known better RSS URL for this domain
            rss_url = None
            for known_domain, known_rss in KNOWN_RSS_MAP.items():
                if known_domain in domain or domain in known_domain:
                    rss_url = known_rss
                    break

            # Get article selectors for this domain
            article_sels = ["p"]
            for known_domain, sels in DOMAIN_ARTICLE_SELECTORS.items():
                if known_domain in domain or domain in known_domain:
                    article_sels = sels
                    break

            if rss_url:
                # Use the RSS feed URL instead
                configs.append({
                    "name":              domain,
                    "url":               rss_url,
                    "listing_selector":  "item",
                    "article_selectors": article_sels,
                    "news_type":         "general",
                    "is_rss":            True,
                })
                print(f"[SCRAPER] Auto-resolved {domain} → RSS: {rss_url}")
            else:
                # Use the plain URL with HTML scraping
                configs.append({
                    "name":              domain,
                    "url":               url,
                    "listing_selector":  "a[href]",
                    "article_selectors": article_sels,
                    "news_type":         "general",
                    "is_rss":            False,
                })
        else:
            print(f"[SCRAPER] Skipping invalid entry: {raw[:60]}")

    return configs


def _is_duplicate(news_url, db_config):
    conn = _open(db_config)
    cur  = conn.cursor()
    cur.execute("SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1", (news_url,))
    found = cur.fetchone() is not None
    cur.close(); conn.close()
    return found


def _is_published_duplicate(news_url, db_config):
    conn = _open(db_config)
    cur  = conn.cursor()
    cur.execute("SELECT id FROM published_news WHERE news_url=%s LIMIT 1", (news_url,))
    found = cur.fetchone() is not None
    cur.close(); conn.close()
    return found


def _insert_non_published(headline, news_text, news_url, news_type, matched_kws, db_config):
    if not headline or not news_url:
        return False, "missing"
    try:
        conn = _open(db_config)
        cur  = conn.cursor()
        cur.execute("SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1", (news_url,))
        if cur.fetchone():
            cur.close(); conn.close()
            return False, "duplicate"
        cur.execute(
            "INSERT INTO non_published_news "
            "(news_date, news_type, news_headline, news_text, news_url, keywords, published) "
            "VALUES (%s,%s,%s,%s,%s,%s,0)",
            (date.today(), news_type, headline, news_text, news_url, ", ".join(matched_kws))
        )
        conn.commit(); cur.close(); conn.close()
        return True, "ok"
    except Exception as e:
        return False, f"db:{e}"


def _insert_published(headline, news_text, news_url, news_type, matched_kws, db_config):
    if not headline or not news_url:
        return None, "missing"
    try:
        conn = _open(db_config)
        cur  = conn.cursor()
        cur.execute("SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1", (news_url,))
        if cur.fetchone():
            cur.close(); conn.close()
            return None, "duplicate"
        cur.execute("SELECT id FROM published_news WHERE news_url=%s LIMIT 1", (news_url,))
        if cur.fetchone():
            cur.close(); conn.close()
            return None, "duplicate"
        cur.execute(
            "INSERT INTO published_news "
            "(news_date, news_type, news_headline, news_text, news_url, keywords, published_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,NOW())",
            (date.today(), news_type, headline, news_text, news_url, ", ".join(matched_kws))
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


# ════════════════════════════════════════════════════════════════
#  RSS PARSER
# ════════════════════════════════════════════════════════════════

def _scrape_rss(xml_text: str, site: dict) -> list:
    """Parse RSS/Atom XML and return list of article dicts."""
    out, seen = [], set()

    # Strip invalid XML characters that some feeds include
    xml_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', xml_text)

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"[SCRAPER] RSS XML parse error for {site['name']}: {e}")
        # Try BeautifulSoup as fallback XML parser
        return _scrape_rss_bs4(xml_text, site)

    ns = {
        'atom':    'http://www.w3.org/2005/Atom',
        'content': 'http://purl.org/rss/1.0/modules/content/',
        'media':   'http://search.yahoo.com/mrss/',
    }

    items = root.findall('.//item') or root.findall('.//atom:entry', ns)

    for item in items:
        # Title
        title_el = item.find('title')
        headline = ""
        if title_el is not None and title_el.text:
            headline = clean_text(title_el.text)
            headline = re.sub(r'<!\[CDATA\[|\]\]>', '', headline).strip()

        if not headline or len(headline) < 15:
            continue

        # URL — try link, then guid, then atom:link
        url = ""
        link_el = item.find('link')
        if link_el is not None:
            url = clean_text(link_el.text or link_el.get('href', ''))
        if not url:
            guid_el = item.find('guid')
            if guid_el is not None:
                val = clean_text(guid_el.text or "")
                if val.startswith('http'):
                    url = val
        if not url:
            atom_link = item.find('atom:link', ns)
            if atom_link is not None:
                url = clean_text(atom_link.get('href', ''))

        if not url or not url.startswith('http'):
            continue
        # Clean tracking params
        url = url.split('?')[0] if '?utm_' in url else url
        if url in seen:
            continue
        seen.add(url)

        # Description — strip HTML tags
        desc = ""
        for tag_name in ['description', 'summary']:
            el = item.find(tag_name)
            if el is not None and el.text:
                raw = re.sub(r'<!\[CDATA\[|\]\]>', '', el.text)
                desc_soup = BeautifulSoup(raw, "html.parser")
                desc = clean_text(desc_soup.get_text(" ", strip=True))
                if len(desc) > 30:
                    break

        # content:encoded — richer body text
        content_el = item.find('content:encoded')
        if content_el is None:
            content_el = item.find('{http://purl.org/rss/1.0/modules/content/}encoded')
        content_text = ""
        if content_el is not None and content_el.text:
            raw = re.sub(r'<!\[CDATA\[|\]\]>', '', content_el.text)
            c_soup = BeautifulSoup(raw, "html.parser")
            content_text = clean_text(c_soup.get_text(" ", strip=True))

        body = content_text if len(content_text) > len(desc) else desc

        out.append({"headline": headline, "url": url, "description": body})

    print(f"[SCRAPER] RSS parsed {len(out)} items from {site['name']}")
    return out


def _scrape_rss_bs4(xml_text: str, site: dict) -> list:
    """Fallback RSS parser using BeautifulSoup for malformed XML."""
    out, seen = [], set()
    soup = BeautifulSoup(xml_text, "xml")
    for item in soup.find_all("item"):
        title = item.find("title")
        headline = clean_text(title.get_text()) if title else ""
        headline = re.sub(r'<!\[CDATA\[|\]\]>', '', headline).strip()
        if not headline or len(headline) < 15:
            continue

        link = item.find("link")
        url = clean_text(link.get_text() if link else "")
        if not url or not url.startswith("http"):
            guid = item.find("guid")
            if guid:
                url = clean_text(guid.get_text())
        if not url or not url.startswith("http"):
            continue
        if url in seen:
            continue
        seen.add(url)

        desc_tag = item.find("description")
        desc = ""
        if desc_tag:
            raw = re.sub(r'<!\[CDATA\[|\]\]>', '', desc_tag.get_text())
            desc = clean_text(BeautifulSoup(raw, "html.parser").get_text())

        out.append({"headline": headline, "url": url, "description": desc})

    print(f"[SCRAPER] RSS(bs4) parsed {len(out)} items from {site['name']}")
    return out


# ════════════════════════════════════════════════════════════════
#  HTML LISTING SCRAPER
# ════════════════════════════════════════════════════════════════

def _scrape_html_listing(html: str, site: dict) -> list:
    """Scrape article links from an HTML page."""
    soup = BeautifulSoup(html, "html.parser")
    out, seen = [], set()
    host = urlparse(site["url"]).netloc.lower()

    for a in soup.select(site.get("listing_selector", "a[href]")):
        headline = clean_text(a.get_text(" ", strip=True))
        href     = clean_text(a.get("href", ""))

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
        out.append({"headline": headline, "url": full, "description": ""})

    return out


# ════════════════════════════════════════════════════════════════
#  UNIFIED LISTING SCRAPER
# ════════════════════════════════════════════════════════════════

def _scrape_listing(site: dict) -> list:
    """Fetch listing page and return articles. Auto-detects RSS vs HTML."""
    try:
        html = _fetch_html(site["url"], LISTING_TIMEOUT)
    except Exception as e:
        raise Exception(f"Fetch failed: {e}")

    # Auto-detect RSS
    is_rss = site.get("is_rss", False)
    if not is_rss:
        stripped = html.strip()
        if stripped.startswith("<?xml") or stripped.startswith("<rss") or "<channel>" in stripped[:500]:
            is_rss = True

    if is_rss:
        return _scrape_rss(html, site)
    else:
        articles = _scrape_html_listing(html, site)
        print(f"[SCRAPER] HTML: {len(articles)} links from {site['name']}")
        return articles


# ════════════════════════════════════════════════════════════════
#  ARTICLE BODY EXTRACTOR
# ════════════════════════════════════════════════════════════════

def _extract_text(url: str, selectors: list) -> str:
    """Fetch article page and extract body text."""
    try:
        html = _fetch_html(url, ARTICLE_TIMEOUT)
    except Exception as e:
        print(f"[SCRAPER] Article fetch failed {url}: {e}")
        return ""

    soup = BeautifulSoup(html, "html.parser")

    # Remove junk
    for tag in soup.select("nav,footer,header,script,style,iframe,noscript,"
                           "[class*='related'],[class*='recommend'],[class*='also-read'],"
                           "[class*='social'],[class*='share'],[class*='comment']"):
        tag.decompose()

    for sel in selectors:
        paras = soup.select(sel)
        if not paras:
            continue
        parts, seen_t = [], set()
        for p in paras:
            t = clean_text(p.get_text(" ", strip=True))
            if t and len(t) > 30 and t not in seen_t:
                seen_t.add(t)
                parts.append(t)
        if parts:
            return "\n".join(parts)

    return ""


# ════════════════════════════════════════════════════════════════
#  PER-ARTICLE PROCESSING
# ════════════════════════════════════════════════════════════════

def _process(article, site, user_keywords, target_categories, publish_mode, db_config):
    headline    = clean_text(article.get("headline", ""))
    url         = clean_text(article.get("url", ""))
    rss_desc    = article.get("description", "")

    if not headline or not url:
        return False, "missing", None

    # Step 1: category headline pre-filter
    if "all" in target_categories:
        cat_pool = [kw for kws in CATEGORY_KEYWORDS.values() for kw in kws]
    else:
        cat_pool = []
        for cat in target_categories:
            cat_pool.extend(CATEGORY_KEYWORDS.get(cat, []))

    if cat_pool and not _has_any_match(headline, cat_pool):
        return False, "headline_no_match", None

    # Step 2: duplicate check
    if _is_duplicate(url, db_config):
        return False, "duplicate", None
    if publish_mode == "auto" and _is_published_duplicate(url, db_config):
        return False, "duplicate", None

    # Step 3: get body — use RSS description first, fetch full article if too short
    body = rss_desc or ""
    if len(body) < 200:
        fetched = _extract_text(url, site["article_selectors"])
        if len(fetched) > len(body):
            body = fetched

    full_text = f"{headline} {body}"

    # Step 4: user keyword match
    if user_keywords:
        matched_user_kws = _get_all_matches(full_text, user_keywords)
        if not matched_user_kws:
            return False, "no_user_kw_match", None
    else:
        matched_user_kws = []

    # Step 5: label category
    detected_cat = detect_best_category(full_text)
    news_type    = detected_cat if detected_cat in CATEGORY_KEYWORDS else site.get("news_type", "general")
    store_kws    = matched_user_kws if matched_user_kws else [news_type]

    # Step 6: insert
    if publish_mode == "auto":
        pub_id, reason = _insert_published(headline, body, url, news_type, store_kws, db_config)
        return (pub_id is not None), reason, pub_id
    else:
        ok, reason = _insert_non_published(headline, body, url, news_type, store_kws, db_config)
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

    if not target_categories:
        target_categories = ["all"]
    target_categories = [c.strip().lower() for c in target_categories]

    user_keywords = _fetch_user_keywords(db_config)
    if not user_keywords:
        print("[SCRAPER] WARNING: No user keywords. All category-matched articles accepted.")

    print(f"[SCRAPER] Starting — cats={target_categories} mode={publish_mode} keywords={len(user_keywords)}")

    sites = _fetch_site_configs(db_config)
    if not sites:
        return {
            "success": False, "inserted": 0, "skipped": 0,
            "failed_sites": 0, "total": 0,
            "message": "No website configs found.",
        }

    print(f"[SCRAPER] {len(sites)} sites loaded")

    total = inserted = skipped = failed = 0
    auto_pub_ids  = []
    auto_articles = []
    files         = []
    skip_reasons  = {}

    for site in sites:
        print(f"[SCRAPER] Listing: {site['url']}")
        try:
            articles = _scrape_listing(site)
        except Exception as e:
            print(f"[SCRAPER] Listing failed {site['url']}: {e}")
            failed += 1
            continue

        total += len(articles)
        if not articles:
            print(f"[SCRAPER] {site['name']}: 0 articles found")
            continue

        try:
            files.append(_save_json(instance_path, site.get("name", "site"), articles))
        except Exception as e:
            print(f"[SCRAPER] JSON save error: {e}")

        n = min(MAX_WORKERS, len(articles))
        with ThreadPoolExecutor(max_workers=n) as ex:
            fmap = {
                ex.submit(_process, a, site, user_keywords,
                          target_categories, publish_mode, db_config): a
                for a in articles
            }
            for fut in as_completed(fmap):
                a = fmap[fut]
                try:
                    ok, reason, pub_id = fut.result(timeout=TASK_TIMEOUT)
                    if ok:
                        inserted += 1
                        print(f"[SCRAPER] ✓ {a['headline'][:70]}")
                        if publish_mode == "auto" and pub_id:
                            auto_pub_ids.append(pub_id)
                            auto_articles.append(a)
                    else:
                        skipped += 1
                        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                        if reason not in ("duplicate", "headline_no_match",
                                          "no_match", "no_user_kw_match",
                                          "category_mismatch"):
                            print(f"[SCRAPER] Skip({reason}): {a['headline'][:70]}")
                except FutureTimeout:
                    skipped += 1
                    print(f"[SCRAPER] Timeout: {a.get('headline','')[:70]}")
                except Exception as e:
                    skipped += 1
                    print(f"[SCRAPER] Error: {a.get('headline','')[:70]} — {e}")

    msg = (f"Done. Scraped:{total} Inserted:{inserted} "
           f"Skipped:{skipped} Failed sites:{failed} "
           f"Categories:{target_categories} Mode:{publish_mode}")
    print(f"[SCRAPER] {msg}")
    if skip_reasons:
        print(f"[SCRAPER] Skip breakdown: {skip_reasons}")

    return {
        "success":       True,
        "inserted":      inserted,
        "skipped":       skipped,
        "failed_sites":  failed,
        "total":         total,
        "files":         files,
        "auto_pub_ids":  auto_pub_ids,
        "auto_articles": auto_articles,
        "message":       msg,
    }