"""
news_scraper.py
---------------
IMPORTANT: This module uses raw mysql.connector ONLY.
It must NEVER import or call Flask's get_db(), g, or current_app.
It is designed to run safely inside background threads.
"""

import json, os, re
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

LISTING_TIMEOUT  = 10   # seconds for listing page fetch
ARTICLE_TIMEOUT  = 6    # seconds for article page fetch
MAX_WORKERS      = 4    # concurrent threads per site
TASK_TIMEOUT     = 15   # max seconds per article task


def clean_text(text):
    return re.sub(r"\s+", " ", text or "").strip()


def find_matches(text, values):
    text = (text or "").lower()
    return [v for v in values if v and re.search(rf"\b{re.escape(v.lower())}\b", text)]


def choose_news_type(full_text, db_news_types, matched_commodities, site_default):
    if matched_commodities:
        return "commodity"
    m = find_matches(full_text, db_news_types)
    return m[0] if m else (site_default or "general")


# ── Raw DB helpers (no Flask) ────────────────────────────────

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
                obj = {"name": urlparse(raw).netloc or raw, "url": raw,
                       "listing_selector": "a[href]",
                       "article_selectors": ["p"], "news_type": "general"}
            else:
                print(f"[SCRAPER] Skipping invalid config: {raw[:60]}")
                continue

        url  = clean_text(obj.get("url"))
        ls   = clean_text(obj.get("listing_selector"))
        ars  = obj.get("article_selectors", [])
        if not url or not ls or not isinstance(ars, list) or not ars:
            continue
        configs.append({
            "name":               clean_text(obj.get("name")) or urlparse(url).netloc,
            "url":                url,
            "listing_selector":   ls,
            "article_selectors":  ars,
            "news_type":          clean_text(obj.get("news_type")) or "general",
        })
    return configs


def _is_duplicate(news_url, db_config):
    conn = _open(db_config)
    cur  = conn.cursor()
    cur.execute("SELECT id FROM non_published_news WHERE news_url=%s LIMIT 1", (news_url,))
    found = cur.fetchone() is not None
    cur.close(); conn.close()
    return found


def _insert(headline, news_text, news_url, news_type, matched_terms, db_config):
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
            "(news_date,news_type,news_headline,news_text,news_url,keywords,published) "
            "VALUES (%s,%s,%s,%s,%s,%s,0)",
            (date.today(), news_type, headline, news_text,
             news_url, ", ".join(matched_terms))
        )
        conn.commit(); cur.close(); conn.close()
        return True, "ok"
    except Exception as e:
        return False, f"db:{e}"


# ── HTTP helpers ─────────────────────────────────────────────

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
        if not headline or len(headline) < 25: continue
        if not href or href.startswith("#") or href.lower().startswith("javascript:"): continue
        full = urljoin(site["url"], href)
        p    = urlparse(full)
        if not p.scheme.startswith("http"): continue
        if host not in p.netloc.lower() and p.netloc.lower() not in host: continue
        if full in seen: continue
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
        if not paras: continue
        parts, seen = [], set()
        for p in paras:
            t = clean_text(p.get_text(" ", strip=True))
            if t and len(t) > 30 and t not in seen:
                seen.add(t); parts.append(t)
        if parts:
            return "\n".join(parts)
    return ""


# ── Per-article task ─────────────────────────────────────────

def _process(article, site, keywords, commodities, news_types, db_config):
    headline = clean_text(article.get("headline"))
    url      = clean_text(article.get("url"))
    if not headline or not url:
        return False, "missing"

    # Fast pre-filter: check headline only first (no network call)
    if not find_matches(headline, keywords) and not find_matches(headline, commodities):
        return False, "headline_no_match"

    # Check duplicate before fetching article body
    if _is_duplicate(url, db_config):
        return False, "duplicate"

    # Fetch full article text only when headline matched
    body      = _extract_text(url, site["article_selectors"])
    full      = f"{headline} {body}"
    kw_match  = find_matches(full, keywords)
    com_match = find_matches(full, commodities)
    matched   = list(dict.fromkeys(kw_match + com_match))
    nt        = choose_news_type(full, news_types, com_match, site.get("news_type", "general"))
    return _insert(headline, body, url, nt, matched, db_config)


def _save_json(instance_path, site_name, articles):
    folder = os.path.join(instance_path, "downloaded_json")
    os.makedirs(folder, exist_ok=True)
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", site_name)
    path = os.path.join(folder, f"{safe}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    return path


# ── Public entry point ────────────────────────────────────────

def run_news_scraper(db_config: dict, instance_path: str) -> dict:
    """
    Fully self-contained. Receives db_config and instance_path as plain
    Python values — no Flask context of any kind required.
    """
    keywords   = _fetch_keywords(db_config)
    commodities = _fetch_commodities(db_config)
    news_types  = _fetch_news_types(db_config)
    sites       = _fetch_site_configs(db_config)

    if not sites:
        return {"success": False, "inserted": 0, "skipped": 0,
                "failed_sites": 0, "total": 0,
                "message": "No website configs found in the websites table."}

    total = inserted = skipped = failed = 0
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
                ex.submit(_process, a, site, keywords, commodities, news_types, db_config): a
                for a in articles
            }
            for fut in as_completed(fmap):
                a = fmap[fut]
                try:
                    ok, reason = fut.result(timeout=TASK_TIMEOUT)
                    if ok:
                        inserted += 1
                        print(f"[SCRAPER] Inserted: {a['headline'][:60]}")
                    else:
                        skipped += 1
                        if reason not in ("duplicate", "headline_no_match"):
                            print(f"[SCRAPER] Skip({reason}): {a['headline'][:60]}")
                except FutureTimeout:
                    skipped += 1
                    print(f"[SCRAPER] Timeout: {a['headline'][:60]}")
                except Exception as e:
                    skipped += 1
                    print(f"[SCRAPER] Error: {a.get('headline','')[:60]} — {e}")

    return {
        "success":      True,
        "inserted":     inserted,
        "skipped":      skipped,
        "failed_sites": failed,
        "total":        total,
        "files":        files,
        "message":      f"Done. Extracted:{total} Inserted:{inserted} Skipped:{skipped} Failed sites:{failed}",
    }
