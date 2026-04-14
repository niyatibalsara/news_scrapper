import asyncio
import json
import re
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

#required update crawl4ai to latest version to run this code
 
async def get_latest_news(crawler, site_config):
    """Fetches all raw news items from a site's latest news page."""
    print(f"\n--- Fetching Latest from {site_config['name']} ---")
   
    extraction_strategy = JsonCssExtractionStrategy(site_config['schema'])
   
    config = CrawlerRunConfig(
        extraction_strategy=extraction_strategy,
        cache_mode=CacheMode.BYPASS,
        magic=True,
        page_timeout=60000
    )
 
    result = await crawler.arun(url=site_config['url'], config=config)
   
    if result.success:
        try:
            return json.loads(result.extracted_content)
        except Exception as e:
            print(f"Error parsing JSON from {site_config['name']}: {e}")
            return []
    else:
        print(f"Error fetching {site_config['name']}: {result.error_message}")
        return []
 
def filter_news(articles, keywords):
    """Filters articles based on keywords in headline or summary."""
    filtered = []
    keywords_lower = [k.lower() for k in keywords]
   
    seen = set()
    for art in articles:
        h = art.get('headline', '').strip()
        s = art.get('summary', '').strip()
       
        if not h or h in seen:
            continue
           
        # Check if any keyword matches
        full_text = f"{h} {s}".lower()
        if any(re.search(rf'\b{k}\b', full_text) for k in keywords_lower):
            filtered.append(art)
            seen.add(h)
           
    return filtered
 
async def main():
    # Keywords to search for today
    keywords = ["AI", "Technology", "Nvidia", "India", "Space", "Startup"]
   
    # Latest news sections
    sites = [
        {
            "name": "NDTV Latest",
            "url": "https://www.ndtv.com/latest",
            "schema": {
                "name": "NDTV",
                "baseSelector": ".news_Itm, .NwsLstPg_ttl, li",
                "fields": [
                    {"name": "headline", "selector": ".NwsLstPg_ttl a, h2 a", "type": "text"},
                    {"name": "summary", "selector": ".NwsLstPg_txt, p", "type": "text"}
                ]
            }
        },
        {
            "name": "The Hindu Latest",
            "url": "https://www.thehindu.com/latest-news/",
            "schema": {
                "name": "The Hindu",
                "baseSelector": "h3.title, .element",
                "fields": [
                    {"name": "headline", "selector": "a", "type": "text"},
                    {"name": "summary", "selector": "p", "type": "text"}
                ]
            }
        },
        {
            "name": "TOI Briefs",
            "url": "https://timesofindia.indiatimes.com/briefs",
            "schema": {
                "name": "TOI",
                "baseSelector": ".brief_story, .story",
                "fields": [
                    {"name": "headline", "selector": "h2 a, h2", "type": "text"},
                    {"name": "summary", "selector": ".summary, p", "type": "text"}
                ]
            }
        }
    ]
 
    browser_config = BrowserConfig(
        headless=True,
        enable_stealth=True,
        browser_type="chromium"
    )
 
    async with AsyncWebCrawler(config=browser_config) as crawler:
        all_results = []
       
        for site in sites:
            articles = await get_latest_news(crawler, site)
            if articles:
                # Filter in memory
                matches = filter_news(articles, keywords)
                for m in matches:
                    all_results.append({
                        "source": site['name'],
                        "headline": m.get('headline', '').strip(),
                        "summary": m.get('summary', 'No summary found').strip()
                    })
 
        # Output the final results in JSON format
        print("\n--- FINAL RESULTS ---")
        print(json.dumps(all_results, indent=2, ensure_ascii=False))
 
if __name__ == "__main__":
    asyncio.run(main())
 