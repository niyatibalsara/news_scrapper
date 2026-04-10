from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    b = p.chromium.launch(headless=True, args=['--no-sandbox'])
    page = b.new_page()
    page.goto('https://www.thehindu.com', wait_until='domcontentloaded', timeout=20000)
    print('Title:', page.title())
    b.close()
    print('Playwright OK')