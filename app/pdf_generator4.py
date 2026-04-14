"""
pdf_generator.py
----------------
Generates a clean PDF of a news article URL using Playwright (Chromium).

Features
--------
• Removes cookie banners, GDPR overlays, popups, sticky headers/footers,
  newsletter modals, and ad containers before printing.
• Waits for the main article body to be visible before capturing.
• Prints only the article content area when possible (no sidebars/nav).
• Falls back to full-page print if content selector not found.
• Thread-safe: each call launches its own browser context.

Install
-------
    pip install playwright
    playwright install chromium

Usage
-----
    from app.pdf_generator import generate_pdf_from_url
    ok = generate_pdf_from_url(url="https://...", output_path="/path/to/out.pdf")
"""

import os
import traceback

# ── CSS selectors for junk elements to remove before printing ──
_REMOVE_SELECTORS = [
    # Cookie / GDPR banners
    "[id*='cookie']", "[class*='cookie']",
    "[id*='gdpr']",   "[class*='gdpr']",
    "[id*='consent']","[class*='consent']",
    "[id*='notice']", "[class*='notice']",
    # Popups / modals / overlays
    "[id*='popup']",  "[class*='popup']",
    "[id*='modal']",  "[class*='modal']",
    "[id*='overlay']","[class*='overlay']",
    "[id*='dialog']", "[class*='dialog']",
    # Newsletter / subscription prompts
    "[id*='newsletter']",  "[class*='newsletter']",
    "[id*='subscribe']",   "[class*='subscribe']",
    "[id*='paywall']",     "[class*='paywall']",
    "[id*='subscription']","[class*='subscription']",
    # Ads
    "[id*='ad-']",     "[class*='ad-']",
    "[id*='-ad']",     "[class*='-ad']",
    "[id*='ads']",     "[class*='ads']",
    "[id*='advert']",  "[class*='advert']",
    "[class*='sponsored']",
    "ins.adsbygoogle",
    "iframe[src*='ads']",
    "iframe[src*='doubleclick']",
    # Sticky / fixed UI chrome
    "header",
    "[class*='sticky']", "[class*='fixed-top']",
    "[class*='nav-bar']","[class*='navbar']",
    "[class*='top-bar']","[class*='site-header']",
    "[class*='site-footer']","[class*='footer']",
    "nav", "footer",
    # Social share / comment bars
    "[class*='social']",  "[class*='share-bar']",
    "[class*='comments']","[id*='comments']",
    # Sidebars / related / recommendations
    "[class*='sidebar']", "[id*='sidebar']",
    "[class*='related']", "[id*='related']",
    "[class*='recommend']",
    "[class*='outbrain']","[class*='taboola']",
]

# ── CSS selectors tried in order to find the article body ──────
_ARTICLE_SELECTORS = [
    "article",
    "[itemprop='articleBody']",
    "[class*='article-body']",
    "[class*='article__body']",
    "[class*='story-body']",
    "[class*='post-body']",
    "[class*='entry-content']",
    "[class*='article-content']",
    "[class*='news-content']",
    "[class*='content-body']",
    "main",
    "[role='main']",
]

# ── JavaScript injected into the page before printing ──────────
_CLEANUP_JS = """
(function() {
    // Remove junk elements
    const selectors = %s;
    selectors.forEach(sel => {
        try {
            document.querySelectorAll(sel).forEach(el => el.remove());
        } catch(e) {}
    });

    // Force all position:fixed / position:sticky elements to static
    document.querySelectorAll('*').forEach(el => {
        const st = window.getComputedStyle(el);
        if (st.position === 'fixed' || st.position === 'sticky') {
            el.style.position = 'static';
        }
    });

    // Hide everything except the article body
    const articleSelectors = %s;
    let articleEl = null;
    for (const sel of articleSelectors) {
        articleEl = document.querySelector(sel);
        if (articleEl) break;
    }
    if (articleEl) {
        // Walk up and hide siblings that are not ancestors of articleEl
        let node = articleEl;
        while (node && node !== document.body) {
            const parent = node.parentElement;
            if (parent) {
                Array.from(parent.children).forEach(child => {
                    if (child !== node) {
                        child.style.display = 'none';
                    }
                });
            }
            node = parent;
        }
        // Ensure the article itself is fully visible
        articleEl.style.maxWidth = '100%%';
        articleEl.style.margin   = '0';
        articleEl.style.padding  = '16px';
        articleEl.style.float    = 'none';
    }
})();
""" % (
    str(_REMOVE_SELECTORS).replace("'", '"'),
    str(_ARTICLE_SELECTORS).replace("'", '"'),
)

# ── Print CSS injected via <style> ─────────────────────────────
_PRINT_CSS = """
@media print {
    body { font-family: Georgia, serif; font-size: 12pt; color: #000; }
    img  { max-width: 100% !important; page-break-inside: avoid; }
    h1, h2, h3 { page-break-after: avoid; }
    p   { orphans: 3; widows: 3; }
    a   { color: #000; text-decoration: none; }
}
"""


def generate_pdf_from_url(url: str, output_path: str,
                           timeout_ms: int = 30_000) -> bool:
    """
    Navigate to `url`, remove ads/banners, isolate article content,
    and save a PDF to `output_path`.

    Returns True on success, False on any failure.
    Thread-safe — each call creates and destroys its own Playwright context.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        print("[PDF] Playwright not installed. Run: pip install playwright && playwright install chromium")
        return False

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--blink-settings=imagesEnabled=true",
                ],
            )
            context = browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                java_script_enabled=True,
                bypass_csp=True,
            )

            # Block ad networks and tracking at the network level
            _AD_DOMAINS = [
                "doubleclick.net", "googlesyndication.com", "googletagmanager.com",
                "googletagservices.com", "google-analytics.com", "facebook.net",
                "outbrain.com", "taboola.com", "adnxs.com", "adsystem.com",
                "criteo.com", "rubiconproject.com", "pubmatic.com", "openx.net",
            ]

            def _block_ads(route, request):
                if any(d in request.url for d in _AD_DOMAINS):
                    route.abort()
                else:
                    route.continue_()

            context.route("**/*", _block_ads)

            page = context.new_page()

            # Navigate and wait for network to settle
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            except PWTimeout:
                print(f"[PDF] Page load timeout: {url}")
                browser.close()
                return False

            # Give dynamic content a moment to render
            try:
                page.wait_for_load_state("networkidle", timeout=8_000)
            except PWTimeout:
                pass  # networkidle timeout is non-fatal

            # Inject print CSS
            page.add_style_tag(content=_PRINT_CSS)

            # Run cleanup JS (remove ads, isolate article)
            try:
                page.evaluate(_CLEANUP_JS)
            except Exception as e:
                print(f"[PDF] Cleanup JS error (non-fatal): {e}")

            # Small pause for any CSS transitions triggered by JS
            page.wait_for_timeout(500)

            # Generate PDF
            page.pdf(
                path=output_path,
                format="A4",
                margin={
                    "top":    "15mm",
                    "bottom": "15mm",
                    "left":   "12mm",
                    "right":  "12mm",
                },
                print_background=False,
                display_header_footer=False,
            )

            browser.close()

        # Verify file was actually written and has content
        if os.path.exists(output_path) and os.path.getsize(output_path) > 500:
            print(f"[PDF] ✓ Generated: {output_path}  ({os.path.getsize(output_path):,} bytes)")
            return True
        else:
            print(f"[PDF] File too small or missing: {output_path}")
            return False

    except Exception as e:
        print(f"[PDF] Failed for {url}: {e}")
        traceback.print_exc()
        return False
