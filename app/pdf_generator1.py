"""
pdf_generator.py
================
Visits the real news article URL using headless Chromium (Playwright)
and saves the rendered page as a PDF.

The PDF is the ORIGINAL article page — not reformatted by us.
Ads, cookie banners, popups, sidebars, and overlay widgets are
suppressed via injected CSS and JS so only the article content prints.

Thread safety
-------------
Each call launches its own Chromium process via sync_playwright().
Fully safe to call from any background thread.

Requirements
------------
    pip install playwright
    playwright install chromium
"""

from __future__ import annotations

import os
import traceback


# ---------------------------------------------------------------------------
#  CSS injected at print time to hide ads/noise
# ---------------------------------------------------------------------------
_HIDE_CSS = """
/* Ad slots — generic */
[class*="ad-"],[class*="-ad"],[class*="_ad"],
[class*="ads-"],[class*="-ads"],[class*="_ads"],
[id*="ad-"],[id*="-ad"],[id*="_ad"],
[id*="ads-"],[id*="-ads"],[id*="_ads"],
[class*="advert"],[id*="advert"],
[class*="sponsor"],[id*="sponsor"],
[class*="promo"],[id*="promo"],
[class*="banner"],[id*="banner"],
/* Overlays / modals */
[class*="modal"],[id*="modal"],
[class*="popup"],[id*="popup"],
[class*="overlay"],[id*="overlay"],
[class*="lightbox"],[id*="lightbox"],
/* Cookie / GDPR */
[class*="cookie"],[id*="cookie"],
[class*="gdpr"],[id*="gdpr"],
[class*="consent"],[id*="consent"],
#onetrust-consent-sdk,#cookielaw-id-necessary,
.fc-dialog-overlay,.fc-dialog-container,
#sp-cc,#qc-cmp2-ui,
/* Newsletter / subscribe */
[class*="newsletter"],[id*="newsletter"],
[class*="subscribe"],[id*="subscribe"],
/* Paywall */
[class*="paywall"],[id*="paywall"],
[class*="gate"],[id*="gate"],
/* Sidebars / widgets */
[class*="sidebar"],[id*="sidebar"],
[class*="widget"],[id*="widget"],
/* Related / recommended */
[class*="related"],[id*="related"],
[class*="recommended"],[id*="recommended"],
[class*="also-read"],[id*="also-read"],
[class*="more-stories"],[id*="more-stories"],
/* Social / comments */
[class*="share"],[id*="share"],
[class*="social"],[id*="social"],
[class*="comment"],[id*="comment"],
[class*="disqus"],[id*="disqus"],
/* Third-party widgets */
[class*="taboola"],[id*="taboola"],
[class*="outbrain"],[id*="outbrain"],
[class*="mgid"],[id*="mgid"],
[class*="zergnet"],[id*="zergnet"],
/* Nav / header / footer */
nav,header,footer,
[role="navigation"],[role="banner"],
[class*="navbar"],[class*="nav-bar"],
[class*="masthead"],[class*="breadcrumb"],
[class*="pagination"],[class*="sticky"],
[class*="fixed-top"],[class*="fixed-bottom"],
/* iframes (video / ads) */
iframe,[class*="iframe"],
[class*="video-container"],
/* Visually hidden / ARIA hidden noise */
.visually-hidden,[aria-hidden="true"] {
    display: none !important;
    visibility: hidden !important;
    height: 0 !important;
    overflow: hidden !important;
}

/* Let body render fully for PDF */
body {
    overflow: visible !important;
    position: static !important;
}
/* Un-fix sticky bars */
[style*="position: fixed"],[style*="position:fixed"],
[style*="position: sticky"],[style*="position:sticky"] {
    position: static !important;
}
"""

# JS to forcibly remove overlay DOM nodes at runtime
_REMOVE_OVERLAYS_JS = """
() => {
    const sels = [
        '[class*="cookie"]','[class*="gdpr"]','[class*="consent"]',
        '[class*="modal"]','[class*="popup"]','[class*="overlay"]',
        '[class*="paywall"]','[class*="newsletter"]','[class*="subscribe"]',
        '#onetrust-consent-sdk','#cookielaw-id-necessary',
        '.fc-dialog-overlay','.fc-dialog-container',
        '#sp-cc','#qc-cmp2-ui',
    ];
    sels.forEach(s => {
        document.querySelectorAll(s).forEach(el => { try { el.remove(); } catch(e){} });
    });
    document.body.style.overflow = 'visible';
    document.body.style.position = 'static';
    document.documentElement.style.overflow = 'visible';
}
"""

# Buttons to click to dismiss cookie/consent dialogs
_DISMISS_BUTTONS = [
    "button[id*='accept']",  "button[class*='accept']",
    "button[id*='agree']",   "button[class*='agree']",
    "button[id*='close']",   "button[class*='close']",
    "button[id*='reject']",
    "[aria-label*='close' i]","[aria-label*='accept' i]",
    "[aria-label*='agree' i]",
    "#onetrust-accept-btn-handler",
    ".fc-cta-consent",
    ".qc-cmp2-summary-buttons button:last-child",
    "#cookiescript_accept",
    "#sp-cc-accept",
    ".css-47sehv",   # The Hindu
]

# Ad/tracker domains to block outright (speeds up load, removes ads)
_BLOCKED_DOMAINS = (
    "googlesyndication", "doubleclick", "adservice",
    "googletagservices", "googletagmanager", "googletag.com",
    "amazon-adsystem", "adsymptotic",
    "taboola", "outbrain", "mgid", "revcontent", "zergnet",
    "disqus", "disquscdn",
    "facebook.net", "fbcdn",
    "tiktok.com", "snap.com",
    "moatads", "scorecardresearch", "quantserve",
    "chartbeat", "newrelic", "hotjar", "fullstory",
    "clarity.ms", "mixpanel", "segment.com",
    "adnxs.com", "rubiconproject", "openx.net",
    "pubmatic.com", "criteo.com", "appnexus.com",
)


def generate_pdf_from_url(url: str, output_path: str) -> bool:
    """
    Visit `url` with headless Chromium, suppress ads and overlays,
    and save the rendered page as an A4 PDF to `output_path`.

    Returns True on success, False on any failure.
    """
    if not url or not url.startswith(("http://", "https://")):
        print(f"[PDF] Invalid URL: {url}")
        return False

    print(f"[PDF] Fetching: {url}")

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        print("[PDF] Playwright not installed. Run:\n"
              "  pip install playwright\n"
              "  playwright install chromium")
        return False

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )

            context = browser.new_context(
                viewport={"width": 1280, "height": 900},
                java_script_enabled=True,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": (
                        "text/html,application/xhtml+xml,"
                        "application/xml;q=0.9,*/*;q=0.8"
                    ),
                },
            )

            # Inject the hide-ads CSS into every page before anything renders
            context.add_init_script(f"""
                (() => {{
                    const s = document.createElement('style');
                    s.textContent = {repr(_HIDE_CSS)};
                    const inject = () => {{
                        if (document.head) document.head.appendChild(s);
                        else document.documentElement.appendChild(s);
                    }};
                    if (document.readyState === 'loading') {{
                        document.addEventListener('DOMContentLoaded', inject);
                    }} else {{
                        inject();
                    }}
                }})();
            """)

            page = context.new_page()
            page.on("dialog", lambda d: d.dismiss())

            # Block ad networks + heavy media
            def _route(route, req):
                url_l = req.url.lower()
                if any(d in url_l for d in _BLOCKED_DOMAINS):
                    route.abort()
                    return
                # Block media/font — not needed for PDF
                if req.resource_type in ("media", "font"):
                    route.abort()
                    return
                route.continue_()

            page.route("**/*", _route)

            # ── Navigate ─────────────────────────────────────────
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except PWTimeout:
                print(f"[PDF] domcontentloaded timeout, retrying with commit…")
                try:
                    page.goto(url, wait_until="commit", timeout=20000)
                except Exception as e:
                    print(f"[PDF] Navigation failed: {e}")
                    browser.close()
                    return False
            except Exception as e:
                print(f"[PDF] Navigation error: {e}")
                browser.close()
                return False

            # Allow JS-rendered content to settle
            try:
                page.wait_for_timeout(2500)
            except Exception:
                pass

            # ── Click dismiss buttons ────────────────────────────
            for sel in _DISMISS_BUTTONS:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=700):
                        btn.click(timeout=700)
                        page.wait_for_timeout(200)
                except Exception:
                    pass

            # ── Remove overlay DOM nodes via JS ──────────────────
            try:
                page.evaluate(_REMOVE_OVERLAYS_JS)
            except Exception:
                pass

            # ── Inject hide-ads CSS directly into the live page ──
            try:
                page.add_style_tag(content=_HIDE_CSS)
            except Exception:
                pass

            # Final settle
            try:
                page.wait_for_timeout(800)
            except Exception:
                pass

            # ── Print to PDF ─────────────────────────────────────
            try:
                page.pdf(
                    path=output_path,
                    format="A4",
                    print_background=True,
                    scale=0.88,          # shrink slightly so wide layouts fit A4
                    margin={
                        "top":    "12mm",
                        "bottom": "12mm",
                        "left":   "10mm",
                        "right":  "10mm",
                    },
                )
            except Exception as e:
                print(f"[PDF] page.pdf() error: {e}")
                browser.close()
                return False

            browser.close()

        # Sanity check
        if os.path.exists(output_path) and os.path.getsize(output_path) > 500:
            kb = os.path.getsize(output_path) // 1024
            print(f"[PDF] OK — {kb} KB → {output_path}")
            return True
        else:
            print(f"[PDF] Output missing or empty: {output_path}")
            return False

    except Exception as e:
        print(f"[PDF] Unexpected error for {url}: {e}")
        traceback.print_exc()
        return False
