"""
pdf_generator.py
================
Visits the real news article URL with headless Chromium and saves the
rendered page as an A4 PDF.

Key fixes for blank PDF issue:
  1. Wait for actual article text to appear in DOM before printing
  2. CSS ONLY hides specific noise elements — never touches article body
  3. Uses networkidle (with timeout fallback) so JS-rendered content loads
  4. Removes fixed/sticky elements that block printing but keeps article text
  5. Scrolls the page to trigger lazy-loaded content
"""

import os
import traceback


# ── CSS: ONLY hides specific noise — never article body ────────────────────
# Previous version used broad class-name patterns that were catching
# article content on sites like The Hindu. This version is surgical.
_HIDE_CSS = """
/* Stop animations for stable PDF */
*, *::before, *::after {
    animation-duration: 0.001s !important;
    transition-duration: 0.001s !important;
}

/* ── Known ad/tracker slots ───────────────────────────── */
ins.adsbygoogle,
.adsbygoogle,
[data-ad-slot],
[data-ad-client],
[data-google-query-id],
#google_ads_iframe,
.google-ad,
.dfp-ad,
.ad-container,
.ad-wrapper,
.advertisement,
.advertising,
.sponsor-content,
.sponsored,

/* ── Cookie banners (specific IDs/classes) ────────────── */
#onetrust-consent-sdk,
#onetrust-banner-sdk,
#onetrust-pc-sdk,
#ot-sdk-overlay,
#cookielaw-id-necessary,
#cookieNotice,
#cookie-notice,
#cookie-banner,
#cookie-consent,
#gdpr-banner,
#gdpr-cookie-notice,
.fc-dialog-overlay,
.fc-dialog-container,
.fc-ab-root,
#sp-cc,
#qc-cmp2-ui,
.qc-cmp2-container,
.cmp-container,
[id^="sp_message"],
[class^="sp_message"],
.evidon-banner,
.trustarc-banner,
.cc-window,
.cookieConsent,
.cookieBanner,

/* ── Paywall overlays (specific) ──────────────────────── */
.paywall-container,
.paywall-overlay,
.subscription-wall,
.premium-overlay,
.piano-overlay,
#piano-id,
.tp-backdrop,
.tp-modal,

/* ── Newsletter popups ─────────────────────────────────── */
.newsletter-popup,
.newsletter-modal,
.email-capture-modal,
.subscribe-popup,
.subscribe-modal,

/* ── Fixed / sticky navigation bars ───────────────────── */
.sticky-header,
.sticky-nav,
.fixed-header,
.fixed-nav,
.sticky-top,
.navbar-fixed-top,
.navbar-fixed-bottom,
#sticky-header,
#fixed-header,

/* ── Chat widgets / livechat ──────────────────────────── */
#launcher,
#chat-widget,
.intercom-launcher,
.intercom-namespace,
.crisp-client,
.zopim,
#hubspot-messages-iframe-container,
#freshworks-container,

/* ── Third-party injected content ─────────────────────── */
.taboola-widget,
.taboola-reading-widget,
.outbrain-widget,
[id*="taboola"],
[id*="outbrain"],

/* ── Comment sections ──────────────────────────────────── */
#disqus_thread,
.disqus-container,
.comments-section,
#comments,

/* ── Social share bars ─────────────────────────────────── */
.social-share-bar,
.share-buttons,
.social-buttons,
.floating-share,

/* ── The Hindu specific ────────────────────────────────── */
.hide-mobile-paywall,
.subscription-nudge,
.gdpr-banner,
.notification-bar,
#notification-bar {
    display: none !important;
    visibility: hidden !important;
    height: 0 !important;
    overflow: hidden !important;
}

/* ── Un-fix sticky/fixed elements so they don't overlap ── */
/* (but DON'T remove them — just make them static) */
[style*="position: fixed"],
[style*="position:fixed"],
[style*="position: sticky"],
[style*="position:sticky"] {
    position: static !important;
    top: auto !important;
    bottom: auto !important;
    z-index: auto !important;
}

/* ── Ensure body is scrollable and not locked ─────────── */
html, body {
    overflow: visible !important;
    padding-top: 0 !important;
}
"""

# ── JS: dismiss consent dialogs + remove specific overlays ─────────────────
# IMPORTANT: Only removes specific known overlay elements.
# Does NOT remove anything with class patterns like "modal" or "popup"
# because news sites use those classes on article figures and image viewers.
_CLEANUP_JS = """
() => {
    // Only remove specific known overlay/consent elements by ID or specific class
    const exactIds = [
        'onetrust-consent-sdk', 'onetrust-banner-sdk', 'onetrust-pc-sdk',
        'ot-sdk-overlay', 'cookieNotice', 'cookie-notice', 'cookie-banner',
        'cookie-consent', 'gdpr-banner', 'sp-cc', 'qc-cmp2-ui',
        'disqus_thread', 'piano-id', 'notification-bar',
        'gdpr-cookie-notice', 'cookieConsentBanner'
    ];

    exactIds.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            try { el.parentNode && el.parentNode.removeChild(el); } catch(e) {}
        }
    });

    const exactClasses = [
        'fc-dialog-overlay', 'fc-dialog-container', 'fc-ab-root',
        'qc-cmp2-container', 'cc-window', 'cookieConsent',
        'tp-backdrop', 'tp-modal', 'intercom-launcher', 'crisp-client'
    ];

    exactClasses.forEach(cls => {
        document.querySelectorAll('.' + cls).forEach(el => {
            try { el.parentNode && el.parentNode.removeChild(el); } catch(e) {}
        });
    });

    // Un-fix sticky/fixed elements (don't remove, just unstick)
    document.querySelectorAll('header, nav, [class*="sticky"], [class*="fixed-top"]').forEach(el => {
        try {
            const pos = window.getComputedStyle(el).position;
            if (pos === 'fixed' || pos === 'sticky') {
                el.style.setProperty('position', 'relative', 'important');
                el.style.setProperty('top', 'auto', 'important');
                el.style.setProperty('z-index', '1', 'important');
            }
        } catch(e) {}
    });

    // Unlock body scroll
    try {
        document.body.style.setProperty('overflow', 'visible', 'important');
        document.documentElement.style.setProperty('overflow', 'visible', 'important');
    } catch(e) {}

    return true;
}
"""

# ── JS: wait for article content to actually appear in DOM ─────────────────
_WAIT_FOR_CONTENT_JS = """
() => {
    // Returns true if article content appears to be rendered
    const articleSelectors = [
        'article',
        '[itemprop="articleBody"]',
        '.article-body',
        '.article__body',
        '.article-content',
        '.article__content',
        '.story-body',
        '.story-content',
        '.post-body',
        '.post-content',
        '.entry-content',
        '.content-body',
        '.news-body',
        '.article-text',
        '[class*="article"][class*="body"]',
        '[class*="article"][class*="content"]',
        '[class*="story"][class*="body"]',
        'main p',
        '.main-content p'
    ];

    for (const sel of articleSelectors) {
        try {
            const el = document.querySelector(sel);
            if (el && el.innerText && el.innerText.trim().length > 100) {
                return { found: true, selector: sel, length: el.innerText.trim().length };
            }
        } catch(e) {}
    }

    // Fallback: check if there are enough paragraphs with real content
    const paras = document.querySelectorAll('p');
    let totalText = 0;
    paras.forEach(p => { totalText += (p.innerText || '').trim().length; });
    return { found: totalText > 300, selector: 'p', length: totalText };
}
"""

# ── Consent dismiss buttons ─────────────────────────────────────────────────
_DISMISS_BTNS = [
    "#onetrust-accept-btn-handler",
    ".fc-cta-consent",
    ".qc-cmp2-summary-buttons button:last-child",
    "#cookiescript_accept",
    "#sp-cc-accept",
    "#cookie-accept",
    ".gdpr-accept",
    ".cookie-accept-btn",
    "[id*='accept'][id*='cookie']",
    "[class*='accept'][class*='cookie']",
    # The Hindu specific
    ".css-47sehv",
    "[data-testid='GDPR-accept']",
    # Generic last resort
    "button[id*='accept']",
    "button[class*='accept']",
    "button[id*='agree']",
    "button[class*='agree']",
]

# ── Ad/tracker network hosts to block ──────────────────────────────────────
_BLOCKED = (
    "googlesyndication.com", "doubleclick.net",
    "googletagmanager.com", "google-analytics.com",
    "amazon-adsystem.com",
    "taboola.com", "outbrain.com",
    "disqus.com", "disquscdn.com",
    "facebook.net", "fbcdn.net",
    "moatads.com", "scorecardresearch.com",
    "chartbeat.com", "newrelic.com", "hotjar.com",
    "fullstory.com", "clarity.ms",
    "adnxs.com", "rubiconproject.com",
    "pubmatic.com", "criteo.com",
)


def generate_pdf_from_url(url: str, output_path: str) -> bool:
    """
    Visit `url` with headless Chromium, wait for article content to render,
    then save as A4 PDF to `output_path`.

    Returns True if PDF created and non-empty, False otherwise.
    """
    if not url or not url.strip():
        print("[PDF] ERROR: Empty URL")
        return False

    url = url.strip()
    if not url.startswith(("http://", "https://")):
        print(f"[PDF] ERROR: Invalid URL: {url}")
        return False

    print(f"[PDF] ── Starting ───────────────────────────────────")
    print(f"[PDF] URL: {url}")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        print("[PDF] ERROR: Playwright not installed.")
        print("[PDF]   Run: pip install playwright && playwright install chromium")
        return False

    try:
        with sync_playwright() as pw:

            # Launch browser
            try:
                browser = pw.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--disable-blink-features=AutomationControlled",
                    ],
                )
                print("[PDF] Browser launched")
            except Exception as e:
                print(f"[PDF] ERROR: Browser launch failed: {e}")
                return False

            context = browser.new_context(
                viewport={"width": 1280, "height": 900},
                java_script_enabled=True,
                ignore_https_errors=True,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )

            # Inject CSS at page init — before anything renders
            hide_css_str = repr(_HIDE_CSS)
            context.add_init_script(f"""
                (() => {{
                    function injectCSS() {{
                        if (document.getElementById('seamless-hide-css')) return;
                        const el = document.createElement('style');
                        el.id = 'seamless-hide-css';
                        el.textContent = {hide_css_str};
                        const target = document.head || document.documentElement;
                        if (target) target.appendChild(el);
                    }}
                    if (document.readyState === 'loading') {{
                        document.addEventListener('DOMContentLoaded', injectCSS, {{once: true}});
                    }} else {{
                        injectCSS();
                    }}
                }})();
            """)

            page = context.new_page()
            page.on("dialog", lambda d: d.dismiss())

            # Block ad networks + media (not images — some sites render text via CSS background)
            def _route(route, request):
                try:
                    u = request.url.lower()
                    if any(h in u for h in _BLOCKED):
                        route.abort()
                        return
                    # Only block video/audio media, not images
                    if request.resource_type in ("media",):
                        route.abort()
                        return
                    route.continue_()
                except Exception:
                    try:
                        route.continue_()
                    except Exception:
                        pass

            page.route("**/*", _route)

            # ── Navigate ─────────────────────────────────────────────────────
            nav_ok = False

            # First try: networkidle — waits for JS to finish (best for SPA/JS-rendered sites)
            try:
                print("[PDF] Navigating (networkidle)...")
                page.goto(url, wait_until="networkidle", timeout=40000)
                print(f"[PDF] Loaded (networkidle) — title: {page.title()[:80]!r}")
                nav_ok = True
            except PWTimeout:
                print("[PDF] networkidle timeout — trying domcontentloaded...")
            except Exception as e:
                if "ERR_NAME_NOT_RESOLVED" in str(e) or "ERR_CONNECTION" in str(e):
                    print(f"[PDF] Network error: {e}")
                    browser.close()
                    return False
                print(f"[PDF] networkidle error: {e}")

            # Second try: domcontentloaded
            if not nav_ok:
                try:
                    print("[PDF] Navigating (domcontentloaded)...")
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    print(f"[PDF] Loaded (domcontentloaded) — title: {page.title()[:80]!r}")
                    nav_ok = True
                except PWTimeout:
                    print("[PDF] domcontentloaded timeout — trying commit...")
                except Exception as e:
                    print(f"[PDF] domcontentloaded error: {e}")

            # Third try: commit — fires on first byte
            if not nav_ok:
                try:
                    print("[PDF] Navigating (commit)...")
                    page.goto(url, wait_until="commit", timeout=20000)
                    print(f"[PDF] Loaded (commit)")
                    nav_ok = True
                except Exception as e:
                    print(f"[PDF] commit error: {e}")

            if not nav_ok:
                print(f"[PDF] All navigation attempts failed for: {url}")
                browser.close()
                return False

            # ── Wait for article content to actually render ───────────────────
            # Poll up to 15 seconds for real article text to appear in DOM
            content_ready = False
            print("[PDF] Waiting for article content to render...")
            for attempt in range(6):
                try:
                    result = page.evaluate(_WAIT_FOR_CONTENT_JS)
                    if result and result.get("found"):
                        print(f"[PDF] Content ready: {result.get('selector')} "
                              f"({result.get('length', 0)} chars)")
                        content_ready = True
                        break
                    else:
                        print(f"[PDF] Content not ready yet (attempt {attempt+1}/6), waiting 2s...")
                        page.wait_for_timeout(2000)
                except Exception:
                    page.wait_for_timeout(2000)

            if not content_ready:
                print("[PDF] WARNING: Content may not have fully rendered, printing anyway...")
            
            # Extra wait to ensure fonts/layout is settled
            page.wait_for_timeout(1500)

            # ── Scroll to trigger lazy-loaded content ─────────────────────────
            try:
                page.evaluate("""
                    () => {
                        window.scrollTo(0, document.body.scrollHeight / 2);
                        return true;
                    }
                """)
                page.wait_for_timeout(500)
                page.evaluate("() => { window.scrollTo(0, 0); return true; }")
                page.wait_for_timeout(500)
            except Exception:
                pass

            # ── Dismiss consent buttons ───────────────────────────────────────
            dismissed = 0
            for sel in _DISMISS_BTNS:
                try:
                    btn = page.locator(sel).first
                    if btn.is_visible(timeout=400):
                        btn.click(timeout=400)
                        dismissed += 1
                        page.wait_for_timeout(200)
                except Exception:
                    pass
            if dismissed:
                print(f"[PDF] Dismissed {dismissed} consent button(s)")
                page.wait_for_timeout(500)

            # ── Remove specific overlay nodes ─────────────────────────────────
            try:
                page.evaluate(_CLEANUP_JS)
            except Exception as e:
                # This warning is non-critical — JS error means element not found
                print(f"[PDF] Cleanup JS note (non-critical): {type(e).__name__}")

            # ── Apply CSS again to live page ──────────────────────────────────
            try:
                page.add_style_tag(content=_HIDE_CSS)
            except Exception:
                pass

            # Final settle
            page.wait_for_timeout(1000)

            # ── Verify content is present before printing ─────────────────────
            try:
                body_text_len = page.evaluate(
                    "() => (document.body && document.body.innerText.trim().length) || 0"
                )
                print(f"[PDF] Page body text length: {body_text_len} chars")
                if body_text_len < 100:
                    print("[PDF] WARNING: Very little text on page — PDF may be blank")
            except Exception:
                pass

            # ── Print to PDF ──────────────────────────────────────────────────
            print("[PDF] Printing to PDF...")
            try:
                page.pdf(
                    path=output_path,
                    format="A4",
                    print_background=True,   # needed for sites that use background-color for text
                    scale=0.85,
                    margin={
                        "top":    "10mm",
                        "bottom": "10mm",
                        "left":   "8mm",
                        "right":  "8mm",
                    },
                )
            except Exception as e:
                print(f"[PDF] ERROR: page.pdf() failed: {e}")
                traceback.print_exc()
                browser.close()
                return False

            browser.close()

        # ── Verify output ─────────────────────────────────────────────────────
        if not os.path.exists(output_path):
            print(f"[PDF] ERROR: File not created: {output_path}")
            return False

        size = os.path.getsize(output_path)
        size_kb = size // 1024

        # A blank/empty PDF is typically < 5KB
        if size < 5000:
            print(f"[PDF] WARNING: PDF is suspiciously small ({size} bytes = {size_kb} KB)")
            print(f"[PDF] This usually means the page content did not render.")
            print(f"[PDF] The site may require login, or content is loaded by JS after timeout.")
            # Still return True so the row shows the download button
            # (user can see it's small and investigate)
            return size > 500

        print(f"[PDF] ✓ SUCCESS: {os.path.basename(output_path)} ({size_kb} KB)")
        return True

    except Exception as e:
        print(f"[PDF] UNEXPECTED ERROR for {url}: {e}")
        traceback.print_exc()
        return False