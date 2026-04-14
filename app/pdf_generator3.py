"""
pdf_generator.py
----------------
Generates a PDF from a live news URL using Playwright (Chromium).

Steps:
  1. Launch a headless Chromium browser via Playwright
  2. Navigate to the real news URL and wait for the page to fully load
  3. Remove common clutter (ads, cookie banners, nav, sidebars, footers)
  4. Print the page to a PDF (full page, A4)
  5. Return True on success, False on failure

Requirements:
    pip install playwright
    playwright install chromium
"""

import os
import traceback
from pathlib import Path


def generate_pdf_from_url(url: str, output_path: str, timeout_ms: int = 30_000) -> bool:
    """
    Visit *url* with a headless Chromium browser and save a full-page PDF
    to *output_path*.

    Parameters
    ----------
    url         : Full HTTP/HTTPS URL of the news article.
    output_path : Absolute path where the resulting .pdf should be written.
    timeout_ms  : Page-load timeout in milliseconds (default 30 s).

    Returns
    -------
    True  – PDF written successfully (file size > 0).
    False – Any error occurred; partial/empty file may exist.
    """
    if not url or not url.startswith(("http://", "https://")):
        print(f"[PDF] Skipping invalid URL: {url!r}")
        return False

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        print("[PDF] Playwright is not installed. Run: pip install playwright && playwright install chromium")
        return False

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )
            context = browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                java_script_enabled=True,
                accept_downloads=False,
            )
            page = context.new_page()

            # Block heavy/unnecessary resources to speed up load
            def _block_heavy(route, request):
                if request.resource_type in ("media", "font"):
                    route.abort()
                else:
                    route.continue_()

            page.route("**/*", _block_heavy)

            # Navigate and wait until network is mostly idle
            try:
                page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            except PWTimeout:
                print(f"[PDF] networkidle timeout for {url} — retrying with domcontentloaded")
                page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # Give JS-rendered content a moment to settle
            try:
                page.wait_for_timeout(2000)
            except Exception:
                pass

            # ── Clean up page clutter ───────────────────────────────
            _REMOVE_SELECTORS = [
                # Cookie / GDPR banners
                "#cookie-banner", ".cookie-banner", "#cookie-consent",
                ".cookie-consent", ".gdpr-banner", "#gdpr-banner",
                "[class*='cookie']", "[id*='cookie']",
                "[class*='consent']", "[id*='consent']",
                # Ads
                ".advertisement", ".ad-container", ".ads", ".ad",
                "[class*='advert']", "[id*='advert']",
                "[class*='sponsored']",
                # Navigation & header/footer
                "header", "nav", "footer",
                ".navbar", ".nav-bar", ".site-header", ".site-footer",
                ".top-bar", ".bottom-bar",
                # Sidebars & related articles
                "aside", ".sidebar", "#sidebar",
                ".related-articles", ".recommended",
                # Social share bars
                ".social-share", ".share-bar", ".social-buttons",
                # Subscription modals / paywalls
                ".paywall", ".subscription-modal", ".modal-overlay",
                "[class*='paywall']", "[class*='subscribe']",
                # Sticky bars / overlays
                ".sticky-header", ".sticky-footer", ".fixed-bar",
            ]

            page.evaluate(
                """(selectors) => {
                    selectors.forEach(sel => {
                        document.querySelectorAll(sel).forEach(el => el.remove());
                    });
                    // Also remove fixed/sticky positioned elements
                    document.querySelectorAll('*').forEach(el => {
                        const st = window.getComputedStyle(el);
                        if (st.position === 'fixed' || st.position === 'sticky') {
                            el.remove();
                        }
                    });
                }""",
                _REMOVE_SELECTORS,
            )

            # ── Inject print-friendly styles ───────────────────────
            page.add_style_tag(content="""
                @media print {
                    * { -webkit-print-color-adjust: exact !important; }
                }
                body {
                    font-family: Georgia, 'Times New Roman', serif !important;
                    font-size: 14px !important;
                    line-height: 1.7 !important;
                    color: #111 !important;
                    background: #fff !important;
                    margin: 0 !important;
                    padding: 0 !important;
                }
                img {
                    max-width: 100% !important;
                    height: auto !important;
                    page-break-inside: avoid;
                }
                h1, h2, h3 {
                    page-break-after: avoid;
                    color: #000 !important;
                }
                p { orphans: 3; widows: 3; }
                a { color: inherit !important; text-decoration: none !important; }
            """)

            # ── Generate PDF ────────────────────────────────────────
            page.pdf(
                path=output_path,
                format="A4",
                print_background=True,
                margin={
                    "top":    "15mm",
                    "bottom": "15mm",
                    "left":   "12mm",
                    "right":  "12mm",
                },
            )

            context.close()
            browser.close()

        # Validate output
        size = Path(output_path).stat().st_size if Path(output_path).exists() else 0
        if size < 500:
            print(f"[PDF] Output file too small ({size} bytes) — treating as failure.")
            return False

        print(f"[PDF] Generated successfully: {output_path} ({size:,} bytes)")
        return True

    except Exception as exc:
        print(f"[PDF] Generation failed for {url}: {exc}")
        traceback.print_exc()
        return False
