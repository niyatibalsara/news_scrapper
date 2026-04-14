"""
main.py
-------
Key behaviours
──────────────
• Scraper scheduling
  - init_scheduler() starts ONE daemon thread on app startup.
  - Ticks every 30 s (fine-grained enough for 1-minute intervals).
  - _is_due() checks whether each active category's schedule has elapsed.
  - 'Run All Together' → only the global schedule is checked; all active
    categories are scraped together when it fires.
  - Per-category mode → each category fires independently on its own schedule.

• Category filtering
  - target_categories passed directly to run_news_scraper().
  - news_scraper.py keeps only articles whose headlines/body contain
    keywords belonging to the selected categories.
  - Selecting 'all' keeps everything that matches any keyword.

• Auto-publish vs Manual
  - publish_mode = 'auto'   → scraper inserts directly into published_news
                               then kicks off PDF + email immediately.
  - publish_mode = 'manual' → scraper inserts into non_published_news (default).

• refresh_news button
  - Behaviour identical to the original: fires _bg_scraper immediately,
    respects current settings (categories + publish_mode).
"""

from .auth import login_required
from app.db import get_db

import json
import os
import smtplib
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import mysql.connector
from flask import (Blueprint, current_app, flash, jsonify,
                   redirect, render_template, request,
                   send_from_directory, url_for)

main_bp = Blueprint("main", __name__)

PER_PAGE = 10

# ── Scraper state (shared across threads) ─────────────────────
_scraper_lock  = threading.Lock()
_scraper_state = {"running": False, "message": "", "success": None}

# ── Scheduler singleton ───────────────────────────────────────
_scheduler_started = False
_scheduler_lock    = threading.Lock()

# ── Scheduler ticks every 30 s so 1-minute intervals work ────
SCHEDULER_TICK_SECONDS = 30


# ════════════════════════════════════════════════════════════════
#  HELPERS — DB / MAIL CONFIG  (call inside request/app context)
# ════════════════════════════════════════════════════════════════

def _raw_conn(db_cfg: dict):
    return mysql.connector.connect(**db_cfg)


# # ── Extract DB config inside a request context ───────────────
# def _get_db_cfg() -> dict:
#     cfg = current_app.config
#     return {
#         "host":     cfg["MYSQL_HOST"],
#         "user":     cfg["MYSQL_USER"],
#         "password": cfg["MYSQL_PASSWORD"],
#         "database": cfg["MYSQL_DB"],
#     }


# # ================================================================
# #  Background: PDF generation + email
# #
# #  Called AFTER:
# #    - articles are already in published_news (visible to user)
# #    - source rows are already DELETED from non_published_news
# #
# #  This thread ONLY handles: PDF → update pdf_path → send email
# # ================================================================

# def _bg_pdf_email(db_cfg, mail_cfg, template_dir,
#                   articles, pub_ids, pdf_folder, settings):
#     """
#     Generate a PDF for each published article by visiting its real URL,
#     update pdf_path in DB, then send the email.

#     Parameters are all plain Python values — no Flask objects.
#     """
#     # Lazy import inside the thread to avoid import errors at app startup
#     try:
#         from app.pdf_generator import generate_pdf_from_url
#     except ImportError as e:
#         print(f"[BG] Cannot import pdf_generator: {e}")
#         generate_pdf_from_url = None

#     pdf_paths = []

#     for art, pub_id in zip(articles, pub_ids):
#         news_url  = (art.get("news_url") or "").strip()
#         safe_name = f"news_{pub_id}.pdf"
#         out_path  = os.path.join(pdf_folder, safe_name)
#         pdf_ok    = False

#         if not news_url:
#             print(f"[BG] pub_id={pub_id} has no URL — skipping PDF")
#         elif generate_pdf_from_url is None:
#             print(f"[BG] pdf_generator not available — skipping PDF for pub_id={pub_id}")
#         else:
#             try:
#                 pdf_ok = generate_pdf_from_url(url=news_url, output_path=out_path)
#             except Exception as e:
#                 print(f"[BG] PDF generation exception for pub_id={pub_id}: {e}")
#                 traceback.print_exc()

#         if pdf_ok:
#             try:
#                 c = _raw_conn(db_cfg)
#                 cur = c.cursor()
#                 cur.execute(
#                     "UPDATE published_news SET pdf_path=%s WHERE id=%s",
#                     (safe_name, pub_id)
#                 )
#                 c.commit()
#                 cur.close()
#                 c.close()
#                 print(f"[BG] PDF saved and DB updated: {safe_name}")
#             except Exception as e:
#                 print(f"[BG] DB update for pdf_path failed pub_id={pub_id}: {e}")
#             pdf_paths.append(out_path)
#         else:
#             print(f"[BG] PDF failed for pub_id={pub_id}, url={news_url}")
#             pdf_paths.append(None)

#     # ── Send email ───────────────────────────────────────────
#     if not settings.get("email_on_publish", 1):
#         print("[BG] email_on_publish=0 — skipping email.")
#         return

#     try:
#         _send_email_bg(
#             articles=articles,
#             pub_ids=pub_ids,
#             pdf_paths=pdf_paths,
#             settings=settings,
#             mail_cfg=mail_cfg,
#             template_dir=template_dir,
#         )
#     except Exception as e:
#         print(f"[BG] Email send failed: {e}")
#         traceback.print_exc()
#         return

#     # Mark email sent in DB
#     try:
#         c = _raw_conn(db_cfg)
#         cur = c.cursor()
#         for pid in pub_ids:
#             cur.execute(
#                 "UPDATE published_news SET email_sent=1, email_sent_at=NOW() WHERE id=%s",
#                 (pid,)
#             )
#         c.commit()
#         cur.close()
#         c.close()
#         print(f"[BG] email_sent marked for {len(pub_ids)} articles.")
#     except Exception as e:
#         print(f"[BG] Mark email_sent error: {e}")


# # ================================================================
# #  Email helpers (no Flask context — safe in any thread)
# # ================================================================

# def _render_email_template(template_dir: str, news_items: list, date_label: str) -> str:
#     from jinja2 import Environment, FileSystemLoader
#     env  = Environment(loader=FileSystemLoader(template_dir))
#     tmpl = env.get_template("email_template.html")
#     return tmpl.render(news_items=news_items, date_label=date_label)


# def _smtp_send(mail_cfg: dict, msg, recipients: list):
#     with smtplib.SMTP(mail_cfg["MAIL_SERVER"], mail_cfg["MAIL_PORT"]) as s:
#         s.ehlo()
#         if mail_cfg.get("MAIL_USE_TLS", True):
#             s.starttls()
#         s.login(mail_cfg["MAIL_USERNAME"], mail_cfg["MAIL_PASSWORD"])
#         s.sendmail(msg["From"], recipients, msg.as_string())
#         print(f"[EMAIL] Sent to: {recipients}")


# def _send_email_bg(articles, pub_ids, pdf_paths, settings, mail_cfg, template_dir):
#     to        = settings.get("email_recipient") or "niyati.b@seamlessautomations.com"
#     cc_raw    = settings.get("email_cc") or ""
#     cc        = [e.strip() for e in cc_raw.split(",") if e.strip()]
#     pfx       = settings.get("email_subject_prefix") or "Daily News Alert"
#     mode      = settings.get("publish_mode", "manual")
#     date_label = datetime.now().strftime("%d %B %Y")

#     if mode == "auto":
#         base  = mail_cfg.get("BASE_URL", "http://127.0.0.1:5000")
#         items = [
#             dict(a, pdf_view_url=f"{base}/download-pdf/{pid}")
#             for a, pid in zip(articles, pub_ids)
#         ]
#         html_body = _render_email_template(template_dir, items, date_label)
#     else:
#         html_body = _render_email_template(template_dir, list(articles), date_label)

#     msg            = MIMEMultipart("mixed")
#     msg["Subject"] = f"{pfx} — {date_label}"
#     msg["From"]    = mail_cfg.get("MAIL_FROM", mail_cfg.get("MAIL_USERNAME", ""))
#     msg["To"]      = to
#     if cc:
#         msg["Cc"]  = ", ".join(cc)

#     msg.attach(MIMEText(html_body, "html", "utf-8"))

#     # Attach PDFs in manual mode
#     if mode != "auto":
#         for p in pdf_paths:
#             if p and os.path.exists(p):
#                 with open(p, "rb") as f:
#                     part = MIMEBase("application", "octet-stream")
#                     part.set_payload(f.read())
#                 encoders.encode_base64(part)
#                 part.add_header(
#                     "Content-Disposition",
#                     f'attachment; filename="{os.path.basename(p)}"'
#                 )
#                 msg.attach(part)

#     _smtp_send(mail_cfg, msg, [to] + cc)


# # ================================================================
# #  Background scraper
# # ================================================================

# def _bg_scraper(db_cfg: dict, instance_path: str):
#     global _scraper_state
#     with _scraper_lock:
#         _scraper_state.update({"running": True, "message": "Scraper running…", "success": None})
#     try:
#         from app.news_scraper import run_news_scraper
#         result = run_news_scraper(db_config=db_cfg, instance_path=instance_path)
#         with _scraper_lock:
#             _scraper_state.update({
#                 "success": result.get("success", False),
#                 "message": result.get("message", "Scraper finished."),
#             })
#     except Exception as e:
#         with _scraper_lock:
#             _scraper_state.update({"success": False, "message": f"Scraper error: {e}"})
#         traceback.print_exc()
#     finally:
#         with _scraper_lock:
#             _scraper_state["running"] = False


# # ================================================================
# #  Routes
# # ================================================================

# @main_bp.route("/")
# def index():
#     return render_template("main/landing.html")


# @main_bp.route("/home")
# @login_required
# def home():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     today = date.today()
#     pub = unpub = 0
#     try:
#         cursor.execute(
#             "SELECT COUNT(*) AS cnt FROM published_news WHERE DATE(published_at)=%s",
#             (today,)
#         )
#         pub = cursor.fetchone()["cnt"]
#         cursor.execute(
#             "SELECT COUNT(*) AS cnt FROM non_published_news WHERE published=0"
#         )
#         unpub = cursor.fetchone()["cnt"]
#     except Exception as e:
#         print(f"[HOME] count error: {e}")
#     finally:
#         cursor.close()
#     return render_template(
#         "main/home.html",
#         today_published_count=pub,
#         today_unpublished_count=unpub,
#     )


# # ── Keywords ──────────────────────────────────────────────────
# @main_bp.route("/view-keywords", methods=["GET", "POST"])
# @login_required
# def view_keywords():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     if request.method == "POST":
#         action = request.form.get("action")
#         if action == "add":
#             sr_no = request.form.get("sr_no", "").strip()
#             kw    = request.form.get("keyword", "").strip()
#             if not sr_no or not kw:
#                 flash("Both Sr. No and Keyword are required.", "danger")
#             else:
#                 try:
#                     sr_no = int(sr_no)
#                     cursor.execute("SELECT id FROM keywords WHERE sr_no=%s", (sr_no,))
#                     if cursor.fetchone():
#                         flash("Sr. No already exists.", "danger")
#                     else:
#                         cursor.execute(
#                             "INSERT INTO keywords(sr_no,keyword) VALUES(%s,%s)",
#                             (sr_no, kw)
#                         )
#                         db.commit()
#                         flash("Keyword added.", "success")
#                 except ValueError:
#                     flash("Sr. No must be a number.", "danger")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_keywords"))
#         elif action == "edit":
#             kid = request.form.get("keyword_id", "").strip()
#             kw  = request.form.get("edit_keyword", "").strip()
#             if not kid or not kw:
#                 flash("Keyword ID and value required.", "danger")
#             else:
#                 try:
#                     cursor.execute("UPDATE keywords SET keyword=%s WHERE id=%s", (kw, kid))
#                     db.commit()
#                     flash("Updated." if cursor.rowcount else "Not found.",
#                           "success" if cursor.rowcount else "warning")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_keywords"))
#         elif action == "bulk_delete":
#             ids = request.form.getlist("selected_keywords")
#             if not ids:
#                 flash("Select at least one.", "warning")
#             else:
#                 try:
#                     ph = ",".join(["%s"] * len(ids))
#                     cursor.execute(f"DELETE FROM keywords WHERE id IN ({ph})", tuple(ids))
#                     db.commit()
#                     flash("Deleted.", "success")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_keywords"))
#     cursor.execute("SELECT id,sr_no,keyword FROM keywords ORDER BY sr_no ASC")
#     keywords = cursor.fetchall()
#     cursor.close()
#     return render_template("main/view_keywords.html", keywords=keywords)


# # ── Websites ───────────────────────────────────────────────────
# @main_bp.route("/view-websites", methods=["GET", "POST"])
# @login_required
# def view_websites():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     if request.method == "POST":
#         action = request.form.get("action")
#         if action == "add":
#             sr_no = request.form.get("sr_no", "").strip()
#             val   = request.form.get("websites", "").strip()
#             if not sr_no or not val:
#                 flash("Both fields required.", "danger")
#             else:
#                 try:
#                     sr_no = int(sr_no)
#                     cursor.execute("SELECT id FROM websites WHERE sr_no=%s", (sr_no,))
#                     if cursor.fetchone():
#                         flash("Sr. No exists.", "danger")
#                     else:
#                         cursor.execute(
#                             "INSERT INTO websites(sr_no,websites) VALUES(%s,%s)",
#                             (sr_no, val)
#                         )
#                         db.commit()
#                         flash("Website added.", "success")
#                 except ValueError:
#                     flash("Sr. No must be a number.", "danger")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_websites"))
#         elif action == "edit":
#             wid = request.form.get("websites_id", "").strip()
#             val = request.form.get("edit_websites", "").strip()
#             if not wid or not val:
#                 flash("Both fields required.", "danger")
#             else:
#                 try:
#                     cursor.execute("UPDATE websites SET websites=%s WHERE id=%s", (val, wid))
#                     db.commit()
#                     flash("Updated." if cursor.rowcount else "Not found.",
#                           "success" if cursor.rowcount else "warning")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_websites"))
#         elif action == "bulk_delete":
#             ids = request.form.getlist("selected_websites")
#             if not ids:
#                 flash("Select at least one.", "warning")
#             else:
#                 try:
#                     ph = ",".join(["%s"] * len(ids))
#                     cursor.execute(f"DELETE FROM websites WHERE id IN ({ph})", tuple(ids))
#                     db.commit()
#                     flash("Deleted.", "success")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_websites"))
#     cursor.execute("SELECT id,sr_no,websites FROM websites ORDER BY sr_no ASC")
#     websites = cursor.fetchall()
#     cursor.close()
#     return render_template("main/view_websites.html", websites=websites)


# # ── News Type ──────────────────────────────────────────────────
# @main_bp.route("/view-news-type", methods=["GET", "POST"])
# @login_required
# def view_news_type():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     if request.method == "POST":
#         action = request.form.get("action")
#         if action == "add":
#             sr_no = request.form.get("sr_no", "").strip()
#             val   = request.form.get("news_type", "").strip()
#             if not sr_no or not val:
#                 flash("Both fields required.", "danger")
#             else:
#                 try:
#                     sr_no = int(sr_no)
#                     cursor.execute("SELECT id FROM news WHERE sr_no=%s", (sr_no,))
#                     if cursor.fetchone():
#                         flash("Sr. No exists.", "danger")
#                     else:
#                         cursor.execute(
#                             "INSERT INTO news(sr_no,news_type) VALUES(%s,%s)",
#                             (sr_no, val)
#                         )
#                         db.commit()
#                         flash("News type added.", "success")
#                 except ValueError:
#                     flash("Sr. No must be a number.", "danger")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_news_type"))
#         elif action == "edit":
#             ntid = request.form.get("news_type_id", "").strip()
#             val  = request.form.get("edit_news_type", "").strip()
#             if not ntid or not val:
#                 flash("Both fields required.", "danger")
#             else:
#                 try:
#                     cursor.execute("UPDATE news SET news_type=%s WHERE id=%s", (val, ntid))
#                     db.commit()
#                     flash("Updated." if cursor.rowcount else "Not found.",
#                           "success" if cursor.rowcount else "warning")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_news_type"))
#         elif action == "bulk_delete":
#             ids = request.form.getlist("selected_news_types")
#             if not ids:
#                 flash("Select at least one.", "warning")
#             else:
#                 try:
#                     ph = ",".join(["%s"] * len(ids))
#                     cursor.execute(f"DELETE FROM news WHERE id IN ({ph})", tuple(ids))
#                     db.commit()
#                     flash("Deleted.", "success")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_news_type"))
#     cursor.execute("SELECT id,sr_no,news_type FROM news ORDER BY sr_no ASC")
#     news_types = cursor.fetchall()
#     cursor.close()
#     return render_template("main/view_news_type.html", news_types=news_types)


# # ── Commodity ──────────────────────────────────────────────────
# @main_bp.route("/view-commodity", methods=["GET", "POST"])
# @login_required
# def view_commodity():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     if request.method == "POST":
#         action = request.form.get("action")
#         if action == "add":
#             sr_no = request.form.get("sr_no", "").strip()
#             val   = request.form.get("commodity", "").strip()
#             if not sr_no or not val:
#                 flash("Both fields required.", "danger")
#             else:
#                 try:
#                     sr_no = int(sr_no)
#                     cursor.execute("SELECT id FROM commodity WHERE sr_no=%s", (sr_no,))
#                     if cursor.fetchone():
#                         flash("Sr. No exists.", "danger")
#                     else:
#                         cursor.execute(
#                             "INSERT INTO commodity(sr_no,commodity) VALUES(%s,%s)",
#                             (sr_no, val)
#                         )
#                         db.commit()
#                         flash("Commodity added.", "success")
#                 except ValueError:
#                     flash("Sr. No must be a number.", "danger")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_commodity"))
#         elif action == "edit":
#             cid = request.form.get("commodity_id", "").strip()
#             val = request.form.get("edit_commodity", "").strip()
#             if not cid or not val:
#                 flash("Both fields required.", "danger")
#             else:
#                 try:
#                     cursor.execute("UPDATE commodity SET commodity=%s WHERE id=%s", (val, cid))
#                     db.commit()
#                     flash("Updated." if cursor.rowcount else "Not found.",
#                           "success" if cursor.rowcount else "warning")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_commodity"))
#         elif action == "bulk_delete":
#             ids = request.form.getlist("selected_commodities")
#             if not ids:
#                 flash("Select at least one.", "warning")
#             else:
#                 try:
#                     ph = ",".join(["%s"] * len(ids))
#                     cursor.execute(f"DELETE FROM commodity WHERE id IN ({ph})", tuple(ids))
#                     db.commit()
#                     flash("Deleted.", "success")
#                 except Exception as e:
#                     db.rollback()
#                     flash(f"Error: {e}", "danger")
#             cursor.close()
#             return redirect(url_for("main.view_commodity"))
#     cursor.execute("SELECT id,sr_no,commodity FROM commodity ORDER BY sr_no ASC")
#     commodities = cursor.fetchall()
#     cursor.close()
#     return render_template("main/view_commodity.html", commodities=commodities)


# # ================================================================
# #  All Non-Published News — GET (paginated)
# # ================================================================

# @main_bp.route("/all-non-published-news")
# @login_required
# def all_non_published_news():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)

#     try:
#         cursor.execute("SELECT * FROM user_settings WHERE id=1")
#         settings = cursor.fetchone() or {}
#     except Exception:
#         settings = {}

#     cats_str = settings.get("content_categories", "all") or "all"
#     cats     = [c.strip().lower() for c in cats_str.split(",") if c.strip()]
#     page     = max(1, int(request.args.get("page", 1)))

#     try:
#         if "all" in cats:
#             cursor.execute(
#                 "SELECT COUNT(*) AS cnt FROM non_published_news WHERE published=0"
#             )
#         else:
#             ph = ",".join(["%s"] * len(cats))
#             cursor.execute(
#                 f"SELECT COUNT(*) AS cnt FROM non_published_news "
#                 f"WHERE published=0 AND LOWER(news_type) IN ({ph})",
#                 tuple(cats)
#             )
#         total = cursor.fetchone()["cnt"]
#         total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
#         page   = min(page, total_pages)
#         offset = (page - 1) * PER_PAGE

#         if "all" in cats:
#             cursor.execute(
#                 "SELECT id,news_date,news_type,news_headline,"
#                 "news_text,news_url,keywords,date_of_insert "
#                 "FROM non_published_news WHERE published=0 "
#                 "ORDER BY date_of_insert DESC LIMIT %s OFFSET %s",
#                 (PER_PAGE, offset)
#             )
#         else:
#             ph = ",".join(["%s"] * len(cats))
#             cursor.execute(
#                 f"SELECT id,news_date,news_type,news_headline,"
#                 f"news_text,news_url,keywords,date_of_insert "
#                 f"FROM non_published_news WHERE published=0 "
#                 f"AND LOWER(news_type) IN ({ph}) "
#                 f"ORDER BY date_of_insert DESC LIMIT %s OFFSET %s",
#                 tuple(cats) + (PER_PAGE, offset)
#             )
#         news = cursor.fetchall()

#     except Exception as e:
#         flash(f"Error loading news: {e}", "danger")
#         news, total, total_pages, page = [], 0, 1, 1
#     finally:
#         cursor.close()

#     with _scraper_lock:
#         scrape_running = _scraper_state["running"]

#     return render_template(
#         "main/all_non_published_news.html",
#         news=news,
#         scrape_running=scrape_running,
#         page=page,
#         total_pages=total_pages,
#         total=total,
#         per_page=PER_PAGE,
#     )


# # ================================================================
# #  All Non-Published News — POST (publish / delete)
# # ================================================================

# @main_bp.route("/all-non-published-news/actions", methods=["POST"])
# @login_required
# def all_non_published_news_actions():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     action = request.form.get("action")
#     ids    = request.form.getlist("selected_news")

#     if not ids:
#         flash("Please select at least one article.", "warning")
#         cursor.close()
#         return redirect(url_for("main.all_non_published_news"))

#     ph = ",".join(["%s"] * len(ids))

#     try:
#         # ── DELETE ───────────────────────────────────────────────
#         if action == "delete":
#             cursor.execute(
#                 f"DELETE FROM non_published_news WHERE id IN ({ph})",
#                 tuple(ids)
#             )
#             db.commit()
#             flash(f"{cursor.rowcount} article(s) deleted.", "success")

#         # ── PUBLISH ──────────────────────────────────────────────
#         elif action == "publish":
#             # Fetch selected articles
#             cursor.execute(
#                 f"SELECT id, news_date, news_type, news_headline, "
#                 f"news_text, news_url, keywords, date_of_insert "
#                 f"FROM non_published_news WHERE id IN ({ph})",
#                 tuple(ids)
#             )
#             articles = cursor.fetchall()

#             if not articles:
#                 flash("No articles found for selected IDs.", "warning")
#                 cursor.close()
#                 return redirect(url_for("main.all_non_published_news"))

#             # Load settings
#             try:
#                 cursor.execute("SELECT * FROM user_settings WHERE id=1")
#                 settings = cursor.fetchone() or {}
#             except Exception:
#                 settings = {}

#             # Ensure PDF output folder exists
#             pdf_folder = current_app.config.get(
#                 "PDF_FOLDER",
#                 os.path.join(current_app.root_path, "static", "pdfs")
#             )
#             os.makedirs(pdf_folder, exist_ok=True)

#             now     = datetime.now()
#             pub_ids = []

#             # STEP 1: Insert ALL articles into published_news immediately
#             # They will appear in Today's Published News right away.
#             for art in articles:
#                 cursor.execute(
#                     "INSERT INTO published_news "
#                     "(source_id, news_date, news_type, news_headline, "
#                     "news_text, news_url, keywords, date_of_insert, published_at) "
#                     "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
#                     (
#                         art["id"],
#                         art["news_date"],
#                         art["news_type"],
#                         art["news_headline"],
#                         art["news_text"],
#                         art["news_url"],
#                         art["keywords"],
#                         art["date_of_insert"],
#                         now,
#                     )
#                 )
#                 db.commit()
#                 pub_ids.append(cursor.lastrowid)

#             # STEP 2: Delete source rows from non_published_news IMMEDIATELY
#             # (NOT after PDF generation — this is the fix for articles
#             #  staying in the non-published list)
#             cursor.execute(
#                 f"DELETE FROM non_published_news WHERE id IN ({ph})",
#                 tuple(ids)
#             )
#             db.commit()
#             print(f"[PUBLISH] Deleted {len(ids)} rows from non_published_news.")

#             # STEP 3: Collect all values needed BEFORE leaving request context
#             db_cfg = _get_db_cfg()
#             cfg    = current_app.config
#             mail_cfg = {
#                 "MAIL_SERVER":   cfg.get("MAIL_SERVER",   "smtp.gmail.com"),
#                 "MAIL_PORT":     cfg.get("MAIL_PORT",     587),
#                 "MAIL_USE_TLS":  cfg.get("MAIL_USE_TLS",  True),
#                 "MAIL_USERNAME": cfg.get("MAIL_USERNAME", ""),
#                 "MAIL_PASSWORD": cfg.get("MAIL_PASSWORD", ""),
#                 "MAIL_FROM":     cfg.get("MAIL_FROM",     cfg.get("MAIL_USERNAME", "")),
#                 "BASE_URL":      cfg.get("BASE_URL",      "http://127.0.0.1:5000"),
#             }
#             template_dir = os.path.join(current_app.root_path, "templates", "main")

#             # STEP 4: PDF generation + email in background thread
#             # Pass only plain Python values — no Flask objects
#             threading.Thread(
#                 target=_bg_pdf_email,
#                 args=(
#                     db_cfg,
#                     mail_cfg,
#                     template_dir,
#                     list(articles),   # plain list of dicts
#                     list(pub_ids),    # plain list of ints
#                     pdf_folder,       # plain string
#                     dict(settings),   # plain dict
#                 ),
#                 daemon=True,
#             ).start()

#             flash(
#                 f"{len(articles)} article(s) published successfully! "
#                 f"They are now visible in Today's Published News. "
#                 f"PDFs are being generated from the original news websites in the background.",
#                 "success",
#             )

#     except Exception as e:
#         db.rollback()
#         flash(f"Error during publish: {e}", "danger")
#         traceback.print_exc()
#     finally:
#         cursor.close()

#     return redirect(url_for("main.all_non_published_news"))


# # ================================================================
# #  Refresh News — background scraper
# # ================================================================

# @main_bp.route("/refresh-news", methods=["POST"])
# @login_required
# def refresh_news():
#     with _scraper_lock:
#         if _scraper_state["running"]:
#             flash("Scraper is already running.", "warning")
#             return redirect(url_for("main.all_non_published_news"))

#     # Extract plain values INSIDE request context, then pass to thread
#     db_cfg        = _get_db_cfg()
#     instance_path = current_app.instance_path

#     threading.Thread(
#         target=_bg_scraper,
#         args=(db_cfg, instance_path),
#         daemon=True,
#     ).start()

#     flash("News refresh started in the background. Page will auto-update when complete.", "info")
#     return redirect(url_for("main.all_non_published_news"))


# @main_bp.route("/refresh-news/status")
# @login_required
# def refresh_news_status():
#     with _scraper_lock:
#         state = dict(_scraper_state)
#     return jsonify(state)


# # ================================================================
# #  Today's Published News — paginated
# # ================================================================

# @main_bp.route("/today-published-news")
# @login_required
# def today_published_news():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     today = date.today()
#     page  = max(1, int(request.args.get("page", 1)))

#     try:
#         cursor.execute(
#             "SELECT COUNT(*) AS cnt FROM published_news WHERE DATE(published_at)=%s",
#             (today,)
#         )
#         total = cursor.fetchone()["cnt"]
#         total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
#         page   = min(page, total_pages)
#         offset = (page - 1) * PER_PAGE

#         cursor.execute(
#             "SELECT id, news_date, news_type, news_headline, "
#             "news_url, pdf_path, published_at "
#             "FROM published_news WHERE DATE(published_at)=%s "
#             "ORDER BY published_at DESC LIMIT %s OFFSET %s",
#             (today, PER_PAGE, offset)
#         )
#         news = cursor.fetchall()

#     except Exception as e:
#         flash(f"Error loading published news: {e}", "danger")
#         news, total, total_pages, page = [], 0, 1, 1
#     finally:
#         cursor.close()

#     return render_template(
#         "main/today_published_news.html",
#         news=news,
#         today=today,
#         page=page,
#         total_pages=total_pages,
#         total=total,
#         per_page=PER_PAGE,
#     )


# # ================================================================
# #  Check PDF ready — polled by JS every 8 s
# # ================================================================

# @main_bp.route("/check-pdf/<int:news_id>")
# @login_required
# def check_pdf_ready(news_id):
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     try:
#         cursor.execute(
#             "SELECT pdf_path FROM published_news WHERE id=%s",
#             (news_id,)
#         )
#         row = cursor.fetchone()
#     finally:
#         cursor.close()

#     ready = False
#     if row and row.get("pdf_path"):
#         pdf_folder = current_app.config.get(
#             "PDF_FOLDER",
#             os.path.join(current_app.root_path, "static", "pdfs")
#         )
#         fp    = os.path.join(pdf_folder, row["pdf_path"])
#         ready = os.path.exists(fp) and os.path.getsize(fp) > 500

#     return jsonify({"ready": ready})


# # ================================================================
# #  Download PDF
# # ================================================================

# @main_bp.route("/download-pdf/<int:news_id>")
# @login_required
# def download_pdf(news_id):
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     try:
#         cursor.execute(
#             "SELECT pdf_path FROM published_news WHERE id=%s",
#             (news_id,)
#         )
#         row = cursor.fetchone()
#     finally:
#         cursor.close()

#     if not row or not row["pdf_path"]:
#         flash("PDF is still being generated — please try again in a moment.", "warning")
#         return redirect(url_for("main.today_published_news"))

#     pdf_folder = current_app.config.get(
#         "PDF_FOLDER",
#         os.path.join(current_app.root_path, "static", "pdfs")
#     )
#     return send_from_directory(pdf_folder, row["pdf_path"], as_attachment=True)


# # ================================================================
# #  Manual re-send email for today
# # ================================================================

# @main_bp.route("/send-email-today")
# @login_required
# def send_email_today():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     today  = date.today()

#     try:
#         cursor.execute(
#             "SELECT id, news_date, news_type, news_headline, news_url, pdf_path "
#             "FROM published_news WHERE DATE(published_at)=%s "
#             "ORDER BY published_at DESC",
#             (today,)
#         )
#         articles = cursor.fetchall()

#         if not articles:
#             flash("No published news for today.", "warning")
#             cursor.close()
#             return redirect(url_for("main.today_published_news"))

#         try:
#             cursor.execute("SELECT * FROM user_settings WHERE id=1")
#             settings = cursor.fetchone() or {}
#         except Exception:
#             settings = {}

#         cfg = current_app.config
#         mail_cfg = {
#             "MAIL_SERVER":   cfg.get("MAIL_SERVER",   "smtp.gmail.com"),
#             "MAIL_PORT":     cfg.get("MAIL_PORT",     587),
#             "MAIL_USE_TLS":  cfg.get("MAIL_USE_TLS",  True),
#             "MAIL_USERNAME": cfg.get("MAIL_USERNAME", ""),
#             "MAIL_PASSWORD": cfg.get("MAIL_PASSWORD", ""),
#             "MAIL_FROM":     cfg.get("MAIL_FROM",     cfg.get("MAIL_USERNAME", "")),
#             "BASE_URL":      cfg.get("BASE_URL",      "http://127.0.0.1:5000"),
#         }
#         pdf_folder   = cfg.get("PDF_FOLDER", os.path.join(current_app.root_path, "static", "pdfs"))
#         template_dir = os.path.join(current_app.root_path, "templates", "main")
#         pdf_paths    = [
#             os.path.join(pdf_folder, a["pdf_path"])
#             for a in articles if a.get("pdf_path")
#         ]
#         pub_ids = [a["id"] for a in articles]

#         _send_email_bg(
#             articles=list(articles),
#             pub_ids=pub_ids,
#             pdf_paths=pdf_paths,
#             settings=dict(settings),
#             mail_cfg=mail_cfg,
#             template_dir=template_dir,
#         )
#         flash("Email sent successfully.", "success")

#     except Exception as e:
#         flash(f"Failed to send email: {e}", "danger")
#         traceback.print_exc()
#     finally:
#         cursor.close()

#     return redirect(url_for("main.today_published_news"))


# # ================================================================
# #  User Settings
# # ================================================================

# @main_bp.route("/user-settings", methods=["GET", "POST"])
# @login_required
# def user_settings():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)

#     if request.method == "POST":
#         try:
#             cursor.execute(
#                 "INSERT INTO user_settings "
#                 "(id, run_frequency, custom_frequency, scraper_enabled, "
#                 "publish_mode, content_categories, email_recipient, "
#                 "email_cc, email_subject_prefix, email_on_publish) "
#                 "VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
#                 "ON DUPLICATE KEY UPDATE "
#                 "run_frequency=VALUES(run_frequency), "
#                 "custom_frequency=VALUES(custom_frequency), "
#                 "scraper_enabled=VALUES(scraper_enabled), "
#                 "publish_mode=VALUES(publish_mode), "
#                 "content_categories=VALUES(content_categories), "
#                 "email_recipient=VALUES(email_recipient), "
#                 "email_cc=VALUES(email_cc), "
#                 "email_subject_prefix=VALUES(email_subject_prefix), "
#                 "email_on_publish=VALUES(email_on_publish)",
#                 (
#                     request.form.get("run_frequency", "1"),
#                     request.form.get("custom_frequency") or None,
#                     1 if request.form.get("scraper_enabled") else 0,
#                     request.form.get("publish_mode", "manual"),
#                     ",".join(request.form.getlist("content_categories")) or "all",
#                     request.form.get("email_recipient", "").strip(),
#                     request.form.get("email_cc", "").strip(),
#                     request.form.get("email_subject_prefix", "Daily News Alert").strip(),
#                     1 if request.form.get("email_on_publish") else 0,
#                 )
#             )
#             db.commit()
#             flash("Settings saved.", "success")
#         except Exception as e:
#             db.rollback()
#             flash(f"Error: {e}", "danger")
#         finally:
#             cursor.close()
#         return redirect(url_for("main.user_settings"))

#     try:
#         cursor.execute("SELECT * FROM user_settings WHERE id=1")
#         settings = cursor.fetchone() or {}
#     except Exception:
#         settings = {}
#     finally:
#         cursor.close()

#     return render_template("main/user_settings.html", settings=settings)


# def get_settings():
#     db = get_db()
#     cursor = db.cursor(dictionary=True)
#     try:
#         cursor.execute("SELECT * FROM user_settings WHERE id=1")
#         return cursor.fetchone() or {}
#     except Exception:
#         return {}
#     finally:
#         cursor.close()

"""
main.py
-------
Updated publish flow:
  1. Articles inserted into published_news immediately
  2. Source rows deleted from non_published_news immediately
  3. Background thread generates PDFs from the ORIGINAL news URL
  4. DB updated with pdf_path only after successful PDF creation
  5. Email is sent after PDF processing completes

This version adds:
  - stronger logging
  - URL validation logs
  - PDF folder creation safety
  - retry-friendly PDF flow
  - non-daemon background thread for safer execution
"""

from .auth import login_required
from app.db import get_db

import os
import json
import requests
import smtplib
import threading
import traceback
from datetime import date, datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import mysql.connector
from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

main_bp = Blueprint("main", __name__)

PER_PAGE = 10

# ── Scraper state ─────────────────────────────────────────────
_scraper_lock = threading.Lock()
_scraper_state = {"running": False, "message": "", "success": None}


# ── Raw DB connection (safe in any thread, no Flask g) ────────
def _raw_conn(db_cfg: dict):
    return mysql.connector.connect(**db_cfg)


# ── Extract DB config inside a request context ───────────────
def _get_db_cfg() -> dict:
    cfg = current_app.config
    return {
        "host": cfg["MYSQL_HOST"],
        "user": cfg["MYSQL_USER"],
        "password": cfg["MYSQL_PASSWORD"],
        "database": cfg["MYSQL_DB"],
    }

def get_weather_overview(temp, humidity, wind, desc):
    wind_status = "Breezy" if wind > 5.0 else "Calm"
    precip_status = "Rain likely" if "rain" in desc.lower() else "Dry"

    impacts = (
        "Hazardous for spraying or sensitive tasks."
        if wind > 5.0 or "rain" in desc.lower()
        else "Ideal for field work."
    )

    overview = (
        f"* ADVISORY: {desc.upper()} in effect.\n"
        f"* WHAT: Temp {temp}°C with {humidity}% humidity.\n"
        f"* WIND: {wind_status} ({wind} m/s).\n"
        f"* IMPACTS: {precip_status} conditions. {impacts}"
    )
    return overview

# Agri and Weather Data
@main_bp.route("/agri-dashboard")
@login_required
def agri_dashboard():
    # 1. Fetch Live Weather (Thane)
    # Get free key from openweathermap.org
    weather_api_key = "2baab2dc7ad18ffef8b81c014e893e1c" 
    weather_url = f"https://api.openweathermap.org/data/2.5/weather?q=Thane&units=metric&appid={weather_api_key}"
    
    weather_data = {"temp": "--", "desc": "Offline", "location": "Thane", "humidity": "--"}
    try:
        w_res = requests.get(weather_url, timeout=3).json()
        if w_res.get("main"):
            weather_data = {
                "temp": round(w_res['main']['temp']),
                "desc": w_res['weather'][0]['description'].capitalize(),
                "location": w_res['name'],
                "humidity": w_res['main']['humidity']
            }
    except:
        pass

    # 2. Agricultural Commodity Prices
    # These can be pulled from your DB or a Scraper later
    commodities = [
        {"name": "Onion", "price": "2,450", "unit": "Quintal", "trend": "up"},
        {"name": "Cotton", "price": "7,100", "unit": "Quintal", "trend": "down"},
        {"name": "Sugarcane", "price": "315", "unit": "Ton", "trend": "up"}
    ]

    return render_template("main/agri_dashboard.html", 
                           weather=weather_data, 
                           commodities=commodities)

# ================================================================
#  Background: PDF generation + email
# ================================================================
def _bg_pdf_email(db_cfg, mail_cfg, template_dir, articles, pub_ids, pdf_folder, settings):
    """
    Generate a PDF for each published article by visiting its real URL,
    update pdf_path in DB, then send email.

    Parameters are plain Python values only.
    """
    try:
        from app.pdf_generator import generate_pdf_from_url
    except ImportError as e:
        print(f"[BG] Cannot import app.pdf_generator: {e}")
        traceback.print_exc()
        generate_pdf_from_url = None

    pdf_paths = []

    os.makedirs(pdf_folder, exist_ok=True)
    print(f"[BG] PDF folder ready: {pdf_folder}")

    for art, pub_id in zip(articles, pub_ids):
        news_url = (art.get("news_url") or "").strip()
        safe_name = f"news_{pub_id}.pdf"
        out_path = os.path.join(pdf_folder, safe_name)

        print("=" * 80)
        print(f"[BG] Starting PDF generation for pub_id={pub_id}")
        print(f"[BG] Headline : {art.get('news_headline', '')}")
        print(f"[BG] URL      : {news_url}")
        print(f"[BG] Output   : {out_path}")

        pdf_ok = False

        if not news_url:
            print(f"[BG] pub_id={pub_id} has empty news_url. Skipping PDF.")
        elif not news_url.startswith(("http://", "https://")):
            print(f"[BG] pub_id={pub_id} has invalid URL: {news_url}")
        elif generate_pdf_from_url is None:
            print(f"[BG] pdf_generator import failed for pub_id={pub_id}. Skipping PDF.")
        else:
            try:
                pdf_ok = generate_pdf_from_url(url=news_url, output_path=out_path)
                print(f"[BG] generate_pdf_from_url returned {pdf_ok} for pub_id={pub_id}")
            except Exception as e:
                print(f"[BG] PDF generation exception for pub_id={pub_id}: {e}")
                traceback.print_exc()
                pdf_ok = False

        if pdf_ok and os.path.exists(out_path):
            try:
                size = os.path.getsize(out_path)
                print(f"[BG] PDF file exists for pub_id={pub_id}, size={size} bytes")

                c = _raw_conn(db_cfg)
                cur = c.cursor()
                cur.execute(
                    "UPDATE published_news SET pdf_path=%s WHERE id=%s",
                    (safe_name, pub_id),
                )
                c.commit()
                cur.close()
                c.close()

                print(f"[BG] DB updated with pdf_path={safe_name} for pub_id={pub_id}")
                pdf_paths.append(out_path)

        except Exception as e:
            print(f"[SCHEDULER] Tick error: {e}")
            traceback.print_exc()


def _scheduler_loop(app):
    import time
    print(
        f"[SCHEDULER] Loop started — "
        f"ticking every {SCHEDULER_TICK_SECONDS}s."
    )
    while True:
        time.sleep(SCHEDULER_TICK_SECONDS)   # tick first, then check
        _scheduler_tick(app)


def init_scheduler(app):
    """
    Call once from create_app() after the app is configured.
    Starts a single daemon thread for schedule checks.

    Flask's development reloader forks the process — the scheduler
    must only start in the CHILD (reloader worker) process, not the
    parent watcher.  We detect this via the WERKZEUG_RUN_MAIN env var:
      - In production (gunicorn/waitress): WERKZEUG_RUN_MAIN is unset
        but there is no reloader, so we always start.
      - In dev with reloader: only start when WERKZEUG_RUN_MAIN == 'true'.
      - In dev without reloader (--no-reload): always start.
    """
    import os as _os
    # If running under Werkzeug reloader, only start in the child process
    werkzeug_main = _os.environ.get("WERKZEUG_RUN_MAIN")
    # werkzeug_main is 'true' in child, None in parent watcher, None in prod
    # We skip only when it's explicitly the parent watcher (None AND reloader active)
    # Safest: check if we're the reloader monitor (parent sets no WERKZEUG_RUN_MAIN)
    # We start in all cases EXCEPT when WERKZEUG_RUN_MAIN is explicitly absent
    # AND we can detect the reloader is active. Simplest reliable check:
    if _os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        # Explicit parent watcher — skip
        return

    global _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        _scheduler_started = True

    t = threading.Thread(target=_scheduler_loop, args=(app,), daemon=True)
    t.start()
    print("[SCHEDULER] Background scheduler initialised.")


# ════════════════════════════════════════════════════════════════
#  EMAIL HELPERS
# ════════════════════════════════════════════════════════════════

def _render_email_template(template_dir: str, news_items: list, date_label: str) -> str:
    from jinja2 import Environment, FileSystemLoader
    env  = Environment(loader=FileSystemLoader(template_dir))
    tmpl = env.get_template("email_template.html")
    return tmpl.render(news_items=news_items, date_label=date_label)


def _smtp_send(mail_cfg: dict, msg, recipients: list):
    with smtplib.SMTP(mail_cfg["MAIL_SERVER"], mail_cfg["MAIL_PORT"]) as s:
        s.ehlo()
        if mail_cfg.get("MAIL_USE_TLS", True):
            s.starttls()
        s.login(mail_cfg["MAIL_USERNAME"], mail_cfg["MAIL_PASSWORD"])
        s.sendmail(msg["From"], recipients, msg.as_string())
        print(f"[EMAIL] Sent to: {recipients}")


def _send_email_bg(articles, pub_ids, pdf_paths, settings, mail_cfg, template_dir):
    to         = settings.get("email_recipient") or "niyati.b@seamlessautomations.com"
    cc_raw     = settings.get("email_cc") or ""
    cc         = [e.strip() for e in cc_raw.split(",") if e.strip()]
    pfx        = settings.get("email_subject_prefix") or "Daily News Alert"
    mode       = settings.get("publish_mode", "manual")
    date_label = datetime.now().strftime("%d %B %Y")

    if mode == "auto":
        base  = mail_cfg.get("BASE_URL", "http://127.0.0.1:5000")
        items = [
            dict(a, pdf_view_url=f"{base}/download-pdf/{pid}")
            for a, pid in zip(articles, pub_ids)
        ]
        html_body = _render_email_template(template_dir, items, date_label)
    else:
        html_body = _render_email_template(template_dir, list(articles), date_label)

    msg            = MIMEMultipart("mixed")
    msg["Subject"] = f"{pfx} — {date_label}"
    msg["From"]    = mail_cfg.get("MAIL_FROM", mail_cfg.get("MAIL_USERNAME", ""))
    msg["To"]      = to
    if cc:
        msg["Cc"]  = ", ".join(cc)

    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if mode != "auto":
        for p in pdf_paths:
            if p and os.path.exists(p):
                with open(p, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f'attachment; filename="{os.path.basename(p)}"'
                )
                msg.attach(part)

    _smtp_send(mail_cfg, msg, [to] + cc)


# ================================================================
#  Background scraper
# ================================================================
def _bg_scraper(db_cfg: dict, instance_path: str):
    global _scraper_state
    with _scraper_lock:
        _scraper_state.update(
            {"running": True, "message": "Scraper running…", "success": None}
        )

    try:
        from app.news_scraper import run_news_scraper

        result = run_news_scraper(db_config=db_cfg, instance_path=instance_path)
        with _scraper_lock:
            _scraper_state.update(
                {
                    "success": result.get("success", False),
                    "message": result.get("message", "Scraper finished."),
                }
            )
    except Exception as e:
        with _scraper_lock:
            _scraper_state.update({"success": False, "message": f"Scraper error: {e}"})
        traceback.print_exc()
    finally:
        with _scraper_lock:
            _scraper_state["running"] = False


# ================================================================
#  Routes
# ================================================================
@main_bp.route("/")
def index():
    return render_template("main/landing.html")

# for agriculture trends
def to_float(value):
    try:
        if value in (None, "", "N/A", "-"):
            return None
        return float(str(value).replace(",", "").replace("₹", "").strip())
    except Exception:
        return None

# for agriculture trends
def get_recent_months(year_str, month_str, count=6):
    months = []
    year_num = int(year_str)
    month_num = int(month_str)

    for _ in range(count):
        months.append((year_num, month_num))
        month_num -= 1
        if month_num == 0:
            month_num = 12
            year_num -= 1

    months.reverse()
    return months

# for agriculture trends
def build_sparkline_points(values, width=220, height=52, padding=6):
    clean_values = [v for v in values if v is not None]

    if not clean_values:
        return {
            "line_points": "",
            "fill_points": "",
        }

    if len(values) == 1:
        values = [values[0], values[0]]

    min_val = min(clean_values)
    max_val = max(clean_values)

    if min_val == max_val:
        max_val = min_val + 1

    usable_width = width - (padding * 2)
    usable_height = height - (padding * 2)
    step_x = usable_width / (len(values) - 1) if len(values) > 1 else usable_width

    points = []
    for idx, value in enumerate(values):
        if value is None:
            value = clean_values[-1]

        x = padding + (idx * step_x)
        y = padding + (max_val - value) / (max_val - min_val) * usable_height
        points.append((round(x, 2), round(y, 2)))

    line_points = " ".join(f"{x},{y}" for x, y in points)
    base_y = height - padding
    fill_points = f"{points[0][0]},{base_y} " + line_points + f" {points[-1][0]},{base_y}"

    return {
        "line_points": line_points,
        "fill_points": fill_points,
    }

def fetch_month_average_for_crop(crop_id, year_num, month_num, agmark_headers, month_names):
    try:
        url = "https://api.agmarknet.gov.in/v1/price-trend/wholesale-prices-monthly"
        params = {
            "report_mode": "Statewise",
            "commodity": crop_id,
            "year": str(year_num),
            "month": str(month_num),
            "state": "0",
            "district": "0",
            "export": "false",
        }

        resp = requests.get(url, params=params, headers=agmark_headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("rows", []) if isinstance(data, dict) else []
        price_key = f"prices_{month_names[int(month_num)]}_{int(year_num)}"

        values = []
        for row in rows:
            price_val = to_float(row.get(price_key))
            if price_val is not None:
                values.append(price_val)

        if not values:
            return None

        return sum(values) / len(values)

    except Exception as e:
        print(f"Month average fetch error for crop_id={crop_id}, {month_num}/{year_num}: {e}")
        return None

@main_bp.route("/home")
@login_required
def home():
    api_key = "2baab2dc7ad18ffef8b81c014e893e1c"

    # 1. PARAMETER CAPTURE
    search_query = request.args.get("city_search", "").strip()

    current_year = datetime.now().year
    current_month = datetime.now().month

    selected_id = request.args.get("cmdt_id", "").strip()
    selected_year = request.args.get("year", str(current_year)).strip()
    selected_month = request.args.get("month", str(current_month)).strip()

    year_options = [str(current_year - 2), str(current_year - 1), str(current_year)]

    month_names = {
        1: "january",
        2: "february",
        3: "march",
        4: "april",
        5: "may",
        6: "june",
        7: "july",
        8: "august",
        9: "september",
        10: "october",
        11: "november",
        12: "december",
    }

    month_short = {
        "1": "Jan",
        "2": "Feb",
        "3": "Mar",
        "4": "Apr",
        "5": "May",
        "6": "Jun",
        "7": "Jul",
        "8": "Aug",
        "9": "Sep",
        "10": "Oct",
        "11": "Nov",
        "12": "Dec",
    }

    def get_previous_month_year(month_str, year_str):
        month_num = int(month_str)
        year_num = int(year_str)
        if month_num == 1:
            return 12, year_num - 1
        return month_num - 1, year_num

    agmark_headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.8",
        "origin": "https://www.agmarknet.gov.in",
        "referer": "https://www.agmarknet.gov.in/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    }

    # 2. WEATHER LOGIC (ONLY SEARCHED CITY)
    weather_card = {
        "location": "Search City",
        "temp": "--",
        "desc": "Waiting for input",
        "icon": "01d",
        "humidity": "--",
        "wind": "--",
        "overview": "Enter a city name to see weather advisories."
    }

    if search_query:
        try:
            geo_url = (
                f"https://api.openweathermap.org/geo/1.0/direct"
                f"?q={search_query}&limit=1&appid={api_key}"
            )
            geo_resp = requests.get(geo_url, timeout=5).json()

            if geo_resp:
                lat = geo_resp[0]["lat"]
                lon = geo_resp[0]["lon"]
                resolved_city = geo_resp[0].get("name", search_query)

                w_url = (
                    f"https://api.openweathermap.org/data/2.5/weather"
                    f"?lat={lat}&lon={lon}&units=metric&appid={api_key}"
                )
                w_resp = requests.get(w_url, timeout=5).json()

                if w_resp.get("main"):
                    weather_desc = w_resp["weather"][0]["description"].title()
                    temp = round(w_resp["main"]["temp"])
                    hum = w_resp["main"]["humidity"]
                    wind = w_resp["wind"]["speed"]

                    weather_card.update({
                        "location": resolved_city,
                        "temp": temp,
                        "desc": weather_desc,
                        "icon": w_resp["weather"][0]["icon"],
                        "humidity": hum,
                        "wind": wind,
                        "overview": get_weather_overview(temp, hum, wind, weather_desc)
                    })
                else:
                    weather_card.update({
                        "location": search_query,
                        "desc": "Weather unavailable",
                        "overview": "Weather data could not be fetched for this city right now."
                    })
            else:
                weather_card.update({
                    "location": search_query,
                    "desc": "City not found",
                    "overview": "No matching city was found. Please check the spelling and try again."
                })

        except Exception as e:
            print(f"Weather Error for {search_query}: {e}")
            weather_card.update({
                "location": search_query or "Search City",
                "desc": "Weather unavailable",
                "overview": "Something went wrong while fetching weather data."
            })

    # 3. COMMODITY DROPDOWN LOGIC (FROM AGMARKNET API)
    all_options = []
    try:
        commodity_url = "https://api.agmarknet.gov.in/v1/dashboard-commodities-filter"
        commodity_resp = requests.get(commodity_url, headers=agmark_headers, timeout=20)
        commodity_resp.raise_for_status()

        commodity_payload = commodity_resp.json()

        if isinstance(commodity_payload, dict):
            raw_commodities = (
                commodity_payload.get("data")
                or commodity_payload.get("rows")
                or commodity_payload.get("result")
                or commodity_payload.get("commodities")
                or []
            )
        elif isinstance(commodity_payload, list):
            raw_commodities = commodity_payload
        else:
            raw_commodities = []

        seen_ids = set()
        normalized_options = []

        for item in raw_commodities:
            if not isinstance(item, dict):
                continue

            item_id = (
                item.get("id")
                or item.get("commodity_id")
                or item.get("value")
                or item.get("commodity")
            )
            item_name = (
                item.get("cmdt_name")
                or item.get("commodity_name")
                or item.get("name")
                or item.get("label")
                or item.get("commodity")
            )

            if item_id in (None, "") or not item_name:
                continue

            item_id = str(item_id).strip()
            item_name = str(item_name).strip()

            if item_id not in seen_ids:
                seen_ids.add(item_id)
                normalized_options.append({
                    "id": item_id,
                    "cmdt_name": item_name,
                })

        all_options = sorted(normalized_options, key=lambda x: x["cmdt_name"].lower())
        print(f"DEBUG: Loaded {len(all_options)} commodities from Agmarknet API.")

    except Exception as e:
        print(f"Commodity API Load Error: {e}")

    featured_cards = []

    def to_float(value):
        try:
            if value in (None, "", "N/A", "-"):
                return None
            return float(str(value).replace(",", "").replace("₹", "").strip())
        except Exception:
            return None

    def build_featured_card(crop_name, crop_id):
        try:
            recent_months = get_recent_months(selected_year, selected_month, count=6)

            history_values = []
            for year_num, month_num in recent_months:
                avg_price = fetch_month_average_for_crop(
                    crop_id=crop_id,
                    year_num=year_num,
                    month_num=month_num,
                    agmark_headers=agmark_headers,
                    month_names=month_names,
                )
                history_values.append(avg_price)

            usable_values = [v for v in history_values if v is not None]
            if not usable_values:
                return None

            for i in range(len(history_values)):
                if history_values[i] is None:
                    history_values[i] = usable_values[0] if i == 0 else history_values[i - 1]

            current_price = history_values[-1]
            previous_price = history_values[-2] if len(history_values) > 1 else history_values[-1]

            if previous_price and previous_price != 0:
                percent_change = ((current_price - previous_price) / previous_price) * 100
            else:
                percent_change = 0

            sparkline = build_sparkline_points(history_values)

            return {
                "name": crop_name,
                "price": f"{current_price:,.2f}",
                "change": f"{abs(percent_change):.2f}",
                "signed_change": round(percent_change, 2),
                "is_positive": percent_change >= 0,
                "line_points": sparkline["line_points"],
                "fill_points": sparkline["fill_points"],
            }

        except Exception as e:
            print(f"Featured card error for {crop_name}: {e}")
            return None

    featured_crop_names = [
        "Rice",
        "Wheat",
        "Maize",
        "Soyabean",
        "Cotton",
        "Groundnut",
        "Onion",
        "Sugarcane",
    ]

    featured_lookup = {item["cmdt_name"].strip().lower(): item["id"] for item in all_options}

    for crop in featured_crop_names:
        crop_id = featured_lookup.get(crop.lower())
        if crop_id:
            card = build_featured_card(crop, crop_id)
            if card:
                featured_cards.append(card)

    # 4. COMMODITY PRICE API LOGIC
    state_data = []
    cmdt_title = ""
    current_price_label = ""
    previous_price_label = ""    
    commodity_summary = {
        "commodity_name": "",
        "avg_price": "N/A",
        "highest_price": "N/A",
        "highest_state": "-",
        "lowest_price": "N/A",
        "lowest_state": "-",
        "avg_change": "N/A",
    }

    if selected_id:
        try:
            current_month_num = int(selected_month)
            current_year_num = int(selected_year)

            prev_month_num, prev_year_num = get_previous_month_year(selected_month, selected_year)

            current_price_key = f"prices_{month_names[current_month_num]}_{current_year_num}"
            previous_price_key = f"prices_{month_names[prev_month_num]}_{prev_year_num}"

            current_price_label = f"{month_short[str(current_month_num)]} {current_year_num}"
            previous_price_label = f"{month_short[str(prev_month_num)]} {prev_year_num}"

            url = "https://api.agmarknet.gov.in/v1/price-trend/wholesale-prices-monthly"
            params = {
                "report_mode": "Statewise",
                "commodity": selected_id,
                "year": selected_year,
                "month": selected_month,
                "state": "0",
                "district": "0",
                "export": "false",
            }

            resp = requests.get(url, params=params, headers=agmark_headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()

            raw_rows = data.get("rows", []) if isinstance(data, dict) else []
            cmdt_title = data.get("title", "Price Analysis") if isinstance(data, dict) else "Price Analysis"

            if not cmdt_title:
                selected_item = next(
                    (item for item in all_options if item["id"] == selected_id),
                    None
                )
                cmdt_title = selected_item["cmdt_name"] if selected_item else "Price Analysis"

            normalized_rows = []
            for row in raw_rows:
                if not isinstance(row, dict):
                    continue

                normalized_rows.append({
                    "state": row.get("state", ""),
                    "current_price": row.get(current_price_key, "N/A"),
                    "previous_price": row.get(previous_price_key, "N/A"),
                    "change_over_previous_month": row.get("change_over_previous_month", 0),
                })

            state_data = normalized_rows

            def to_float(value):
                try:
                    if value in (None, "", "N/A", "-"):
                        return None
                    return float(str(value).replace(",", "").replace("₹", "").strip())
                except Exception:
                    return None

            if state_data:
                price_rows = []
                change_values = []

                for row in state_data:
                    curr_price = to_float(row.get("current_price"))
                    change_val = to_float(row.get("change_over_previous_month"))

                    if curr_price is not None:
                        price_rows.append({
                            "state": row.get("state", "-"),
                            "price": curr_price
                        })

                    if change_val is not None:
                        change_values.append(change_val)

                if price_rows:
                    avg_price = sum(item["price"] for item in price_rows) / len(price_rows)
                    highest_item = max(price_rows, key=lambda x: x["price"])
                    lowest_item = min(price_rows, key=lambda x: x["price"])

                    commodity_summary.update({
                        "commodity_name": cmdt_title,
                        "avg_price": f"{avg_price:,.2f}",
                        "highest_price": f"{highest_item['price']:,.2f}",
                        "highest_state": highest_item["state"],
                        "lowest_price": f"{lowest_item['price']:,.2f}",
                        "lowest_state": lowest_item["state"],
                    })

                if change_values:
                    avg_change = sum(change_values) / len(change_values)
                    commodity_summary["avg_change"] = f"{avg_change:.1f}"
            print(f"DEBUG: Received {len(state_data)} rows from Agmarknet")

        except Exception as e:
            print(f"Agmarknet API Error: {e}")


    # 5. DATABASE LOGIC (PENDING NEWS)
    pending_count = 0
    recent_news = []

    try:
        db = get_db()
        cursor = db.cursor(dictionary=True)

        cursor.execute("SELECT COUNT(*) AS count FROM non_published_news")
        pending_count = cursor.fetchone()["count"]

        cursor.execute("SELECT * FROM non_published_news ORDER BY id DESC LIMIT 5")
        recent_news = cursor.fetchall()

    except Exception as e:
        print(f"Database Error: {e}")

    finally:
        try:
            cursor.close()
        except Exception:
            pass

    # 6. RENDER TEMPLATE
    return render_template(
        "main/home.html",
        weather_card=weather_card,
        search_query=search_query,
        all_options=all_options,
        state_data=state_data,
        table_title=cmdt_title,
        selected_id=selected_id,
        selected_year=selected_year,
        selected_month=selected_month,
        year_options=year_options,
        current_price_label=current_price_label,
        previous_price_label=previous_price_label,
        pending_count=pending_count,
        recent_news=recent_news,
        commodity_summary=commodity_summary,
        featured_cards=featured_cards
    )

# ── Keywords ──────────────────────────────────────────────────
@main_bp.route("/view-keywords", methods=["GET", "POST"])
@login_required
def view_keywords():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            sr_no = request.form.get("sr_no", "").strip()
            kw    = request.form.get("keyword", "").strip()
            if not sr_no or not kw:
                flash("Both Sr. No and Keyword are required.", "danger")
            else:
                try:
                    sr_no = int(sr_no)
                    cursor.execute("SELECT id FROM keywords WHERE sr_no=%s", (sr_no,))
                    if cursor.fetchone():
                        flash("Sr. No already exists.", "danger")
                    else:
                        cursor.execute(
                            "INSERT INTO keywords(sr_no,keyword) VALUES(%s,%s)",
                            (sr_no, kw)
                        )
                        db.commit()
                        flash("Keyword added.", "success")
                except ValueError:
                    flash("Sr. No must be a number.", "danger")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_keywords"))
        elif action == "edit":
            kid = request.form.get("keyword_id", "").strip()
            kw  = request.form.get("edit_keyword", "").strip()
            if not kid or not kw:
                flash("Keyword ID and value required.", "danger")
            else:
                try:
                    cursor.execute("UPDATE keywords SET keyword=%s WHERE id=%s", (kw, kid))
                    db.commit()
                    flash("Updated." if cursor.rowcount else "Not found.",
                          "success" if cursor.rowcount else "warning")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_keywords"))
        elif action == "bulk_delete":
            ids = request.form.getlist("selected_keywords")
            if not ids:
                flash("Select at least one.", "warning")
            else:
                try:
                    ph = ",".join(["%s"] * len(ids))
                    cursor.execute(f"DELETE FROM keywords WHERE id IN ({ph})", tuple(ids))
                    db.commit()
                    flash("Deleted.", "success")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_keywords"))
    cursor.execute("SELECT id,sr_no,keyword FROM keywords ORDER BY sr_no ASC")
    keywords = cursor.fetchall()
    cursor.close()
    return render_template("main/view_keywords.html", keywords=keywords)


# ── Websites ───────────────────────────────────────────────────
@main_bp.route("/view-websites", methods=["GET", "POST"])
@login_required
def view_websites():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            sr_no = request.form.get("sr_no", "").strip()
            val   = request.form.get("websites", "").strip()
            if not sr_no or not val:
                flash("Both fields required.", "danger")
            else:
                try:
                    sr_no = int(sr_no)
                    cursor.execute("SELECT id FROM websites WHERE sr_no=%s", (sr_no,))
                    if cursor.fetchone():
                        flash("Sr. No exists.", "danger")
                    else:
                        cursor.execute(
                            "INSERT INTO websites(sr_no,websites) VALUES(%s,%s)",
                            (sr_no, val)
                        )
                        db.commit()
                        flash("Website added.", "success")
                except ValueError:
                    flash("Sr. No must be a number.", "danger")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_websites"))
        elif action == "edit":
            wid = request.form.get("websites_id", "").strip()
            val = request.form.get("edit_websites", "").strip()
            if not wid or not val:
                flash("Both fields required.", "danger")
            else:
                try:
                    cursor.execute("UPDATE websites SET websites=%s WHERE id=%s", (val, wid))
                    db.commit()
                    flash("Updated." if cursor.rowcount else "Not found.",
                          "success" if cursor.rowcount else "warning")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_websites"))
        elif action == "bulk_delete":
            ids = request.form.getlist("selected_websites")
            if not ids:
                flash("Select at least one.", "warning")
            else:
                try:
                    ph = ",".join(["%s"] * len(ids))
                    cursor.execute(f"DELETE FROM websites WHERE id IN ({ph})", tuple(ids))
                    db.commit()
                    flash("Deleted.", "success")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_websites"))
    cursor.execute("SELECT id,sr_no,websites FROM websites ORDER BY sr_no ASC")
    websites = cursor.fetchall()
    cursor.close()
    return render_template("main/view_websites.html", websites=websites)


# ── News Type ──────────────────────────────────────────────────
@main_bp.route("/view-news-type", methods=["GET", "POST"])
@login_required
def view_news_type():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            sr_no = request.form.get("sr_no", "").strip()
            val   = request.form.get("news_type", "").strip()
            if not sr_no or not val:
                flash("Both fields required.", "danger")
            else:
                try:
                    sr_no = int(sr_no)
                    cursor.execute("SELECT id FROM news WHERE sr_no=%s", (sr_no,))
                    if cursor.fetchone():
                        flash("Sr. No exists.", "danger")
                    else:
                        cursor.execute(
                            "INSERT INTO news(sr_no,news_type) VALUES(%s,%s)",
                            (sr_no, val)
                        )
                        db.commit()
                        flash("News type added.", "success")
                except ValueError:
                    flash("Sr. No must be a number.", "danger")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_news_type"))
        elif action == "edit":
            ntid = request.form.get("news_type_id", "").strip()
            val  = request.form.get("edit_news_type", "").strip()
            if not ntid or not val:
                flash("Both fields required.", "danger")
            else:
                try:
                    cursor.execute("UPDATE news SET news_type=%s WHERE id=%s", (val, ntid))
                    db.commit()
                    flash("Updated." if cursor.rowcount else "Not found.",
                          "success" if cursor.rowcount else "warning")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_news_type"))
        elif action == "bulk_delete":
            ids = request.form.getlist("selected_news_types")
            if not ids:
                flash("Select at least one.", "warning")
            else:
                try:
                    ph = ",".join(["%s"] * len(ids))
                    cursor.execute(f"DELETE FROM news WHERE id IN ({ph})", tuple(ids))
                    db.commit()
                    flash("Deleted.", "success")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_news_type"))
    cursor.execute("SELECT id,sr_no,news_type FROM news ORDER BY sr_no ASC")
    news_types = cursor.fetchall()
    cursor.close()
    return render_template("main/view_news_type.html", news_types=news_types)


# ── Commodity ──────────────────────────────────────────────────
@main_bp.route("/view-commodity", methods=["GET", "POST"])
@login_required
def view_commodity():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            sr_no = request.form.get("sr_no", "").strip()
            val   = request.form.get("commodity", "").strip()
            if not sr_no or not val:
                flash("Both fields required.", "danger")
            else:
                try:
                    sr_no = int(sr_no)
                    cursor.execute("SELECT id FROM commodity WHERE sr_no=%s", (sr_no,))
                    if cursor.fetchone():
                        flash("Sr. No exists.", "danger")
                    else:
                        cursor.execute(
                            "INSERT INTO commodity(sr_no,commodity) VALUES(%s,%s)",
                            (sr_no, val)
                        )
                        db.commit()
                        flash("Commodity added.", "success")
                except ValueError:
                    flash("Sr. No must be a number.", "danger")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_commodity"))
        elif action == "edit":
            cid = request.form.get("commodity_id", "").strip()
            val = request.form.get("edit_commodity", "").strip()
            if not cid or not val:
                flash("Both fields required.", "danger")
            else:
                try:
                    cursor.execute("UPDATE commodity SET commodity=%s WHERE id=%s", (val, cid))
                    db.commit()
                    flash("Updated." if cursor.rowcount else "Not found.",
                          "success" if cursor.rowcount else "warning")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_commodity"))
        elif action == "bulk_delete":
            ids = request.form.getlist("selected_commodities")
            if not ids:
                flash("Select at least one.", "warning")
            else:
                try:
                    ph = ",".join(["%s"] * len(ids))
                    cursor.execute(f"DELETE FROM commodity WHERE id IN ({ph})", tuple(ids))
                    db.commit()
                    flash("Deleted.", "success")
                except Exception as e:
                    db.rollback()
                    flash(f"Error: {e}", "danger")
            cursor.close()
            return redirect(url_for("main.view_commodity"))
    cursor.execute("SELECT id,sr_no,commodity FROM commodity ORDER BY sr_no ASC")
    commodities = cursor.fetchall()
    cursor.close()
    return render_template("main/view_commodity.html", commodities=commodities)


# ════════════════════════════════════════════════════════════════
#  ALL NON-PUBLISHED NEWS
# ════════════════════════════════════════════════════════════════

@main_bp.route("/all-non-published-news")
@login_required
def all_non_published_news():
    db = get_db()
    cursor = db.cursor(dictionary=True)

    try:
        cursor.execute("SELECT * FROM user_settings WHERE id=1")
        settings = cursor.fetchone() or {}
    except Exception:
        settings = {}

    cats_str = settings.get("content_categories", "all") or "all"
    cats     = [c.strip().lower() for c in cats_str.split(",") if c.strip()]
    page     = max(1, int(request.args.get("page", 1)))

    try:
        if "all" in cats:
            cursor.execute(
                "SELECT COUNT(*) AS cnt FROM non_published_news WHERE published=0"
            )
        else:
            ph = ",".join(["%s"] * len(cats))
            cursor.execute(
                f"SELECT COUNT(*) AS cnt FROM non_published_news "
                f"WHERE published=0 AND LOWER(news_type) IN ({ph})",
                tuple(cats)
            )
        total = cursor.fetchone()["cnt"]
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page   = min(page, total_pages)
        offset = (page - 1) * PER_PAGE

        if "all" in cats:
            cursor.execute(
                "SELECT id,news_date,news_type,news_headline,"
                "news_text,news_url,keywords,date_of_insert "
                "FROM non_published_news WHERE published=0 "
                "ORDER BY date_of_insert DESC LIMIT %s OFFSET %s",
                (PER_PAGE, offset)
            )
        else:
            ph = ",".join(["%s"] * len(cats))
            cursor.execute(
                f"SELECT id,news_date,news_type,news_headline,"
                f"news_text,news_url,keywords,date_of_insert "
                f"FROM non_published_news WHERE published=0 "
                f"AND LOWER(news_type) IN ({ph}) "
                f"ORDER BY date_of_insert DESC LIMIT %s OFFSET %s",
                tuple(cats) + (PER_PAGE, offset)
            )
        news = cursor.fetchall()

    except Exception as e:
        flash(f"Error loading news: {e}", "danger")
        news, total, total_pages, page = [], 0, 1, 1
    finally:
        cursor.close()

    with _scraper_lock:
        scrape_running = _scraper_state["running"]

    return render_template(
        "main/all_non_published_news.html",
        news=news,
        scrape_running=scrape_running,
        page=page,
        total_pages=total_pages,
        total=total,
        per_page=PER_PAGE,
    )


# ════════════════════════════════════════════════════════════════
#  ALL NON-PUBLISHED NEWS — POST ACTIONS
# ════════════════════════════════════════════════════════════════

@main_bp.route("/all-non-published-news/actions", methods=["POST"])
@login_required
def all_non_published_news_actions():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    action = request.form.get("action")
    ids    = request.form.getlist("selected_news")

    if not ids:
        flash("Please select at least one article.", "warning")
        cursor.close()
        return redirect(url_for("main.all_non_published_news"))

    ph = ",".join(["%s"] * len(ids))

    try:
        if action == "delete":
            cursor.execute(
                f"DELETE FROM non_published_news WHERE id IN ({ph})",
                tuple(ids)
            )
            db.commit()
            flash(f"{cursor.rowcount} article(s) deleted.", "success")

        elif action == "publish":
            cursor.execute(
                f"SELECT id, news_date, news_type, news_headline, "
                f"news_text, news_url, keywords, date_of_insert "
                f"FROM non_published_news WHERE id IN ({ph})",
                tuple(ids)
            )
            articles = cursor.fetchall()

            if not articles:
                flash("No articles found for selected IDs.", "warning")
                cursor.close()
                return redirect(url_for("main.all_non_published_news"))

            try:
                cursor.execute("SELECT * FROM user_settings WHERE id=1")
                settings = cursor.fetchone() or {}
            except Exception:
                settings = {}

            pdf_folder = current_app.config.get(
                "PDF_FOLDER",
                os.path.join(current_app.root_path, "static", "pdfs")
            )
            os.makedirs(pdf_folder, exist_ok=True)

            now     = datetime.now()
            pub_ids = []

            # Insert all into published_news immediately
            for art in articles:
                cursor.execute(
                    "INSERT INTO published_news "
                    "(source_id, news_date, news_type, news_headline, "
                    "news_text, news_url, keywords, date_of_insert, published_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        art["id"], art["news_date"], art["news_type"],
                        art["news_headline"], art["news_text"], art["news_url"],
                        art["keywords"], art["date_of_insert"], now,
                    )
                )
                db.commit()
                pub_ids.append(cursor.lastrowid)

            # Delete from non_published_news immediately
            cursor.execute(
                f"DELETE FROM non_published_news WHERE id IN ({ph})",
                tuple(ids)
            )
            db.commit()

            db_cfg       = _get_db_cfg()
            mail_cfg     = _get_mail_cfg()
            template_dir = os.path.join(current_app.root_path, "templates", "main")

            threading.Thread(
                target=_bg_pdf_email,
                args=(db_cfg, mail_cfg, template_dir,
                      list(articles), list(pub_ids), pdf_folder, dict(settings)),
                daemon=True,
            ).start()

            flash(
                f"{len(articles)} article(s) published successfully! "
                f"PDFs are being generated in the background.",
                "success",
            )

    except Exception as e:
        db.rollback()
        flash(f"Error during publish: {e}", "danger")
        traceback.print_exc()
    finally:
        cursor.close()

    return redirect(url_for("main.all_non_published_news"))


# ════════════════════════════════════════════════════════════════
#  REFRESH NEWS  (manual trigger from Non-Published page)
#  ── Behaviour identical to original; now also passes categories
#     and publish_mode so auto-publish works from the button too.
# ════════════════════════════════════════════════════════════════

@main_bp.route("/refresh-news", methods=["POST"])
@login_required
def refresh_news():
    # Block if already running
    with _scraper_lock:
        if _scraper_state["running"]:
            flash("Scraper is already running.", "warning")
            return redirect(url_for("main.all_non_published_news"))

    # Load settings to pick up category selection and publish mode
    db = get_db()
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM user_settings WHERE id=1")
        settings = cursor.fetchone() or {}
    except Exception:
        settings = {}
    finally:
        cursor.close()

    cats_str     = settings.get("content_categories", "all") or "all"
    target_cats  = [c.strip().lower() for c in cats_str.split(",") if c.strip()]
    publish_mode = settings.get("publish_mode", "manual")

    db_cfg        = _get_db_cfg()
    mail_cfg      = _get_mail_cfg()
    instance_path = current_app.instance_path
    pdf_folder    = current_app.config.get(
        "PDF_FOLDER",
        os.path.join(current_app.root_path, "static", "pdfs")
    )
    template_dir  = os.path.join(current_app.root_path, "templates", "main")

    threading.Thread(
        target=_bg_scraper,
        args=(db_cfg, instance_path),
        kwargs=dict(
            target_categories=target_cats,
            publish_mode=publish_mode,
            mail_cfg=mail_cfg,
            template_dir=template_dir,
            pdf_folder=pdf_folder,
            settings=dict(settings),
        ),
        daemon=True,
    ).start()

    flash(
        "News refresh started in the background. "
        "Page will auto-update when complete.",
        "info",
    )
    return redirect(url_for("main.all_non_published_news"))


@main_bp.route("/refresh-news/status")
@login_required
def refresh_news_status():
    with _scraper_lock:
        state = dict(_scraper_state)
    return jsonify(state)


# ════════════════════════════════════════════════════════════════
#  TODAY'S PUBLISHED NEWS
# ════════════════════════════════════════════════════════════════

@main_bp.route("/today-published-news")
@login_required
def today_published_news():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    today = date.today()
    page  = max(1, int(request.args.get("page", 1)))

    try:
        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM published_news WHERE DATE(published_at)=%s",
            (today,)
        )
        total = cursor.fetchone()["cnt"]
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page   = min(page, total_pages)
        offset = (page - 1) * PER_PAGE

        cursor.execute(
            "SELECT id, news_date, news_type, news_headline, "
            "news_url, pdf_path, published_at "
            "FROM published_news WHERE DATE(published_at)=%s "
            "ORDER BY published_at DESC LIMIT %s OFFSET %s",
            (today, PER_PAGE, offset)
        )
        news = cursor.fetchall()
    except Exception as e:
        flash(f"Error loading published news: {e}", "danger")
        news, total, total_pages, page = [], 0, 1, 1
    finally:
        cursor.close()

    return render_template(
        "main/today_published_news.html",
        news=news, today=today,
        page=page, total_pages=total_pages,
        total=total, per_page=PER_PAGE,
    )


@main_bp.route("/check-pdf/<int:news_id>")
@login_required
def check_pdf_ready(news_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("SELECT pdf_path FROM published_news WHERE id=%s", (news_id,))
        row = cursor.fetchone()
    finally:
        cursor.close()

    ready = False
    if row and row.get("pdf_path"):
        pdf_folder = current_app.config.get(
            "PDF_FOLDER",
            os.path.join(current_app.root_path, "static", "pdfs")
        )
        fp    = os.path.join(pdf_folder, row["pdf_path"])
        ready = os.path.exists(fp) and os.path.getsize(fp) > 500

    return jsonify({"ready": ready})


@main_bp.route("/download-pdf/<int:news_id>")
@login_required
def download_pdf(news_id):
    db = get_db()
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("SELECT pdf_path FROM published_news WHERE id=%s", (news_id,))
        row = cursor.fetchone()
    finally:
        cursor.close()

    if not row or not row["pdf_path"]:
        flash("PDF is still being generated — please try again in a moment.", "warning")
        return redirect(url_for("main.today_published_news"))

    pdf_folder = current_app.config.get(
        "PDF_FOLDER",
        os.path.join(current_app.root_path, "static", "pdfs")
    )
    return send_from_directory(pdf_folder, row["pdf_path"], as_attachment=True)


@main_bp.route("/send-email-today")
@login_required
def send_email_today():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    today  = date.today()

    try:
        cursor.execute(
            "SELECT id, news_date, news_type, news_headline, news_url, pdf_path "
            "FROM published_news WHERE DATE(published_at)=%s "
            "ORDER BY published_at DESC",
            (today,)
        )
        articles = cursor.fetchall()

        if not articles:
            flash("No published news for today.", "warning")
            cursor.close()
            return redirect(url_for("main.today_published_news"))

        try:
            cursor.execute("SELECT * FROM user_settings WHERE id=1")
            settings = cursor.fetchone() or {}
        except Exception:
            settings = {}

        mail_cfg     = _get_mail_cfg()
        pdf_folder   = current_app.config.get(
            "PDF_FOLDER",
            os.path.join(current_app.root_path, "static", "pdfs")
        )
        template_dir = os.path.join(current_app.root_path, "templates", "main")
        pdf_paths    = [
            os.path.join(pdf_folder, a["pdf_path"])
            for a in articles if a.get("pdf_path")
        ]
        pub_ids = [a["id"] for a in articles]

        _send_email_bg(
            articles=list(articles), pub_ids=pub_ids,
            pdf_paths=pdf_paths, settings=dict(settings),
            mail_cfg=mail_cfg, template_dir=template_dir,
        )
        flash("Email sent successfully.", "success")

    except Exception as e:
        flash(f"Failed to send email: {e}", "danger")
        traceback.print_exc()
    finally:
        cursor.close()

    return redirect(url_for("main.today_published_news"))


# ════════════════════════════════════════════════════════════════
#  USER SETTINGS
# ════════════════════════════════════════════════════════════════

@main_bp.route("/user-settings", methods=["GET", "POST"])
@login_required
def user_settings():
    db = get_db()
    cursor = db.cursor(dictionary=True)

    if request.method == "POST":
        try:
            sched_all          = _parse_sched_from_form(request.form, "all")
            sched_agricultural = _parse_sched_from_form(request.form, "agricultural")
            sched_weather      = _parse_sched_from_form(request.form, "weather")
            sched_financial    = _parse_sched_from_form(request.form, "financial")
            sched_energy       = _parse_sched_from_form(request.form, "energy")
            sched_global       = _parse_sched_from_form(request.form, "global")

            cursor.execute(
                """
                INSERT INTO user_settings
                  (id, publish_mode, content_categories, sync_all_schedules,
                   schedule_all, schedule_agricultural, schedule_weather,
                   schedule_financial, schedule_energy, schedule_global,
                   email_recipient, email_cc, email_subject_prefix, email_on_publish)
                VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  publish_mode=VALUES(publish_mode),
                  content_categories=VALUES(content_categories),
                  sync_all_schedules=VALUES(sync_all_schedules),
                  schedule_all=VALUES(schedule_all),
                  schedule_agricultural=VALUES(schedule_agricultural),
                  schedule_weather=VALUES(schedule_weather),
                  schedule_financial=VALUES(schedule_financial),
                  schedule_energy=VALUES(schedule_energy),
                  schedule_global=VALUES(schedule_global),
                  email_recipient=VALUES(email_recipient),
                  email_cc=VALUES(email_cc),
                  email_subject_prefix=VALUES(email_subject_prefix),
                  email_on_publish=VALUES(email_on_publish)
                """,
                (
                    request.form.get("publish_mode", "manual"),
                    ",".join(request.form.getlist("content_categories")) or "all",
                    1 if request.form.get("sync_all_schedules") else 0,
                    sched_all, sched_agricultural, sched_weather,
                    sched_financial, sched_energy, sched_global,
                    request.form.get("email_recipient", "").strip(),
                    request.form.get("email_cc", "").strip(),
                    request.form.get("email_subject_prefix",
                                     "Daily News Alert").strip(),
                    1 if request.form.get("email_on_publish") else 0,
                )
            )
            db.commit()
            flash("Settings saved.", "success")
        except Exception as e:
            db.rollback()
            flash(f"Error saving settings: {e}", "danger")
            traceback.print_exc()
        finally:
            cursor.close()
        return redirect(url_for("main.user_settings"))

    # GET — load and deserialise schedule JSON for template
    try:
        cursor.execute("SELECT * FROM user_settings WHERE id=1")
        settings = cursor.fetchone() or {}
    except Exception:
        settings = {}
    finally:
        cursor.close()

    for cat in ("all", "agricultural", "weather", "financial", "energy", "global"):
        col = f"schedule_{cat}"
        raw = (settings.get(col) or "").strip()
        try:
            settings[col] = json.loads(raw) if raw and raw != "{}" else {}
        except Exception:
            settings[col] = {}

    return render_template("main/user_settings.html", settings=settings)


def get_settings():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM user_settings WHERE id=1")
        return cursor.fetchone() or {}
    except Exception:
        return {}
    finally:
        cursor.close()
