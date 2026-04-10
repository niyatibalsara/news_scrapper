"""
main.py
-------
Identical to document-6 version with one change:
  generate_pdf_reportlab() is replaced by generate_pdf_from_url()
  imported from app.pdf_generator (Playwright-based — real page capture).

Threading model (unchanged from doc 6):
  1. SCRAPER  — background thread, receives plain db_config + instance_path
  2. PDF+EMAIL — background thread after publish, uses raw mysql.connector
"""

from .auth import login_required
from app.db import get_db

import os
import smtplib
import threading
import traceback
from datetime import date, datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import mysql.connector
from flask import (Blueprint, current_app, flash, jsonify,
                   redirect, render_template, request,
                   send_from_directory, url_for)

# ── The only change from doc-6: import real-page Playwright PDF generator
from app.pdf_generator import generate_pdf_from_url

main_bp = Blueprint("main", __name__)

_scraper_lock  = threading.Lock()
_scraper_state = {"running": False, "message": "", "success": None}

PER_PAGE = 10


def _raw_conn(db_cfg):
    return mysql.connector.connect(**db_cfg)


def _get_db_cfg():
    cfg = current_app.config
    return {
        "host":     cfg["MYSQL_HOST"],
        "user":     cfg["MYSQL_USER"],
        "password": cfg["MYSQL_PASSWORD"],
        "database": cfg["MYSQL_DB"],
    }


# ================================================================
#  Background: generate PDFs + update DB + send email
# ================================================================

def _bg_pdf_email(db_cfg, mail_cfg, template_dir, articles,
                  pub_ids, pdf_folder, source_ids, settings):
    pdf_paths = []

    for art, pub_id in zip(articles, pub_ids):
        safe_name = f"news_{pub_id}.pdf"
        out_path  = os.path.join(pdf_folder, safe_name)

        # Visit the real article URL and save as PDF (Playwright)
        ok = generate_pdf_from_url(
            url=art.get("news_url", ""),
            output_path=out_path,
        )

        if ok:
            try:
                c = _raw_conn(db_cfg); cur = c.cursor()
                cur.execute(
                    "UPDATE published_news SET pdf_path=%s WHERE id=%s",
                    (safe_name, pub_id)
                )
                c.commit(); cur.close(); c.close()
                print(f"[PDF] DB updated pub_id={pub_id}")
            except Exception as e:
                print(f"[PDF] DB update failed pub_id={pub_id}: {e}")
            pdf_paths.append(out_path)
        else:
            pdf_paths.append(None)

    # Delete source rows
    try:
        c = _raw_conn(db_cfg); cur = c.cursor()
        ph = ",".join(["%s"] * len(source_ids))
        cur.execute(f"DELETE FROM non_published_news WHERE id IN ({ph})", source_ids)
        c.commit(); cur.close(); c.close()
        print(f"[PUBLISH] Deleted {len(source_ids)} source rows.")
    except Exception as e:
        print(f"[PUBLISH] Delete source rows error: {e}")

    # Send email
    if not settings.get("email_on_publish", 1):
        print("[EMAIL] email_on_publish=0 — skipping.")
        return

    try:
        _send_email_background(
            articles=articles, pub_ids=pub_ids, pdf_paths=pdf_paths,
            settings=settings, mail_cfg=mail_cfg, template_dir=template_dir,
        )
    except Exception as e:
        print(f"[EMAIL] Failed: {e}"); traceback.print_exc()

    try:
        c = _raw_conn(db_cfg); cur = c.cursor()
        for pid in pub_ids:
            cur.execute(
                "UPDATE published_news SET email_sent=1, email_sent_at=NOW() WHERE id=%s",
                (pid,)
            )
        c.commit(); cur.close(); c.close()
        print(f"[EMAIL] email_sent marked for {len(pub_ids)} articles.")
    except Exception as e:
        print(f"[EMAIL] Mark sent error: {e}")


# ================================================================
#  Email helpers (no Flask context — safe in any thread)
# ================================================================

def _render_email_template(template_dir, news_items, date_label):
    from jinja2 import Environment, FileSystemLoader
    env  = Environment(loader=FileSystemLoader(template_dir))
    tmpl = env.get_template("email_template.html")
    return tmpl.render(news_items=news_items, date_label=date_label)


def _smtp_send(mail_cfg, msg, recipients):
    with smtplib.SMTP(mail_cfg["MAIL_SERVER"], mail_cfg["MAIL_PORT"]) as s:
        s.ehlo()
        if mail_cfg.get("MAIL_USE_TLS", True):
            s.starttls()
        s.login(mail_cfg["MAIL_USERNAME"], mail_cfg["MAIL_PASSWORD"])
        s.sendmail(msg["From"], recipients, msg.as_string())
        print(f"[EMAIL] Sent to {recipients}")


def _send_email_background(articles, pub_ids, pdf_paths,
                            settings, mail_cfg, template_dir):
    to   = settings.get("email_recipient") or "niyati.b@seamlessautomations.com"
    cc   = [e.strip() for e in (settings.get("email_cc") or "").split(",") if e.strip()]
    pfx  = settings.get("email_subject_prefix") or "Daily News Alert"
    mode = settings.get("publish_mode", "manual")
    date_label = datetime.now().strftime("%d %B %Y")

    if mode == "auto":
        base  = mail_cfg.get("BASE_URL", "http://127.0.0.1:5000")
        items = [dict(a, pdf_view_url=f"{base}/download-pdf/{pid}")
                 for a, pid in zip(articles, pub_ids)]
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
#  Background: scraper
# ================================================================

def _bg_scraper(db_cfg, instance_path):
    global _scraper_state
    with _scraper_lock:
        _scraper_state.update({"running": True, "message": "Scraper started…", "success": None})
    try:
        from app.news_scraper import run_news_scraper
        result = run_news_scraper(db_config=db_cfg, instance_path=instance_path)
        with _scraper_lock:
            _scraper_state.update({
                "success": result.get("success", False),
                "message": result.get("message", "Scraper finished."),
            })
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


@main_bp.route("/home")
@login_required
def home():
    db = get_db(); cursor = db.cursor(dictionary=True)
    today = date.today(); pub = unpub = 0
    try:
        cursor.execute("SELECT COUNT(*) AS cnt FROM published_news WHERE DATE(published_at)=%s", (today,))
        pub = cursor.fetchone()["cnt"]
        cursor.execute("SELECT COUNT(*) AS cnt FROM non_published_news WHERE published=0")
        unpub = cursor.fetchone()["cnt"]
    except Exception as e:
        print(f"[HOME] {e}")
    finally:
        cursor.close()
    return render_template("main/home.html",
                           today_published_count=pub,
                           today_unpublished_count=unpub)


# ── Keywords ──────────────────────────────────────────────────────
@main_bp.route("/view-keywords", methods=["GET", "POST"])
@login_required
def view_keywords():
    db = get_db(); cursor = db.cursor(dictionary=True)
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
                        cursor.execute("INSERT INTO keywords(sr_no,keyword) VALUES(%s,%s)", (sr_no, kw))
                        db.commit(); flash("Keyword added.", "success")
                except ValueError: flash("Sr. No must be a number.", "danger")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_keywords"))
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
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_keywords"))
        elif action == "bulk_delete":
            ids = request.form.getlist("selected_keywords")
            if not ids:
                flash("Select at least one.", "warning")
            else:
                try:
                    ph = ",".join(["%s"] * len(ids))
                    cursor.execute(f"DELETE FROM keywords WHERE id IN ({ph})", tuple(ids))
                    db.commit(); flash("Deleted.", "success")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_keywords"))
    cursor.execute("SELECT id,sr_no,keyword FROM keywords ORDER BY sr_no ASC")
    keywords = cursor.fetchall(); cursor.close()
    return render_template("main/view_keywords.html", keywords=keywords)


# ── Websites ──────────────────────────────────────────────────────
@main_bp.route("/view-websites", methods=["GET", "POST"])
@login_required
def view_websites():
    db = get_db(); cursor = db.cursor(dictionary=True)
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            sr_no = request.form.get("sr_no", "").strip()
            val   = request.form.get("websites", "").strip()
            if not sr_no or not val: flash("Both fields required.", "danger")
            else:
                try:
                    sr_no = int(sr_no)
                    cursor.execute("SELECT id FROM websites WHERE sr_no=%s", (sr_no,))
                    if cursor.fetchone(): flash("Sr. No exists.", "danger")
                    else:
                        cursor.execute("INSERT INTO websites(sr_no,websites) VALUES(%s,%s)", (sr_no, val))
                        db.commit(); flash("Website added.", "success")
                except ValueError: flash("Sr. No must be a number.", "danger")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_websites"))
        elif action == "edit":
            wid = request.form.get("websites_id", "").strip()
            val = request.form.get("edit_websites", "").strip()
            if not wid or not val: flash("Both fields required.", "danger")
            else:
                try:
                    cursor.execute("UPDATE websites SET websites=%s WHERE id=%s", (val, wid))
                    db.commit()
                    flash("Updated." if cursor.rowcount else "Not found.",
                          "success" if cursor.rowcount else "warning")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_websites"))
        elif action == "bulk_delete":
            ids = request.form.getlist("selected_websites")
            if not ids: flash("Select at least one.", "warning")
            else:
                try:
                    ph = ",".join(["%s"] * len(ids))
                    cursor.execute(f"DELETE FROM websites WHERE id IN ({ph})", tuple(ids))
                    db.commit(); flash("Deleted.", "success")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_websites"))
    cursor.execute("SELECT id,sr_no,websites FROM websites ORDER BY sr_no ASC")
    websites = cursor.fetchall(); cursor.close()
    return render_template("main/view_websites.html", websites=websites)


# ── News Type ──────────────────────────────────────────────────────
@main_bp.route("/view-news-type", methods=["GET", "POST"])
@login_required
def view_news_type():
    db = get_db(); cursor = db.cursor(dictionary=True)
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            sr_no = request.form.get("sr_no", "").strip()
            val   = request.form.get("news_type", "").strip()
            if not sr_no or not val: flash("Both fields required.", "danger")
            else:
                try:
                    sr_no = int(sr_no)
                    cursor.execute("SELECT id FROM news WHERE sr_no=%s", (sr_no,))
                    if cursor.fetchone(): flash("Sr. No exists.", "danger")
                    else:
                        cursor.execute("INSERT INTO news(sr_no,news_type) VALUES(%s,%s)", (sr_no, val))
                        db.commit(); flash("News type added.", "success")
                except ValueError: flash("Sr. No must be a number.", "danger")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_news_type"))
        elif action == "edit":
            ntid = request.form.get("news_type_id", "").strip()
            val  = request.form.get("edit_news_type", "").strip()
            if not ntid or not val: flash("Both fields required.", "danger")
            else:
                try:
                    cursor.execute("UPDATE news SET news_type=%s WHERE id=%s", (val, ntid))
                    db.commit()
                    flash("Updated." if cursor.rowcount else "Not found.",
                          "success" if cursor.rowcount else "warning")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_news_type"))
        elif action == "bulk_delete":
            ids = request.form.getlist("selected_news_types")
            if not ids: flash("Select at least one.", "warning")
            else:
                try:
                    ph = ",".join(["%s"] * len(ids))
                    cursor.execute(f"DELETE FROM news WHERE id IN ({ph})", tuple(ids))
                    db.commit(); flash("Deleted.", "success")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_news_type"))
    cursor.execute("SELECT id,sr_no,news_type FROM news ORDER BY sr_no ASC")
    news_types = cursor.fetchall(); cursor.close()
    return render_template("main/view_news_type.html", news_types=news_types)


# ── Commodity ──────────────────────────────────────────────────────
@main_bp.route("/view-commodity", methods=["GET", "POST"])
@login_required
def view_commodity():
    db = get_db(); cursor = db.cursor(dictionary=True)
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            sr_no = request.form.get("sr_no", "").strip()
            val   = request.form.get("commodity", "").strip()
            if not sr_no or not val: flash("Both fields required.", "danger")
            else:
                try:
                    sr_no = int(sr_no)
                    cursor.execute("SELECT id FROM commodity WHERE sr_no=%s", (sr_no,))
                    if cursor.fetchone(): flash("Sr. No exists.", "danger")
                    else:
                        cursor.execute("INSERT INTO commodity(sr_no,commodity) VALUES(%s,%s)", (sr_no, val))
                        db.commit(); flash("Commodity added.", "success")
                except ValueError: flash("Sr. No must be a number.", "danger")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_commodity"))
        elif action == "edit":
            cid = request.form.get("commodity_id", "").strip()
            val = request.form.get("edit_commodity", "").strip()
            if not cid or not val: flash("Both fields required.", "danger")
            else:
                try:
                    cursor.execute("UPDATE commodity SET commodity=%s WHERE id=%s", (val, cid))
                    db.commit()
                    flash("Updated." if cursor.rowcount else "Not found.",
                          "success" if cursor.rowcount else "warning")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_commodity"))
        elif action == "bulk_delete":
            ids = request.form.getlist("selected_commodities")
            if not ids: flash("Select at least one.", "warning")
            else:
                try:
                    ph = ",".join(["%s"] * len(ids))
                    cursor.execute(f"DELETE FROM commodity WHERE id IN ({ph})", tuple(ids))
                    db.commit(); flash("Deleted.", "success")
                except Exception as e: db.rollback(); flash(f"Error: {e}", "danger")
            cursor.close(); return redirect(url_for("main.view_commodity"))
    cursor.execute("SELECT id,sr_no,commodity FROM commodity ORDER BY sr_no ASC")
    commodities = cursor.fetchall(); cursor.close()
    return render_template("main/view_commodity.html", commodities=commodities)


# ================================================================
#  All Non-Published News — GET (paginated)
# ================================================================

@main_bp.route("/all-non-published-news")
@login_required
def all_non_published_news():
    db = get_db(); cursor = db.cursor(dictionary=True)
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
            cursor.execute("SELECT COUNT(*) AS cnt FROM non_published_news WHERE published=0")
        else:
            ph = ",".join(["%s"] * len(cats))
            cursor.execute(
                f"SELECT COUNT(*) AS cnt FROM non_published_news "
                f"WHERE published=0 AND LOWER(news_type) IN ({ph})", tuple(cats))
        total = cursor.fetchone()["cnt"]
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page  = min(page, total_pages)
        offset = (page - 1) * PER_PAGE

        if "all" in cats:
            cursor.execute(
                "SELECT id,news_date,news_type,news_headline,"
                "news_text,news_url,keywords,date_of_insert "
                "FROM non_published_news WHERE published=0 "
                "ORDER BY date_of_insert DESC LIMIT %s OFFSET %s",
                (PER_PAGE, offset))
        else:
            ph = ",".join(["%s"] * len(cats))
            cursor.execute(
                f"SELECT id,news_date,news_type,news_headline,"
                f"news_text,news_url,keywords,date_of_insert "
                f"FROM non_published_news WHERE published=0 "
                f"AND LOWER(news_type) IN ({ph}) "
                f"ORDER BY date_of_insert DESC LIMIT %s OFFSET %s",
                tuple(cats) + (PER_PAGE, offset))
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
        news=news, scrape_running=scrape_running,
        page=page, total_pages=total_pages,
        total=total, per_page=PER_PAGE,
    )


# ================================================================
#  All Non-Published News — POST (publish / delete)
# ================================================================

@main_bp.route("/all-non-published-news/actions", methods=["POST"])
@login_required
def all_non_published_news_actions():
    db = get_db(); cursor = db.cursor(dictionary=True)
    action = request.form.get("action")
    ids    = request.form.getlist("selected_news")

    if not ids:
        flash("Please select at least one article.", "warning")
        cursor.close(); return redirect(url_for("main.all_non_published_news"))

    ph = ",".join(["%s"] * len(ids))

    try:
        if action == "delete":
            cursor.execute(f"DELETE FROM non_published_news WHERE id IN ({ph})", tuple(ids))
            db.commit(); flash(f"{cursor.rowcount} article(s) deleted.", "success")

        elif action == "publish":
            cursor.execute(
                f"SELECT id,news_date,news_type,news_headline,"
                f"news_text,news_url,keywords,date_of_insert "
                f"FROM non_published_news WHERE id IN ({ph})", tuple(ids))
            articles = cursor.fetchall()

            if not articles:
                flash("No articles found for selected IDs.", "warning")
                cursor.close(); return redirect(url_for("main.all_non_published_news"))

            try:
                cursor.execute("SELECT * FROM user_settings WHERE id=1")
                settings = cursor.fetchone() or {}
            except Exception:
                settings = {}

            pdf_folder = current_app.config.get(
                "PDF_FOLDER",
                os.path.join(current_app.root_path, "static", "pdfs"))
            os.makedirs(pdf_folder, exist_ok=True)

            now = datetime.now(); pub_ids = []

            # Insert into published_news IMMEDIATELY
            for art in articles:
                cursor.execute(
                    "INSERT INTO published_news "
                    "(source_id,news_date,news_type,news_headline,"
                    "news_text,news_url,keywords,date_of_insert,published_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (art["id"], art["news_date"], art["news_type"],
                     art["news_headline"], art["news_text"],
                     art["news_url"], art["keywords"],
                     art["date_of_insert"], now))
                db.commit()
                pub_ids.append(cursor.lastrowid)

            # Collect everything needed BEFORE leaving request context
            db_cfg = _get_db_cfg()
            cfg    = current_app.config
            mail_cfg = {
                "MAIL_SERVER":   cfg.get("MAIL_SERVER",   "smtp.gmail.com"),
                "MAIL_PORT":     cfg.get("MAIL_PORT",     587),
                "MAIL_USE_TLS":  cfg.get("MAIL_USE_TLS",  True),
                "MAIL_USERNAME": cfg.get("MAIL_USERNAME", ""),
                "MAIL_PASSWORD": cfg.get("MAIL_PASSWORD", ""),
                "MAIL_FROM":     cfg.get("MAIL_FROM",     cfg.get("MAIL_USERNAME", "")),
                "BASE_URL":      cfg.get("BASE_URL",      "http://127.0.0.1:5000"),
            }
            template_dir = os.path.join(current_app.root_path, "templates", "main")

            threading.Thread(
                target=_bg_pdf_email,
                args=(db_cfg, mail_cfg, template_dir,
                      list(articles), pub_ids, pdf_folder,
                      tuple(ids), dict(settings)),
                daemon=True,
            ).start()

            flash(
                f"{len(articles)} article(s) published and now visible in "
                f"Today's Published News. PDFs are being generated from the "
                f"original news websites — email will follow automatically.",
                "success",
            )

    except Exception as e:
        db.rollback(); flash(f"Error: {e}", "danger"); traceback.print_exc()
    finally:
        cursor.close()

    return redirect(url_for("main.all_non_published_news"))


# ================================================================
#  Refresh News (scraper)
# ================================================================

@main_bp.route("/refresh-news", methods=["POST"])
@login_required
def refresh_news():
    with _scraper_lock:
        if _scraper_state["running"]:
            flash("Scraper is already running.", "warning")
            return redirect(url_for("main.all_non_published_news"))

    db_cfg        = _get_db_cfg()
    instance_path = current_app.instance_path

    threading.Thread(target=_bg_scraper, args=(db_cfg, instance_path),
                     daemon=True).start()

    flash("News refresh started in the background. "
          "Page will auto-update when complete.", "info")
    return redirect(url_for("main.all_non_published_news"))


@main_bp.route("/refresh-news/status")
@login_required
def refresh_news_status():
    with _scraper_lock:
        state = dict(_scraper_state)
    return jsonify(state)


# ================================================================
#  Today's Published News — paginated
# ================================================================

@main_bp.route("/today-published-news")
@login_required
def today_published_news():
    db = get_db(); cursor = db.cursor(dictionary=True)
    today = date.today(); page = max(1, int(request.args.get("page", 1)))

    try:
        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM published_news WHERE DATE(published_at)=%s", (today,))
        total = cursor.fetchone()["cnt"]
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        page  = min(page, total_pages)
        offset = (page - 1) * PER_PAGE

        cursor.execute(
            "SELECT id,news_date,news_type,news_headline,"
            "news_url,pdf_path,published_at "
            "FROM published_news WHERE DATE(published_at)=%s "
            "ORDER BY published_at DESC LIMIT %s OFFSET %s",
            (today, PER_PAGE, offset))
        news = cursor.fetchall()
    except Exception as e:
        flash(f"Error: {e}", "danger"); news, total, total_pages, page = [], 0, 1, 1
    finally:
        cursor.close()

    return render_template(
        "main/today_published_news.html",
        news=news, today=today,
        page=page, total_pages=total_pages,
        total=total, per_page=PER_PAGE,
    )


# ================================================================
#  Check PDF ready — polled by JS on Today's Published News
# ================================================================

@main_bp.route("/check-pdf/<int:news_id>")
@login_required
def check_pdf_ready(news_id):
    db = get_db(); cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("SELECT pdf_path FROM published_news WHERE id=%s", (news_id,))
        row = cursor.fetchone()
    finally:
        cursor.close()

    ready = False
    if row and row.get("pdf_path"):
        pdf_folder = current_app.config.get(
            "PDF_FOLDER", os.path.join(current_app.root_path, "static", "pdfs"))
        fp = os.path.join(pdf_folder, row["pdf_path"])
        ready = os.path.exists(fp) and os.path.getsize(fp) > 500

    return jsonify({"ready": ready})


# ================================================================
#  Download PDF
# ================================================================

@main_bp.route("/download-pdf/<int:news_id>")
@login_required
def download_pdf(news_id):
    db = get_db(); cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("SELECT pdf_path FROM published_news WHERE id=%s", (news_id,))
        row = cursor.fetchone()
    finally:
        cursor.close()

    if not row or not row["pdf_path"]:
        flash("PDF is still being generated — please try again in a moment.", "warning")
        return redirect(url_for("main.today_published_news"))

    pdf_folder = current_app.config.get(
        "PDF_FOLDER", os.path.join(current_app.root_path, "static", "pdfs"))
    return send_from_directory(pdf_folder, row["pdf_path"], as_attachment=True)


# ================================================================
#  Send email for today (manual resend)
# ================================================================

@main_bp.route("/send-email-today")
@login_required
def send_email_today():
    db = get_db(); cursor = db.cursor(dictionary=True); today = date.today()
    try:
        cursor.execute(
            "SELECT id,news_date,news_type,news_headline,news_url,pdf_path "
            "FROM published_news WHERE DATE(published_at)=%s ORDER BY published_at DESC",
            (today,))
        articles = cursor.fetchall()

        if not articles:
            flash("No published news for today.", "warning")
            cursor.close(); return redirect(url_for("main.today_published_news"))

        try:
            cursor.execute("SELECT * FROM user_settings WHERE id=1")
            settings = cursor.fetchone() or {}
        except Exception:
            settings = {}

        cfg = current_app.config
        mail_cfg = {
            "MAIL_SERVER":   cfg.get("MAIL_SERVER",   "smtp.gmail.com"),
            "MAIL_PORT":     cfg.get("MAIL_PORT",     587),
            "MAIL_USE_TLS":  cfg.get("MAIL_USE_TLS",  True),
            "MAIL_USERNAME": cfg.get("MAIL_USERNAME", ""),
            "MAIL_PASSWORD": cfg.get("MAIL_PASSWORD", ""),
            "MAIL_FROM":     cfg.get("MAIL_FROM",     cfg.get("MAIL_USERNAME", "")),
            "BASE_URL":      cfg.get("BASE_URL",      "http://127.0.0.1:5000"),
        }
        pdf_folder   = cfg.get("PDF_FOLDER", os.path.join(current_app.root_path, "static", "pdfs"))
        template_dir = os.path.join(current_app.root_path, "templates", "main")
        pdf_paths    = [os.path.join(pdf_folder, a["pdf_path"])
                        for a in articles if a.get("pdf_path")]
        pub_ids      = [a["id"] for a in articles]

        _send_email_background(
            articles=list(articles), pub_ids=pub_ids, pdf_paths=pdf_paths,
            settings=dict(settings), mail_cfg=mail_cfg, template_dir=template_dir)
        flash("Email sent successfully.", "success")

    except Exception as e:
        flash(f"Failed to send email: {e}", "danger"); traceback.print_exc()
    finally:
        cursor.close()

    return redirect(url_for("main.today_published_news"))


# ================================================================
#  User Settings
# ================================================================

@main_bp.route("/user-settings", methods=["GET", "POST"])
@login_required
def user_settings():
    db = get_db(); cursor = db.cursor(dictionary=True)
    if request.method == "POST":
        try:
            cursor.execute(
                "INSERT INTO user_settings "
                "(id,run_frequency,custom_frequency,scraper_enabled,"
                "publish_mode,content_categories,email_recipient,"
                "email_cc,email_subject_prefix,email_on_publish) "
                "VALUES (1,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE "
                "run_frequency=VALUES(run_frequency),"
                "custom_frequency=VALUES(custom_frequency),"
                "scraper_enabled=VALUES(scraper_enabled),"
                "publish_mode=VALUES(publish_mode),"
                "content_categories=VALUES(content_categories),"
                "email_recipient=VALUES(email_recipient),"
                "email_cc=VALUES(email_cc),"
                "email_subject_prefix=VALUES(email_subject_prefix),"
                "email_on_publish=VALUES(email_on_publish)",
                (request.form.get("run_frequency", "1"),
                 request.form.get("custom_frequency") or None,
                 1 if request.form.get("scraper_enabled") else 0,
                 request.form.get("publish_mode", "manual"),
                 ",".join(request.form.getlist("content_categories")) or "all",
                 request.form.get("email_recipient", "").strip(),
                 request.form.get("email_cc", "").strip(),
                 request.form.get("email_subject_prefix", "Daily News Alert").strip(),
                 1 if request.form.get("email_on_publish") else 0))
            db.commit(); flash("Settings saved.", "success")
        except Exception as e:
            db.rollback(); flash(f"Error: {e}", "danger")
        finally:
            cursor.close()
        return redirect(url_for("main.user_settings"))

    try:
        cursor.execute("SELECT * FROM user_settings WHERE id=1")
        settings = cursor.fetchone() or {}
    except Exception:
        settings = {}
    finally:
        cursor.close()
    return render_template("main/user_settings.html", settings=settings)


def get_settings():
    db = get_db(); cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM user_settings WHERE id=1")
        return cursor.fetchone() or {}
    except Exception:
        return {}
    finally:
        cursor.close()
