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


def _get_db_cfg() -> dict:
    cfg = current_app.config
    return {
        "host":     cfg["MYSQL_HOST"],
        "user":     cfg["MYSQL_USER"],
        "password": cfg["MYSQL_PASSWORD"],
        "database": cfg["MYSQL_DB"],
    }


def _get_mail_cfg() -> dict:
    cfg = current_app.config
    return {
        "MAIL_SERVER":   cfg.get("MAIL_SERVER",   "smtp.gmail.com"),
        "MAIL_PORT":     cfg.get("MAIL_PORT",     587),
        "MAIL_USE_TLS":  cfg.get("MAIL_USE_TLS",  True),
        "MAIL_USERNAME": cfg.get("MAIL_USERNAME", ""),
        "MAIL_PASSWORD": cfg.get("MAIL_PASSWORD", ""),
        "MAIL_FROM":     cfg.get("MAIL_FROM",     cfg.get("MAIL_USERNAME", "")),
        "BASE_URL":      cfg.get("BASE_URL",      "http://127.0.0.1:5000"),
    }


def _load_settings_raw(db_cfg: dict) -> dict:
    """Load user_settings row id=1 without Flask context."""
    try:
        conn = _raw_conn(db_cfg)
        cur  = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM user_settings WHERE id=1")
        row = cur.fetchone() or {}
        cur.close(); conn.close()
        return row
    except Exception as e:
        print(f"[SETTINGS] Load error: {e}")
        return {}


# ════════════════════════════════════════════════════════════════
#  SCHEDULE HELPERS
# ════════════════════════════════════════════════════════════════

def _parse_schedule(row: dict, cat_key: str) -> dict:
    """
    Read schedule_{cat_key} column (JSON string).
    Returns parsed dict or {} if not set / invalid.
    """
    col = f"schedule_{cat_key}"
    raw = (row.get(col) or "").strip()
    if not raw or raw == "{}":
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _is_due(sched: dict, last_run_str: str) -> bool:
    """
    Return True if the schedule has elapsed since last_run.

    sched fields:
      type           : 'interval' | 'daily' | 'weekly'
      interval_hours : numeric string (used for both hours AND minutes runs)
      interval_unit  : 'hours' | 'minutes'
      time_hhmm      : 'HH:MM'
      weekly_day     : 'monday' … 'sunday'
    """
    if not sched:
        return False

    now   = datetime.now()
    stype = sched.get("type", "")

    # ── Interval ──────────────────────────────────────────────
    if stype == "interval":
        n    = int(sched.get("interval_hours", 1))   # the numeric value
        unit = sched.get("interval_unit", "hours")
        delta = timedelta(minutes=n) if unit == "minutes" else timedelta(hours=n)

        if not last_run_str:
            return True                               # never run → run now
        try:
            last = datetime.fromisoformat(last_run_str)
            return (now - last) >= delta
        except Exception:
            return True                               # unparseable → run now

    # ── Daily ─────────────────────────────────────────────────
    elif stype == "daily":
        hhmm = sched.get("time_hhmm", "08:00")
        try:
            h, m   = map(int, hhmm.split(":"))
            target = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if now < target:
                return False                          # not yet today
            if not last_run_str:
                return True
            last = datetime.fromisoformat(last_run_str)
            return last.date() < now.date()           # not run today yet
        except Exception:
            return False

    # ── Weekly ────────────────────────────────────────────────
    elif stype == "weekly":
        day_map = {
            "monday": 0, "tuesday": 1, "wednesday": 2,
            "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
        }
        day  = sched.get("weekly_day", "monday")
        hhmm = sched.get("time_hhmm", "08:00")
        try:
            h, m       = map(int, hhmm.split(":"))
            target_dow = day_map.get(day, 0)
            if now.weekday() != target_dow:
                return False                          # wrong day
            target = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if now < target:
                return False                          # not yet this week
            if not last_run_str:
                return True
            last = datetime.fromisoformat(last_run_str)
            # Due if last run was before this week's Monday
            now_monday  = (now  - timedelta(days=now.weekday())).date()
            last_monday = (last - timedelta(days=last.weekday())).date()
            return last_monday < now_monday
        except Exception:
            return False

    return False


def _update_last_run(db_cfg: dict, cat_key: str):
    """Persist NOW() into schedule_last_run_{cat_key}."""
    col = f"schedule_last_run_{cat_key}"
    now_str = datetime.now().isoformat(timespec="seconds")
    try:
        conn = _raw_conn(db_cfg)
        cur  = conn.cursor()
        # ALTER-safe: only update if column exists; ignore error silently
        cur.execute(
            f"UPDATE user_settings SET `{col}`=%s WHERE id=1",
            (now_str,)
        )
        conn.commit(); cur.close(); conn.close()
        print(f"[SCHEDULER] last_run updated: {col} = {now_str}")
    except Exception as e:
        print(f"[SCHEDULER] last_run update failed ({col}): {e}")


def _parse_sched_from_form(form, cat_key: str) -> str:
    """
    Read sched_{cat_key}_* fields from a Flask form.
    Returns a JSON string for storing in schedule_{cat_key} column.
    """
    stype = (form.get(f"sched_{cat_key}_type") or "").strip()
    if not stype:
        return "{}"
    if stype == "interval":
        return json.dumps({
            "type":           "interval",
            "interval_hours": (form.get(f"sched_{cat_key}_interval_hours") or "1").strip(),
            "interval_unit":  (form.get(f"sched_{cat_key}_interval_unit") or "hours").strip(),
        })
    elif stype == "daily":
        return json.dumps({
            "type":      "daily",
            "time_hhmm": (form.get(f"sched_{cat_key}_time") or "08:00").strip(),
        })
    elif stype == "weekly":
        return json.dumps({
            "type":       "weekly",
            "weekly_day": (form.get(f"sched_{cat_key}_weekly_day") or "monday").strip(),
            "time_hhmm":  (form.get(f"sched_{cat_key}_weekly_time") or "08:00").strip(),
        })
    return "{}"


# ════════════════════════════════════════════════════════════════
#  BACKGROUND: PDF GENERATION + EMAIL
#  (identical to original — no changes here)
# ════════════════════════════════════════════════════════════════

def _bg_pdf_email(db_cfg, mail_cfg, template_dir,
                  articles, pub_ids, pdf_folder, settings):
    try:
        from app.pdf_generator import generate_pdf_from_url
    except ImportError as e:
        print(f"[BG] Cannot import pdf_generator: {e}")
        generate_pdf_from_url = None

    pdf_paths = []

    for art, pub_id in zip(articles, pub_ids):
        news_url  = (art.get("news_url") or "").strip()
        safe_name = f"news_{pub_id}.pdf"
        out_path  = os.path.join(pdf_folder, safe_name)
        pdf_ok    = False

        if not news_url:
            print(f"[BG] pub_id={pub_id} has no URL — skipping PDF")
        elif generate_pdf_from_url is None:
            print(f"[BG] pdf_generator not available — skipping PDF for pub_id={pub_id}")
        else:
            try:
                pdf_ok = generate_pdf_from_url(url=news_url, output_path=out_path)
            except Exception as e:
                print(f"[BG] PDF exception pub_id={pub_id}: {e}")
                traceback.print_exc()

        if pdf_ok:
            try:
                c = _raw_conn(db_cfg)
                cur = c.cursor()
                cur.execute(
                    "UPDATE published_news SET pdf_path=%s WHERE id=%s",
                    (safe_name, pub_id)
                )
                c.commit(); cur.close(); c.close()
                print(f"[BG] PDF saved: {safe_name}")
            except Exception as e:
                print(f"[BG] DB update for pdf_path failed pub_id={pub_id}: {e}")
            pdf_paths.append(out_path)
        else:
            print(f"[BG] PDF failed pub_id={pub_id}")
            pdf_paths.append(None)

    if not settings.get("email_on_publish", 1):
        print("[BG] email_on_publish=0 — skipping email.")
        return

    try:
        _send_email_bg(
            articles=articles, pub_ids=pub_ids, pdf_paths=pdf_paths,
            settings=settings, mail_cfg=mail_cfg, template_dir=template_dir,
        )
    except Exception as e:
        print(f"[BG] Email send failed: {e}")
        traceback.print_exc()
        return

    try:
        c = _raw_conn(db_cfg)
        cur = c.cursor()
        for pid in pub_ids:
            cur.execute(
                "UPDATE published_news SET email_sent=1, email_sent_at=NOW() WHERE id=%s",
                (pid,)
            )
        c.commit(); cur.close(); c.close()
        print(f"[BG] email_sent marked for {len(pub_ids)} articles.")
    except Exception as e:
        print(f"[BG] Mark email_sent error: {e}")


# ════════════════════════════════════════════════════════════════
#  BACKGROUND: SCRAPER
# ════════════════════════════════════════════════════════════════

def _bg_scraper(db_cfg: dict, instance_path: str,
                target_categories: list = None,
                publish_mode: str = "manual",
                mail_cfg: dict = None,
                template_dir: str = None,
                pdf_folder: str = None,
                settings: dict = None):
    """
    Runs in a daemon thread. Calls run_news_scraper with the given
    category filter and publish mode.

    If publish_mode='auto', newly inserted published_news rows are passed
    to _bg_pdf_email in a sub-thread.
    """
    with _scraper_lock:
        _scraper_state.update({
            "running": True,
            "message": (
                f"Scraper running… "
                f"categories={target_categories} mode={publish_mode}"
            ),
            "success": None,
        })
    try:
        from app.news_scraper import run_news_scraper
        result = run_news_scraper(
            db_config=db_cfg,
            instance_path=instance_path,
            target_categories=target_categories or ["all"],
            publish_mode=publish_mode,
        )

        # Auto mode: trigger PDF+email for newly published articles
        if publish_mode == "auto":
            pub_ids  = result.get("auto_pub_ids",  [])
            raw_arts = result.get("auto_articles", [])
            if pub_ids and mail_cfg and template_dir and pdf_folder and settings:
                try:
                    conn = _raw_conn(db_cfg)
                    cur  = conn.cursor(dictionary=True)
                    ph   = ",".join(["%s"] * len(pub_ids))
                    cur.execute(
                        f"SELECT * FROM published_news WHERE id IN ({ph})",
                        tuple(pub_ids)
                    )
                    pub_articles = cur.fetchall()
                    cur.close(); conn.close()
                except Exception as e:
                    print(f"[BG-AUTO] fetch pub articles error: {e}")
                    pub_articles = raw_arts

                os.makedirs(pdf_folder, exist_ok=True)
                threading.Thread(
                    target=_bg_pdf_email,
                    args=(db_cfg, mail_cfg, template_dir,
                          pub_articles, pub_ids, pdf_folder, settings),
                    daemon=True,
                ).start()
                print(f"[BG-AUTO] {len(pub_ids)} articles published; PDF+email queued.")

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


# ════════════════════════════════════════════════════════════════
#  SCHEDULER
# ════════════════════════════════════════════════════════════════

def _scheduler_tick(app):
    """
    Called every SCHEDULER_TICK_SECONDS.
    Reads settings, checks which categories are due, fires _bg_scraper.
    Pushes its own app context — safe to call from a daemon thread.
    """
    with app.app_context():
        try:
            db_cfg       = _get_db_cfg()
            settings     = _load_settings_raw(db_cfg)
            publish_mode = settings.get("publish_mode", "manual")
            cats_str     = settings.get("content_categories", "all") or "all"
            active_cats  = [c.strip().lower() for c in cats_str.split(",") if c.strip()]
            sync_all     = bool(settings.get("sync_all_schedules", 0))

            due_keys:   list = []   # schedule keys that fired
            merged_cats: list = []  # categories to scrape this tick

            if sync_all:
                # ── Global mode: one schedule for all active categories ──
                sched    = _parse_schedule(settings, "global")
                last_run = settings.get("schedule_last_run_global") or ""
                if _is_due(sched, last_run):
                    due_keys.append("global")
                    merged_cats = active_cats if active_cats else ["all"]
            else:
                # ── Per-category mode ────────────────────────────────────
                for cat in active_cats:
                    sched    = _parse_schedule(settings, cat)
                    last_run = settings.get(f"schedule_last_run_{cat}") or ""
                    if _is_due(sched, last_run):
                        due_keys.append(cat)
                        if cat not in merged_cats:
                            merged_cats.append(cat)

            if not due_keys:
                return

            # Don't double-fire if scraper is still running
            with _scraper_lock:
                if _scraper_state["running"]:
                    print("[SCHEDULER] Tick skipped — scraper already running.")
                    return

            # Mark last_run BEFORE firing (prevents double-trigger on slow scrapes)
            for key in due_keys:
                _update_last_run(db_cfg, key)

            mail_cfg     = _get_mail_cfg()
            instance_path = app.instance_path
            pdf_folder    = app.config.get(
                "PDF_FOLDER",
                os.path.join(app.root_path, "static", "pdfs")
            )
            template_dir  = os.path.join(app.root_path, "templates", "main")

            print(
                f"[SCHEDULER] Firing scraper — "
                f"cats={merged_cats} mode={publish_mode}"
            )
            threading.Thread(
                target=_bg_scraper,
                args=(db_cfg, instance_path),
                kwargs=dict(
                    target_categories=merged_cats,
                    publish_mode=publish_mode,
                    mail_cfg=mail_cfg,
                    template_dir=template_dir,
                    pdf_folder=pdf_folder,
                    settings=dict(settings),
                ),
                daemon=True,
            ).start()

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
    Safe to call multiple times — only starts once.
    """
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


# ════════════════════════════════════════════════════════════════
#  ROUTES
# ════════════════════════════════════════════════════════════════

@main_bp.route("/")
def index():
    return render_template("main/landing.html")


@main_bp.route("/home")
@login_required
def home():
    db = get_db()
    cursor = db.cursor(dictionary=True)
    today  = date.today()
    pub = unpub = 0
    try:
        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM published_news WHERE DATE(published_at)=%s",
            (today,)
        )
        pub = cursor.fetchone()["cnt"]
        cursor.execute("SELECT COUNT(*) AS cnt FROM non_published_news WHERE published=0")
        unpub = cursor.fetchone()["cnt"]
    except Exception as e:
        print(f"[HOME] count error: {e}")
    finally:
        cursor.close()
    return render_template(
        "main/home.html",
        today_published_count=pub,
        today_unpublished_count=unpub,
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
