import mysql.connector
from flask import current_app, g


def create_database_if_not_exists():
    db_name = current_app.config['MYSQL_DB']

    conn = mysql.connector.connect(
        host=current_app.config['MYSQL_HOST'],
        user=current_app.config['MYSQL_USER'],
        password=current_app.config['MYSQL_PASSWORD']
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    conn.commit()
    cursor.close()
    conn.close()


def get_db():
    if 'db' not in g:
        create_database_if_not_exists()

        g.db = mysql.connector.connect(
            host=current_app.config['MYSQL_HOST'],
            user=current_app.config['MYSQL_USER'],
            password=current_app.config['MYSQL_PASSWORD'],
            database=current_app.config['MYSQL_DB']
        )
    return g.db


def close_db(e=None):
    db = g.pop('db', None)
    if db is not None and db.is_connected():
        db.close()


def init_db():
    db = get_db()
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(150) NOT NULL UNIQUE,
            password VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news_articles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(500) NOT NULL,
            source VARCHAR(255),
            url TEXT,
            published_date DATETIME,
            content LONGTEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS keywords (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sr_no INT NOT NULL UNIQUE,
            keyword LONGTEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS websites (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sr_no INT NOT NULL UNIQUE,
            websites LONGTEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sr_no INT NOT NULL UNIQUE,
            news_type LONGTEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS commodity (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sr_no INT NOT NULL UNIQUE,
            commodity LONGTEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
       CREATE TABLE IF NOT EXISTS non_published_news (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    news_date       DATE,
    news_type       LONGTEXT,
    news_headline   LONGTEXT,
    news_text       LONGTEXT,
    news_url        TEXT,
    keywords        LONGTEXT,
    date_of_insert  DATETIME DEFAULT CURRENT_TIMESTAMP,
    published       TINYINT(1) NOT NULL DEFAULT 0
);
    """)
    cursor.execute("""
       CREATE TABLE IF NOT EXISTS published_news (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    source_id           INT,                          -- original id from non_published_news
    news_date           DATE,
    news_type           LONGTEXT,
    news_headline       LONGTEXT,
    news_text           LONGTEXT,
    news_url            TEXT,
    keywords            LONGTEXT,
    date_of_insert      DATETIME,                     -- original insert date from source
    published_at        DATETIME DEFAULT CURRENT_TIMESTAMP,  -- when it was published
    pdf_path            VARCHAR(500),                 -- path to generated PDF file
    email_sent          TINYINT(1) DEFAULT 0,         -- 0 = not sent, 1 = sent
    email_sent_at       DATETIME DEFAULT NULL
);
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_settings (
    id                    INT AUTO_INCREMENT PRIMARY KEY,
    user_id               INT DEFAULT 1,          -- link to users.id if needed
    run_frequency         VARCHAR(20) DEFAULT '1', -- '1','2','5','10','24','custom'
    custom_frequency      INT DEFAULT NULL,         -- hours, only if run_frequency='custom'
    scraper_enabled       TINYINT(1) DEFAULT 1,
    publish_mode          VARCHAR(10) DEFAULT 'manual', -- 'auto' or 'manual'
    content_categories    VARCHAR(255) DEFAULT 'all',   -- comma-separated: 'all','agricultural','weather', etc.
    email_recipient       VARCHAR(255) DEFAULT 'niyati.b@seamlessautomations.com',
    email_cc              TEXT DEFAULT NULL,
    email_subject_prefix  VARCHAR(255) DEFAULT 'Daily News Alert',
    email_on_publish      TINYINT(1) DEFAULT 1,
    updated_at            DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
    """)

    # ═══ SCHEMA MIGRATIONS ═══
    # Upgrade existing published_news table columns to LONGTEXT if needed
    try:
        cursor.execute("SHOW COLUMNS FROM published_news LIKE 'news_type'")
        col = cursor.fetchone()
        if col and 'VARCHAR' in str(col).upper():
            print("[DB] Upgrading published_news columns to LONGTEXT...")
            cursor.execute("ALTER TABLE published_news MODIFY news_type LONGTEXT")
            cursor.execute("ALTER TABLE published_news MODIFY news_headline LONGTEXT")
            cursor.execute("ALTER TABLE published_news MODIFY keywords LONGTEXT")
            db.commit()
            print("[DB] Schema migration completed")
    except Exception as e:
        print(f"[DB] Schema check failed (might be first run): {e}")

    
    db.commit()
    cursor.close()


def init_app(app):
    app.teardown_appcontext(close_db)

    with app.app_context():
        create_database_if_not_exists()
        init_db()