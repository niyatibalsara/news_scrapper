-- ================================================================
--  Run each statement one at a time in MySQL Workbench.
--  If a column already exists, that ALTER will error — just skip it
--  and run the next one. All other statements are independent.
-- ================================================================

-- 1. Per-category schedule columns (stored as JSON text)
ALTER TABLE user_settings ADD COLUMN schedule_all          TEXT DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_agricultural TEXT DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_weather      TEXT DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_financial    TEXT DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_energy       TEXT DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_global       TEXT DEFAULT NULL;

-- 2. Last-run timestamp columns (ISO-8601 string written by the scheduler)
ALTER TABLE user_settings ADD COLUMN schedule_last_run_all          VARCHAR(32) DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_last_run_agricultural VARCHAR(32) DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_last_run_weather      VARCHAR(32) DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_last_run_financial    VARCHAR(32) DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_last_run_energy       VARCHAR(32) DEFAULT NULL;
ALTER TABLE user_settings ADD COLUMN schedule_last_run_global       VARCHAR(32) DEFAULT NULL;

-- 3. Run-all-together toggle flag
ALTER TABLE user_settings ADD COLUMN sync_all_schedules TINYINT(1) DEFAULT 0;

-- 4. news_url column on published_news (needed for duplicate detection)
ALTER TABLE published_news ADD COLUMN news_url VARCHAR(2048) DEFAULT NULL;

-- 5. Index for faster duplicate checks on published_news
CREATE INDEX idx_published_url ON published_news (news_url(512));
