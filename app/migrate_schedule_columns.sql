-- ═══════════════════════════════════════════════════════════════
--  Migration: add schedule + last_run columns to user_settings
--  Run once against your MySQL database.
-- ═══════════════════════════════════════════════════════════════

ALTER TABLE user_settings

  -- Per-category schedule stored as JSON string
  ADD COLUMN IF NOT EXISTS schedule_all           TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_agricultural  TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_weather       TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_financial     TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_energy        TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_global        TEXT DEFAULT NULL,

  -- Last-run timestamps (ISO-8601 string, updated by scheduler)
  ADD COLUMN IF NOT EXISTS schedule_last_run_all           VARCHAR(32) DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_last_run_agricultural  VARCHAR(32) DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_last_run_weather       VARCHAR(32) DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_last_run_financial     VARCHAR(32) DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_last_run_energy        VARCHAR(32) DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS schedule_last_run_global        VARCHAR(32) DEFAULT NULL,

  -- sync_all_schedules flag (already may exist — use MODIFY if so)
  ADD COLUMN IF NOT EXISTS sync_all_schedules     TINYINT(1) DEFAULT 0;

-- Ensure published_news has a news_url column for duplicate detection
ALTER TABLE published_news
  ADD COLUMN IF NOT EXISTS news_url VARCHAR(2048) DEFAULT NULL;

-- Optional: index for faster duplicate checks
CREATE INDEX IF NOT EXISTS idx_published_url
  ON published_news (news_url(512));
