-- ============================================================
-- CHANNEL + SUBSCRIPTION SYSTEM - FULL SCHEMA
-- Run this in PostgreSQL: psql -U postgres -d airstreamx -f subscription_schema.sql
-- ============================================================

-- 1. Channels table
CREATE TABLE IF NOT EXISTS channels (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(255) UNIQUE NOT NULL,
    channel_name    VARCHAR(255) NOT NULL,
    description     TEXT DEFAULT '',
    banner_url      TEXT,
    subscriber_count INTEGER DEFAULT 0,
    video_count      INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- 2. Subscriptions table
CREATE TABLE IF NOT EXISTS subscriptions (
    id                    SERIAL PRIMARY KEY,
    subscriber_email      VARCHAR(255) NOT NULL,
    subscriber_uid        VARCHAR(255) NOT NULL,
    channel_email         VARCHAR(255) NOT NULL,
    notifications_enabled BOOLEAN DEFAULT true,
    subscribed_at         TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(subscriber_email, channel_email),
    CHECK (subscriber_email != channel_email)
);

-- 3. Indexes
CREATE INDEX IF NOT EXISTS idx_subscriptions_subscriber ON subscriptions(subscriber_email);
CREATE INDEX IF NOT EXISTS idx_subscriptions_channel    ON subscriptions(channel_email);
CREATE INDEX IF NOT EXISTS idx_channels_email           ON channels(email);

-- 4. Auto-populate channels for existing video uploaders
INSERT INTO channels (email, channel_name, subscriber_count, video_count)
SELECT 
    uploader_email                              AS email,
    split_part(uploader_email, '@', 1)         AS channel_name,
    0                                          AS subscriber_count,
    COUNT(*)                                   AS video_count
FROM videos
WHERE status = 'ready'
GROUP BY uploader_email
ON CONFLICT (email) DO UPDATE
    SET video_count = EXCLUDED.video_count,
        updated_at  = NOW();

-- 5. Add last_viewed_at column to videos if missing
ALTER TABLE videos ADD COLUMN IF NOT EXISTS last_viewed_at TIMESTAMP;

-- 6. Add file_size column to videos if missing
ALTER TABLE videos ADD COLUMN IF NOT EXISTS file_size BIGINT;

-- 7. Verify
SELECT 'channels' as table_name, COUNT(*) as rows FROM channels
UNION ALL
SELECT 'subscriptions', COUNT(*) FROM subscriptions
UNION ALL
SELECT 'videos', COUNT(*) FROM videos;
