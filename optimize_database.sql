-- ============================================================================
-- DATABASE OPTIMIZATIONS FOR ZERO-LAG VIDEO STREAMING
-- ============================================================================

-- Add new columns to videos table
ALTER TABLE videos 
ADD COLUMN IF NOT EXISTS file_size BIGINT,
ADD COLUMN IF NOT EXISTS duration FLOAT,
ADD COLUMN IF NOT EXISTS processed_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS last_viewed_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS error_message TEXT;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_videos_views ON videos(views DESC);
CREATE INDEX IF NOT EXISTS idx_videos_uploader ON videos(uploader_email);
CREATE INDEX IF NOT EXISTS idx_videos_search ON videos USING gin(to_tsvector('english', title || ' ' || description));

-- Create video_analytics table for detailed tracking
CREATE TABLE IF NOT EXISTS video_analytics (
    id SERIAL PRIMARY KEY,
    video_id INTEGER REFERENCES videos(id) ON DELETE CASCADE,
    watch_duration INTEGER NOT NULL, -- seconds watched
    watched_at TIMESTAMP DEFAULT NOW(),
    user_email VARCHAR(255),
    country_code VARCHAR(2),
    device_type VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_analytics_video ON video_analytics(video_id);
CREATE INDEX IF NOT EXISTS idx_analytics_watched_at ON video_analytics(watched_at DESC);

-- Optimize video_likes table
CREATE INDEX IF NOT EXISTS idx_likes_video ON video_likes(video_id);
CREATE INDEX IF NOT EXISTS idx_likes_user ON video_likes(user_email);

-- Create materialized view for trending videos (updated hourly)
CREATE MATERIALIZED VIEW IF NOT EXISTS trending_videos AS
SELECT 
    v.id,
    v.title,
    v.filename,
    v.views,
    v.likes,
    v.created_at,
    -- Trending score: recent views weighted more heavily
    (
        COALESCE(v.views, 0) * 0.3 + 
        COALESCE(v.likes, 0) * 2 +
        CASE 
            WHEN v.created_at > NOW() - INTERVAL '7 days' THEN 50
            WHEN v.created_at > NOW() - INTERVAL '30 days' THEN 20
            ELSE 0
        END
    ) as trending_score
FROM videos v
WHERE v.status = 'ready'
ORDER BY trending_score DESC
LIMIT 100;

CREATE UNIQUE INDEX IF NOT EXISTS idx_trending_id ON trending_videos(id);

-- Function to refresh trending videos (call this hourly via cron)
CREATE OR REPLACE FUNCTION refresh_trending_videos()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY trending_videos;
END;
$$ LANGUAGE plpgsql;

-- View for video statistics
CREATE OR REPLACE VIEW video_stats AS
SELECT 
    v.id,
    v.title,
    v.views,
    v.likes,
    COALESCE(SUM(va.watch_duration), 0) as total_watch_time,
    COALESCE(AVG(va.watch_duration), 0) as avg_watch_time,
    COUNT(DISTINCT va.user_email) as unique_viewers
FROM videos v
LEFT JOIN video_analytics va ON v.id = va.video_id
GROUP BY v.id, v.title, v.views, v.likes;

-- Partitioning for video_analytics (if you expect high volume)
-- This splits the table by month for better performance
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'video_analytics_y2025m02') THEN
        CREATE TABLE video_analytics_y2025m02 PARTITION OF video_analytics
        FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
    END IF;
END $$;

-- Add comment for documentation
COMMENT ON TABLE video_analytics IS 'Stores detailed watch analytics including duration and device info';
COMMENT ON MATERIALIZED VIEW trending_videos IS 'Cached view of trending videos, refreshed hourly';
