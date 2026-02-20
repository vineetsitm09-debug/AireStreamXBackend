#!/bin/bash
# check-database.sh
# Quick script to verify your AirStream database structure

echo "🔍 AirStream Database Check"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

DB_NAME="airstream"

echo "📊 Current videos table structure:"
sudo -u postgres psql -d $DB_NAME -c "\d videos"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📈 Video statistics:"
sudo -u postgres psql -d $DB_NAME -c "
SELECT 
    COUNT(*) as total_videos,
    COUNT(*) FILTER (WHERE status = 'ready') as ready_videos,
    COUNT(*) FILTER (WHERE status = 'processing') as processing_videos,
    COUNT(*) FILTER (WHERE status = 'queued') as queued_videos,
    COUNT(*) FILTER (WHERE status = 'error') as error_videos,
    SUM(views) as total_views,
    SUM(likes) as total_likes
FROM videos;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🏆 Top 5 most viewed videos:"
sudo -u postgres psql -d $DB_NAME -c "
SELECT id, title, views, likes, status, created_at 
FROM videos 
ORDER BY views DESC NULLS LAST 
LIMIT 5;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 Current indexes on videos table:"
sudo -u postgres psql -d $DB_NAME -c "
SELECT indexname, indexdef 
FROM pg_indexes 
WHERE tablename = 'videos';
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "💾 Database size:"
sudo -u postgres psql -d $DB_NAME -c "
SELECT 
    pg_size_pretty(pg_database_size('$DB_NAME')) as database_size;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔗 Checking for video_likes table:"
sudo -u postgres psql -d $DB_NAME -c "
SELECT EXISTS (
    SELECT 1 
    FROM information_schema.tables 
    WHERE table_name = 'video_likes'
) as video_likes_exists;
"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Database check complete!"
