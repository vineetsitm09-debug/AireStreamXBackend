// db.js — Optimized PostgreSQL Connection (AirStream)
import pkg from "pg";
const { Pool } = pkg;

// ============================================================
// OPTIMIZED CONNECTION POOL
// ============================================================
export const pool = new Pool({
  user: "vineet",
  host: "localhost",
  database: "airstream",
  password: "vineet123",
  port: 5432,

  // CONNECTION POOL OPTIMIZATION
  max: 20,                    // Maximum 20 connections
  min: 5,                     // Minimum 5 connections maintained
  idleTimeoutMillis: 30000,   // Close idle connections after 30 seconds
  connectionTimeoutMillis: 5000,  // Timeout if connection takes > 5 seconds

  // KEEP-ALIVE (Important for long-running connections)
  keepAlive: true,
  keepAliveInitialDelayMillis: 10000,  // Start keep-alive after 10 seconds

  // STATEMENT TIMEOUT (Prevent hanging queries)
  statement_timeout: 30000,   // Kill queries taking > 30 seconds
});

// ============================================================
// CONNECTION ERROR HANDLING
// ============================================================
pool.on('error', (err, client) => {
  console.error('❌ Unexpected database error:', err);
  process.exit(-1);  // Exit process on critical DB error
});

pool.on('connect', () => {
  console.log('✅ New database connection established');
});

pool.on('remove', () => {
  console.log('🗑️  Database connection removed from pool');
});

// ============================================================
// DATABASE SCHEMA INITIALIZATION & OPTIMIZATION
// ============================================================
(async () => {
  const client = await pool.connect();
  try {
    console.log("🔧 Checking database schema...");

    // --------------------------------------------------------
    // USERS TABLE (Enhanced for subscription system)
    // --------------------------------------------------------
    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(255) UNIQUE NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        full_name VARCHAR(255),
        avatar_url TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
      CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
    `);

    // --------------------------------------------------------
    // CHANNELS TABLE (NEW - For subscription system)
    // --------------------------------------------------------
    await client.query(`
      CREATE TABLE IF NOT EXISTS channels (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        channel_name VARCHAR(255) UNIQUE NOT NULL,
        description TEXT,
        avatar_url TEXT,
        banner_url TEXT,
        subscriber_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_channels_user ON channels(user_id);
      CREATE INDEX IF NOT EXISTS idx_channels_subscribers ON channels(subscriber_count DESC);
      CREATE INDEX IF NOT EXISTS idx_channels_name ON channels(LOWER(channel_name));
    `);

    // --------------------------------------------------------
    // SUBSCRIPTIONS TABLE (NEW - Core subscription system)
    // --------------------------------------------------------
    await client.query(`
      CREATE TABLE IF NOT EXISTS subscriptions (
        id SERIAL PRIMARY KEY,
        subscriber_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
        subscribed_at TIMESTAMP DEFAULT NOW(),
        notifications_enabled BOOLEAN DEFAULT TRUE,
        UNIQUE(subscriber_id, channel_id)
      );

      CREATE INDEX IF NOT EXISTS idx_subscriptions_subscriber ON subscriptions(subscriber_id);
      CREATE INDEX IF NOT EXISTS idx_subscriptions_channel ON subscriptions(channel_id);
      CREATE INDEX IF NOT EXISTS idx_subscriptions_date ON subscriptions(subscribed_at DESC);
    `);

    // --------------------------------------------------------
    // NOTIFICATIONS TABLE (NEW - In-app notifications)
    // --------------------------------------------------------
    await client.query(`
      CREATE TABLE IF NOT EXISTS notifications (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
        type VARCHAR(50) NOT NULL,
        channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
        video_id INTEGER REFERENCES videos(id) ON DELETE CASCADE,
        message TEXT NOT NULL,
        read BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, read, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_notifications_unread ON notifications(user_id) WHERE read = false;
    `);

    // --------------------------------------------------------
    // VIDEOS TABLE (ENHANCED with channel_id)
    // --------------------------------------------------------
    await client.query(`
      CREATE TABLE IF NOT EXISTS videos (
        id SERIAL PRIMARY KEY,
        channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
        filename VARCHAR(255) UNIQUE NOT NULL,
        title VARCHAR(255) NOT NULL DEFAULT 'Untitled',
        description TEXT,
        category VARCHAR(50) DEFAULT 'General',
        uploader_email VARCHAR(255) NOT NULL,
        video_url TEXT,
        thumbnail_url TEXT,
        status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'ready', 'error')),
        duration INTEGER DEFAULT 0,
        views INTEGER DEFAULT 0,
        likes INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW(),
        processed_at TIMESTAMP,
        error_message TEXT,
        
        -- Processing progress fields (from enhanced worker)
        processing_progress INTEGER DEFAULT 0,
        processing_stage VARCHAR(100) DEFAULT '',
        resolution VARCHAR(20),
        fps VARCHAR(10),
        failed_at TIMESTAMP,
        
        -- Additional metadata
        thumbnails_base TEXT,
        file_size BIGINT,
        codec VARCHAR(50)
      );
    `);

    // --------------------------------------------------------
    // CREATE INDEXES FOR VIDEOS TABLE
    // --------------------------------------------------------
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id);
      CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status) WHERE status = 'ready';
      CREATE INDEX IF NOT EXISTS idx_videos_uploader ON videos(uploader_email);
      CREATE INDEX IF NOT EXISTS idx_videos_category ON videos(category);
      CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_videos_status_created ON videos(status, created_at DESC) WHERE status = 'ready';
      CREATE INDEX IF NOT EXISTS idx_videos_title_lower ON videos(LOWER(title));
      CREATE INDEX IF NOT EXISTS idx_videos_progress ON videos(processing_progress) WHERE status = 'processing';
    `);

    // --------------------------------------------------------
    // VIDEO COMMENTS TABLE
    // --------------------------------------------------------
    await client.query(`
      CREATE TABLE IF NOT EXISTS video_comments (
        id SERIAL PRIMARY KEY,
        video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
        user_email VARCHAR(255) NOT NULL,
        comment_text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP,
        
        CONSTRAINT comment_not_empty CHECK (length(trim(comment_text)) > 0)
      );

      CREATE INDEX IF NOT EXISTS idx_comments_video ON video_comments(video_id, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_comments_user ON video_comments(user_email);
    `);

    // --------------------------------------------------------
    // VIDEO LIKES TABLE
    // --------------------------------------------------------
    await client.query(`
      CREATE TABLE IF NOT EXISTS video_likes (
        id SERIAL PRIMARY KEY,
        video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
        user_email VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        
        UNIQUE(video_id, user_email)
      );

      CREATE INDEX IF NOT EXISTS idx_likes_video ON video_likes(video_id);
      CREATE INDEX IF NOT EXISTS idx_likes_user ON video_likes(user_email);
    `);

    // --------------------------------------------------------
    // LIVE STREAMS TABLE
    // --------------------------------------------------------
    await client.query(`
      CREATE TABLE IF NOT EXISTS live_streams (
        id VARCHAR(255) PRIMARY KEY,
        user_email VARCHAR(255) NOT NULL,
        user_uid VARCHAR(255) NOT NULL,
        title VARCHAR(255) NOT NULL,
        description TEXT,
        thumbnail TEXT,
        stream_key VARCHAR(255) UNIQUE NOT NULL,
        stream_url TEXT,
        hls_url TEXT,
        status VARCHAR(20) DEFAULT 'starting' CHECK (status IN ('starting', 'live', 'ended', 'error')),
        viewers INTEGER DEFAULT 0,
        peak_viewers INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW(),
        ended_at TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_streams_status ON live_streams(status) WHERE status IN ('starting', 'live');
      CREATE INDEX IF NOT EXISTS idx_streams_user ON live_streams(user_email);
    `);

    // --------------------------------------------------------
    // COMPANIES TABLE
    // --------------------------------------------------------
    await client.query(`
      CREATE TABLE IF NOT EXISTS companies (
        id SERIAL PRIMARY KEY,
        company_name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE INDEX IF NOT EXISTS idx_companies_email ON companies(email);
    `);

    // --------------------------------------------------------
    // CREATE DATABASE FUNCTIONS
    // --------------------------------------------------------
    
    // Function to update subscriber count automatically
    await client.query(`
      CREATE OR REPLACE FUNCTION update_subscriber_count()
      RETURNS TRIGGER AS $$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          UPDATE channels 
          SET subscriber_count = subscriber_count + 1 
          WHERE id = NEW.channel_id;
          RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
          UPDATE channels 
          SET subscriber_count = GREATEST(subscriber_count - 1, 0)
          WHERE id = OLD.channel_id;
          RETURN OLD;
        END IF;
      END;
      $$ LANGUAGE plpgsql;
    `);

    // Function to update timestamps
    await client.query(`
      CREATE OR REPLACE FUNCTION update_updated_at_column()
      RETURNS TRIGGER AS $$
      BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);

    // --------------------------------------------------------
    // CREATE TRIGGERS
    // --------------------------------------------------------
    
    // Trigger for automatic subscriber count updates
    await client.query(`
      DROP TRIGGER IF EXISTS trg_update_subscriber_count ON subscriptions;
      CREATE TRIGGER trg_update_subscriber_count
        AFTER INSERT OR DELETE ON subscriptions
        FOR EACH ROW
        EXECUTE FUNCTION update_subscriber_count();
    `);

    // Trigger for channels updated_at
    await client.query(`
      DROP TRIGGER IF EXISTS trg_channels_updated_at ON channels;
      CREATE TRIGGER trg_channels_updated_at
        BEFORE UPDATE ON channels
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    `);

    // Trigger for users updated_at
    await client.query(`
      DROP TRIGGER IF EXISTS trg_users_updated_at ON users;
      CREATE TRIGGER trg_users_updated_at
        BEFORE UPDATE ON users
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    `);

    // --------------------------------------------------------
    // CREATE VIEWS FOR COMMON QUERIES
    // --------------------------------------------------------
    
    // Channel details view
    await client.query(`
      CREATE OR REPLACE VIEW v_channel_details AS
      SELECT 
        c.id,
        c.channel_name,
        c.description,
        c.avatar_url,
        c.banner_url,
        c.subscriber_count,
        c.created_at,
        u.username as owner_username,
        u.email as owner_email,
        (SELECT COUNT(*) FROM videos WHERE channel_id = c.id AND status = 'ready') as video_count,
        (SELECT SUM(views) FROM videos WHERE channel_id = c.id) as total_views
      FROM channels c
      JOIN users u ON u.id = c.user_id;
    `);

    // Subscription feed view
    await client.query(`
      CREATE OR REPLACE VIEW v_subscription_feed AS
      SELECT 
        v.id as video_id,
        v.title,
        v.description,
        v.thumbnail_url,
        v.video_url,
        v.duration,
        v.views,
        v.created_at,
        c.id as channel_id,
        c.channel_name,
        c.avatar_url as channel_avatar,
        s.subscriber_id,
        s.notifications_enabled
      FROM videos v
      JOIN channels c ON c.id = v.channel_id
      JOIN subscriptions s ON s.channel_id = c.id
      WHERE v.status = 'ready';
    `);

    // --------------------------------------------------------
    // MIGRATE EXISTING DATA
    // --------------------------------------------------------
    
    // Check if companies table has data that needs migration to users
    const companiesExist = await client.query(`
      SELECT COUNT(*) FROM companies
    `);

    if (parseInt(companiesExist.rows[0].count) > 0) {
      console.log("🔄 Migrating existing company data to users table...");
      
      // Insert companies as users (if not already done)
      await client.query(`
        INSERT INTO users (username, email, password_hash)
        SELECT 
          company_name,
          email,
          password_hash
        FROM companies
        WHERE email NOT IN (SELECT email FROM users)
        ON CONFLICT (email) DO NOTHING;
      `);

      // Create channels for migrated users
      await client.query(`
        INSERT INTO channels (user_id, channel_name, description)
        SELECT 
          u.id,
          u.username,
          'Welcome to my channel!'
        FROM users u
        WHERE NOT EXISTS (
          SELECT 1 FROM channels WHERE channels.user_id = u.id
        );
      `);

      console.log("✅ Migration completed");
    }

    // Update existing videos with channel_id
    await client.query(`
      UPDATE videos v
      SET channel_id = c.id
      FROM channels c
      JOIN users u ON u.id = c.user_id
      WHERE v.uploader_email = u.email
        AND v.channel_id IS NULL;
    `);

    // --------------------------------------------------------
    // VERIFY SETUP
    // --------------------------------------------------------
    const tableCount = await client.query(`
      SELECT COUNT(*) FROM information_schema.tables 
      WHERE table_schema = 'public';
    `);

    const indexCount = await client.query(`
      SELECT COUNT(*) FROM pg_indexes 
      WHERE schemaname = 'public';
    `);

    const triggerCount = await client.query(`
      SELECT COUNT(*) FROM information_schema.triggers
      WHERE trigger_schema = 'public';
    `);

    console.log("✅ Database schema verified");
    console.log(`📊 Tables: ${tableCount.rows[0].count}`);
    console.log(`📊 Indexes: ${indexCount.rows[0].count}`);
    console.log(`📊 Triggers: ${triggerCount.rows[0].count}`);
    console.log("🎯 Subscription system ready!");

  } catch (err) {
    console.error("❌ DB init error:", err.message);
    console.error(err.stack);
  } finally {
    client.release();
  }
})();

// ============================================================
// HELPER FUNCTIONS
// ============================================================

// Test database connection
export async function testConnection() {
  try {
    const result = await pool.query('SELECT NOW()');
    console.log('✅ Database connection test successful:', result.rows[0].now);
    return true;
  } catch (err) {
    console.error('❌ Database connection test failed:', err.message);
    return false;
  }
}

// Get connection pool stats
export function getPoolStats() {
  return {
    total: pool.totalCount,
    idle: pool.idleCount,
    waiting: pool.waitingCount,
  };
}

// Graceful shutdown
export async function closePool() {
  console.log('🔄 Closing database connection pool...');
  await pool.end();
  console.log('✅ Database pool closed');
}

// Handle process termination
process.on('SIGTERM', async () => {
  await closePool();
  process.exit(0);
});

process.on('SIGINT', async () => {
  await closePool();
  process.exit(0);
});

// Export pool as default for backward compatibility
export default pool;
