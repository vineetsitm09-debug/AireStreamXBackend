// ============================================================
// SUBSCRIPTION CONTROLLER - Fixed Version
// KEY FIX: Uses Firebase uid/email to look up or create DB users
//          instead of req.user.id (which doesn't exist)
// ============================================================
import { pool } from '../db.js';

// ============================================================
// HELPER: Get or create a DB user from Firebase token
// This is the ROOT FIX - Firebase gives uid+email, not DB id
// ============================================================
async function getOrCreateUser(client, req) {
  const firebaseUid = req.user.uid;
  const email = req.user.email;
  const username = email ? email.split('@')[0] : firebaseUid;

  const result = await client.query(
    `INSERT INTO users (username, email, firebase_uid)
     VALUES ($1, $2, $3)
     ON CONFLICT (email) DO UPDATE
       SET firebase_uid = EXCLUDED.firebase_uid
     RETURNING id`,
    [username, email, firebaseUid]
  );

  return result.rows[0].id;
}

// ============================================================
// CORE SUBSCRIPTION FUNCTIONS
// ============================================================

/**
 * Subscribe to a channel
 * POST /api/subscribe
 */
export async function subscribe(req, res) {
  const { channelId } = req.body;

  if (!channelId) {
    return res.status(400).json({ error: 'Channel ID is required' });
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // ? FIX: Auto-create user from Firebase token
    const userId = await getOrCreateUser(client, req);

    // Check if channel exists
    const channelCheck = await client.query(
      'SELECT id, user_id FROM channels WHERE id = $1',
      [channelId]
    );

    if (channelCheck.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Channel not found' });
    }

    // Check if already subscribed
    const existing = await client.query(
      'SELECT id FROM subscriptions WHERE subscriber_id = $1 AND channel_id = $2',
      [userId, channelId]
    );

    if (existing.rows.length > 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Already subscribed' });
    }

    // Create subscription
    await client.query(
      'INSERT INTO subscriptions (subscriber_id, channel_id) VALUES ($1, $2)',
      [userId, channelId]
    );

    // Update subscriber count
    await client.query(
      'UPDATE channels SET subscriber_count = subscriber_count + 1 WHERE id = $1',
      [channelId]
    );

    // Get updated count
    const countResult = await client.query(
      'SELECT subscriber_count FROM channels WHERE id = $1',
      [channelId]
    );

    await client.query('COMMIT');

    res.json({
      success: true,
      message: 'Subscribed successfully',
      subscriber_count: countResult.rows[0].subscriber_count
    });

  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Subscribe error:', err);
    res.status(500).json({ error: 'Failed to subscribe', detail: err.message });
  } finally {
    client.release();
  }
}

/**
 * Unsubscribe from a channel
 * DELETE /api/subscribe/:channelId
 */
export async function unsubscribe(req, res) {
  const { channelId } = req.params;

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // ? FIX: Auto-create user from Firebase token
    const userId = await getOrCreateUser(client, req);

    const result = await client.query(
      'DELETE FROM subscriptions WHERE subscriber_id = $1 AND channel_id = $2 RETURNING *',
      [userId, channelId]
    );

    if (result.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Not subscribed' });
    }

    // Update subscriber count
    await client.query(
      'UPDATE channels SET subscriber_count = GREATEST(subscriber_count - 1, 0) WHERE id = $1',
      [channelId]
    );

    const countResult = await client.query(
      'SELECT subscriber_count FROM channels WHERE id = $1',
      [channelId]
    );

    await client.query('COMMIT');

    res.json({
      success: true,
      message: 'Unsubscribed successfully',
      subscriber_count: countResult.rows[0]?.subscriber_count || 0
    });

  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Unsubscribe error:', err);
    res.status(500).json({ error: 'Failed to unsubscribe', detail: err.message });
  } finally {
    client.release();
  }
}

/**
 * Check subscription status
 * GET /api/subscribe/status/:channelId
 */
export async function checkSubscription(req, res) {
  const { channelId } = req.params;

  const client = await pool.connect();

  try {
    // ? FIX: Auto-create user from Firebase token
    const userId = await getOrCreateUser(client, req);

    const result = await client.query(
      `SELECT
        s.id,
        s.subscribed_at,
        s.notifications_enabled,
        c.subscriber_count
       FROM subscriptions s
       JOIN channels c ON c.id = s.channel_id
       WHERE s.subscriber_id = $1 AND s.channel_id = $2`,
      [userId, channelId]
    );

    if (result.rows.length > 0) {
      res.json({
        subscribed: true,
        ...result.rows[0]
      });
    } else {
      const countResult = await client.query(
        'SELECT subscriber_count FROM channels WHERE id = $1',
        [channelId]
      );

      res.json({
        subscribed: false,
        subscriber_count: countResult.rows[0]?.subscriber_count || 0
      });
    }

  } catch (err) {
    console.error('Check subscription error:', err);
    res.status(500).json({ error: 'Failed to check subscription', detail: err.message });
  } finally {
    client.release();
  }
}

// ============================================================
// SUBSCRIPTION LISTS
// ============================================================

/**
 * Get user's subscriptions
 * GET /api/subscriptions
 */
export async function getMySubscriptions(req, res) {
  const { limit = 50, offset = 0 } = req.query;

  const client = await pool.connect();

  try {
    // ? FIX: Auto-create user from Firebase token
    const userId = await getOrCreateUser(client, req);

    const result = await client.query(
      `SELECT
        c.id,
        c.channel_name,
        c.description,
        c.avatar_url,
        c.banner_url,
        c.subscriber_count,
        s.subscribed_at,
        s.notifications_enabled,
        (SELECT COUNT(*) FROM videos WHERE channel_id = c.id) as video_count,
        (SELECT created_at FROM videos WHERE channel_id = c.id ORDER BY created_at DESC LIMIT 1) as last_video_at
       FROM subscriptions s
       JOIN channels c ON c.id = s.channel_id
       WHERE s.subscriber_id = $1
       ORDER BY s.subscribed_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );

    const countResult = await client.query(
      'SELECT COUNT(*) FROM subscriptions WHERE subscriber_id = $1',
      [userId]
    );

    res.json({
      subscriptions: result.rows,
      total: parseInt(countResult.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset)
    });

  } catch (err) {
    console.error('Get subscriptions error:', err);
    res.status(500).json({ error: 'Failed to get subscriptions', detail: err.message });
  } finally {
    client.release();
  }
}

/**
 * Get channel's subscribers
 * GET /api/channels/:channelId/subscribers
 */
export async function getChannelSubscribers(req, res) {
  const { channelId } = req.params;
  const { limit = 50, offset = 0 } = req.query;

  try {
    const result = await pool.query(
      `SELECT
        u.id,
        u.username,
        u.email,
        s.subscribed_at,
        s.notifications_enabled
       FROM subscriptions s
       JOIN users u ON u.id = s.subscriber_id
       WHERE s.channel_id = $1
       ORDER BY s.subscribed_at DESC
       LIMIT $2 OFFSET $3`,
      [channelId, limit, offset]
    );

    const countResult = await pool.query(
      'SELECT subscriber_count FROM channels WHERE id = $1',
      [channelId]
    );

    res.json({
      subscribers: result.rows,
      total: countResult.rows[0]?.subscriber_count || 0,
      limit: parseInt(limit),
      offset: parseInt(offset)
    });

  } catch (err) {
    console.error('Get subscribers error:', err);
    res.status(500).json({ error: 'Failed to get subscribers', detail: err.message });
  }
}

// ============================================================
// SUBSCRIPTION FEED
// ============================================================

/**
 * Get videos from subscribed channels
 * GET /api/feed/subscriptions
 */
export async function getSubscriptionFeed(req, res) {
  const { limit = 20, offset = 0 } = req.query;

  const client = await pool.connect();

  try {
    const userId = await getOrCreateUser(client, req);

    const result = await client.query(
      `SELECT
        v.id,
        v.title,
        v.thumbnail,
        v.duration,
        v.views,
        v.created_at,
        c.id as channel_id,
        c.channel_name,
        c.avatar_url
       FROM videos v
       JOIN channels c ON c.id = v.channel_id
       JOIN subscriptions s ON s.channel_id = c.id
       WHERE s.subscriber_id = $1
       ORDER BY v.created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );

    res.json({
      videos: result.rows,
      total: result.rows.length,
      limit: parseInt(limit),
      offset: parseInt(offset)
    });

  } catch (err) {
    console.error('Get subscription feed error:', err);
    res.status(500).json({ error: 'Failed to get subscription feed', detail: err.message });
  } finally {
    client.release();
  }
}

// ============================================================
// NOTIFICATION SETTINGS
// ============================================================

/**
 * Toggle notifications for a channel
 * PUT /api/subscribe/:channelId/notifications
 */
export async function toggleNotifications(req, res) {
  const { channelId } = req.params;
  const { enabled } = req.body;

  const client = await pool.connect();

  try {
    const userId = await getOrCreateUser(client, req);

    const result = await client.query(
      `UPDATE subscriptions
       SET notifications_enabled = $1
       WHERE subscriber_id = $2 AND channel_id = $3
       RETURNING *`,
      [enabled, userId, channelId]
    );

    if (result.rows.length === 0) {
      return res.status(400).json({ error: 'Not subscribed to this channel' });
    }

    res.json({
      success: true,
      notifications_enabled: result.rows[0].notifications_enabled
    });

  } catch (err) {
    console.error('Toggle notifications error:', err);
    res.status(500).json({ error: 'Failed to toggle notifications', detail: err.message });
  } finally {
    client.release();
  }
}

/**
 * Get user's notifications
 * GET /api/notifications
 */
export async function getNotifications(req, res) {
  const { limit = 20, offset = 0, unread_only = false } = req.query;

  const client = await pool.connect();

  try {
    const userId = await getOrCreateUser(client, req);

    const whereClause = unread_only === 'true' ? 'AND n.is_read = false' : '';

    const result = await client.query(
      `SELECT
        n.id,
        n.type,
        n.message,
        n.is_read,
        n.created_at,
        n.channel_id
       FROM notifications n
       WHERE n.user_id = $1 ${whereClause}
       ORDER BY n.created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );

    const unreadCount = await client.query(
      'SELECT COUNT(*) FROM notifications WHERE user_id = $1 AND is_read = false',
      [userId]
    );

    res.json({
      notifications: result.rows,
      unread_count: parseInt(unreadCount.rows[0].count),
      limit: parseInt(limit),
      offset: parseInt(offset)
    });

  } catch (err) {
    console.error('Get notifications error:', err);
    res.status(500).json({ error: 'Failed to get notifications', detail: err.message });
  } finally {
    client.release();
  }
}

/**
 * Mark notification as read
 * PUT /api/notifications/:notificationId/read
 */
export async function markNotificationRead(req, res) {
  const { notificationId } = req.params;

  const client = await pool.connect();

  try {
    const userId = await getOrCreateUser(client, req);

    await client.query(
      'UPDATE notifications SET is_read = true WHERE id = $1 AND user_id = $2',
      [notificationId, userId]
    );

    res.json({ success: true });

  } catch (err) {
    console.error('Mark notification read error:', err);
    res.status(500).json({ error: 'Failed to mark notification', detail: err.message });
  } finally {
    client.release();
  }
}

/**
 * Mark all notifications as read
 * PUT /api/notifications/read-all
 */
export async function markAllNotificationsRead(req, res) {
  const client = await pool.connect();

  try {
    const userId = await getOrCreateUser(client, req);

    await client.query(
      'UPDATE notifications SET is_read = true WHERE user_id = $1',
      [userId]
    );

    res.json({ success: true });

  } catch (err) {
    console.error('Mark all notifications read error:', err);
    res.status(500).json({ error: 'Failed to mark notifications', detail: err.message });
  } finally {
    client.release();
  }
}

// ============================================================
// CHANNEL MANAGEMENT
// ============================================================

/**
 * Get channel details
 * GET /api/channels/:channelId
 */
export async function getChannel(req, res) {
  const { channelId } = req.params;

  try {
    const result = await pool.query(
      `SELECT
        c.id,
        c.channel_name,
        c.description,
        c.avatar_url,
        c.banner_url,
        c.subscriber_count,
        c.created_at,
        u.email as owner_email
       FROM channels c
       LEFT JOIN users u ON u.id = c.user_id
       WHERE c.id = $1`,
      [channelId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Channel not found' });
    }

    res.json({ channel: result.rows[0], subscriber_count: result.rows[0].subscriber_count });

  } catch (err) {
    console.error('Get channel error:', err);
    res.status(500).json({ error: 'Failed to get channel', detail: err.message });
  }
}

/**
 * Update channel details
 * PUT /api/channels/:channelId
 */
export async function updateChannel(req, res) {
  const { channelId } = req.params;
  const { channel_name, description, avatar_url, banner_url } = req.body;

  const client = await pool.connect();

  try {
    const userId = await getOrCreateUser(client, req);

    const result = await client.query(
      `UPDATE channels
       SET
         channel_name = COALESCE($1, channel_name),
         description  = COALESCE($2, description),
         avatar_url   = COALESCE($3, avatar_url),
         banner_url   = COALESCE($4, banner_url)
       WHERE id = $5 AND user_id = $6
       RETURNING *`,
      [channel_name, description, avatar_url, banner_url, channelId, userId]
    );

    if (result.rows.length === 0) {
      return res.status(403).json({ error: 'Not authorized to update this channel' });
    }

    res.json({ success: true, channel: result.rows[0] });

  } catch (err) {
    console.error('Update channel error:', err);
    res.status(500).json({ error: 'Failed to update channel', detail: err.message });
  } finally {
    client.release();
  }
}

/**
 * Get channel statistics
 * GET /api/channels/:channelId/stats
 */
export async function getChannelStats(req, res) {
  const { channelId } = req.params;

  try {
    const result = await pool.query(
      `SELECT
        c.id,
        c.channel_name,
        c.subscriber_count,
        COUNT(DISTINCT v.id) as total_videos,
        COALESCE(SUM(v.views), 0) as total_views
       FROM channels c
       LEFT JOIN videos v ON v.channel_id = c.id
       WHERE c.id = $1
       GROUP BY c.id, c.channel_name, c.subscriber_count`,
      [channelId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Channel not found' });
    }

    res.json(result.rows[0]);

  } catch (err) {
    console.error('Get channel stats error:', err);
    res.status(500).json({ error: 'Failed to get channel stats', detail: err.message });
  }
}