import express from 'express';
import { verifyFirebaseToken as authenticate } from '../middleware/verifyFirebaseToken.js';
import {
  subscribe,
  unsubscribe,
  checkSubscription,
  getMySubscriptions,
  getChannelSubscribers,
  getSubscriptionFeed,
  toggleNotifications,
  getNotifications,
  markNotificationRead,
  markAllNotificationsRead,
  getChannel,
  updateChannel,
  getChannelStats,
} from '../controllers/subscription.js';

const router = express.Router();

// Core subscription
router.post('/subscribe', authenticate, subscribe);
router.delete('/subscribe/:channelId', authenticate, unsubscribe);
router.get('/subscribe/status/:channelId', authenticate, checkSubscription);

// Lists
router.get('/subscriptions', authenticate, getMySubscriptions);
router.get('/channels/:channelId/subscribers', getChannelSubscribers);

// Feed
router.get('/feed/subscriptions', authenticate, getSubscriptionFeed);

// Notifications
router.put('/subscribe/:channelId/notifications', authenticate, toggleNotifications);
router.get('/notifications', authenticate, getNotifications);
router.put('/notifications/:notificationId/read', authenticate, markNotificationRead);
router.put('/notifications/read-all', authenticate, markAllNotificationsRead);

// Channel
router.get('/channels/:channelId', getChannel);
router.put('/channels/:channelId', authenticate, updateChannel);
router.get('/channels/:channelId/stats', getChannelStats);

export default router;
