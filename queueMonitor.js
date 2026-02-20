// queueMonitor.js  —  Works with BullMQ v5.x + Node 20 (ESM project)
// ---------------------------------------------------------------

import express from "express";
import pkg from "bullmq";

const { Queue, QueueEvents } = pkg;

// BullMQ >=5: QueueScheduler is optional for small queues.
// We'll use the safer dynamic import below for compatibility.
let QueueScheduler;
try {
  ({ QueueScheduler } = await import("bullmq"));
} catch {
  console.warn("??  QueueScheduler not found in bullmq, continuing without it.");
}

const app = express();
const PORT = 5050;

// Redis connection
const connection = { host: "127.0.0.1", port: 6379 };

// Create queue
const videoQueue = new Queue("video-processing", { connection });

// Optional QueueScheduler (only if available)
if (QueueScheduler) {
  const scheduler = new QueueScheduler("video-processing", { connection });
  await scheduler.waitUntilReady();
  console.log("?? QueueScheduler ready");
} else {
  console.log("?? Running without QueueScheduler (fine for small setups)");
}

// Queue events
const queueEvents = new QueueEvents("video-processing", { connection });

queueEvents.on("completed", ({ jobId }) =>
  console.log(`? Job ${jobId} completed`)
);
queueEvents.on("failed", ({ jobId, failedReason }) =>
  console.log(`? Job ${jobId} failed: ${failedReason}`)
);

// REST endpoint
app.get("/jobs", async (req, res) => {
  const waiting = await videoQueue.getWaiting();
  const active = await videoQueue.getActive();
  const completed = await videoQueue.getCompleted();
  const failed = await videoQueue.getFailed();

  res.json({
    waiting: waiting.length,
    active: active.length,
    completed: completed.length,
    failed: failed.length,
  });
});

// Start server
app.listen(PORT, () =>
  console.log(`?? Queue monitor running on http://localhost:${PORT}/jobs`)
);
