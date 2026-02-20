// ----------------------------------------------------------------------
// ULTRA-FAST WORKER - OPTIMIZED FOR SPEED (2-3 MIN FOR 5MIN VIDEO)
// Trade-off: Slightly lower quality, fewer renditions
// ----------------------------------------------------------------------
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { promisify } from "util";
import { exec as execCb } from "child_process";
import { Worker } from "bullmq";
import { Client as MinioClient } from "minio";
import { pool } from "./db.js";
import os from "os";

const exec = promisify(execCb);

// ---------------- PATH HELPERS ----------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---------------- CONFIG ----------------
const REDIS_HOST = "127.0.0.1";
const REDIS_PORT = 6379;
const API_HOST = process.env.API_HOST || "127.0.0.1";
const API_PORT = parseInt(process.env.API_PORT || "5000", 10);

// OPTIMIZED: More parallel processes
const MAX_CONCURRENT_PROCESSES = 6; // Increased from 4

// ---------------- MINIO ----------------
const minio = new MinioClient({
  endPoint: "18.218.164.106",
  port: 9000,
  useSSL: false,
  accessKey: "admin",
  secretKey: "password123",
  pathStyle: true
});

const HLS_BUCKET = "hls";

(async () => {
  const exists = await minio.bucketExists(HLS_BUCKET).catch(() => false);
  if (!exists) {
    await minio.makeBucket(HLS_BUCKET);
    log("MinIO bucket created:", HLS_BUCKET);
  }
})();

// ---------------- DIRECTORIES ----------------
const UPLOAD_DIR = path.join(__dirname, "uploads");
const HLS_ROOT = path.join(__dirname, "hls");
const THUMBS_ROOT = path.join(__dirname, "thumbnails");

for (const d of [HLS_ROOT, THUMBS_ROOT]) {
  fs.mkdirSync(d, { recursive: true });
}

// ---------------- OPTIMIZED: ONLY 2 RENDITIONS (FASTER!) ----------------
const RENDITIONS = [
{ 
    name: "480p", 
    size: "854x480", 
    videoBitrate: "1000k",
    audioBitrate: "96k",
    maxrate: "1100k",
    bufsize: "2000k"
  },
  { 
    name: "720p", 
    size: "1280x720", 
    videoBitrate: "2000k",  // Reduced from 2500k
    audioBitrate: "128k",
    maxrate: "2200k",
    bufsize: "4000k"
  },
  { 
    name: "1080p", 
    size: "1920x1080", 
    videoBitrate: "4000k",  // Reduced from 5000k
    audioBitrate: "128k",   // Reduced from 192k
    maxrate: "4400k",
    bufsize: "8000k"
  },
];

// ---------------- HELPERS ----------------
const log = (...a) => console.log(`[WORKER ${new Date().toISOString()}]`, ...a);
const error = (...a) => console.error(`[ERROR ${new Date().toISOString()}]`, ...a);

async function run(cmd) {
  log("Running:", cmd.substring(0, 100) + "...");
  const { stdout, stderr } = await exec(cmd, { maxBuffer: 10 * 1024 * 1024 });
  return stdout;
}

// OPTIMIZED: Faster batch upload
async function uploadDir(localDir, prefix) {
  const files = [];
  
  function scanDir(dir, pre) {
    for (const file of fs.readdirSync(dir)) {
      const full = path.join(dir, file);
      const key = `${pre}/${file}`;

      if (fs.statSync(full).isDirectory()) {
        scanDir(full, key);
      } else {
        files.push({ full, key });
      }
    }
  }

  scanDir(localDir, prefix);

  // OPTIMIZED: Larger batches
  const BATCH_SIZE = 20; // Increased from 10
  for (let i = 0; i < files.length; i += BATCH_SIZE) {
    const batch = files.slice(i, i + BATCH_SIZE);
    await Promise.all(
      batch.map(({ full, key }) => 
        minio.fPutObject(HLS_BUCKET, key, full, {
          'Content-Type': getContentType(key),
          'Cache-Control': 'public, max-age=31536000, immutable'
        })
      )
    );
    log(`Uploaded batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(files.length / BATCH_SIZE)}`);
  }
}

function getContentType(filename) {
  if (filename.endsWith('.m3u8')) return 'application/vnd.apple.mpegurl';
  if (filename.endsWith('.ts')) return 'video/mp2t';
  if (filename.endsWith('.jpg')) return 'image/jpeg';
  if (filename.endsWith('.webp')) return 'image/webp';
  return 'application/octet-stream';
}

async function getVideoInfo(inputPath) {
  try {
    const cmd = `ffprobe -v quiet -print_format json -show_format -show_streams "${inputPath}"`;
    const { stdout } = await exec(cmd);
    const info = JSON.parse(stdout);
    
    const videoStream = info.streams.find(s => s.codec_type === 'video');
    const width = videoStream?.width || 1920;
    const height = videoStream?.height || 1080;
const duration = parseFloat(info.format.duration) ||
  parseFloat(videoStream?.duration) ||
  parseFloat(info.streams.find(s => s.duration)?.duration) || 0;    
    return { width, height, duration };
  } catch (err) {
    error("Failed to get video info:", err.message);
    return { width: 1920, height: 1080, duration: 0 };
  }
}

function selectRenditions(sourceWidth, sourceHeight) {
  const sourceLong = Math.max(sourceWidth, sourceHeight);
  const sourceShort = Math.min(sourceWidth, sourceHeight);
  return RENDITIONS.filter(r => {
    const [tw, th] = r.size.split('x').map(Number);
    return Math.max(tw, th) <= sourceLong && Math.min(tw, th) <= sourceShort;
  });
}

// OPTIMIZED: Faster encoding settings
async function generateRendition(inputPath, rendition, hlsDir) {
  const { name, size, videoBitrate, audioBitrate, maxrate, bufsize } = rendition;
  
  const outputPattern = path.join(hlsDir, `${name}_%03d.ts`);
  const playlistPath = path.join(hlsDir, `${name}.m3u8`);
const [targetW, targetH] = size.split('x').map(Number);
  // OPTIMIZED FFmpeg settings for SPEED
  const cmd = `ffmpeg -y -i "${inputPath}" \
-vf "scale=${targetW}:${targetH}:force_original_aspect_ratio=decrease:flags=fast_bilinear,pad=${targetW}:${targetH}:(ow-iw)/2:(oh-ih)/2:black,setsar=1" \    -c:v libx264 \
    -preset superfast \
    -tune zerolatency \
    -crf 28 \
    -profile:v main \
    -level 4.0 \
    -pix_fmt yuv420p \
    -b:v ${videoBitrate} \
    -maxrate ${maxrate} \
    -bufsize ${bufsize} \
    -g 60 \
    -keyint_min 60 \
    -sc_threshold 0 \
    -c:a aac \
    -b:a ${audioBitrate} \
    -ar 44100 \
    -ac 2 \
    -map_metadata -1 \
    -movflags +faststart \
    -hls_time 6 \
    -hls_playlist_type vod \
    -hls_segment_type mpegts \
    -hls_flags independent_segments \
    -hls_segment_filename "${outputPattern}" \
    "${playlistPath}"`;

  await run(cmd);
  log(`? Generated ${name}`);
}

// OPTIMIZED: Minimal thumbnails (only 2 instead of 4)
async function generateThumbnails(inputPath, thumbsDir, duration) {
  if (duration === 0) {
    log("??  Duration is 0, skipping thumbnails");
    return;
  }

  const timestamps = [
    { name: "thumb_0001.jpg", time: Math.min(1, duration * 0.1) },
    { name: "thumb_0002.jpg", time: duration * 0.5 },
    // Removed thumb_0003 and thumb_0004 for speed
  ];

  // Generate thumbnails
  await Promise.all(
    timestamps.map(async ({ name, time }) => {
      const output = path.join(thumbsDir, name);
      const cmd = `ffmpeg -y -ss ${time} -i "${inputPath}" \
        -vframes 1 \
        -vf "scale=1280:720:force_original_aspect_ratio=decrease" \
        -q:v 3 \
        "${output}"`;
      await run(cmd);
    })
  );

  // OPTIMIZED: Skip WebP conversion for speed
  log("? Generated thumbnails (JPG only, no WebP for speed)");
}

// OPTIMIZED: Skip sprite sheet for speed
async function generateSpriteSheet(inputPath, thumbsDir, duration) {
  // Skipped for speed - sprite sheet takes 30-60 seconds
  log("??  Sprite sheet skipped for faster processing");
}

function createMasterPlaylist(hlsDir, renditions) {
  const lines = ["#EXTM3U", "#EXT-X-VERSION:3"];

  renditions.forEach(r => {
    const bandwidth = (parseInt(r.videoBitrate) + parseInt(r.audioBitrate)) * 1000;
    const [width, height] = r.size.split('x');
    
    lines.push(
      `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${width}x${height},CODECS="avc1.4d401f,mp4a.40.2"`,
      `${r.name}.m3u8`
    );
  });

  const masterPath = path.join(hlsDir, "master.m3u8");
  fs.writeFileSync(masterPath, lines.join("\n"));
  log("? Created master playlist");
}

// ---------------- WORKER ----------------
log("?? ULTRA-FAST Worker started");
log(`CPU Cores: ${os.cpus().length}`);
log(`Max Concurrent: ${MAX_CONCURRENT_PROCESSES}`);
log(`Optimizations: Fewer renditions, faster encoding, minimal thumbnails`);

new Worker(
  "video-processing",
  async (job) => {
    const startTime = Date.now();
    const { fileName, videoId } = job.data;
    
    log(`?????????????????????????????????????????????????`);
    log(`?? FAST Processing: ${fileName} (ID: ${videoId})`);

    const inputPath = path.join(UPLOAD_DIR, fileName);
    const baseName = fileName.replace(/\.[^/.]+$/, "");

    const hlsDir = path.join(HLS_ROOT, baseName);
    const thumbsDir = path.join(THUMBS_ROOT, baseName);

    [hlsDir, thumbsDir].forEach(dir => {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
      }
      fs.mkdirSync(dir, { recursive: true });
    });

    try {
      await pool.query("UPDATE videos SET status='processing' WHERE id=$1", [videoId]);

      // Step 1: Quick analysis
      log("?? Quick analysis...");
      const { width, height, duration } = await getVideoInfo(inputPath);
      log(`${width}x${height}, ${duration.toFixed(1)}s`);

      // Step 2: Select renditions (only 2 now)
      const selectedRenditions = selectRenditions(width, height);
      log(`Renditions: ${selectedRenditions.map(r => r.name).join(', ')}`);

      if (selectedRenditions.length === 0) {
        throw new Error("Source resolution too low");
      }

      // Step 3: FAST parallel transcoding
      log("?? FAST transcoding...");
      await Promise.all(
        selectedRenditions.map(r => generateRendition(inputPath, r, hlsDir))
      );

      // Step 4: Master playlist
      createMasterPlaylist(hlsDir, selectedRenditions);

      // Step 5: Minimal thumbnails (no sprite)
      log("???  Quick thumbnails...");
      await generateThumbnails(inputPath, thumbsDir, duration);
      // Skip sprite sheet for speed

      // Step 6: Fast batch upload
      log("??  Fast upload...");
      await Promise.all([
        uploadDir(hlsDir, baseName),
        uploadDir(thumbsDir, `thumbnails/${baseName}`)
      ]);

      // Step 7: Update database
      const videoUrl = `http://${API_HOST}:${API_PORT}/hls/${baseName}/master.m3u8`;
      await pool.query(
        `UPDATE videos 
         SET status='ready', video_url=$1, duration=$2, processed_at=NOW()
         WHERE id=$3`,
        [videoUrl, duration, videoId]
      );

      // Step 8: Cleanup
      log("?? Cleanup...");
      try {
        fs.rmSync(hlsDir, { recursive: true, force: true });
        fs.rmSync(thumbsDir, { recursive: true, force: true });
        fs.unlinkSync(inputPath);
      } catch {}

      const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
      log(`? DONE: ${fileName} in ${totalTime}s ?`);
      log(`?????????????????????????????????????????????????`);

    } catch (e) {
      error(`? FAILED: ${fileName}`, e.message);
      
      await pool.query(
        `UPDATE videos SET status='error', error_message=$1 WHERE id=$2`,
        [e.message, videoId]
      );

      try {
        if (fs.existsSync(hlsDir)) fs.rmSync(hlsDir, { recursive: true, force: true });
        if (fs.existsSync(thumbsDir)) fs.rmSync(thumbsDir, { recursive: true, force: true });
        if (fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
      } catch {}

      throw e;
    }
  },
  { 
    connection: { host: REDIS_HOST, port: REDIS_PORT },
    concurrency: 3, // Process 3 videos simultaneously (increased from 2)
    limiter: {
      max: 15, // Increased from 10
      duration: 60000,
    }
  }
);

process.on('SIGTERM', () => process.exit(0));
process.on('SIGINT', () => process.exit(0));