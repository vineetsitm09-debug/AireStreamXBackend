// firebaseAdmin.js — AirStream Backend
// -------------------------------------
import admin from "firebase-admin";
import { readFileSync } from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

let credentials;

try {
  // ? Try to load from environment variable first (recommended for production)
  if (process.env.FIREBASE_ADMIN_CREDENTIALS) {
    credentials = JSON.parse(process.env.FIREBASE_ADMIN_CREDENTIALS);
    console.log("? Firebase credentials loaded from environment variable");
  } else {
    // ? Fallback: load from local JSON file
    const serviceAccountPath = path.join(__dirname, "serviceAccountKey.json");
    const raw = readFileSync(serviceAccountPath, "utf-8");
    credentials = JSON.parse(raw);
    console.log("? Firebase credentials loaded from file");
  }

  admin.initializeApp({
    credential: admin.credential.cert(credentials),
  });
} catch (error) {
  console.error("? Failed to initialize Firebase Admin SDK:", error.message);
  process.exit(1);
}

export default admin;
