import admin from "../firebaseAdmin.js";

export async function verifyFirebaseToken(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader) return res.status(401).json({ error: "No token" });

  const token = authHeader.split(" ")[1];
  try {
    const decoded = await admin.auth().verifyIdToken(token);
    req.user = decoded;
    next();
  } catch (err) {
    console.error("? Firebase token verification failed:", err.message);
    res.status(401).json({ error: "Invalid or expired token" });
  }
}
