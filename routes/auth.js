import express from "express";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const router = express.Router();

// In-memory storage for demo (replace with DB later)
const companies = [];

// Register new company
router.post("/signup", async (req, res) => {
  try {
    const { companyName, email, password } = req.body;
    if (!companyName || !email || !password) {
      return res.status(400).json({ error: "All fields required" });
    }

    const existing = companies.find((c) => c.email === email);
    if (existing) return res.status(400).json({ error: "Email already registered" });

    const hashed = await bcrypt.hash(password, 10);
    const newCompany = { id: Date.now().toString(), companyName, email, password: hashed };
    companies.push(newCompany);

    res.json({ success: true, message: "Signup successful", company: { companyName, email } });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Login company
router.post("/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    const company = companies.find((c) => c.email === email);
    if (!company) return res.status(401).json({ error: "Invalid credentials" });

    const isMatch = await bcrypt.compare(password, company.password);
    if (!isMatch) return res.status(401).json({ error: "Invalid credentials" });

    const token = jwt.sign({ id: company.id, email: company.email }, process.env.JWT_SECRET, {
      expiresIn: "12h",
    });

    res.json({
      success: true,
      token,
      company: { id: company.id, companyName: company.companyName, email: company.email },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Verify token
router.get("/me", (req, res) => {
  try {
    const token = req.headers.authorization?.split(" ")[1];
    if (!token) return res.status(401).json({ error: "No token provided" });
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    res.json({ success: true, company: decoded });
  } catch (err) {
    res.status(401).json({ error: "Invalid token" });
  }
});

export default router;
