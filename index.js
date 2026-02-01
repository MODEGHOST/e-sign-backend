require("dotenv").config();
const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const nodemailer = require("nodemailer");
const puppeteer = require("puppeteer");
const { PDFDocument } = require("pdf-lib");

const fs = require("fs");
const https = require("https");
const { URL } = require("url");
const crypto = require("crypto");

const app = express();

app.use(cors());
app.use(express.json({ limit: "15mb" }));

// ---------- Logger Helpers ----------
function now() {
  return new Date().toISOString();
}
function rid() {
  return crypto.randomBytes(6).toString("hex"); // short request id
}
function log(reqId, ...args) {
  console.log(`[${now()}][${reqId}]`, ...args);
}
function warn(reqId, ...args) {
  console.warn(`[${now()}][${reqId}][WARN]`, ...args);
}
function errlog(reqId, ...args) {
  console.error(`[${now()}][${reqId}][ERROR]`, ...args);
}
function redact(s) {
  if (!s) return "";
  const str = String(s);
  if (str.length <= 10) return "***";
  return str.slice(0, 4) + "..." + str.slice(-4);
}

// ---------- DB ----------
const db = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS || "",
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// ---------- Mail ----------
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: process.env.MAIL_USER,
    pass: process.env.MAIL_PASS,
  },
});

// ---------- Utils ----------
const isDataUrl = (s) => typeof s === "string" && s.startsWith("data:image/");
const uniqEmails = (arr) => [
  ...new Set((arr || []).map((s) => String(s || "").trim()).filter(Boolean)),
];
const isEmail = (s) =>
  /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(s || "").trim());

function safeStr(s, max = 255) {
  if (s == null) return null;
  const v = String(s).trim();
  if (!v) return null;
  return v.length > max ? v.slice(0, max) : v;
}

/**
 * รองรับ 2 แบบ
 * 1) signatures: { role: "data:image/..." }
 * 2) signatures: { role: { image: "data:image/...", name: "...", position: "..." } }
 */
function normalizeSignaturesPayload(signatures) {
  const out = [];
  if (!signatures || typeof signatures !== "object") return out;

  for (const role of Object.keys(signatures)) {
    const v = signatures[role];

    if (typeof v === "string") {
      out.push({ role, image: v, name: null, position: null });
      continue;
    }

    if (v && typeof v === "object") {
      const image =
        v.image || v.signature_image || v.dataUrl || v.signature || null;
      const name = safeStr(
        v.signer_name ?? v.name ?? v.signerName ?? null,
        100,
      );
      const position = safeStr(
        v.signer_position ?? v.position ?? v.signerPosition ?? null,
        255,
      );

      out.push({ role, image, name, position });
      continue;
    }

    out.push({ role, image: null, name: null, position: null });
  }

  return out;
}

function validateRoleBasic(role) {
  if (!role || typeof role !== "string") return false;
  if (role.length > 100) return false;
  return true;
}

/**
 * ดึง role ที่ "ต้องเซ็น" จาก config.signatures (array)
 * โดยใช้ field: role หรือ id (แล้วแต่คุณเก็บ)
 * - ถ้าคุณใช้ role ใน config.signatures ให้มันอ่าน role ก่อน
 * - ถ้าไม่เจอ role จะ fallback ใช้ id
 */
function extractRequiredRolesFromConfig(config) {
  const sigs = Array.isArray(config?.signatures) ? config.signatures : [];
  const roles = [];

  for (const s of sigs) {
    const r = safeStr(s?.role, 100) || safeStr(s?.id, 100);
    if (r) roles.push(r);
  }

  // unique
  return [...new Set(roles)];
}

/**
 * แบ่ง required roles ต่อ signer_role (CUSTOMER/COMPANY)
 * rule: ใช้ prefix จาก role (ปรับได้ตาม naming ที่คุณใช้จริง)
 * - ถ้า role มีคำว่า customer => CUSTOMER
 * - ถ้า role มีคำว่า company  => COMPANY
 * - ถ้าไม่เข้าเงื่อนไข: ใส่รวม (ทั้งสอง) หรือคุณจะบังคับ pattern ก็ได้
 */
function splitRolesBySigner(requiredRoles) {
  const customer = [];
  const company = [];

  for (const r of requiredRoles) {
    const low = String(r).toLowerCase();
    if (low.includes("customer")) customer.push(r);
    else if (low.includes("company")) company.push(r);
    else {
      // fallback: ถ้าคุณตั้ง role ไม่เป็น prefix
      // ให้ถือว่าเป็น role ที่ต้องเซ็นทั้ง 2 ฝั่งไม่ได้
      // แต่เพื่อกันพัง เราจะ "ไม่ใส่" ในฝั่งใดฝั่งหนึ่งแบบสุ่ม
      // => แนะนำให้ตั้ง role ให้ชัด เช่น customer_director / company_witness
    }
  }

  return { customer, company };
}

const FRONTEND_BASE_URL =
  process.env.FRONTEND_BASE_URL || "http://localhost:5173";
const SIGN_LINK_BASE =
  process.env.SIGN_LINK_BASE || `${FRONTEND_BASE_URL}/sign`;
const FINAL_VIEW_PATH = process.env.FINAL_VIEW_PATH || "/view-signed";

// Puppeteer: allow override chrome path via env
const PUPPETEER_EXECUTABLE_PATH =
  process.env.PUPPETEER_EXECUTABLE_PATH || undefined;

// ---------- OneAuthen config ----------
const ONEAUTHEN_ENABLED =
  String(process.env.ONEAUTHEN_ENABLED || "false") === "true";
const ONEAUTHEN_ENDPOINT =
  process.env.ONEAUTHEN_ENDPOINT ||
  "https://uat-sign.one.th/webservice/api/v2/signing/pdfSigning-V3";

const ONEAUTHEN_CAD_DATA = process.env.ONEAUTHEN_CAD_DATA || "";
const ONEAUTHEN_CERTIFY_LEVEL =
  process.env.ONEAUTHEN_CERTIFY_LEVEL || "NON-CERTIFY";
const ONEAUTHEN_VISIBLE_SIGNATURE =
  process.env.ONEAUTHEN_VISIBLE_SIGNATURE || "Invisible";
const ONEAUTHEN_OVERWRITE_ORIGINAL =
  String(process.env.ONEAUTHEN_OVERWRITE_ORIGINAL || "true") === "true";

const ONEAUTHEN_P12_PATH = process.env.ONEAUTHEN_P12_PATH || "";
const ONEAUTHEN_P12_PASSPHRASE = process.env.ONEAUTHEN_P12_PASSPHRASE || "";
const ONEAUTHEN_CERT_PATH = process.env.ONEAUTHEN_CERT_PATH || "";
const ONEAUTHEN_KEY_PATH = process.env.ONEAUTHEN_KEY_PATH || "";
const ONEAUTHEN_CA_PATH = process.env.ONEAUTHEN_CA_PATH || "";

// ---------- Boot logs ----------
console.log("========================================");
console.log("🚀 Backend starting...");
console.log("PORT:", process.env.PORT || 4000);
console.log("FRONTEND_BASE_URL:", FRONTEND_BASE_URL);
console.log("FINAL_VIEW_PATH:", FINAL_VIEW_PATH);
console.log("ONEAUTHEN_ENABLED:", ONEAUTHEN_ENABLED);
console.log("ONEAUTHEN_ENDPOINT:", ONEAUTHEN_ENDPOINT);
console.log(
  "ONEAUTHEN_CAD_DATA:",
  ONEAUTHEN_CAD_DATA ? redact(ONEAUTHEN_CAD_DATA) : "(empty)",
);
console.log("mTLS using:", ONEAUTHEN_P12_PATH ? ".p12" : ".cert+.key");
console.log("ONEAUTHEN_P12_PATH:", ONEAUTHEN_P12_PATH || "(empty)");
console.log("========================================");

// ---------- PDF render ----------
async function renderPdfFromUrl(url, reqId = "sys") {
  log(reqId, "🖨️ renderPdfFromUrl start:", url);
  const t0 = Date.now();

  const browser = await puppeteer.launch({
    headless: "new",
    executablePath: PUPPETEER_EXECUTABLE_PATH,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
    ],
  });

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 794, height: 1123, deviceScaleFactor: 2 });
    await page.emulateMediaType("print");

    await page.goto(url, { waitUntil: "networkidle0", timeout: 120000 });
    await page.waitForSelector(".a4-page", { timeout: 30000 });
    await page.evaluateHandle("document.fonts.ready");

    const coverBuffer = await page.pdf({
      preferCSSPageSize: true,
      printBackground: true,
      format: "A4",
      margin: { top: "0mm", right: "0mm", bottom: "0mm", left: "0mm" },
      displayHeaderFooter: false,
      pageRanges: "1",
    });

    const contentBuffer = await page.pdf({
      preferCSSPageSize: true,
      printBackground: true,
      format: "A4",
      margin: { top: "0mm", right: "0mm", bottom: "0mm", left: "0mm" },
      displayHeaderFooter: false,
      pageRanges: "2-",
    });

    const coverPdf = await PDFDocument.load(coverBuffer);
    const contentPdf = await PDFDocument.load(contentBuffer);

    if (contentPdf.getPageCount() === 0) {
      const outBuf = Buffer.from(await coverPdf.save());
      log(
        reqId,
        "🖨️ renderPdfFromUrl done (cover only), bytes:",
        outBuf.length,
        "ms:",
        Date.now() - t0,
      );
      return outBuf;
    }

    const out = await PDFDocument.create();
    const [coverPage] = await out.copyPages(coverPdf, [0]);
    out.addPage(coverPage);

    const contentPages = await out.copyPages(
      contentPdf,
      contentPdf.getPageIndices(),
    );
    contentPages.forEach((p) => out.addPage(p));

    const outBuf = Buffer.from(await out.save());
    log(
      reqId,
      "🖨️ renderPdfFromUrl done, pages:",
      1 + contentPdf.getPageCount(),
      "bytes:",
      outBuf.length,
      "ms:",
      Date.now() - t0,
    );
    return outBuf;
  } finally {
    await browser.close();
  }
}

// ---------- OneAuthen: HTTPS mTLS helper ----------
function buildOneAuthenAgent(reqId = "sys") {
  const agentOptions = { keepAlive: true };

  if (ONEAUTHEN_P12_PATH) {
    if (!fs.existsSync(ONEAUTHEN_P12_PATH)) {
      throw new Error(`ONEAUTHEN_P12_PATH not found: ${ONEAUTHEN_P12_PATH}`);
    }
    log(reqId, "🔐 mTLS using P12:", ONEAUTHEN_P12_PATH);
    agentOptions.pfx = fs.readFileSync(ONEAUTHEN_P12_PATH);
    agentOptions.passphrase = ONEAUTHEN_P12_PASSPHRASE || undefined;
  } else {
    if (!ONEAUTHEN_CERT_PATH || !ONEAUTHEN_KEY_PATH) {
      throw new Error(
        "Missing mTLS cert config: set ONEAUTHEN_P12_PATH or (ONEAUTHEN_CERT_PATH + ONEAUTHEN_KEY_PATH)",
      );
    }
    if (!fs.existsSync(ONEAUTHEN_CERT_PATH)) {
      throw new Error(`ONEAUTHEN_CERT_PATH not found: ${ONEAUTHEN_CERT_PATH}`);
    }
    if (!fs.existsSync(ONEAUTHEN_KEY_PATH)) {
      throw new Error(`ONEAUTHEN_KEY_PATH not found: ${ONEAUTHEN_KEY_PATH}`);
    }
    log(
      reqId,
      "🔐 mTLS using CERT+KEY:",
      ONEAUTHEN_CERT_PATH,
      ONEAUTHEN_KEY_PATH,
    );
    agentOptions.cert = fs.readFileSync(ONEAUTHEN_CERT_PATH);
    agentOptions.key = fs.readFileSync(ONEAUTHEN_KEY_PATH);
  }

  if (ONEAUTHEN_CA_PATH) {
    if (!fs.existsSync(ONEAUTHEN_CA_PATH)) {
      throw new Error(`ONEAUTHEN_CA_PATH not found: ${ONEAUTHEN_CA_PATH}`);
    }
    log(reqId, "🔐 mTLS using CA chain:", ONEAUTHEN_CA_PATH);
    agentOptions.ca = fs.readFileSync(ONEAUTHEN_CA_PATH);
  }

  return new https.Agent(agentOptions);
}

function httpsJsonRequest(
  urlString,
  bodyObj,
  agent,
  timeoutMs = 120000,
  reqId = "sys",
) {
  const u = new URL(urlString);
  const body = Buffer.from(JSON.stringify(bodyObj), "utf8");

  const options = {
    hostname: u.hostname,
    port: u.port || 443,
    path: u.pathname + (u.search || ""),
    method: "POST",
    agent,
    headers: {
      "Content-Type": "application/json",
      "Content-Length": body.length,
    },
  };

  return new Promise((resolve, reject) => {
    const t0 = Date.now();
    const req = https.request(options, (res) => {
      const chunks = [];
      res.on("data", (d) => chunks.push(d));
      res.on("end", () => {
        const buf = Buffer.concat(chunks);
        log(
          reqId,
          "🌐 OneAuthen HTTP done:",
          res.statusCode,
          "ct:",
          res.headers["content-type"] || "-",
          "bytes:",
          buf.length,
          "ms:",
          Date.now() - t0,
        );
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          buffer: buf,
        });
      });
    });

    req.on("error", (e) => {
      errlog(reqId, "🌐 OneAuthen HTTP error:", e.message || e);
      reject(e);
    });

    req.setTimeout(timeoutMs, () => {
      errlog(reqId, "🌐 OneAuthen HTTP timeout:", timeoutMs, "ms");
      req.destroy(new Error("OneAuthen request timeout"));
    });

    req.write(body);
    req.end();
  });
}

async function signPdfWithOneAuthen(pdfBuffer, reqId = "sys") {
  if (!ONEAUTHEN_CAD_DATA) {
    throw new Error("ONEAUTHEN_CAD_DATA is required (CAD จากทีม oneauthen)");
  }

  log(reqId, "🔏 signPdfWithOneAuthen start. pdf bytes:", pdfBuffer.length);
  log(reqId, "🔏 endpoint:", ONEAUTHEN_ENDPOINT);
  log(
    reqId,
    "🔏 certifyLevel:",
    ONEAUTHEN_CERTIFY_LEVEL,
    "visibleSignature:",
    ONEAUTHEN_VISIBLE_SIGNATURE,
    "overwriteOriginal:",
    ONEAUTHEN_OVERWRITE_ORIGINAL,
  );

  const agent = buildOneAuthenAgent(reqId);

  const payload = {
    pdfData: pdfBuffer.toString("base64"),
    cadData: ONEAUTHEN_CAD_DATA,
    certifyLevel: ONEAUTHEN_CERTIFY_LEVEL,
  };

  if (ONEAUTHEN_VISIBLE_SIGNATURE)
    payload.visibleSignature = ONEAUTHEN_VISIBLE_SIGNATURE;
  if (typeof ONEAUTHEN_OVERWRITE_ORIGINAL === "boolean")
    payload.overwriteOriginal = ONEAUTHEN_OVERWRITE_ORIGINAL;

  const resp = await httpsJsonRequest(
    ONEAUTHEN_ENDPOINT,
    payload,
    agent,
    120000,
    reqId,
  );
  const ct = String(resp.headers["content-type"] || "").toLowerCase();

  if (!(resp.statusCode >= 200 && resp.statusCode < 300)) {
    const preview = resp.buffer.toString("utf8").slice(0, 400);
    warn(
      reqId,
      "⚠️ OneAuthen non-2xx:",
      resp.statusCode,
      "body preview:",
      preview,
    );
  }

  if (ct.includes("application/pdf")) {
    log(reqId, "✅ OneAuthen returned PDF binary. bytes:", resp.buffer.length);
    return resp.buffer;
  }

  let json;
  try {
    json = JSON.parse(resp.buffer.toString("utf8"));
  } catch (e) {
    throw new Error(
      `OneAuthen unexpected response (status=${resp.statusCode}, content-type=${ct}): ${resp.buffer
        .toString("utf8")
        .slice(0, 500)}`,
    );
  }

  const base64 =
    json.pdfData ||
    json.pdfBase64 ||
    json.data?.pdfData ||
    json.result?.pdfData ||
    null;

  if (!base64) {
    throw new Error(
      `OneAuthen response has no pdfData (status=${resp.statusCode}): ${JSON.stringify(json).slice(0, 500)}`,
    );
  }

  const out = Buffer.from(String(base64), "base64");
  log(reqId, "✅ OneAuthen returned pdfData(base64). bytes:", out.length);
  return out;
}

// ---------- DB helpers ----------
async function getContractByDocumentId(conn, documentId) {
  const [rows] = await conn.query(
    "SELECT id, document_id, config, status, company_email, customer_email, final_sent_at FROM contracts WHERE document_id = ?",
    [documentId],
  );
  return rows[0] || null;
}

async function getSignaturesByContractId(conn, contractId) {
  const [rows] = await conn.query(
    `
    SELECT role, signature_image, signer_name, signer_position, signer_role, signed_at
    FROM signatures
    WHERE contract_id = ?
    ORDER BY signer_role ASC, role ASC, signed_at ASC
    `,
    [contractId],
  );
  return rows;
}

function groupSignaturesToMap(rows) {
  // map: { CUSTOMER: { role: row }, COMPANY: { role: row } }
  const out = { CUSTOMER: {}, COMPANY: {} };
  for (const r of rows) {
    const sr = String(r.signer_role || "").toUpperCase();
    if (sr === "CUSTOMER" || sr === "COMPANY") {
      out[sr][r.role] = r;
    }
  }
  return out;
}

async function upsertSignature(
  conn,
  {
    contractId,
    role,
    signerRole, // 'CUSTOMER' | 'COMPANY'
    image,
    name,
    position,
  },
) {
  await conn.query(
    `
    INSERT INTO signatures
      (contract_id, role, signature_image, signer_name, signer_position, signer_role, signed_at)
    VALUES
      (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON DUPLICATE KEY UPDATE
      signature_image = VALUES(signature_image),
      signer_name = VALUES(signer_name),
      signer_position = VALUES(signer_position),
      signed_at = CURRENT_TIMESTAMP
    `,
    [contractId, role, image, name, position, signerRole],
  );
}

function assertRolesAllowed(payloadRoles, allowedRoles) {
  const allowed = new Set(allowedRoles || []);
  for (const r of payloadRoles) {
    if (!allowed.has(r)) {
      const a = Array.from(allowed).join(", ");
      throw new Error(`ROLE_NOT_ALLOWED:${r}:${a}`);
    }
  }
}

function assertSignedAll(requiredRoles, signedMap, signerRole) {
  const missing = [];
  for (const r of requiredRoles) {
    if (!signedMap?.[signerRole]?.[r]?.signature_image) missing.push(r);
  }
  return missing;
}

// ---------- Routes ----------
app.get("/api/health", async (req, res) => {
  const reqId = rid();
  try {
    await db.query("SELECT 1");
    log(reqId, "✅ /api/health ok");
    res.json({ status: "ok", db: true });
  } catch (e) {
    errlog(reqId, "❌ /api/health db error:", e.message || e);
    res.status(500).json({ status: "error", error: e.message });
  }
});

app.get("/health/puppeteer", async (req, res) => {
  const reqId = rid();
  try {
    const b = await puppeteer.launch({
      headless: "new",
      executablePath: PUPPETEER_EXECUTABLE_PATH,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
    await b.close();
    log(reqId, "✅ /health/puppeteer ok");
    res.json({ ok: true });
  } catch (e) {
    errlog(reqId, "❌ /health/puppeteer error:", e.message || e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/health/oneauthen", async (req, res) => {
  const reqId = rid();
  try {
    log(reqId, "➡️ /health/oneauthen called. enabled =", ONEAUTHEN_ENABLED);

    if (!ONEAUTHEN_ENABLED) {
      return res.json({
        ok: true,
        enabled: false,
        note: "ONEAUTHEN_ENABLED=false",
      });
    }

    const agent = buildOneAuthenAgent(reqId);

    const payload = {
      pdfData: "AA==",
      cadData: ONEAUTHEN_CAD_DATA || "MISSING_CAD",
      certifyLevel: ONEAUTHEN_CERTIFY_LEVEL,
      visibleSignature: ONEAUTHEN_VISIBLE_SIGNATURE,
      overwriteOriginal: ONEAUTHEN_OVERWRITE_ORIGINAL,
    };

    const resp = await httpsJsonRequest(
      ONEAUTHEN_ENDPOINT,
      payload,
      agent,
      30000,
      reqId,
    );

    log(
      reqId,
      "✅ /health/oneauthen done:",
      resp.statusCode,
      resp.headers["content-type"] || "-",
    );

    res.json({
      ok: true,
      enabled: true,
      statusCode: resp.statusCode,
      contentType: resp.headers["content-type"] || null,
    });
  } catch (e) {
    errlog(reqId, "❌ /health/oneauthen error:", e.message || e);
    res.status(500).json({
      ok: false,
      enabled: ONEAUTHEN_ENABLED,
      error: String(e.message || e),
    });
  }
});

app.post("/api/contracts", async (req, res) => {
  const reqId = rid();
  try {
    const { config } = req.body;
    if (!config) return res.status(400).json({ message: "config required" });

    const companyEmail = process.env.COMPANY_EMAIL;
    if (!companyEmail) {
      return res.status(500).json({
        message: "Server misconfiguration: COMPANY_EMAIL not set",
      });
    }

    const documentId = "DOC-" + Date.now();

    await db.query(
      "INSERT INTO contracts (document_id, config, status, company_email) VALUES (?, ?, ?, ?)",
      [documentId, JSON.stringify(config), "PENDING", companyEmail],
    );

    log(reqId, "✅ Contract created:", documentId);
    res.json({ documentId, message: "Contract created successfully" });
  } catch (e) {
    errlog(reqId, "❌ Create contract failed:", e.message || e);
    res.status(500).json({ message: "Create contract failed" });
  }
});

app.get("/api/contracts/:documentId", async (req, res) => {
  const reqId = rid();
  try {
    const { documentId } = req.params;
    const [rows] = await db.query(
      "SELECT * FROM contracts WHERE document_id = ?",
      [documentId],
    );

    if (!rows.length)
      return res.status(404).json({ message: "Contract not found" });

    log(reqId, "✅ Get contract:", documentId, "status:", rows[0].status);
    res.json({
      id: rows[0].id,
      documentId: rows[0].document_id,
      config: JSON.parse(rows[0].config),
      status: rows[0].status,
      createdAt: rows[0].created_at,
      company_email: rows[0].company_email,
      customer_email: rows[0].customer_email,
      final_sent_at: rows[0].final_sent_at,
    });
  } catch (e) {
    errlog(reqId, "❌ Fetch contract failed:", e.message || e);
    res.status(500).json({ message: "Fetch contract failed" });
  }
});

app.get("/api/contracts/:documentId/signatures", async (req, res) => {
  const reqId = rid();
  const { documentId } = req.params;

  try {
    const [rows] = await db.query(
      `
      SELECT role, signature_image, signer_name, signer_position, signer_role, signed_at
      FROM signatures
      WHERE contract_id = (SELECT id FROM contracts WHERE document_id = ?)
      ORDER BY signer_role ASC, role ASC, signed_at ASC
      `,
      [documentId],
    );

    if (!rows.length) {
      warn(reqId, "No signatures for:", documentId);
      return res
        .status(404)
        .json({ message: "No signatures found for this contract" });
    }

    log(reqId, "✅ Signatures fetched:", documentId, "count:", rows.length);
    res.json({ signatures: rows });
  } catch (e) {
    errlog(reqId, "❌ Fetch signatures failed:", e.message || e);
    res.status(500).json({ message: "Fetch signatures failed" });
  }
});

app.post("/api/send-sign-email", async (req, res) => {
  const reqId = rid();
  try {
    const { email, documentId } = req.body;
    if (!email || !documentId) {
      return res.status(400).json({ message: "email & documentId required" });
    }
    if (!isEmail(email)) {
      return res.status(400).json({ message: "invalid email" });
    }

    const [rows] = await db.query(
      "SELECT id FROM contracts WHERE document_id = ?",
      [documentId],
    );
    if (!rows.length)
      return res.status(404).json({ message: "Contract not found" });

    const contractId = rows[0].id;

    await db.query("UPDATE contracts SET customer_email = ? WHERE id = ?", [
      String(email).trim(),
      contractId,
    ]);

    const signLink = `${SIGN_LINK_BASE}/${documentId}`;

    log(reqId, "📧 Sending sign email to:", email, "doc:", documentId);

    await transporter.sendMail({
      from: `"E-Sign System" <${process.env.MAIL_USER}>`,
      to: String(email).trim(),
      subject: "กรุณาเซ็นเอกสาร",
      html: `
        <h3>เรียน ผู้มีอำนาจลงนาม</h3>
        <p>บริษัท บิลด์มีอัพ คอนซัลแทนท์ จำกัด ขอส่งเอกสารเพื่อให้ท่านตรวจสอบและลงนามผ่านระบบลงนามอิเล็กทรอนิกส์ (E-Sign)</p>
        <p>กรุณาคลิกลิงก์ด้านล่างเพื่อเข้าดูรายละเอียดและลงนามในเอกสาร</p>
        <p><a href="${signLink}">${signLink}</a></p>
        <p>หากมีข้อสงสัยสามารถติดต่อกลับบริษัทได้ตามช่องทางที่ท่านสะดวก</p>
        <p>ขอขอบพระคุณ<br/>บริษัท บิลด์มีอัพ คอนซัลแทนท์ จำกัด</p>
      `,
    });

    await db.query(
      "INSERT INTO email_logs (contract_id, email) VALUES (?, ?)",
      [contractId, String(email).trim()],
    );

    log(reqId, "✅ Email sent:", email, "doc:", documentId);
    res.json({ message: "Email sent successfully" });
  } catch (e) {
    errlog(reqId, "❌ Send email failed:", e.message || e);
    res.status(500).json({ message: "Send email failed" });
  }
});

// ---------- CUSTOMER SIGN ----------
app.post("/api/contracts/:documentId/customer-sign", async (req, res) => {
  const reqId = rid();
  const { documentId } = req.params;
  const { signatures } = req.body;

  if (!signatures || typeof signatures !== "object") {
    return res.status(400).json({ message: "signatures required" });
  }

  const items = normalizeSignaturesPayload(signatures);
  if (!items.length)
    return res.status(400).json({ message: "signatures payload is empty" });

  const conn = await db.getConnection();
  try {
    log(
      reqId,
      "✍️ Customer sign start:",
      documentId,
      "roles:",
      items.map((x) => x.role).join(","),
    );

    await conn.beginTransaction();

    const contract = await getContractByDocumentId(conn, documentId);
    if (!contract) {
      await conn.rollback();
      return res.status(404).json({ message: "Contract not found" });
    }

    if (contract.status === "COMPLETED") {
      await conn.rollback();
      return res.status(409).json({ message: "Contract already completed" });
    }
    if (contract.status === "CUSTOMER_SIGNED") {
      await conn.rollback();
      return res.status(409).json({ message: "Customer already signed" });
    }

    const config = JSON.parse(contract.config || "{}");
    const requiredAll = extractRequiredRolesFromConfig(config);
    const split = splitRolesBySigner(requiredAll);
    const requiredCustomer = split.customer;

    if (!requiredCustomer.length) {
      await conn.rollback();
      return res.status(500).json({
        message:
          "Cannot determine required CUSTOMER roles from config.signatures. Please use role naming like 'customer_*'.",
      });
    }

    // validate payload roles
    for (const it of items) {
      if (!validateRoleBasic(it.role)) {
        await conn.rollback();
        return res.status(400).json({ message: `Invalid role: ${it.role}` });
      }
      if (!isDataUrl(it.image)) {
        await conn.rollback();
        return res
          .status(400)
          .json({ message: `Invalid signature image for role: ${it.role}` });
      }
    }
    try {
      assertRolesAllowed(
        items.map((x) => x.role),
        requiredCustomer,
      );
    } catch (e) {
      await conn.rollback();
      const msg = String(e.message || "");
      if (msg.startsWith("ROLE_NOT_ALLOWED:")) {
        const parts = msg.split(":");
        return res.status(400).json({
          message: `Role not allowed for CUSTOMER: ${parts[1]}`,
          allowed: (parts[2] || "")
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
        });
      }
      return res.status(400).json({ message: msg });
    }

    // upsert signatures
    for (const it of items) {
      await upsertSignature(conn, {
        contractId: contract.id,
        role: it.role,
        signerRole: "CUSTOMER",
        image: it.image,
        name: it.name,
        position: it.position,
      });
    }

    // check signed all required CUSTOMER
    const sigRows = await getSignaturesByContractId(conn, contract.id);
    const map = groupSignaturesToMap(sigRows);
    const missing = assertSignedAll(requiredCustomer, map, "CUSTOMER");

    if (missing.length) {
      // ยังไม่ครบ -> ยังไม่อัปเดต status
      await conn.commit();
      log(reqId, "✅ Customer partial signed. missing:", missing.join(","));
      return res.json({
        message: "Customer signed partially",
        signedCount: Object.keys(map.CUSTOMER || {}).length,
        requiredCount: requiredCustomer.length,
        missing,
      });
    }

    // ครบ -> update status
    await conn.query(
      "UPDATE contracts SET status = 'CUSTOMER_SIGNED' WHERE id = ?",
      [contract.id],
    );
    await conn.commit();
    log(reqId, "✅ Customer signed ALL. status => CUSTOMER_SIGNED", documentId);

    // notify company
    if (contract.company_email) {
      const adminLink = `${FRONTEND_BASE_URL}/admin/sign/${documentId}`;
      await transporter.sendMail({
        from: `"E-Sign System" <${process.env.MAIL_USER}>`,
        to: String(contract.company_email).trim(),
        subject: "ลูกค้าเซ็นเอกสารเรียบร้อยแล้ว",
        html: `
          <h3>แจ้งเตือน: ลูกค้าเซ็นเอกสารเรียบร้อยแล้ว</h3>
          <p>เอกสารหมายเลข <b>${documentId}</b> ลูกค้าได้ดำเนินการลงนามครบเรียบร้อยแล้ว</p>
          <p><a href="${adminLink}">${adminLink}</a></p>
          <p>ระบบลงนามอิเล็กทรอนิกส์ (E-Sign System)<br/>บริษัท บิลด์มีอัพ คอนซัลแทนท์ จำกัด</p>
        `,
      });
    }

    return res.json({ message: "Customer signed successfully" });
  } catch (e) {
    await conn.rollback();
    errlog(reqId, "❌ Customer sign failed:", e.message || e);
    return res
      .status(500)
      .json({ message: "Customer sign failed", error: String(e.message || e) });
  } finally {
    conn.release();
  }
});

// ---------- COMPANY SIGN ----------
app.post("/api/contracts/:documentId/company-sign", async (req, res) => {
  const reqId = rid();
  const { documentId } = req.params;
  const { signatures } = req.body;

  if (!signatures || typeof signatures !== "object") {
    return res.status(400).json({ message: "signatures required" });
  }

  const items = normalizeSignaturesPayload(signatures);
  if (!items.length)
    return res.status(400).json({ message: "signatures payload is empty" });

  const conn = await db.getConnection();
  try {
    log(
      reqId,
      "🏢 Company sign start:",
      documentId,
      "roles:",
      items.map((x) => x.role).join(","),
    );

    await conn.beginTransaction();

    const contract = await getContractByDocumentId(conn, documentId);
    if (!contract) {
      await conn.rollback();
      return res.status(404).json({ message: "Contract not found" });
    }

    if (contract.status !== "CUSTOMER_SIGNED") {
      await conn.rollback();
      return res
        .status(400)
        .json({ message: "Contract is not ready for company sign" });
    }

    const config = JSON.parse(contract.config || "{}");
    const requiredAll = extractRequiredRolesFromConfig(config);
    const split = splitRolesBySigner(requiredAll);
    const requiredCompany = split.company;

    if (!requiredCompany.length) {
      await conn.rollback();
      return res.status(500).json({
        message:
          "Cannot determine required COMPANY roles from config.signatures. Please use role naming like 'company_*'.",
      });
    }

    // validate payload roles
    for (const it of items) {
      if (!validateRoleBasic(it.role)) {
        await conn.rollback();
        return res.status(400).json({ message: `Invalid role: ${it.role}` });
      }
      if (!isDataUrl(it.image)) {
        await conn.rollback();
        return res
          .status(400)
          .json({ message: `Invalid signature image for role: ${it.role}` });
      }
    }

    try {
      assertRolesAllowed(
        items.map((x) => x.role),
        requiredCompany,
      );
    } catch (e) {
      await conn.rollback();
      const msg = String(e.message || "");
      if (msg.startsWith("ROLE_NOT_ALLOWED:")) {
        const parts = msg.split(":");
        return res.status(400).json({
          message: `Role not allowed for COMPANY: ${parts[1]}`,
          allowed: (parts[2] || "")
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
        });
      }
      return res.status(400).json({ message: msg });
    }

    // upsert company signatures
    for (const it of items) {
      await upsertSignature(conn, {
        contractId: contract.id,
        role: it.role,
        signerRole: "COMPANY",
        image: it.image,
        name: it.name,
        position: it.position,
      });
    }

    // check signed all required COMPANY
    const sigRows = await getSignaturesByContractId(conn, contract.id);
    const map = groupSignaturesToMap(sigRows);
    const missing = assertSignedAll(requiredCompany, map, "COMPANY");

    if (missing.length) {
      await conn.commit();
      log(reqId, "✅ Company partial signed. missing:", missing.join(","));
      return res.json({
        message: "Company signed partially",
        signedCount: Object.keys(map.COMPANY || {}).length,
        requiredCount: requiredCompany.length,
        missing,
      });
    }

    // all signed -> COMPLETED
    await conn.query("UPDATE contracts SET status = 'COMPLETED' WHERE id = ?", [
      contract.id,
    ]);
    await conn.commit();
    log(reqId, "✅ Company signed ALL. status => COMPLETED", documentId);

    // recipients
    const toList = uniqEmails([
      contract.company_email,
      contract.customer_email,
    ]);
    if (!toList.length) {
      warn(reqId, "No recipients, finish without email.");
      return res.json({
        message: "Company signed successfully (no recipients)",
      });
    }

    if (contract.final_sent_at) {
      warn(
        reqId,
        "PDF already sent before. final_sent_at =",
        contract.final_sent_at,
      );
      return res.json({
        message: "Company signed successfully (PDF already sent)",
      });
    }

    // 1) Render PDF
    const pdfUrl = `${FRONTEND_BASE_URL}${FINAL_VIEW_PATH}/${documentId}`;
    log(reqId, "🧾 Rendering PDF from:", pdfUrl);

    let pdfBuffer;
    try {
      pdfBuffer = await renderPdfFromUrl(pdfUrl, reqId);
    } catch (e) {
      errlog(reqId, "renderPdfFromUrl failed:", e.message || e);
      return res.status(500).json({
        message: "Failed to render PDF",
        error: String(e.message || e),
      });
    }

    // 2) OneAuthen sign
    if (ONEAUTHEN_ENABLED) {
      log(reqId, "🔐 OneAuthen enabled => signing PDF...");
      try {
        pdfBuffer = await signPdfWithOneAuthen(pdfBuffer, reqId);
      } catch (e) {
        errlog(reqId, "OneAuthen signing failed:", e.message || e);
        return res.status(500).json({
          message: "Failed to sign PDF with OneAuthen",
          error: String(e.message || e),
        });
      }
      log(
        reqId,
        "🎉 OneAuthen sign success. signed pdf bytes:",
        pdfBuffer.length,
      );
    } else {
      warn(reqId, "🔕 OneAuthen disabled, skipping sign.");
    }

    // 3) Email PDF
    log(reqId, "📨 Sending final PDF email to:", toList.join(","));

    await transporter.sendMail({
      from: `"E-Sign System" <${process.env.MAIL_USER}>`,
      to: toList.join(","),
      subject: "เอกสารเซ็นครบเรียบร้อยแล้ว ✅ (แนบ PDF)",
      html: `
        <h3>เอกสารถูกเซ็นครบเรียบร้อยแล้ว</h3>
        <p>เลขที่เอกสาร: <b>${documentId}</b></p>
        <p>แนบไฟล์ PDF ฉบับสมบูรณ์ไว้ในอีเมลนี้</p>
        <p>OneAuthen enabled: <b>${ONEAUTHEN_ENABLED}</b></p>
      `,
      attachments: [
        {
          filename: `${documentId}.pdf`,
          content: pdfBuffer,
          contentType: "application/pdf",
        },
      ],
    });

    const [upd] = await db.query(
      "UPDATE contracts SET final_sent_at = CURRENT_TIMESTAMP WHERE id = ? AND final_sent_at IS NULL",
      [contract.id],
    );

    if (upd.affectedRows === 0) {
      warn(reqId, "final_sent_at already set by another process.");
      return res.json({
        message:
          "Company signed successfully (PDF emailed, final_sent_at was already set)",
      });
    }

    log(reqId, "✅ Final PDF emailed + final_sent_at updated. Done.");
    return res.json({
      message: `Company signed successfully (PDF emailed${ONEAUTHEN_ENABLED ? ", OneAuthen signed" : ""})`,
    });
  } catch (e) {
    await conn.rollback();
    errlog(reqId, "❌ Company sign failed:", e.message || e);
    return res
      .status(500)
      .json({ message: "Company sign failed", error: String(e.message || e) });
  } finally {
    conn.release();
  }
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🚀 Backend running at http://localhost:${PORT}`);
});
