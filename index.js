require("dotenv").config();
const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const nodemailer = require("nodemailer");
const puppeteer = require("puppeteer");
const { PDFDocument } = require("pdf-lib");

const app = express();

app.use(cors());
// ลายเซ็นเป็น dataURL ควรขยาย limit
app.use(express.json({ limit: "15mb" }));

// ---------- DB ----------
const db = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS || "",
  database: process.env.DB_NAME,
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
const uniqEmails = (arr) =>
  [...new Set((arr || []).map((s) => String(s || "").trim()).filter(Boolean))];
const isEmail = (s) =>
  /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(s || "").trim());

const FRONTEND_BASE_URL =
  process.env.FRONTEND_BASE_URL || "http://localhost:5173";
const SIGN_LINK_BASE =
  process.env.SIGN_LINK_BASE || `${FRONTEND_BASE_URL}/sign`;
const FINAL_VIEW_PATH = process.env.FINAL_VIEW_PATH || "/view-signed";

// ให้ตั้งได้ผ่าน .env ถ้าอยากบังคับใช้ Chrome ที่ติดตั้งไว้
const PUPPETEER_EXECUTABLE_PATH = process.env.PUPPETEER_EXECUTABLE_PATH || undefined;

// ---------- PDF render ----------
async function renderPdfFromUrl(url) {
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

    // โหลดหน้าให้เสถียร
    await page.goto(url, { waitUntil: "networkidle0", timeout: 120000 });
    await page.waitForSelector(".a4-page", { timeout: 30000 });
    await page.evaluateHandle("document.fonts.ready");

    // ----- PASS 1: Cover only (no header/footer) -----
    const coverBuffer = await page.pdf({
      preferCSSPageSize: true,
      printBackground: true,
      format: "A4",
      margin: { top: "0mm", right: "0mm", bottom: "0mm", left: "0mm" },
      displayHeaderFooter: false,
      pageRanges: "1",
    });

    // ----- PASS 2: Content (with header + page numbers) -----
    const contentBuffer = await page.pdf({
      preferCSSPageSize: true,
      printBackground: true,
      format: "A4",
      margin: { top: "18mm", right: "10mm", bottom: "15mm", left: "10mm" },
      displayHeaderFooter: true,
      headerTemplate: `
        <div style="
          width:100%; height:14mm; background:#222; position:relative;
          -webkit-print-color-adjust: exact; print-color-adjust: exact;
        ">
          <div style="
            position:absolute; right:0; top:0; bottom:0; width:42mm;
            background:linear-gradient(135deg,
              transparent 0 28%,
              #f39c12 28% 45%,
              transparent 45% 55%,
              #f39c12 55% 72%,
              transparent 72% 100%
            );
          "></div>
          <div style="position:absolute; left:0; right:0; bottom:0; height:1.5px; background:#fff; opacity:.9;"></div>
        </div>
      `,
      footerTemplate: `
        <div style="
          width:100%; font-size:10px; color:#666;
          padding-top:5px; border-top:1px solid #ddd;
          text-align:right; padding-right:12mm;
        ">
          หน้า <span class="pageNumber"></span> / <span class="totalPages"></span>
        </div>
      `,
      pageRanges: "2-",
    });

    // ----- Merge PDFs: cover + content -----
    const coverPdf = await PDFDocument.load(coverBuffer);
    const contentPdf = await PDFDocument.load(contentBuffer);

    // ถ้ามีแค่หน้าปก
    if (contentPdf.getPageCount() === 0) {
      return Buffer.from(await coverPdf.save());
    }

    const out = await PDFDocument.create();
    const [coverPage] = await out.copyPages(coverPdf, [0]);
    out.addPage(coverPage);

    const contentPages = await out.copyPages(contentPdf, contentPdf.getPageIndices());
    contentPages.forEach(p => out.addPage(p));

    const merged = await out.save();
    return Buffer.from(merged);
  } finally {
    await browser.close();
  }
}


// ---------- Routes ----------
app.get("/health", async (req, res) => {
  try {
    await db.query("SELECT 1");
    res.json({ status: "ok", db: true });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

app.get("/health/puppeteer", async (req, res) => {
  try {
    const b = await puppeteer.launch({
      headless: "new",
      executablePath: PUPPETEER_EXECUTABLE_PATH,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
    await b.close();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/contracts", async (req, res) => {
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
      [documentId, JSON.stringify(config), "PENDING", companyEmail]
    );

    res.json({ documentId, message: "Contract created successfully" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Create contract failed" });
  }
});

app.get("/api/contracts/:documentId", async (req, res) => {
  try {
    const { documentId } = req.params;

    const [rows] = await db.query(
      "SELECT * FROM contracts WHERE document_id = ?",
      [documentId]
    );

    if (!rows.length)
      return res.status(404).json({ message: "Contract not found" });

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
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Fetch contract failed" });
  }
});

app.get("/api/contracts/:documentId/signatures", async (req, res) => {
  const { documentId } = req.params;

  try {
    const [signatures] = await db.query(
      `
      SELECT role, signature_image, signer_role, signed_at
      FROM signatures
      WHERE contract_id = (SELECT id FROM contracts WHERE document_id = ?)
      ORDER BY signed_at ASC
      `,
      [documentId]
    );

    if (!signatures.length) {
      return res
        .status(404)
        .json({ message: "No signatures found for this contract" });
    }

    res.json({ signatures });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Fetch signatures failed" });
  }
});

app.post("/send-sign-email", async (req, res) => {
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
      [documentId]
    );
    if (!rows.length)
      return res.status(404).json({ message: "Contract not found" });

    const contractId = rows[0].id;

    await db.query("UPDATE contracts SET customer_email = ? WHERE id = ?", [
      String(email).trim(),
      contractId,
    ]);

    const signLink = `${SIGN_LINK_BASE}/${documentId}`;

    await transporter.sendMail({
      from: `"E-Sign System" <${process.env.MAIL_USER}>`,
      to: String(email).trim(),
      subject: "กรุณาเซ็นเอกสาร",
      html: `
        <h3>กรุณาเซ็นเอกสาร</h3>
        <p>คลิกลิงก์ด้านล่างเพื่อเซ็นเอกสาร</p>
        <a href="${signLink}">${signLink}</a>
      `,
    });

    await db.query("INSERT INTO email_logs (contract_id, email) VALUES (?, ?)", [
      contractId,
      String(email).trim(),
    ]);

    res.json({ message: "Email sent successfully" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Send email failed" });
  }
});

app.post("/api/contracts/:documentId/customer-sign", async (req, res) => {
  const { documentId } = req.params;
  const { signatures } = req.body;

  if (!signatures || typeof signatures !== "object") {
    return res.status(400).json({ message: "signatures required" });
  }

  const conn = await db.getConnection();

  try {
    await conn.beginTransaction();

    const [contracts] = await conn.query(
      "SELECT id, company_email FROM contracts WHERE document_id = ?",
      [documentId]
    );

    if (!contracts.length) {
      await conn.rollback();
      return res.status(404).json({ message: "Contract not found" });
    }

    const { id: contractId, company_email: companyEmail } = contracts[0];

    for (const role of Object.keys(signatures)) {
      const image = signatures[role];

      if (!isDataUrl(image)) {
        await conn.rollback();
        return res.status(400).json({ message: "Invalid signature image" });
      }

      await conn.query(
        `
        INSERT INTO signatures (contract_id, role, signature_image, signer_role)
        VALUES (?, ?, ?, 'CUSTOMER')
        ON DUPLICATE KEY UPDATE
          signature_image = VALUES(signature_image),
          signed_at = CURRENT_TIMESTAMP
        `,
        [contractId, role, image]
      );
    }

    await conn.query("UPDATE contracts SET status = 'CUSTOMER_SIGNED' WHERE id = ?", [
      contractId,
    ]);

    await conn.commit();

    if (companyEmail) {
      const adminLink = `${FRONTEND_BASE_URL}/admin/sign/${documentId}`;
      await transporter.sendMail({
        from: `"E-Sign System" <${process.env.MAIL_USER}>`,
        to: String(companyEmail).trim(),
        subject: "ลูกค้าเซ็นเอกสารเรียบร้อยแล้ว",
        html: `
          <h3>ลูกค้าได้เซ็นเอกสารเรียบร้อย</h3>
          <p>เลขที่เอกสาร: <b>${documentId}</b></p>
          <p>คลิกเพื่อตรวจสอบเอกสาร</p>
          <a href="${adminLink}">${adminLink}</a>
        `,
      });
    }

    res.json({ message: "Customer signed successfully" });
  } catch (err) {
    await conn.rollback();
    console.error(err);
    res.status(500).json({ message: "Customer sign failed" });
  } finally {
    conn.release();
  }
});

app.post("/api/contracts/:documentId/company-sign", async (req, res) => {
  const { documentId } = req.params;
  const { signatures } = req.body;

  if (!signatures || typeof signatures !== "object") {
    return res.status(400).json({ message: "signatures required" });
  }

  const conn = await db.getConnection();

  try {
    await conn.beginTransaction();

    const [contracts] = await conn.query(
      "SELECT id, status, company_email, customer_email, final_sent_at FROM contracts WHERE document_id = ?",
      [documentId]
    );

    if (!contracts.length) {
      await conn.rollback();
      return res.status(404).json({ message: "Contract not found" });
    }

    const contract = contracts[0];

    if (contract.status !== "CUSTOMER_SIGNED") {
      await conn.rollback();
      return res
        .status(400)
        .json({ message: "Contract is not ready for company sign" });
    }

    const contractId = contract.id;

    for (const role of Object.keys(signatures)) {
      const image = signatures[role];

      if (!isDataUrl(image)) {
        await conn.rollback();
        return res.status(400).json({ message: "Invalid signature image" });
      }

      await conn.query(
        `
        INSERT INTO signatures (contract_id, role, signature_image, signer_role)
        VALUES (?, ?, ?, 'COMPANY')
        ON DUPLICATE KEY UPDATE
          signature_image = VALUES(signature_image),
          signed_at = CURRENT_TIMESTAMP
        `,
        [contractId, role, image]
      );
    }

    await conn.query("UPDATE contracts SET status = 'COMPLETED' WHERE id = ?", [
      contractId,
    ]);

    await conn.commit();

    const toList = uniqEmails([contract.company_email, contract.customer_email]);
    if (!toList.length) {
      return res.json({
        message: "Company signed successfully (no recipients)",
      });
    }

    // กันส่งซ้ำเบื้องต้น
    if (contract.final_sent_at) {
      return res.json({
        message: "Company signed successfully (PDF already sent)",
      });
    }

    const pdfUrl = `${FRONTEND_BASE_URL}${FINAL_VIEW_PATH}/${documentId}`;
    let pdfBuffer;
    try {
      pdfBuffer = await renderPdfFromUrl(pdfUrl);
    } catch (e) {
      console.error("renderPdfFromUrl failed:", e);
      return res
        .status(500)
        .json({ message: "Failed to render PDF", error: String(e.message || e) });
    }

    await transporter.sendMail({
      from: `"E-Sign System" <${process.env.MAIL_USER}>`,
      to: toList.join(","),
      subject: "เอกสารเซ็นครบเรียบร้อยแล้ว ✅ (แนบ PDF)",
      html: `
        <h3>เอกสารถูกเซ็นครบเรียบร้อยแล้ว</h3>
        <p>เลขที่เอกสาร: <b>${documentId}</b></p>
        <p>แนบไฟล์ PDF ฉบับสมบูรณ์ไว้ในอีเมลนี้</p>
      `,
      attachments: [
        {
          filename: `${documentId}.pdf`,
          content: pdfBuffer,
          contentType: "application/pdf",
        },
      ],
    });

    // อัปเดตแบบมีเงื่อนไข กันเรซบางส่วน
    const [upd] = await db.query(
      "UPDATE contracts SET final_sent_at = CURRENT_TIMESTAMP WHERE id = ? AND final_sent_at IS NULL",
      [contractId]
    );
    if (upd.affectedRows === 0) {
      // มีใครอัปเดตก่อนหน้าแล้ว แต่เมลเพิ่งส่งไปตอนนี้ — แจ้งเฉยๆ
      return res.json({
        message:
          "Company signed successfully (PDF emailed, final_sent_at was already set)",
      });
    }

    res.json({ message: "Company signed successfully (PDF emailed)" });
  } catch (err) {
    await conn.rollback();
    console.error(err);
    res.status(500).json({ message: "Company sign failed" });
  } finally {
    conn.release();
  }
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🚀 Backend running at http://localhost:${PORT}`);
});
