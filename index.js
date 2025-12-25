require("dotenv").config();
const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const nodemailer = require("nodemailer");

const app = express();

/* ================= middleware ================= */
app.use(cors());
app.use(express.json());

/* ================= MySQL ================= */
const db = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS || "",
  database: process.env.DB_NAME,
});

/* ================= Mail (Gmail App Password) ================= */
const transporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: process.env.MAIL_USER,
    pass: process.env.MAIL_PASS,
  },
});

/* ================= Health check ================= */
app.get("/health", async (req, res) => {
  try {
    await db.query("SELECT 1");
    res.json({ status: "ok", db: true });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});


app.post("/api/contracts", async (req, res) => {
  try {
    const { config } = req.body;

    if (!config) {
      return res.status(400).json({ message: "config required" });
    }

    // ✅ ดึง email จาก env
    const companyEmail = process.env.COMPANY_EMAIL;

    // ❌ ถ้า env ไม่มา ให้ fail ทันที
    if (!companyEmail) {
      console.error("❌ COMPANY_EMAIL is missing in .env");
      return res.status(500).json({
        message: "Server misconfiguration: COMPANY_EMAIL not set",
      });
    }

    const documentId = "DOC-" + Date.now();

    // ✅ log ให้เห็นชัด
    console.log("CREATE CONTRACT");
    console.log("documentId =", documentId);
    console.log("companyEmail =", companyEmail);

    const [result] = await db.query(
      `
      INSERT INTO contracts
        (document_id, config, status, company_email)
      VALUES (?, ?, ?, ?)
      `,
      [
        documentId,
        JSON.stringify(config),
        "PENDING",
        companyEmail,
      ]
    );

    console.log("INSERT RESULT =", result.insertId);

    res.json({
      documentId,
      message: "Contract created successfully",
    });
  } catch (err) {
    console.error("❌ CREATE CONTRACT ERROR", err);
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

    if (!rows.length) {
      return res.status(404).json({ message: "Contract not found" });
    }

    res.json({
      id: rows[0].id,
      documentId: rows[0].document_id,
      config: JSON.parse(rows[0].config),
      status: rows[0].status,
      createdAt: rows[0].created_at,
    });
  } catch (err) {
    res.status(500).json({ message: "Fetch contract failed" });
  }
});


app.get("/api/contracts/:documentId/signatures", async (req, res) => {
  const { documentId } = req.params;
  
  try {
    const [signatures] = await db.query(
      "SELECT role, signature_image FROM signatures WHERE contract_id = (SELECT id FROM contracts WHERE document_id = ?)",
      [documentId]
    );

    if (!signatures.length) {
      return res.status(404).json({ message: "No signatures found for this contract" });
    }

    res.json({ signatures });
  } catch (err) {
    console.error("Error fetching signatures:", err);
    res.status(500).json({ message: "Fetch signatures failed" });
  }
});




app.post("/send-sign-email", async (req, res) => {
  try {
    const { email, documentId } = req.body;

    if (!email || !documentId) {
      return res.status(400).json({ message: "email & documentId required" });
    }

    // 🔥 หา contract ก่อน
    const [rows] = await db.query(
      "SELECT id FROM contracts WHERE document_id = ?",
      [documentId]
    );

    if (!rows.length) {
      return res.status(404).json({ message: "Contract not found" });
    }

    const contractId = rows[0].id;

    const signLink = `http://localhost:5173/sign/${documentId}`;

    // ✅ ส่งเมล
    await transporter.sendMail({
      from: `"E-Sign System" <${process.env.MAIL_USER}>`,
      to: email,
      subject: "กรุณาเซ็นเอกสาร",
      html: `
        <h3>กรุณาเซ็นเอกสาร</h3>
        <p>คลิกลิงก์ด้านล่างเพื่อเซ็นเอกสาร</p>
        <a href="${signLink}">${signLink}</a>
      `,
    });

    // ✅ log ด้วย id ที่ถูกต้อง
    await db.query(
      "INSERT INTO email_logs (contract_id, email) VALUES (?, ?)",
      [contractId, email]
    );

    res.json({ message: "Email sent successfully" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Send email failed" });
  }
});

/* ================= Customer Sign ================= */
app.post("/api/contracts/:documentId/customer-sign", async (req, res) => {
  const { documentId } = req.params;
  const { signatures } = req.body;

  if (!signatures || typeof signatures !== "object") {
    return res.status(400).json({ message: "signatures required" });
  }

  const conn = await db.getConnection();

  try {
    await conn.beginTransaction();

    // 1️⃣ หา contract + company_email
    const [contracts] = await conn.query(
      "SELECT id, company_email FROM contracts WHERE document_id = ?",
      [documentId]
    );

    if (!contracts.length) {
      await conn.rollback();
      return res.status(404).json({ message: "Contract not found" });
    }

    const { id: contractId, company_email: companyEmail } = contracts[0];

    // 2️⃣ บันทึกลายเซ็นลูกค้า
    for (const role of Object.keys(signatures)) {
      const image = signatures[role];

      if (!image.startsWith("data:image/")) {
        await conn.rollback();
        return res.status(400).json({ message: "Invalid signature image" });
      }

      await conn.query(
        `
        INSERT INTO signatures
          (contract_id, role, signature_image, signer_role)
        VALUES (?, ?, ?, 'CUSTOMER')
        ON DUPLICATE KEY UPDATE
          signature_image = VALUES(signature_image),
          signed_at = CURRENT_TIMESTAMP
        `,
        [contractId, role, image]
      );
    }

    // 3️⃣ อัปเดตสถานะ
    await conn.query(
      "UPDATE contracts SET status = 'CUSTOMER_SIGNED' WHERE id = ?",
      [contractId]
    );

    await conn.commit();

    // 4️⃣ 🔔 แจ้งบริษัท (ใช้ company_email โดยตรง)
    if (companyEmail) {
      const adminLink = `http://localhost:5173/admin/sign/${documentId}`;

      await transporter.sendMail({
        from: `"E-Sign System" <${process.env.MAIL_USER}>`,
        to: companyEmail,
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



/* ================= Company Sign ================= */
app.post("/api/contracts/:documentId/company-sign", async (req, res) => {
  const { documentId } = req.params;
  const { signatures } = req.body;
  // signatures = { "กรรมการ": "data:image/png;base64,..." }

  if (!signatures || typeof signatures !== "object") {
    return res.status(400).json({ message: "signatures required" });
  }

  const conn = await db.getConnection();

  try {
    await conn.beginTransaction();

    // 1️⃣ หา contract
    const [contracts] = await conn.query(
      "SELECT id, status FROM contracts WHERE document_id = ?",
      [documentId]
    );

    if (!contracts.length) {
      await conn.rollback();
      return res.status(404).json({ message: "Contract not found" });
    }

    const contract = contracts[0];

    if (contract.status !== "CUSTOMER_SIGNED") {
      await conn.rollback();
      return res.status(400).json({
        message: "Contract is not ready for company sign",
      });
    }

    const contractId = contract.id;

    // 2️⃣ บันทึกลายเซ็นบริษัท
    for (const role of Object.keys(signatures)) {
      const image = signatures[role];

      await conn.query(
        `
        INSERT INTO signatures
          (contract_id, role, signature_image, signer_role)
        VALUES (?, ?, ?, 'COMPANY')
        `,
        [contractId, role, image]
      );
    }

    // 3️⃣ อัปเดตสถานะเป็น COMPLETED
    await conn.query(
      "UPDATE contracts SET status = 'COMPLETED' WHERE id = ?",
      [contractId]
    );

    await conn.commit();

    res.json({ message: "Company signed successfully" });
  } catch (err) {
    await conn.rollback();

    if (err.code === "ER_DUP_ENTRY") {
      return res.status(409).json({
        message: "Role นี้ถูกเซ็นไปแล้ว",
      });
    }

    console.error(err);
    res.status(500).json({ message: "Company sign failed" });
  } finally {
    conn.release();
  }
});



/* ================= Start Server ================= */
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log(`🚀 Backend running at http://localhost:${PORT}`);
});
