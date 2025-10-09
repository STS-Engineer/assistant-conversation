"use strict";

const express = require("express");
const PDFDocument = require("pdfkit");
const nodemailer = require("nodemailer");
const cors = require("cors");

const app = express();

/* ========================= CONFIG FIXE ========================= */
const SMTP_HOST = "avocarbon-com.mail.protection.outlook.com";
const SMTP_PORT = 25;
const EMAIL_FROM_NAME = "Administration STS";
const EMAIL_FROM = "administration.STS@avocarbon.com";

/* ========================= MIDDLEWARES ========================= */
app.use(express.json({ limit: "50mb" })); // Augmenté pour les images
app.use(express.urlencoded({ extended: true, limit: "50mb" }));

// CORS plus permissif pour ChatGPT
app.use(
  cors({
    origin: true,
    credentials: true,
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "openai-conversation-id", "openai-ephemeral-user-id"],
  })
);

// Préflight pour toutes les routes
app.options("*", cors());

app.use((req, _res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
  next();
});

/* ====================== TRANSPORTEUR SMTP ====================== */
const transporter = nodemailer.createTransport({
  host: SMTP_HOST,
  port: SMTP_PORT,
  secure: false,
  tls: { minVersion: "TLSv1.2" },
});

transporter
  .verify()
  .then(() => console.log("✅ SMTP EOP prêt"))
  .catch((err) => console.error("❌ SMTP erreur:", err.message));

/* ============================ UTILS ============================ */
function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

// Fonction utilitaire pour nettoyer et valider le base64
function cleanAndValidateBase64(imageData) {
  let base64Data = imageData;
  
  // Nettoyer
  if (typeof base64Data === 'string' && base64Data.includes('data:image')) {
    const parts = base64Data.split(',');
    base64Data = parts.length > 1 ? parts[1] : parts[0];
  }
  
  // Supprimer tous les espaces et caractères invisibles
  base64Data = base64Data.replace(/[\s\n\r\t]/g, '');
  
  // Supprimer les caractères non-base64
  base64Data = base64Data.replace(/[^A-Za-z0-9+/=]/g, '');
  
  return base64Data;
}

function validateImageBuffer(buffer) {
  // Vérifier la taille minimale
  if (buffer.length < 500) {
    throw new Error(`Image trop petite (${buffer.length} octets). Minimum 500 octets requis.`);
  }
  
  // Vérifier que c'est vraiment une image (magic bytes)
  const isPNG = buffer[0] === 0x89 && buffer[1] === 0x50;
  const isJPEG = buffer[0] === 0xFF && buffer[1] === 0xD8;
  const isGIF = buffer[0] === 0x47 && buffer[1] === 0x49;
  
  if (!isPNG && !isJPEG && !isGIF) {
    throw new Error("Format d'image non supporté (attendu: PNG, JPEG ou GIF)");
  }
  
  return true;
}

function generatePDF(content) {
  return new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument({
        margin: 50,
        size: "A4",
        bufferPages: true,
        info: { Title: content.title, Author: "Assistant GPT", Subject: content.title },
      });

      const chunks = [];
      doc.on("data", (c) => chunks.push(c));
      doc.on("end", () => resolve(Buffer.concat(chunks)));
      doc.on("error", reject);

      // En-tête
      doc.fontSize(26).font("Helvetica-Bold").fillColor("#1e40af").text(content.title, { align: "center" });
      doc.moveDown(0.5);
      doc.strokeColor("#3b82f6").lineWidth(2).moveTo(50, doc.y).lineTo(doc.page.width - 50, doc.y).stroke();
      doc.moveDown();

      // Date
      doc
        .fontSize(10)
        .fillColor("#6b7280")
        .font("Helvetica")
        .text(
          `Date: ${new Date().toLocaleDateString("fr-FR", { year: "numeric", month: "long", day: "numeric" })}`,
          { align: "right" }
        );
      doc.moveDown(2);

      // Introduction
      if (content.introduction) {
        doc.fontSize(16).font("Helvetica-Bold").fillColor("#1f2937").text("Introduction");
        doc.moveDown(0.5);
        doc.fontSize(11).font("Helvetica").fillColor("#374151")
          .text(content.introduction, { align: "justify", lineGap: 3 });
        doc.moveDown(2);
      }

      // Sections
      if (Array.isArray(content.sections)) {
        content.sections.forEach((section, index) => {
          // Vérifier si besoin d'une nouvelle page
          if (doc.y > doc.page.height - 150) {
            doc.addPage();
          }
          
          doc.fontSize(14).font("Helvetica-Bold").fillColor("#1e40af").text(`${index + 1}. ${section.title}`);
          doc.moveDown(0.5);
          
          // Contenu texte
          if (section.content) {
            doc.fontSize(11).font("Helvetica").fillColor("#374151")
              .text(section.content, { align: "justify", lineGap: 3 });
            doc.moveDown(1);
          }
          
          // Image (si présente)
          if (section.image) {
            try {
              // Nettoyer le base64 avec la fonction utilitaire
              const cleanedBase64 = cleanAndValidateBase64(section.image);
              
              // Créer le buffer
              let imageBuffer;
              try {
                imageBuffer = Buffer.from(cleanedBase64, 'base64');
              } catch (bufferError) {
                throw new Error("Impossible de décoder le base64");
              }
              
              // Valider l'image avec la fonction utilitaire
              validateImageBuffer(imageBuffer);
              
              // Calculer les dimensions
              const maxWidth = doc.page.width - 100; // Marges
              const maxHeight = 300; // Hauteur max de l'image
              
              // Vérifier si on a assez d'espace, sinon nouvelle page
              if (doc.y > doc.page.height - maxHeight - 100) {
                doc.addPage();
              }
              
              // Sauvegarder la position Y avant l'image
              const startY = doc.y;
              
              // Ajouter l'image
              doc.image(imageBuffer, {
                fit: [maxWidth, maxHeight],
                align: 'center'
              });
              
              // Calculer combien d'espace l'image a pris
              const imageHeight = doc.y - startY;
              
              // S'assurer qu'on avance après l'image
              if (imageHeight < 50) {
                doc.moveDown(3);
              } else {
                doc.moveDown(1);
              }
              
              // Légende (si présente)
              if (section.imageCaption) {
                doc.fontSize(9).fillColor("#6b7280").font("Helvetica-Oblique")
                  .text(section.imageCaption, { align: "center" });
                doc.moveDown(1);
              }
              
            } catch (imgError) {
              console.error("Erreur chargement image:", imgError.message);
              doc.fontSize(10).fillColor("#ef4444")
                .text("⚠️ Erreur lors du chargement de l'image", { align: "center" });
              doc.fontSize(8).fillColor("#9ca3af")
                .text(`(${imgError.message})`, { align: "center" });
              doc.moveDown(1);
            }
          }
          
          doc.moveDown(1.5);
        });
      }

      // Conclusion
      if (content.conclusion) {
        if (doc.y > doc.page.height - 150) {
          doc.addPage();
        }
        
        doc.fontSize(16).font("Helvetica-Bold").fillColor("#1f2937").text("Conclusion");
        doc.moveDown(0.5);
        doc.fontSize(11).font("Helvetica").fillColor("#374151")
          .text(content.conclusion, { align: "justify", lineGap: 3 });
      }

      // Numéros de page
      const range = doc.bufferedPageRange();
      
      for (let i = 0; i < range.count; i++) {
        doc.switchToPage(i);
        
        const oldY = doc.y;
        
        doc.fontSize(8).fillColor("#9ca3af");
        doc.text(
          `Page ${i + 1} sur ${range.count}`,
          50,
          doc.page.height - 50,
          { 
            align: "center",
            lineBreak: false,
            width: doc.page.width - 100
          }
        );
        
        if (i < range.count - 1) {
          doc.switchToPage(i);
          doc.y = oldY;
        }
      }

      doc.end();
    } catch (err) {
      console.error("Erreur génération PDF:", err);
      reject(err);
    }
  });
}

async function sendEmailWithPdf({ to, subject, messageHtml, pdfBuffer, pdfFilename }) {
  return transporter.sendMail({
    from: { name: EMAIL_FROM_NAME, address: EMAIL_FROM },
    to,
    subject,
    html: messageHtml,
    attachments: [
      { filename: pdfFilename, content: pdfBuffer, contentType: "application/pdf" },
    ],
  });
}

/* ============================ ROUTES ============================ */
app.post("/api/generate-and-send", async (req, res) => {
  try {
    const { email, subject, reportContent } = req.body || {};

    if (!email || !subject || !reportContent) {
      return res.status(400).json({
        success: false,
        error: "Données manquantes",
        details: "Envoyez email, subject, reportContent",
      });
    }
    if (!isValidEmail(email)) {
      return res.status(400).json({ success: false, error: "Email invalide" });
    }
    if (
      !reportContent.title ||
      !reportContent.introduction ||
      !Array.isArray(reportContent.sections) ||
      !reportContent.conclusion
    ) {
      return res.status(400).json({
        success: false,
        error: "Structure du rapport invalide",
      });
    }

    const pdfBuffer = await generatePDF(reportContent);
    const pdfName = `rapport_${reportContent.title.replace(/[^a-z0-9]/gi, "_").toLowerCase()}_${Date.now()}.pdf`;

    const html = `
      <!DOCTYPE html>
      <html>
        <body style="font-family: Arial, sans-serif; line-height:1.6; color:#111827;">
          <h2 style="margin:0 0 8px 0;">📄 Votre rapport est prêt</h2>
          <div style="background:#e0e7ff;padding:12px;border-left:4px solid #667eea;border-radius:6px;margin:12px 0;">
            <strong>📊 Sujet :</strong> ${subject}<br>
            <strong>📌 Titre :</strong> ${reportContent.title}<br>
            <strong>📅 Date :</strong> ${new Date().toLocaleDateString("fr-FR")}
          </div>
          <p>Vous trouverez le rapport complet en pièce jointe au format PDF.</p>
          <p style="color:#6b7280;font-size:12px">© ${new Date().getFullYear()} ${EMAIL_FROM_NAME}</p>
        </body>
      </html>
    `;

    await sendEmailWithPdf({
      to: email,
      subject: `Rapport : ${reportContent.title}`,
      messageHtml: html,
      pdfBuffer,
      pdfFilename: pdfName,
    });

    return res.json({
      success: true,
      message: "Rapport généré et envoyé avec succès",
      details: {
        email,
        pdfSize: `${(pdfBuffer.length / 1024).toFixed(2)} KB`,
      },
    });
  } catch (err) {
    console.error("❌ Erreur:", err);
    return res.status(500).json({
      success: false,
      error: "Erreur lors du traitement",
      details: err.message,
    });
  }
});

app.post("/api/test-image", async (req, res) => {
  try {
    const { imageData } = req.body;
    
    if (!imageData) {
      return res.status(400).json({ error: "imageData requis" });
    }
    
    // Utiliser la fonction utilitaire de nettoyage
    const cleanedBase64 = cleanAndValidateBase64(imageData);
    
    // Créer buffer
    let buffer;
    try {
      buffer = Buffer.from(cleanedBase64, 'base64');
    } catch (err) {
      return res.status(400).json({ 
        error: "Impossible de décoder le base64",
        details: err.message
      });
    }
    
    // Valider avec la fonction utilitaire
    try {
      validateImageBuffer(buffer);
    } catch (validationError) {
      return res.status(400).json({ 
        error: validationError.message
      });
    }
    
    // Détecter le type via magic bytes
    let type = "inconnu";
    if (buffer[0] === 0xFF && buffer[1] === 0xD8) {
      type = "JPEG";
    } else if (buffer[0] === 0x89 && buffer[1] === 0x50) {
      type = "PNG";
    } else if (buffer[0] === 0x47 && buffer[1] === 0x49) {
      type = "GIF";
    }
    
    return res.json({
      success: true,
      imageType: type,
      size: `${(buffer.length / 1024).toFixed(2)} KB`,
      sizeBytes: buffer.length,
      dimensions: "OK - Image valide pour PDFKit",
      magicBytes: `${buffer[0].toString(16).padStart(2, '0')} ${buffer[1].toString(16).padStart(2, '0')}`
    });
    
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/health", (_req, res) => {
  res.json({
    status: "OK",
    timestamp: new Date().toISOString(),
    uptime: Math.floor(process.uptime()),
    service: "PDF Report API"
  });
});

app.get("/", (_req, res) => {
  res.json({
    name: "GPT PDF Email API",
    version: "1.0.0",
    status: "running",
    endpoints: {
      health: "GET /health",
      generateAndSend: "POST /api/generate-and-send",
    },
  });
});

app.use((req, res) => res.status(404).json({ error: "Route non trouvée", path: req.path }));

app.use((err, _req, res, _next) => {
  console.error("Erreur:", err);
  res.status(500).json({ error: "Erreur serveur", message: err.message });
});

/* ============================ START ============================ */
const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 API démarrée sur port ${PORT}`);
});

process.on("unhandledRejection", (r) => console.error("Unhandled Rejection:", r));
process.on("uncaughtException", (e) => { console.error("Uncaught Exception:", e); process.exit(1); });
