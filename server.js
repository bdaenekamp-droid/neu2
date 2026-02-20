const express = require("express");
const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { spawn } = require("child_process");

const app = express();
const port = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, "public")));

const multipartParser = express.raw({ type: "multipart/form-data", limit: "30mb" });

const parseMultipart = (req) => {
  const contentType = req.headers["content-type"] || "";
  const boundaryMatch = contentType.match(/boundary=(.+)$/i);
  if (!boundaryMatch) {
    throw new Error("Ungültiger multipart request: boundary fehlt.");
  }
  const boundary = Buffer.from(`--${boundaryMatch[1]}`);
  const body = req.body;
  const segments = [];
  let start = body.indexOf(boundary);

  while (start !== -1) {
    const next = body.indexOf(boundary, start + boundary.length);
    if (next === -1) break;
    segments.push(body.slice(start + boundary.length + 2, next - 2));
    start = next;
  }

  let fileBuffer = null;
  let payload = {};
  let confirmMismatch = false;

  segments.forEach((segment) => {
    const separatorIndex = segment.indexOf(Buffer.from("\r\n\r\n"));
    if (separatorIndex < 0) return;
    const headerText = segment.slice(0, separatorIndex).toString("utf8");
    const value = segment.slice(separatorIndex + 4);
    const nameMatch = headerText.match(/name="([^"]+)"/i);
    if (!nameMatch) return;
    const fieldName = nameMatch[1];

    if (fieldName === "file") {
      fileBuffer = value;
      return;
    }
    const rawText = value.toString("utf8");
    if (fieldName === "payload") {
      payload = rawText ? JSON.parse(rawText) : {};
    }
    if (fieldName === "confirmMismatch") {
      confirmMismatch = rawText === "true";
    }
  });

  if (!fileBuffer) throw new Error("PDF-Datei fehlt.");
  return { fileBuffer, payload, confirmMismatch };
};

const runPythonXfa = async ({ action, fileBuffer, payload, confirmMismatch = false }) => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "zim-xfa-"));
  const inputPath = path.join(tmpDir, "input.pdf");
  const outputPath = path.join(tmpDir, "output.pdf");

  try {
    await fs.writeFile(inputPath, fileBuffer);
    const args = [
      path.join(__dirname, "scripts", "zim_xfa.py"),
      action,
      "--input",
      inputPath,
      "--payload",
      JSON.stringify(payload),
    ];

    if (action === "fill") {
      args.push("--output", outputPath, "--confirm-mismatch", confirmMismatch ? "true" : "false");
    }

    const result = await new Promise((resolve, reject) => {
      const proc = spawn("python3", args, { cwd: __dirname });
      let stdout = "";
      let stderr = "";

      proc.stdout.on("data", (chunk) => {
        stdout += chunk.toString();
      });
      proc.stderr.on("data", (chunk) => {
        stderr += chunk.toString();
      });
      proc.on("error", reject);
      proc.on("close", (code) => {
        if (code !== 0) {
          return reject(new Error(stderr || `Python process failed with exit code ${code}`));
        }
        try {
          resolve(JSON.parse(stdout || "{}"));
        } catch (error) {
          reject(new Error(`Ungültige Python-Antwort: ${stdout}`));
        }
      });
    });

    if (action === "fill") {
      const pdfBuffer = await fs.readFile(outputPath);
      return { ...result, pdfBuffer };
    }

    return result;
  } finally {
    await fs.rm(tmpDir, { recursive: true, force: true });
  }
};

app.post("/api/zim/fields", multipartParser, async (req, res) => {
  try {
    const { fileBuffer, payload } = parseMultipart(req);
    const analysis = await runPythonXfa({ action: "analyze", fileBuffer, payload });
    res.json(analysis);
  } catch (error) {
    res.status(400).json({ error: error.message || "PDF-Analyse fehlgeschlagen." });
  }
});

app.post("/api/zim/fill", multipartParser, async (req, res) => {
  try {
    const { fileBuffer, payload, confirmMismatch } = parseMultipart(req);
    const response = await runPythonXfa({ action: "fill", fileBuffer, payload, confirmMismatch });

    if (response.mismatch && !confirmMismatch) {
      return res.status(409).json(response);
    }

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${response.downloadName || "Mantelbogen.pdf"}"`);
    res.send(response.pdfBuffer);
  } catch (error) {
    res.status(400).json({ error: error.message || "PDF-Befüllung fehlgeschlagen." });
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
