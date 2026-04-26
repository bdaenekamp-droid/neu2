const express = require("express");
const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { spawn } = require("child_process");

const app = express();
const port = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, "public")));
app.use(express.json({ limit: "30mb" }));

const multipartParser = express.raw({ type: "multipart/form-data", limit: "30mb" });


const PDF_SEARCH_ROOTS = [".", "public", "app", "assets", "static", "uploads", "docs"];
const A701590_SEARCH_PATTERNS = ["zim antrag a701590", "zim a701590", "a701590"];

const normalizePdfStem = (value = "") =>
  `${value}`
    .toLowerCase()
    .replace(/\.pdf$/i, "")
    .replace(/[\s_-]+/g, " ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const isInsideRoot = (rootDir, targetPath) => {
  const rel = path.relative(rootDir, targetPath);
  return !rel.startsWith("..") && !path.isAbsolute(rel);
};

const listPdfCandidates = async (baseDir) => {
  const queue = [];
  const seen = new Set();
  PDF_SEARCH_ROOTS.forEach((entry) => {
    const abs = path.resolve(baseDir, entry);
    if (!seen.has(abs)) {
      seen.add(abs);
      queue.push(abs);
    }
  });

  const results = [];
  while (queue.length > 0) {
    const current = queue.shift();
    let stat;
    try {
      stat = await fs.stat(current);
    } catch (_error) {
      continue;
    }
    if (!stat.isDirectory()) continue;

    let dirEntries = [];
    try {
      dirEntries = await fs.readdir(current, { withFileTypes: true });
    } catch (_error) {
      continue;
    }

    dirEntries.forEach((entry) => {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) {
        if (entry.name === 'node_modules' || entry.name === '.git') return;
        queue.push(fullPath);
        return;
      }
      if (!entry.isFile()) return;
      if (!/\.pdf$/i.test(entry.name)) return;
      const relative = path.relative(baseDir, fullPath).split(path.sep).join('/');
      const normalized = normalizePdfStem(entry.name);
      results.push({
        fileName: entry.name,
        absolutePath: fullPath,
        relativePath: relative,
        normalized,
      });
    });
  }
  return results;
};

const scorePdfCandidate = (candidate) => {
  const normalizedName = normalizePdfStem(candidate?.fileName || "");
  const normalizedPath = normalizePdfStem(candidate?.relativePath || "");
  const haystack = `${normalizedName} ${normalizedPath}`.trim();

  let score = 0;
  if (!haystack) return score;

  A701590_SEARCH_PATTERNS.forEach((pattern) => {
    if (haystack.includes(pattern)) score += 70;
  });

  if (haystack.includes('a701590')) score += 120;
  if (haystack.includes('zim')) score += 25;
  if (haystack.includes('antrag')) score += 25;
  if (normalizedName === 'zim antrag a701590') score += 220;
  if (normalizedName.startsWith('zim antrag a701590')) score += 80;
  if (normalizedName === 'a701590') score += 50;

  return score;
};

const resolveA701590Pdf = async (baseDir) => {
  const candidates = await listPdfCandidates(baseDir);
  const ranked = candidates
    .map((candidate) => ({ ...candidate, score: scorePdfCandidate(candidate) }))
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return a.relativePath.localeCompare(b.relativePath, 'de');
    });

  const resolved = ranked.find((entry) => entry.score > 0) || null;
  const debug = {
    candidates: ranked.map((entry) => ({
      path: entry.relativePath,
      normalized: entry.normalized,
      score: entry.score,
    })),
    rejectionReason: resolved ? null : 'Kein PDF mit ausreichender Übereinstimmung zu ZIM-Antrag-A701590 gefunden.',
  };

  return {
    resolved,
    debug,
  };
};

let cachedResolvedMantelbogenPdf = null;

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


app.post("/api/project/save", async (req, res) => {
  try {
    const payload = req.body && typeof req.body === "object" ? req.body : {};
    const projectId = `${payload.projectId || payload.name || ""}`.trim();
    if (!projectId) {
      return res.status(400).json({ error: "projectId fehlt." });
    }

    const dirPath = path.join(__dirname, ".data", "projects");
    await fs.mkdir(dirPath, { recursive: true });
    const filePath = path.join(dirPath, `${encodeURIComponent(projectId)}.json`);
    await fs.writeFile(filePath, JSON.stringify(payload, null, 2), "utf8");

    return res.json({ ok: true, projectId });
  } catch (error) {
    return res.status(500).json({ error: error.message || "Speichern fehlgeschlagen." });
  }
});

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


app.get("/api/zim/mantelbogen/company-one/source", async (_req, res) => {
  try {
    const { resolved, debug } = await resolveA701590Pdf(__dirname);
    cachedResolvedMantelbogenPdf = resolved;

    console.log("Found PDF candidates:", debug.candidates);
    console.log("Resolved A701590 PDF:", resolved?.relativePath || null);

    return res.json({
      found: Boolean(resolved),
      resolvedPath: resolved?.relativePath || null,
      fileName: resolved?.fileName || null,
      normalizedName: resolved?.normalized || null,
      debug,
    });
  } catch (error) {
    return res.status(500).json({ error: error.message || "PDF-Auflösung fehlgeschlagen." });
  }
});

app.get("/api/zim/mantelbogen/company-one/pdf", async (_req, res) => {
  try {
    if (!cachedResolvedMantelbogenPdf) {
      const { resolved } = await resolveA701590Pdf(__dirname);
      cachedResolvedMantelbogenPdf = resolved;
    }

    if (!cachedResolvedMantelbogenPdf) {
      return res.status(404).json({ error: "A701590 PDF nicht gefunden." });
    }

    const absolutePath = cachedResolvedMantelbogenPdf.absolutePath;
    if (!isInsideRoot(__dirname, absolutePath)) {
      return res.status(403).json({ error: "Ungültiger Dateipfad." });
    }

    return res.sendFile(absolutePath);
  } catch (error) {
    return res.status(500).json({ error: error.message || "PDF-Datei konnte nicht geladen werden." });
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
