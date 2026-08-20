const express = require("express");
const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { spawn } = require("child_process");
const { ProjectRepository } = require("./project-repository");

const app = express();
const port = process.env.PORT || 3000;
const databaseUrl = process.env.DATABASE_URL;
const pool = databaseUrl ? new (require("pg").Pool)({
  connectionString: databaseUrl,
  ssl: databaseUrl.includes("localhost") ? false : { rejectUnauthorized: false },
  connectionTimeoutMillis: 5000,
}) : null;
const projects = pool ? new ProjectRepository(pool) : null;
let databaseState = databaseUrl ? "connecting" : "not_configured";
let initializationPromise = null;

const safeDatabaseError = (error) => {
  const message = `${error?.message || error?.code || "unknown error"}`.split("\n")[0];
  return message
    .replace(/postgres(?:ql)?:\/\/[^\s]+/gi, "[database URL redacted]")
    .replace(/password\s*[=:]\s*[^\s,;]+/gi, "password=[redacted]")
    .slice(0, 240);
};

if (pool) {
  pool.on("error", (error) => {
    databaseState = "connection_failed";
    console.error(`PostgreSQL pool error: ${safeDatabaseError(error)}`);
  });
}

app.use(express.static(path.join(__dirname, "public")));
app.use(express.json({ limit: "30mb" }));

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


const validProject = (body) => body && typeof body === "object" && body.state && typeof body.state === "object" && !Array.isArray(body.state);
const isConnectionError = (error) => !error?.code
  || `${error.code}`.startsWith("08")
  || ["57P01", "57P02", "57P03", "ECONNREFUSED", "ECONNRESET", "ENOTFOUND", "ETIMEDOUT"].includes(error.code);
const apiError = (res, error) => {
  console.error(`Project database request failed: ${safeDatabaseError(error)}`);
  if (error?.code === "42P01") {
    databaseState = "schema_failed";
    return res.status(503).json({ status: "error", database: "schema_failed", databaseUrlConfigured: Boolean(pool) });
  }
  if (isConnectionError(error)) {
    databaseState = pool ? "connection_failed" : "not_configured";
    return res.status(503).json({ status: "error", database: databaseState, databaseUrlConfigured: Boolean(pool) });
  }
  return res.status(500).json({ status: "error", error: "Backend error while accessing projects." });
};

const initializeDatabase = async () => {
  if (!pool) return false;
  if (databaseState === "connected") return true;
  if (initializationPromise) return initializationPromise;

  databaseState = "connecting";
  console.log("Connecting to PostgreSQL...");
  initializationPromise = (async () => {
    try {
      await pool.query("SELECT 1");
      console.log("PostgreSQL connected");
    } catch (error) {
      databaseState = "connection_failed";
      console.error(`PostgreSQL connection failed: ${safeDatabaseError(error)}`);
      return false;
    }

    try {
      await projects.inspectAndInitialize();
      databaseState = "connected";
      console.log("Projects table ready");
      return true;
    } catch (error) {
      databaseState = "schema_failed";
      console.error(`PostgreSQL schema initialization failed: ${safeDatabaseError(error)}`);
      return false;
    }
  })().finally(() => { initializationPromise = null; });
  return initializationPromise;
};

const requireDatabase = async (_req, res, next) => {
  if (!pool) {
    return res.status(503).json({
      status: "error",
      database: "not_configured",
      databaseUrlConfigured: false,
      message: "DATABASE_URL is not configured for this Railway service.",
    });
  }
  if (await initializeDatabase()) return next();
  return res.status(503).json({
    status: "error",
    database: databaseState,
    databaseUrlConfigured: true,
  });
};

app.get("/api/health", async (_req, res) => {
  if (!pool) return res.status(503).json({
    status: "error",
    database: "not_configured",
    databaseUrlConfigured: false,
    message: "DATABASE_URL is not configured for this Railway service.",
  });
  if (await initializeDatabase()) return res.json({ status: "ok", database: "connected", databaseUrlConfigured: true });
  return res.status(503).json({ status: "error", database: databaseState, databaseUrlConfigured: true });
});

app.use(["/api/projects", "/api/projects-backup"], requireDatabase);

app.get("/api/projects", async (_req, res) => {
  try { res.json(await projects.list()); } catch (error) { apiError(res, error); }
});

app.get("/api/projects/:id", async (req, res) => {
  try {
    const project = await projects.get(req.params.id);
    project ? res.json(project) : res.status(404).json({ error: "Projekt nicht gefunden." });
  } catch (error) { apiError(res, error); }
});

app.post("/api/projects", async (req, res) => {
  if (!validProject(req.body)) return res.status(400).json({ error: "Ein vollständiger Projekt-State fehlt." });
  const projectId = `${req.body.projectId || req.body.name || ""}`.trim();
  if (!projectId) return res.status(400).json({ error: "projectId fehlt." });
  try {
    const created = await projects.create({ ...req.body, projectId, name: `${req.body.name || projectId}`.trim() || projectId });
    created ? res.status(201).json(created) : res.status(409).json({ error: "Projekt bereits vorhanden.", current: await projects.get(projectId) });
  } catch (error) { apiError(res, error); }
});

app.put("/api/projects/:id", async (req, res) => {
  if (!validProject(req.body) || !Number.isInteger(req.body.expectedRevision)) return res.status(400).json({ error: "Vollständiger State und ganzzahlige expectedRevision sind erforderlich." });
  try {
    const result = await projects.update(req.params.id, req.body);
    if (!result.project) return res.status(404).json({ error: "Projekt nicht gefunden." });
    if (result.conflict) return res.status(409).json({ error: "Das Projekt wurde zwischenzeitlich geändert. Bitte neu laden.", current: result.project });
    res.json(result.project);
  } catch (error) { apiError(res, error); }
});

app.delete("/api/projects/:id", async (req, res) => {
  try { (await projects.delete(req.params.id)) ? res.status(204).end() : res.status(404).json({ error: "Projekt nicht gefunden." }); }
  catch (error) { apiError(res, error); }
});

app.get("/api/projects-backup", async (_req, res) => {
  try { res.json({ format: "campusalliance-zim-database-backup", formatVersion: 1, exportedAt: new Date().toISOString(), projects: await projects.allComplete() }); }
  catch (error) { apiError(res, error); }
});

app.post("/api/projects-backup", async (req, res) => {
  const backup = req.body;
  if (!backup || backup.format !== "campusalliance-zim-database-backup" || backup.formatVersion !== 1 || !Array.isArray(backup.projects)) {
    return res.status(400).json({ error: "Ungültiges Datenbank-Backup." });
  }
  try {
    for (const project of backup.projects) {
      if (!validProject(project)) return res.status(400).json({ error: "Ein Projekt im Backup besitzt keinen vollständigen State." });
      await projects.replace(`${project.projectId || project.name}`, project);
    }
    res.json({ ok: true, imported: backup.projects.length });
  } catch (error) { apiError(res, error); }
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


console.log("Server starting...");
console.log(`DATABASE_URL configured: ${databaseUrl ? "yes" : "no"}`);
if (!databaseUrl) console.warn("DATABASE_URL is not configured for this service.");
app.listen(port, () => console.log(`Server listening on port ${port}`));
if (databaseUrl) void initializeDatabase();
