const express = require("express");
const path = require("path");
const fs = require("fs/promises");
const os = require("os");
const { spawn } = require("child_process");
const { ProjectRepository } = require("./project-repository");

const app = express();
const port = process.env.PORT || 3000;

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL fehlt. Projekte dürfen nicht in einer flüchtigen Container-Datei gespeichert werden.");
}
const { Pool } = require("pg");
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false },
});
const projects = new ProjectRepository(pool);

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
const apiError = (res, error) => res.status(500).json({ error: error.message || "Datenbankfehler." });

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
  if (!validProject(req.body) || !Number.isInteger(req.body.revision)) return res.status(400).json({ error: "Vollständiger State und ganzzahlige revision sind erforderlich." });
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


projects.initialize().then(() => app.listen(port, () => console.log(`Server running on port ${port}`))).catch((error) => {
  console.error("PostgreSQL konnte nicht initialisiert werden", error);
  process.exit(1);
});
