const test = require("node:test");
const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");

test("health and projects explain a missing DATABASE_URL without exposing a fallback", async (t) => {
  const port = 31000 + Math.floor(Math.random() * 1000);
  const child = spawn(process.execPath, ["server.js"], {
    cwd: `${__dirname}/..`,
    env: { ...process.env, PORT: `${port}`, DATABASE_URL: "" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  t.after(() => child.kill());

  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("server startup timed out")), 5000);
    child.stdout.on("data", (chunk) => {
      if (`${chunk}`.includes(`Server listening on port ${port}`)) {
        clearTimeout(timeout);
        resolve();
      }
    });
    child.once("exit", (code) => reject(new Error(`server exited with ${code}`)));
  });

  const health = await fetch(`http://127.0.0.1:${port}/api/health`);
  assert.equal(health.status, 503);
  assert.deepEqual(await health.json(), {
    status: "error",
    database: "not_configured",
    message: "DATABASE_URL is not configured.",
  });

  const projects = await fetch(`http://127.0.0.1:${port}/api/projects`);
  assert.equal(projects.status, 503);
  assert.deepEqual(await projects.json(), {
    status: "error",
    database: "not_configured",
    message: "DATABASE_URL is not configured.",
  });
});
