const test = require("node:test");
const assert = require("node:assert/strict");

const { ProjectRepository } = require("../project-repository");

test("startup counts an existing projects table before non-destructive initialization", async () => {
  const queries = [];
  const pool = {
    async query(sql) {
      queries.push(sql.replace(/\s+/g, " ").trim());
      if (sql.includes("to_regclass")) return { rows: [{ table_name: "projects" }] };
      if (sql.includes("COUNT(*)")) return { rows: [{ count: 7 }] };
      return { rows: [] };
    },
  };

  const result = await new ProjectRepository(pool).inspectAndInitialize();

  assert.deepEqual(result, { tableExisted: true, projectCount: 7 });
  assert.match(queries[0], /to_regclass/);
  assert.match(queries[1], /COUNT\(\*\)/);
  assert.match(queries[2], /CREATE TABLE IF NOT EXISTS/);
  assert.equal(queries.some((sql) => /DROP TABLE|DELETE FROM/i.test(sql)), false);
});

test("startup creates only a missing table and never invents an existing count", async () => {
  const queries = [];
  const pool = {
    async query(sql) {
      queries.push(sql.replace(/\s+/g, " ").trim());
      if (sql.includes("to_regclass")) return { rows: [{ table_name: null }] };
      return { rows: [] };
    },
  };

  const result = await new ProjectRepository(pool).inspectAndInitialize();

  assert.deepEqual(result, { tableExisted: false, projectCount: 0 });
  assert.equal(queries.some((sql) => sql.includes("COUNT(*)")), false);
  assert.equal(queries.some((sql) => /DROP TABLE|DELETE FROM/i.test(sql)), false);
});
