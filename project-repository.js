const PROJECT_SCHEMA_VERSION = 1;

class ProjectRepository {
  constructor(pool) {
    this.pool = pool;
  }

  async inspectAndInitialize() {
    const inspection = await this.pool.query("SELECT to_regclass('public.projects') AS table_name");
    const tableExisted = Boolean(inspection.rows[0]?.table_name);
    let projectCount = 0;
    if (tableExisted) {
      const count = await this.pool.query("SELECT COUNT(*)::integer AS count FROM projects");
      projectCount = Number(count.rows[0].count);
    }
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS projects (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        revision INTEGER NOT NULL DEFAULT 1,
        schema_version INTEGER NOT NULL DEFAULT 1,
        state JSONB NOT NULL
      )
    `);
    await this.pool.query("CREATE INDEX IF NOT EXISTS projects_updated_at_idx ON projects (updated_at DESC)");
    return { tableExisted, projectCount };
  }

  summary(row) {
    return {
      projectId: row.id,
      name: row.name,
      createdAt: new Date(row.created_at).toISOString(),
      lastModified: new Date(row.updated_at).toISOString(),
      revision: Number(row.revision),
      schemaVersion: Number(row.schema_version),
    };
  }

  complete(row) {
    return { ...this.summary(row), state: row.state };
  }

  async list() {
    const { rows } = await this.pool.query("SELECT id, name, created_at, updated_at, revision, schema_version FROM projects ORDER BY updated_at DESC");
    return rows.map((row) => this.summary(row));
  }

  async get(id) {
    const { rows } = await this.pool.query("SELECT * FROM projects WHERE id = $1", [id]);
    return rows[0] ? this.complete(rows[0]) : null;
  }

  async create(payload) {
    const now = new Date().toISOString();
    const { rows } = await this.pool.query(
      `INSERT INTO projects (id, name, created_at, updated_at, revision, schema_version, state)
       VALUES ($1, $2, $3, $4, 1, $5, $6::jsonb)
       ON CONFLICT (id) DO NOTHING RETURNING *`,
      [payload.projectId, payload.name, payload.createdAt || now, payload.lastModified || now, payload.schemaVersion || PROJECT_SCHEMA_VERSION, JSON.stringify(payload.state)]
    );
    return rows[0] ? this.complete(rows[0]) : null;
  }

  async update(id, payload) {
    const { rows } = await this.pool.query(
      `UPDATE projects SET name = $2, state = $3::jsonb, schema_version = $4,
       revision = revision + 1, updated_at = NOW()
       WHERE id = $1 AND revision = $5 RETURNING *`,
      [id, payload.name || id, JSON.stringify(payload.state), payload.schemaVersion || PROJECT_SCHEMA_VERSION, payload.revision]
    );
    if (rows[0]) return { project: this.complete(rows[0]), conflict: false };
    const current = await this.get(id);
    return { project: current, conflict: Boolean(current) };
  }

  async replace(id, payload) {
    const current = await this.get(id);
    if (!current) return this.create({ ...payload, projectId: id });
    return (await this.update(id, { ...payload, revision: current.revision })).project;
  }

  async delete(id) {
    return (await this.pool.query("DELETE FROM projects WHERE id = $1 RETURNING id", [id])).rowCount > 0;
  }

  async allComplete() {
    const { rows } = await this.pool.query("SELECT * FROM projects ORDER BY updated_at DESC");
    return rows.map((row) => this.complete(row));
  }
}

module.exports = { ProjectRepository, PROJECT_SCHEMA_VERSION };
