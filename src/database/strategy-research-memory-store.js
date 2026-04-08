import Database from 'better-sqlite3';

export class StrategyResearchMemoryStore {
  constructor(dbPath = process.env.DB_PATH || './data/sentiment_arb.db') {
    this.db = new Database(dbPath);
    this.init();
  }

  init() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS strategy_research_memory (
        memory_id TEXT PRIMARY KEY,
        memory_type TEXT NOT NULL,
        title TEXT NOT NULL,
        summary TEXT,
        scope TEXT,
        strategy_id TEXT,
        candidate_id TEXT,
        source_run_id TEXT,
        evidence_json TEXT,
        metrics_json TEXT,
        tags_json TEXT,
        next_actions_json TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_strategy_research_memory_type_updated
        ON strategy_research_memory(memory_type, updated_at DESC);
      CREATE INDEX IF NOT EXISTS idx_strategy_research_memory_status_updated
        ON strategy_research_memory(status, updated_at DESC);
      CREATE INDEX IF NOT EXISTS idx_strategy_research_memory_strategy_updated
        ON strategy_research_memory(strategy_id, updated_at DESC);
    `);
  }

  recordFinding(finding = {}) {
    const now = new Date().toISOString();
    const payload = {
      memoryId: finding.memoryId,
      memoryType: finding.memoryType || 'general',
      title: finding.title || 'Untitled finding',
      summary: finding.summary || null,
      scope: finding.scope || null,
      strategyId: finding.strategyId || null,
      candidateId: finding.candidateId || null,
      sourceRunId: finding.sourceRunId || null,
      evidence: finding.evidence || null,
      metrics: finding.metrics || null,
      tags: Array.isArray(finding.tags) ? finding.tags : [],
      nextActions: Array.isArray(finding.nextActions) ? finding.nextActions : [],
      status: finding.status || 'active',
      createdAt: finding.createdAt || now,
      updatedAt: now
    };

    if (!payload.memoryId) {
      throw new Error('memoryId is required');
    }

    this.db.prepare(`
      INSERT INTO strategy_research_memory (
        memory_id, memory_type, title, summary, scope, strategy_id, candidate_id,
        source_run_id, evidence_json, metrics_json, tags_json, next_actions_json,
        status, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(memory_id) DO UPDATE SET
        memory_type = excluded.memory_type,
        title = excluded.title,
        summary = excluded.summary,
        scope = excluded.scope,
        strategy_id = excluded.strategy_id,
        candidate_id = excluded.candidate_id,
        source_run_id = excluded.source_run_id,
        evidence_json = excluded.evidence_json,
        metrics_json = excluded.metrics_json,
        tags_json = excluded.tags_json,
        next_actions_json = excluded.next_actions_json,
        status = excluded.status,
        updated_at = excluded.updated_at
    `).run(
      payload.memoryId,
      payload.memoryType,
      payload.title,
      payload.summary,
      payload.scope,
      payload.strategyId,
      payload.candidateId,
      payload.sourceRunId,
      JSON.stringify(payload.evidence),
      JSON.stringify(payload.metrics),
      JSON.stringify(payload.tags),
      JSON.stringify(payload.nextActions),
      payload.status,
      payload.createdAt,
      payload.updatedAt
    );

    return this.getById(payload.memoryId);
  }

  listRecent(limit = 10, memoryType = null) {
    const rows = memoryType
      ? this.db.prepare(`
          SELECT * FROM strategy_research_memory
          WHERE memory_type = ?
          ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC
          LIMIT ?
        `).all(memoryType, limit)
      : this.db.prepare(`
          SELECT * FROM strategy_research_memory
          ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC
          LIMIT ?
        `).all(limit);

    return rows.map((row) => this.#hydrate(row));
  }

  getActiveFindings(limit = 10) {
    return this.db.prepare(`
      SELECT * FROM strategy_research_memory
      WHERE status = 'active'
      ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC
      LIMIT ?
    `).all(limit).map((row) => this.#hydrate(row));
  }

  getById(memoryId) {
    const row = this.db.prepare(`
      SELECT * FROM strategy_research_memory WHERE memory_id = ?
    `).get(memoryId);
    return row ? this.#hydrate(row) : null;
  }

  #hydrate(row) {
    return {
      memoryId: row.memory_id,
      memoryType: row.memory_type,
      title: row.title,
      summary: row.summary,
      scope: row.scope,
      strategyId: row.strategy_id,
      candidateId: row.candidate_id,
      sourceRunId: row.source_run_id,
      evidence: JSON.parse(row.evidence_json || 'null'),
      metrics: JSON.parse(row.metrics_json || 'null'),
      tags: JSON.parse(row.tags_json || '[]'),
      nextActions: JSON.parse(row.next_actions_json || '[]'),
      status: row.status,
      createdAt: row.created_at,
      updatedAt: row.updated_at
    };
  }
}

export default StrategyResearchMemoryStore;
