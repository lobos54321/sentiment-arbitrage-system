import Database from 'better-sqlite3';

export class ExperimentStore {
  constructor(dbPath = process.env.DB_PATH || './data/sentiment_arb.db') {
    this.db = new Database(dbPath);
    this.init();
  }

  init() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS strategy_experiments (
        candidate_id TEXT PRIMARY KEY,
        parent_id TEXT,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        created_by TEXT NOT NULL,
        config_version INTEGER NOT NULL,
        mutation_set_json TEXT,
        dataset_refs_json TEXT,
        metrics_json TEXT,
        guardrail_results_json TEXT,
        strategy_config_json TEXT,
        notes TEXT,
        promoted_at TEXT,
        retired_at TEXT,
        qualified_at TEXT,
        activated_at TEXT,
        paused_at TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_strategy_experiments_status ON strategy_experiments(status);
      CREATE INDEX IF NOT EXISTS idx_strategy_experiments_created_at ON strategy_experiments(created_at DESC);
    `);

    const columns = this.db.prepare(`PRAGMA table_info(strategy_experiments)`).all();
    const columnNames = new Set(columns.map((column) => column.name));
    if (!columnNames.has('qualified_at')) {
      this.db.exec(`ALTER TABLE strategy_experiments ADD COLUMN qualified_at TEXT`);
    }
    if (!columnNames.has('activated_at')) {
      this.db.exec(`ALTER TABLE strategy_experiments ADD COLUMN activated_at TEXT`);
    }
    if (!columnNames.has('paused_at')) {
      this.db.exec(`ALTER TABLE strategy_experiments ADD COLUMN paused_at TEXT`);
    }
  }

  upsert(candidate) {
    this.db.prepare(`
      INSERT INTO strategy_experiments (
        candidate_id, parent_id, status, created_at, created_by, config_version,
        mutation_set_json, dataset_refs_json, metrics_json, guardrail_results_json,
        strategy_config_json, notes, promoted_at, retired_at, qualified_at, activated_at, paused_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(candidate_id) DO UPDATE SET
        parent_id = excluded.parent_id,
        status = excluded.status,
        mutation_set_json = excluded.mutation_set_json,
        dataset_refs_json = excluded.dataset_refs_json,
        metrics_json = excluded.metrics_json,
        guardrail_results_json = excluded.guardrail_results_json,
        strategy_config_json = excluded.strategy_config_json,
        notes = excluded.notes,
        promoted_at = excluded.promoted_at,
        retired_at = excluded.retired_at,
        qualified_at = excluded.qualified_at,
        activated_at = excluded.activated_at,
        paused_at = excluded.paused_at
    `).run(
      candidate.id,
      candidate.parentId,
      candidate.status,
      candidate.createdAt,
      candidate.createdBy,
      candidate.configVersion,
      JSON.stringify(candidate.mutationSet || []),
      JSON.stringify(candidate.datasetRefs || []),
      JSON.stringify(candidate.metrics || {}),
      JSON.stringify(candidate.guardrailResults || {}),
      JSON.stringify(candidate.strategyConfig || {}),
      candidate.notes || null,
      candidate.promotedAt || null,
      candidate.retiredAt || null,
      candidate.qualifiedAt || null,
      candidate.activatedAt || null,
      candidate.pausedAt || null
    );
    return candidate;
  }

  get(candidateId) {
    const row = this.db.prepare(`SELECT * FROM strategy_experiments WHERE candidate_id = ?`).get(candidateId);
    return row ? this.#hydrate(row) : null;
  }

  list(limit = 50) {
    return this.db.prepare(`SELECT * FROM strategy_experiments ORDER BY created_at DESC LIMIT ?`).all(limit).map((row) => this.#hydrate(row));
  }

  getLeaderboard(limit = 10) {
    return this.db.prepare(`
      SELECT candidate_id, status, metrics_json, guardrail_results_json, created_at, promoted_at
      FROM strategy_experiments
      ORDER BY json_extract(metrics_json, '$.comparisonToBaseline') DESC,
               json_extract(metrics_json, '$.expectancy') DESC
      LIMIT ?
    `).all(limit).map((row) => ({
      candidateId: row.candidate_id,
      status: row.status,
      metrics: JSON.parse(row.metrics_json || '{}'),
      guardrailResults: JSON.parse(row.guardrail_results_json || '{}'),
      createdAt: row.created_at,
      promotedAt: row.promoted_at
    }));
  }

  #hydrate(row) {
    return {
      id: row.candidate_id,
      parentId: row.parent_id,
      status: row.status,
      createdAt: row.created_at,
      createdBy: row.created_by,
      configVersion: row.config_version,
      mutationSet: JSON.parse(row.mutation_set_json || '[]'),
      datasetRefs: JSON.parse(row.dataset_refs_json || '[]'),
      metrics: JSON.parse(row.metrics_json || '{}'),
      guardrailResults: JSON.parse(row.guardrail_results_json || '{}'),
      strategyConfig: JSON.parse(row.strategy_config_json || '{}'),
      notes: row.notes,
      promotedAt: row.promoted_at,
      retiredAt: row.retired_at,
      qualifiedAt: row.qualified_at,
      activatedAt: row.activated_at,
      pausedAt: row.paused_at
    };
  }
}

export default ExperimentStore;
