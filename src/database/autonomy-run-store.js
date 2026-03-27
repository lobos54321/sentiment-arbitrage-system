import Database from 'better-sqlite3';

export class AutonomyRunStore {
  constructor(dbPath = process.env.DB_PATH || './data/sentiment_arb.db') {
    this.db = new Database(dbPath);
    this.init();
  }

  init() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS autonomy_runs (
        run_id TEXT PRIMARY KEY,
        started_at TEXT NOT NULL,
        ended_at TEXT,
        trigger_source TEXT,
        trigger_event_id TEXT,
        state TEXT,
        stage_name TEXT,
        tasks_json TEXT,
        research_summary TEXT,
        candidate_ids_json TEXT,
        promotion_decision_json TEXT,
        machine_summary_json TEXT,
        errors_json TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      );
    `);

    const columns = this.db.prepare(`PRAGMA table_info(autonomy_runs)`).all();
    const columnNames = new Set(columns.map((column) => column.name));
    if (!columnNames.has('trigger_event_id')) {
      this.db.exec(`ALTER TABLE autonomy_runs ADD COLUMN trigger_event_id TEXT`);
    }
    if (!columnNames.has('stage_name')) {
      this.db.exec(`ALTER TABLE autonomy_runs ADD COLUMN stage_name TEXT`);
    }
    if (!columnNames.has('machine_summary_json')) {
      this.db.exec(`ALTER TABLE autonomy_runs ADD COLUMN machine_summary_json TEXT`);
    }

    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_autonomy_runs_started_at ON autonomy_runs(started_at DESC);
      CREATE INDEX IF NOT EXISTS idx_autonomy_runs_event_id ON autonomy_runs(trigger_event_id, started_at DESC);
    `);
  }

  startRun(run) {
    this.db.prepare(`
      INSERT INTO autonomy_runs (
        run_id, started_at, trigger_source, trigger_event_id, state, stage_name,
        tasks_json, candidate_ids_json, machine_summary_json, errors_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      run.runId,
      run.startedAt,
      run.trigger || 'manual',
      run.triggerEventId || null,
      run.state || 'started',
      run.stageName || null,
      JSON.stringify(run.tasks || []),
      JSON.stringify(run.candidateIds || []),
      JSON.stringify(run.machineSummary || null),
      JSON.stringify(run.errors || [])
    );
    return run;
  }

  finishRun(runId, patch = {}) {
    this.db.prepare(`
      UPDATE autonomy_runs
      SET ended_at = ?,
          state = ?,
          stage_name = ?,
          tasks_json = ?,
          research_summary = ?,
          candidate_ids_json = ?,
          promotion_decision_json = ?,
          machine_summary_json = ?,
          errors_json = ?
      WHERE run_id = ?
    `).run(
      patch.endedAt || new Date().toISOString(),
      patch.state || 'completed',
      patch.stageName || null,
      JSON.stringify(patch.tasks || []),
      patch.researchSummary || null,
      JSON.stringify(patch.candidateIds || []),
      JSON.stringify(patch.promotionDecision || null),
      JSON.stringify(patch.machineSummary || null),
      JSON.stringify(patch.errors || []),
      runId
    );
  }

  getLatest(limit = 10) {
    return this.db.prepare(`SELECT * FROM autonomy_runs ORDER BY started_at DESC LIMIT ?`).all(limit).map((row) => this.#hydrate(row));
  }

  getById(runId) {
    const row = this.db.prepare(`SELECT * FROM autonomy_runs WHERE run_id = ?`).get(runId);
    return row ? this.#hydrate(row) : null;
  }

  #hydrate(row) {
    return {
      runId: row.run_id,
      startedAt: row.started_at,
      endedAt: row.ended_at,
      trigger: row.trigger_source,
      triggerEventId: row.trigger_event_id,
      state: row.state,
      stageName: row.stage_name,
      tasks: JSON.parse(row.tasks_json || '[]'),
      researchSummary: row.research_summary,
      candidateIds: JSON.parse(row.candidate_ids_json || '[]'),
      promotionDecision: JSON.parse(row.promotion_decision_json || 'null'),
      machineSummary: JSON.parse(row.machine_summary_json || 'null'),
      errors: JSON.parse(row.errors_json || '[]')
    };
  }
}

export default AutonomyRunStore;
