import Database from 'better-sqlite3';

const TERMINAL_STATES = new Set(['completed', 'failed', 'dead_letter', 'suppressed']);

export class AutonomyEventStore {
  constructor(dbPath = process.env.DB_PATH || './data/sentiment_arb.db') {
    this.db = new Database(dbPath);
    this.init();
  }

  init() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS autonomy_events (
        event_id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        payload_json TEXT,
        dedupe_key TEXT,
        state TEXT NOT NULL DEFAULT 'pending',
        available_at TEXT NOT NULL,
        cooldown_until TEXT,
        lease_owner TEXT,
        lease_expires_at TEXT,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 5,
        last_error TEXT,
        parent_event_id TEXT,
        run_id TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        completed_at TEXT
      );
      CREATE UNIQUE INDEX IF NOT EXISTS idx_autonomy_events_dedupe_active
        ON autonomy_events(dedupe_key)
        WHERE dedupe_key IS NOT NULL AND state IN ('pending', 'leased');
      CREATE INDEX IF NOT EXISTS idx_autonomy_events_state_available
        ON autonomy_events(state, available_at);
      CREATE INDEX IF NOT EXISTS idx_autonomy_events_type_created
        ON autonomy_events(event_type, created_at DESC);
    `);
  }

  enqueue(event) {
    const now = new Date().toISOString();
    const payload = {
      eventId: event.eventId,
      eventType: event.eventType,
      payload: event.payload || {},
      dedupeKey: event.dedupeKey || null,
      state: event.state || 'pending',
      availableAt: event.availableAt || now,
      cooldownUntil: event.cooldownUntil || null,
      leaseOwner: null,
      leaseExpiresAt: null,
      attempts: event.attempts || 0,
      maxAttempts: event.maxAttempts || 5,
      lastError: event.lastError || null,
      parentEventId: event.parentEventId || null,
      runId: event.runId || null,
      createdAt: event.createdAt || now,
      updatedAt: now,
      completedAt: event.completedAt || null
    };

    if (payload.dedupeKey) {
      const existing = this.db.prepare(`
        SELECT * FROM autonomy_events
        WHERE dedupe_key = ? AND state IN ('pending', 'leased')
        ORDER BY datetime(created_at) DESC
        LIMIT 1
      `).get(payload.dedupeKey);
      if (existing) {
        return this.#hydrate(existing);
      }
    }

    this.db.prepare(`
      INSERT INTO autonomy_events (
        event_id, event_type, payload_json, dedupe_key, state, available_at, cooldown_until,
        lease_owner, lease_expires_at, attempts, max_attempts, last_error, parent_event_id,
        run_id, created_at, updated_at, completed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        payload_json = excluded.payload_json,
        state = excluded.state,
        available_at = excluded.available_at,
        cooldown_until = excluded.cooldown_until,
        attempts = excluded.attempts,
        max_attempts = excluded.max_attempts,
        last_error = excluded.last_error,
        run_id = excluded.run_id,
        updated_at = excluded.updated_at,
        completed_at = excluded.completed_at
    `).run(
      payload.eventId,
      payload.eventType,
      JSON.stringify(payload.payload),
      payload.dedupeKey,
      payload.state,
      payload.availableAt,
      payload.cooldownUntil,
      payload.leaseOwner,
      payload.leaseExpiresAt,
      payload.attempts,
      payload.maxAttempts,
      payload.lastError,
      payload.parentEventId,
      payload.runId,
      payload.createdAt,
      payload.updatedAt,
      payload.completedAt
    );

    return payload;
  }

  leaseNext({ leaseOwner, leaseMs, now = new Date().toISOString() }) {
    const tx = this.db.transaction(() => {
      const row = this.db.prepare(`
        SELECT * FROM autonomy_events
        WHERE state = 'pending'
          AND datetime(available_at) <= datetime(?)
          AND (cooldown_until IS NULL OR datetime(cooldown_until) <= datetime(?))
        ORDER BY datetime(available_at) ASC, datetime(created_at) ASC
        LIMIT 1
      `).get(now, now);

      if (!row) return null;

      const leaseExpiresAt = new Date(Date.now() + leaseMs).toISOString();
      this.db.prepare(`
        UPDATE autonomy_events
        SET state = 'leased',
            lease_owner = ?,
            lease_expires_at = ?,
            updated_at = ?
        WHERE event_id = ?
      `).run(leaseOwner, leaseExpiresAt, now, row.event_id);

      return this.getById(row.event_id);
    });

    return tx();
  }

  recoverExpiredLeases(now = new Date().toISOString()) {
    return this.db.prepare(`
      UPDATE autonomy_events
      SET state = 'pending',
          lease_owner = NULL,
          lease_expires_at = NULL,
          updated_at = ?
      WHERE state = 'leased'
        AND lease_expires_at IS NOT NULL
        AND datetime(lease_expires_at) <= datetime(?)
    `).run(now, now).changes;
  }

  complete(eventId, patch = {}) {
    const now = new Date().toISOString();
    this.db.prepare(`
      UPDATE autonomy_events
      SET state = ?,
          payload_json = ?,
          run_id = ?,
          last_error = ?,
          lease_owner = NULL,
          lease_expires_at = NULL,
          updated_at = ?,
          completed_at = ?
      WHERE event_id = ?
    `).run(
      patch.state || 'completed',
      JSON.stringify(patch.payload || this.getById(eventId)?.payload || {}),
      patch.runId || null,
      patch.lastError || null,
      now,
      now,
      eventId
    );
    return this.getById(eventId);
  }

  fail(eventId, { error, retryAt, maxAttempts } = {}) {
    const current = this.getById(eventId);
    if (!current) return null;
    const now = new Date().toISOString();
    const attempts = Number(current.attempts || 0) + 1;
    const limit = maxAttempts || current.maxAttempts || 5;
    const terminal = attempts >= limit;
    this.db.prepare(`
      UPDATE autonomy_events
      SET state = ?,
          attempts = ?,
          max_attempts = ?,
          last_error = ?,
          available_at = ?,
          lease_owner = NULL,
          lease_expires_at = NULL,
          updated_at = ?,
          completed_at = ?
      WHERE event_id = ?
    `).run(
      terminal ? 'dead_letter' : 'pending',
      attempts,
      limit,
      error || null,
      retryAt || now,
      now,
      terminal ? now : null,
      eventId
    );
    return this.getById(eventId);
  }

  suppress(eventId, reason = 'suppressed') {
    const current = this.getById(eventId);
    if (!current) return null;
    const now = new Date().toISOString();
    this.db.prepare(`
      UPDATE autonomy_events
      SET state = 'suppressed',
          last_error = ?,
          lease_owner = NULL,
          lease_expires_at = NULL,
          updated_at = ?,
          completed_at = ?
      WHERE event_id = ?
    `).run(reason, now, now, eventId);
    return this.getById(eventId);
  }

  countPending(now = new Date().toISOString()) {
    const row = this.db.prepare(`
      SELECT COUNT(*) AS count
      FROM autonomy_events
      WHERE state = 'pending'
        AND datetime(available_at) <= datetime(?)
        AND (cooldown_until IS NULL OR datetime(cooldown_until) <= datetime(?))
    `).get(now, now);
    return Number(row?.count || 0);
  }

  getById(eventId) {
    const row = this.db.prepare(`SELECT * FROM autonomy_events WHERE event_id = ?`).get(eventId);
    return row ? this.#hydrate(row) : null;
  }

  listRecent(limit = 50) {
    return this.db.prepare(`SELECT * FROM autonomy_events ORDER BY datetime(created_at) DESC LIMIT ?`).all(limit).map((row) => this.#hydrate(row));
  }

  getLatestByType(eventType) {
    const row = this.db.prepare(`
      SELECT * FROM autonomy_events
      WHERE event_type = ?
      ORDER BY datetime(created_at) DESC
      LIMIT 1
    `).get(eventType);
    return row ? this.#hydrate(row) : null;
  }

  getLatestTerminalByType(eventType) {
    const states = [...TERMINAL_STATES];
    const row = this.db.prepare(`
      SELECT * FROM autonomy_events
      WHERE event_type = ? AND state IN (${states.map(() => '?').join(',')})
      ORDER BY datetime(updated_at) DESC
      LIMIT 1
    `).get(eventType, ...states);
    return row ? this.#hydrate(row) : null;
  }

  #hydrate(row) {
    return {
      eventId: row.event_id,
      eventType: row.event_type,
      payload: JSON.parse(row.payload_json || '{}'),
      dedupeKey: row.dedupe_key,
      state: row.state,
      availableAt: row.available_at,
      cooldownUntil: row.cooldown_until,
      leaseOwner: row.lease_owner,
      leaseExpiresAt: row.lease_expires_at,
      attempts: row.attempts,
      maxAttempts: row.max_attempts,
      lastError: row.last_error,
      parentEventId: row.parent_event_id,
      runId: row.run_id,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      completedAt: row.completed_at
    };
  }
}

export default AutonomyEventStore;
