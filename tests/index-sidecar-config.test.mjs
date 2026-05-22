import assert from 'node:assert/strict';
import fs from 'node:fs';
import { test } from 'node:test';

test('startup supervises v27 read model refresh worker', () => {
  const source = fs.readFileSync('src/index.js', 'utf8');

  assert.match(source, /V27_READ_MODEL_REFRESH_WORKER_ENABLED/);
  assert.match(source, /name:\s*'v27-read-model-refresh'/);
  assert.match(source, /scripts\/v27_read_model_refresh\.py/);
  assert.match(source, /'--loop'/);
  assert.match(source, /V27_EVENT_LOG_DIR/);
  assert.match(source, /V27_READ_MODEL_DIR/);
  assert.match(source, /V27_READ_MODEL_REFRESH_LOCK_FILE/);
});
