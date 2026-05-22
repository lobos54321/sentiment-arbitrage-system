import dotenv from 'dotenv';
import { quarantineLiveSecretsForPaperMode } from './v27-paper-mode-safety.js';

dotenv.config();
process.env.V27_DOTENV_ALREADY_LOADED = '1';

const result = quarantineLiveSecretsForPaperMode({
  env: process.env,
  reason: 'node_preload_before_app_import',
});

if (result.quarantine_applied) {
  console.error(
    `[v27-paper-mode-preload] quarantined live secret names before app import: ${result.quarantined_live_secret_names.join(',')}`
  );
}
