# Raw Dog Label Cleaning Rerun Notes

Status: research baseline to regenerate the clean pack.

## Problem

The frozen raw dog cohort contained rows where recorded peak labels were incompatible with the stored native price path. The failure mode is a unit mix: recorded peak labels can be measured against a USD-denominated path while the stored baseline and raw bars are native/SOL-denominated.

## Rule

Use the stored native path as the first-pass consistency check:

- `clean`: recorded peak multiple is within tolerance of observed native path peak.
- `label_unit_corrupt`: recorded peak multiple exceeds observed native path peak by more than the configured threshold.
- `no_native_bars`: there is no stored native path, so the row goes to quarantine instead of being deleted.

Default threshold: `2x`.

## Output Layers

- `clean-dogs.json`
- `clean-duds.json`
- `quarantine-rows.json`
- `polluted-rows.json`
- `no-bars-rows.json`
- token files for GMGN and chain truth audits

Rows in quarantine remain part of the audit universe until chain truth adjudicates them.

## Tools

- `scripts/run-raw-dog-label-cleaning-audit.js`
- `scripts/build-clean-rawdog-pack.js`
- `scripts/filter-gmgn-touch-by-clean-pack.js`

