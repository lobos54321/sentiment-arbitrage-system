/** Shared declarative numeric-evidence schema for evaluator manifests. */

import fs from 'fs';
import { createHash } from 'crypto';
import { dirname, resolve } from 'path';
import { fileURLToPath } from 'url';


export const EVIDENCE_SCHEMA_VERSION_FIELD = 'numeric_evidence_schema_version';
export const EVIDENCE_SCHEMA_SHA256_FIELD = 'numeric_evidence_schema_sha256';
export const EVIDENCE_SCHEMA_VALIDATED_FIELD = (
  'numeric_evidence_schema_validated_before_publish'
);

export const EVIDENCE_SCHEMA_PATH = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '../../config/evaluator-snapshot-evidence-schema.json',
);

export function canonicalEvidenceJson(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalEvidenceJson(item)).join(',')}]`;
  }
  const keys = Object.keys(value).sort();
  return `{${keys
    .map((key) => `${JSON.stringify(key)}:${canonicalEvidenceJson(value[key])}`)
    .join(',')}}`;
}

function failSchema(message) {
  throw new Error(`evaluator evidence schema ${message}`);
}

function validateSelectors(rule, category) {
  let selectorCount = 0;
  if (Object.hasOwn(rule, 'suffixes')) {
    failSchema(`${category}.${rule.id} suffix selectors forbidden`);
  }
  for (const field of ['path_patterns', 'fields', 'parent_path_patterns']) {
    const values = rule[field];
    if (values == null) continue;
    if (
      !Array.isArray(values)
      || values.length === 0
      || values.some((item) => typeof item !== 'string' || item.length === 0)
    ) failSchema(`${category}.${rule.id}.${field} invalid`);
    if (field !== 'parent_path_patterns') selectorCount += values.length;
  }
  if (rule.fields != null && rule.parent_path_patterns == null) {
    failSchema(`${category}.${rule.id} field selectors require parent path patterns`);
  }
  if (rule.parent_path_patterns != null && rule.fields == null) {
    failSchema(`${category}.${rule.id} parent path patterns require field selectors`);
  }
  if (rule.parent_path_patterns?.includes('*')) {
    failSchema(`${category}.${rule.id} unbounded parent path forbidden`);
  }
  if (selectorCount === 0) failSchema(`${category}.${rule.id} has no selector`);
}

function finiteSchemaBound(value, field) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    failSchema(`${field} must be finite numeric`);
  }
  return value;
}

function loadEvidenceSchema() {
  let payload;
  try {
    payload = JSON.parse(fs.readFileSync(EVIDENCE_SCHEMA_PATH, 'utf8'));
  } catch (error) {
    failSchema(`unreadable: ${error?.name || 'Error'}:${error?.message || error}`);
  }
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    failSchema('root must be an object');
  }
  if (payload.schema_version !== 'evaluator_snapshot_numeric_evidence.v3') {
    failSchema('version invalid');
  }
  if (payload.unknown_numeric_policy !== 'reject') {
    failSchema('must reject unknown numerics');
  }
  if (
    payload.selector_policy
    !== 'explicit_path_or_field_with_parent_path_only'
  ) {
    failSchema('selector policy invalid');
  }
  if (
    payload.path_syntax
    !== 'dot_segments_with_single_segment_wildcards_array_suffixes_and_root_dollar'
  ) failSchema('path syntax invalid');

  const seenIds = new Set();
  for (const category of ['container_rules', 'scalar_rules']) {
    const rules = payload[category];
    if (!Array.isArray(rules) || rules.length === 0) failSchema(`${category} invalid`);
    for (const rule of rules) {
      if (!rule || typeof rule !== 'object' || Array.isArray(rule)) {
        failSchema(`${category} rule invalid`);
      }
      if (
        typeof rule.id !== 'string'
        || rule.id.length === 0
        || seenIds.has(rule.id)
      ) failSchema(`rule id invalid: ${rule.id}`);
      seenIds.add(rule.id);
      validateSelectors(rule, category);
      let boundPrefix;
      if (category === 'container_rules') {
        if (!['object', 'array'].includes(rule.container_type)) {
          failSchema(`container type invalid: ${rule.id}`);
        }
        if (
          rule.element_kind != null
          && !['safe_integer', 'finite_number'].includes(rule.element_kind)
        ) failSchema(`element kind invalid: ${rule.id}`);
        if (rule.element_kind != null && typeof rule.element_nullable !== 'boolean') {
          failSchema(`element nullability invalid: ${rule.id}`);
        }
        boundPrefix = 'element_';
      } else {
        if (![
          'safe_integer',
          'finite_number',
          'safe_integer_or_decimal_identifier',
          'safe_integer_or_iso8601_timestamp',
          'finite_number_or_iso8601_timestamp',
          'finite_number_or_evidence_timestamp',
        ].includes(rule.kind)) {
          failSchema(`scalar kind invalid: ${rule.id}`);
        }
        if (typeof rule.nullable !== 'boolean') {
          failSchema(`nullability invalid: ${rule.id}`);
        }
        if (
          rule.null_policy != null
          && ![
            'watermark_time_set_may_be_null_only_when_empty_or_alternate_present',
            'watermark_table_may_be_null_only_when_rows_copied_zero',
          ].includes(rule.null_policy)
        ) failSchema(`null policy invalid: ${rule.id}`);
        if (rule.null_policy != null && rule.nullable !== true) {
          failSchema(`null policy requires nullable rule: ${rule.id}`);
        }
        boundPrefix = '';
      }
      const minimum = rule[`${boundPrefix}minimum`];
      const maximum = rule[`${boundPrefix}maximum`];
      if (minimum != null) finiteSchemaBound(minimum, `${rule.id}.${boundPrefix}minimum`);
      if (maximum != null) finiteSchemaBound(maximum, `${rule.id}.${boundPrefix}maximum`);
      if (minimum != null && maximum != null && minimum > maximum) {
        failSchema(`bounds inverted: ${rule.id}`);
      }
    }
  }
  return payload;
}

export const EVIDENCE_SCHEMA = loadEvidenceSchema();
export const EVIDENCE_SCHEMA_VERSION = EVIDENCE_SCHEMA.schema_version;
export const EVIDENCE_SCHEMA_SHA256 = createHash('sha256')
  .update(canonicalEvidenceJson(EVIDENCE_SCHEMA))
  .digest('hex');
export const JSON_NUMERIC_EVIDENCE_CONTRACT_SHA256 = EVIDENCE_SCHEMA_SHA256;
export const EVIDENCE_SCHEMA_FIELD_NAMES = new Set(
  [...EVIDENCE_SCHEMA.container_rules, ...EVIDENCE_SCHEMA.scalar_rules]
    .flatMap((rule) => rule.fields || []),
);

export function isJsonSafeInteger(value) {
  return Number.isSafeInteger(value);
}

export function isJsonFiniteNumber(value) {
  return typeof value === 'number'
    && Number.isFinite(value)
    && Math.abs(value) <= Number.MAX_SAFE_INTEGER;
}

const ISO8601_TIMESTAMP_RE = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:\d{2})$/;
const SQLITE_UTC_TIMESTAMP_RE = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$/;
const DECIMAL_IDENTIFIER_RE = /^(?:0|[1-9]\d{0,15})$/;

function validTimestampParts(year, month, day, hour, minute, second) {
  if (year < 1970 || month < 1 || month > 12 || hour > 23 || minute > 59 || second > 59) {
    return false;
  }
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return day >= 1 && day <= days[month - 1];
}

export function isIso8601Timestamp(value) {
  if (typeof value !== 'string') return false;
  const match = value.match(ISO8601_TIMESTAMP_RE);
  if (!match) return false;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, , zone] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  if (!validTimestampParts(year, month, day, hour, minute, second)) return false;
  if (zone !== 'Z') {
    const offsetHour = Number(zone.slice(1, 3));
    const offsetMinute = Number(zone.slice(4, 6));
    if (offsetHour > 23 || offsetMinute > 59) return false;
  }
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) && timestamp >= 0;
}

export function isSqliteUtcTimestamp(value) {
  if (typeof value !== 'string') return false;
  const match = value.match(SQLITE_UTC_TIMESTAMP_RE);
  if (!match) return false;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText] = match;
  const parts = [yearText, monthText, dayText, hourText, minuteText, secondText]
    .map((item) => Number(item));
  if (!validTimestampParts(...parts)) return false;
  const timestamp = Date.parse(`${value.replace(' ', 'T')}Z`);
  return Number.isFinite(timestamp) && timestamp >= 0;
}

export function isEvidenceTimestamp(value) {
  return isIso8601Timestamp(value) || isSqliteUtcTimestamp(value);
}

export function isDecimalIdentifier(value) {
  return typeof value === 'string'
    && DECIMAL_IDENTIFIER_RE.test(value)
    && BigInt(value) <= BigInt(Number.MAX_SAFE_INTEGER);
}

function pathMatches(pattern, path) {
  if (pattern === '$') return path === '';
  const expected = pattern ? pattern.split('.') : [];
  const actual = path ? path.split('.') : [];
  return expected.length === actual.length && expected.every(
    (wanted, index) => wanted === '*' || wanted === actual[index],
  );
}

function ruleMatches(rule, path, field) {
  const dot = path.lastIndexOf('.');
  const parentPath = dot < 0 ? '' : path.slice(0, dot);
  return Boolean(
    (rule.path_patterns || []).some((pattern) => pathMatches(pattern, path))
    || (
      (rule.fields || []).includes(field)
      && (rule.parent_path_patterns || []).some(
        (pattern) => pathMatches(pattern, parentPath),
      )
    )
  );
}

export function numericEvidenceRule(path, field = null) {
  const pathLeaf = String(path.split('.').at(-1) ?? '');
  if (field != null && String(field) !== pathLeaf) return null;
  const leaf = pathLeaf;
  for (const rule of EVIDENCE_SCHEMA.container_rules) {
    if (ruleMatches(rule, path, leaf)) return ['container', rule];
  }
  for (const rule of EVIDENCE_SCHEMA.scalar_rules) {
    if (ruleMatches(rule, path, leaf)) return ['scalar', rule];
  }
  return null;
}

function validateNumber(value, rule, element = false) {
  const prefix = element ? 'element_' : '';
  const kind = rule[`${prefix}kind`];
  const nullable = rule[`${prefix}nullable`];
  if (value === null) return nullable ? null : 'null_not_allowed';
  if (kind.endsWith('_or_iso8601_timestamp') && isIso8601Timestamp(value)) {
    return null;
  }
  if (kind.endsWith('_or_evidence_timestamp') && isEvidenceTimestamp(value)) {
    return null;
  }
  let numericValue = value;
  if (kind === 'safe_integer_or_decimal_identifier') {
    if (isDecimalIdentifier(value)) numericValue = Number(value);
    else if (!Number.isSafeInteger(value)) {
      return 'expected_json_safe_integer_or_decimal_identifier';
    }
  } else if (kind.startsWith('safe_integer')) {
    if (!Number.isSafeInteger(value)) return 'expected_json_safe_integer';
  } else if (!isJsonFiniteNumber(value)) {
    return 'expected_finite_json_number';
  }
  const minimum = rule[`${prefix}minimum`];
  const maximum = rule[`${prefix}maximum`];
  if (minimum != null && numericValue < minimum) return 'below_minimum';
  if (maximum != null && numericValue > maximum) return 'above_maximum';
  return null;
}

function conditionalNullAllowed(payload, path, rule) {
  if (rule.null_policy == null) return true;
  const segments = path.split('.');
  if (
    segments.length !== 5
    || segments[0] !== 'databases'
    || !['source_upper_watermarks', 'upper_watermarks'].includes(segments[2])
    || !payload
    || typeof payload !== 'object'
    || Array.isArray(payload)
  ) return false;
  const databases = payload.databases;
  if (!databases || typeof databases !== 'object' || Array.isArray(databases)) {
    return false;
  }
  const database = databases[segments[1]];
  if (!database || typeof database !== 'object' || Array.isArray(database)) {
    return false;
  }
  const selectedTables = database.selected_tables;
  if (!selectedTables || typeof selectedTables !== 'object' || Array.isArray(selectedTables)) {
    return false;
  }
  const selected = selectedTables[segments[3]];
  if (!selected || typeof selected !== 'object' || Array.isArray(selected)) return false;
  const tableEmpty = Number.isSafeInteger(selected.rows_copied)
    && selected.rows_copied === 0;
  if (
    rule.null_policy
    === 'watermark_table_may_be_null_only_when_rows_copied_zero'
  ) return tableEmpty;
  let timeColumns = selected.time_columns;
  if (
    !Array.isArray(timeColumns)
    || !timeColumns.every((item) => typeof item === 'string' && item.length > 0)
  ) {
    const selectedTimeColumn = selected.time_column || selected.indexed_time_anchor;
    timeColumns = selectedTimeColumn ? [selectedTimeColumn] : [];
  }
  if (!timeColumns.includes(segments[4])) return true;
  if (tableEmpty) return true;
  const watermarkCopies = database[segments[2]];
  if (!watermarkCopies || typeof watermarkCopies !== 'object' || Array.isArray(watermarkCopies)) {
    return false;
  }
  const tableWatermarks = watermarkCopies[segments[3]];
  if (!tableWatermarks || typeof tableWatermarks !== 'object' || Array.isArray(tableWatermarks)) {
    return false;
  }
  return timeColumns.some(
    (candidate) => candidate !== segments[4]
      && (
        (
          isJsonFiniteNumber(tableWatermarks[candidate])
        )
        || isEvidenceTimestamp(tableWatermarks[candidate])
      ),
  );
}

export function validateNumericEvidenceValue(payload, path, value) {
  let matched = numericEvidenceRule(path);
  let element = false;
  if (!matched) {
    const parentPath = path.endsWith('[]')
      ? path.slice(0, -2)
      : path.slice(0, Math.max(0, path.lastIndexOf('.')));
    const parent = numericEvidenceRule(parentPath);
    if (parent?.[0] === 'container' && parent[1].element_kind != null) {
      matched = parent;
      element = true;
    }
  }
  if (!matched || !['scalar', 'container'].includes(matched[0])) {
    return {
      accepted: false,
      path,
      code: 'undeclared_numeric_evidence',
      rule_id: null,
    };
  }
  const rule = matched[1];
  let code;
  if (value === null && !conditionalNullAllowed(payload, path, rule)) {
    code = 'null_condition_failed';
  } else {
    code = validateNumber(value, rule, element);
  }
  return {
    accepted: code == null,
    path,
    code: code ?? null,
    rule_id: rule.id,
  };
}

export function numericEvidenceSchemaBindingValid(payload) {
  return Boolean(
    payload
    && typeof payload === 'object'
    && !Array.isArray(payload)
    && payload[EVIDENCE_SCHEMA_VERSION_FIELD] === EVIDENCE_SCHEMA_VERSION
    && payload[EVIDENCE_SCHEMA_SHA256_FIELD] === EVIDENCE_SCHEMA_SHA256
    && payload[EVIDENCE_SCHEMA_VALIDATED_FIELD] === true
  );
}

export function validateNumericEvidenceSchema(
  payload,
  { requireBinding = false, maxErrors = 256 } = {},
) {
  const errors = [];
  let errorCount = 0;
  let numericLeafCount = 0;
  let declaredNumericLeafCount = 0;
  const ruleMatchCounts = {};

  const reject = (path, code, ruleId = null) => {
    errorCount += 1;
    if (errors.length < Math.max(1, Number(maxErrors) || 1)) {
      const error = { path: path || '$', code };
      if (ruleId) error.rule_id = ruleId;
      errors.push(error);
    }
  };
  const validateDeclared = (value, path, rule, element = false) => {
    ruleMatchCounts[rule.id] = (ruleMatchCounts[rule.id] || 0) + 1;
    if (value === null && !conditionalNullAllowed(payload, path, rule)) {
      reject(path, 'null_condition_failed', rule.id);
      return;
    }
    if (typeof value === 'number') declaredNumericLeafCount += 1;
    const code = validateNumber(value, rule, element);
    if (code) reject(path, code, rule.id);
  };
  const visit = (value, path) => {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      for (const [field, child] of Object.entries(value)) {
        const childPath = path ? `${path}.${field}` : field;
        const matched = numericEvidenceRule(childPath, field);
        if (matched?.[0] === 'container') {
          const rule = matched[1];
          const shapeValid = rule.container_type === 'object'
            ? Boolean(child && typeof child === 'object' && !Array.isArray(child))
            : Array.isArray(child);
          if (!shapeValid) {
            reject(childPath, `expected_${rule.container_type}`, rule.id);
            continue;
          }
          ruleMatchCounts[rule.id] = (ruleMatchCounts[rule.id] || 0) + 1;
          if (rule.element_kind == null) {
            visit(child, childPath);
            continue;
          }
          const items = Array.isArray(child)
            ? child.map((element, index) => [index, element])
            : Object.entries(child);
          for (const [elementKey, element] of items) {
            const elementPath = Array.isArray(child)
              ? `${childPath}[]`
              : `${childPath}.${elementKey}`;
            if (element && typeof element === 'object') {
              reject(elementPath, 'numeric_container_element_not_scalar', rule.id);
            } else {
              if (typeof element === 'number') numericLeafCount += 1;
              validateDeclared(element, elementPath, rule, true);
            }
          }
          continue;
        }
        if (matched?.[0] === 'scalar') {
          if (typeof child === 'number') numericLeafCount += 1;
          validateDeclared(child, childPath, matched[1]);
          continue;
        }
        if (EVIDENCE_SCHEMA_FIELD_NAMES.has(field)) {
          reject(childPath, 'declared_numeric_field_parent_path_mismatch');
          continue;
        }
        if (child && typeof child === 'object') {
          visit(child, childPath);
        } else if (typeof child === 'number') {
          numericLeafCount += 1;
          reject(childPath, 'undeclared_numeric_evidence');
        }
      }
      return;
    }
    if (Array.isArray(value)) {
      for (const child of value) {
        const childPath = `${path}[]`;
        if (child && typeof child === 'object') visit(child, childPath);
        else if (typeof child === 'number') {
          numericLeafCount += 1;
          reject(childPath, 'undeclared_numeric_evidence');
        }
      }
      return;
    }
    reject(path, 'root_not_object');
  };

  if (requireBinding && !numericEvidenceSchemaBindingValid(payload)) {
    reject('$', 'schema_binding_invalid');
  }
  visit(payload, '');
  return {
    accepted: errorCount === 0,
    schema_version: EVIDENCE_SCHEMA_VERSION,
    schema_sha256: EVIDENCE_SCHEMA_SHA256,
    binding_valid: numericEvidenceSchemaBindingValid(payload),
    numeric_leaf_count: numericLeafCount,
    declared_numeric_leaf_count: declaredNumericLeafCount,
    error_count: errorCount,
    errors,
    rule_match_counts: Object.fromEntries(
      Object.entries(ruleMatchCounts).sort(([left], [right]) => left.localeCompare(right)),
    ),
  };
}

export function jsonNumericEvidenceTypesValid(payload, options = {}) {
  return validateNumericEvidenceSchema(payload, {
    requireBinding: Boolean(options.requireBinding),
    maxErrors: 1,
  }).accepted;
}
