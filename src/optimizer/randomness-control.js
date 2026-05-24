import crypto from 'crypto';

export const STRATEGY_EXPERIMENT_RNG_VERSION = 'v2.7.0.strategy_experiment_rng.v1';
export const STRATEGY_EXPERIMENT_RANDOMIZATION_UNIT = 'strategy_experiment_candidate';

export function canonicalJson(value) {
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(',')}]`;
  }
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

export function sha256Hex(value) {
  return crypto.createHash('sha256').update(canonicalJson(value)).digest('hex');
}

function randomEntropy() {
  return crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(32).toString('hex');
}

function assignmentIdFromSeed(seedHash) {
  return `candidate-${seedHash.slice(0, 8)}-${seedHash.slice(8, 12)}-${seedHash.slice(12, 16)}-${seedHash.slice(16, 20)}-${seedHash.slice(20, 32)}`;
}

export function createStrategyExperimentRandomnessControl({
  assignmentId = null,
  parentId = null,
  createdAt = new Date().toISOString(),
  createdBy = 'strategy-experiment',
  source = 'strategy-experiment',
  entropy = randomEntropy(),
  randomizationUnit = STRATEGY_EXPERIMENT_RANDOMIZATION_UNIT,
  assignedBucket = 'candidate',
} = {}) {
  const seedMaterial = {
    parent_id: parentId,
    created_at: createdAt,
    created_by: createdBy,
    source,
    entropy,
    rng_version: STRATEGY_EXPERIMENT_RNG_VERSION,
  };
  const seedHash = sha256Hex(seedMaterial);
  const resolvedAssignmentId = assignmentId || assignmentIdFromSeed(seedHash);
  const rngSeed = `sha256:${seedHash}`;
  const assignmentHash = sha256Hex({
    assignment_id: resolvedAssignmentId,
    randomization_unit: randomizationUnit,
    rng_seed: rngSeed,
    rng_version: STRATEGY_EXPERIMENT_RNG_VERSION,
    assigned_bucket: assignedBucket,
  });

  return {
    rng_seed: rngSeed,
    rng_version: STRATEGY_EXPERIMENT_RNG_VERSION,
    randomization_unit: randomizationUnit,
    assignment_id: resolvedAssignmentId,
    assignment_status: 'draft',
    randomization_enabled: true,
    deterministic_assignment: false,
    assignment_algorithm: 'seeded_sha256_prng_v1',
    assigned_bucket: assignedBucket,
    assignment_hash: assignmentHash,
    evidence_source: source,
    rng_seed_material_hash: seedHash,
  };
}

export class SeededRng {
  constructor(seed) {
    this.seed = String(seed || '');
    this.counter = 0;
  }

  next() {
    const hash = crypto.createHash('sha256')
      .update(`${this.seed}:${this.counter}`)
      .digest();
    this.counter += 1;
    const value = hash.readUInt32BE(0);
    return value / 0x100000000;
  }

  int(maxExclusive) {
    return Math.floor(this.next() * Math.max(1, maxExclusive));
  }

  pick(items) {
    if (!items.length) return null;
    return items[this.int(items.length)];
  }
}

export function flattenRandomnessControl(randomnessControl) {
  return {
    randomnessControl,
    rng_seed: randomnessControl.rng_seed,
    rng_version: randomnessControl.rng_version,
    randomization_unit: randomnessControl.randomization_unit,
    assignment_id: randomnessControl.assignment_id,
    assignment_status: randomnessControl.assignment_status,
    randomization_enabled: randomnessControl.randomization_enabled,
    deterministic_assignment: randomnessControl.deterministic_assignment,
    assignment_algorithm: randomnessControl.assignment_algorithm,
    assigned_bucket: randomnessControl.assigned_bucket,
    assignment_hash: randomnessControl.assignment_hash,
  };
}
