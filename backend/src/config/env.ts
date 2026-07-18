import dotenv from 'dotenv';
import { z } from 'zod';
import { compactPath, formatZodIssues } from '../core/utils/zod_path.js';

dotenv.config();

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  HOST: z.string().default('0.0.0.0'),
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z.string().url(),
  TIMESCALEDB_URL: z.string().url(),
  REDIS_URL: z.string().url().default('redis://localhost:6379'),
  SOROBAN_RPC_URL: z.string().url(),
  SOROBAN_NETWORK_PASSPHRASE: z.string(),
  CONTRACT_ID: z.string().optional(),
  ADMIN_SECRET_KEY: z.string().optional(),
  JWT_SECRET: z.string().min(32),
  // Legacy informational value; access-token lifetime is now driven by the
  // jittered, keepalive-aligned settings below (issue #59).
  JWT_EXPIRES_IN: z.string().default('15m'),
  JWT_REFRESH_EXPIRES_IN: z.string().default('7d'),
  CHALLENGE_TTL_SECONDS: z.coerce.number().int().positive().default(300),
  // --- Session lifecycle / WebSocket keepalive alignment (issue #59) ---------
  // The WebSocket keepalive interval. The access-token lifetime must comfortably
  // exceed this so a token can never expire silently between two keepalive
  // pings (invariant: token_expiry > last_frame_timestamp + keepalive).
  WS_KEEPALIVE_INTERVAL_SECONDS: z.coerce.number().int().positive().default(60),
  // Base access-token TTL, aligned with the keepalive interval
  // (keepalive * N + buffer). Default 1260s (21m) keeps the token valid across
  // many keepalive windows instead of the old 15m boundary that raced them.
  ACCESS_TOKEN_TTL_SECONDS: z.coerce.number().int().positive().default(1260),
  // Per-token random expiry spread. Each token expires at base + random(0,
  // jitter) seconds so 100k devices do not all expire on the same instant and
  // stampede the auth endpoint. Default spreads load across a 2-minute window.
  ACCESS_TOKEN_JITTER_SECONDS: z.coerce.number().int().nonnegative().default(120),
  // Tokens expired by no more than this are still honoured once, with an
  // `X-Token-Expiring` hint, so a device can refresh asynchronously without
  // dropping the in-flight telemetry frame.
  TOKEN_GRACE_PERIOD_SECONDS: z.coerce.number().int().nonnegative().default(30),
  // When a still-valid token is within this many seconds of expiry, the auth
  // layer asks the client to refresh proactively (via response headers).
  TOKEN_REFRESH_HINT_SECONDS: z.coerce.number().int().nonnegative().default(120),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().url().optional(),
  OTEL_SERVICE_NAME: z.string().default('iot-billing-backend'),
  MAX_PAYLOAD_SIZE_BYTES: z.coerce.number().int().positive().default(65536),
  NONCE_WINDOW_MS: z.coerce.number().int().positive().default(5000),
  LEDGER_START: z.coerce.number().int().nonnegative().default(0),
  LEDGER_SYNC_CONCURRENCY: z.coerce.number().int().positive().default(10),
  SKIP_MIGRATION_ON_STARTUP: z.coerce.boolean().default(true),
  // Telemetry hypertable retention (must match add_retention_policy in
  // timescale_setup.sql) and a safety margin kept clear of the retention
  // boundary. Continuous-aggregate refreshes never touch data older than
  // (retention - margin) days, so a refresh can't race a chunk drop (issue #51).
  TELEMETRY_RETENTION_DAYS: z.coerce.number().int().positive().default(365),
  RETENTION_SAFETY_MARGIN_DAYS: z.coerce.number().int().nonnegative().default(5),
  TELEMETRY_TARGET_CHUNK_SIZE_GB: z.coerce.number().min(1).max(10).default(5),
  TELEMETRY_COMPRESSION_DAYS: z.coerce.number().int().positive().default(7),
  TELEMETRY_NUM_PARTITIONS: z.coerce.number().int().positive().default(8),
  // --- Multi-region replication and disaster recovery (issue #88) -----------
  // The region this instance is serving. Used for metrics labelling and
  // routing decisions. Examples: "us-east-1", "eu-west-1".
  REGION: z.string().default('us-east-1'),
  // Comma-separated list of secondary region names. Empty string disables
  // multi-region mode. Example: "eu-west-1,ap-southeast-1"
  SECONDARY_REGIONS: z.string().default(''),
  // Maximum tolerated replication lag before the region is marked degraded.
  REPLICATION_LAG_WARN_MS: z.coerce.number().int().nonnegative().default(5000),
  REPLICATION_LAG_CRITICAL_MS: z.coerce.number().int().nonnegative().default(30000),
  // How frequently (ms) the replication monitor polls replica health.
  REPLICATION_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(10000),
  // Whether this instance is currently acting as the primary region.
  IS_PRIMARY_REGION: z.coerce.boolean().default(true),
  // Optional connection string for a read-replica / standby database. When
  // set, the replication monitor probes this endpoint to measure lag.
  REPLICA_DATABASE_URL: z.string().url().optional(),
  // Optional secondary Redis URL for cross-region state replication checks.
  REPLICA_REDIS_URL: z.string().url().optional(),
});

export type Env = z.infer<typeof envSchema>;

/** One environment-validation failure, preserving the field, code, and reason. */
export interface EnvValidationIssue {
  path: string;
  code: string;
  message: string;
}

/**
 * Convert a {@link z.ZodError} into one structured entry per issue.
 *
 * Unlike `error.flatten()`, this preserves the issue `code` and the full,
 * compacted path for *every* failure, so no failing field is collapsed away or
 * hidden behind truncation. Callers can log each entry as its own structured
 * record. Built on the shared {@link formatZodIssues} helper.
 */
export function formatEnvIssues(error: z.ZodError): EnvValidationIssue[] {
  return formatZodIssues(error).map(({ path, code, message }) => ({ path, code, message }));
}

// Re-exported so existing callers (and tests) can import the path helper here.
export { compactPath };

let cachedEnv: Env | null = null;

export function loadEnv(): Env {
  if (cachedEnv) return cachedEnv;
  const parsed = envSchema.safeParse(process.env);
  if (!parsed.success) {
    const issues = formatEnvIssues(parsed.error);
    const detail = issues.map((issue) => `  ${issue.path}: ${issue.message} (${issue.code})`);
    throw new Error(['Environment validation failed:', ...detail].join('\n'));
  }
  cachedEnv = parsed.data;
  return cachedEnv;
}

export function getEnv(): Env {
  if (!cachedEnv) return loadEnv();
  return cachedEnv;
}

export function clearEnvCache(): void {
  cachedEnv = null;
}
