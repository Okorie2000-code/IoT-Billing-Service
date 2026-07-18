/**
 * ReplicationMonitor — polls replica health and tracks replication lag.
 *
 * Follows the same observability patterns as the existing chaos monitoring
 * and pool metrics collector: a polling loop that updates Prometheus gauges
 * so Grafana dashboards and alerting rules can consume the data without any
 * new infrastructure dependency.
 *
 * Design notes:
 * - No external chaos or replication framework is required.
 * - Health checks are lightweight SQL/Redis pings against the replica URLs
 *   supplied in environment variables (`REPLICA_DATABASE_URL`,
 *   `REPLICA_REDIS_URL`). If no replica URL is configured the monitor is
 *   a no-op and the metrics stay at their initial values.
 * - The polling interval is controlled by `REPLICATION_POLL_INTERVAL_MS`
 *   (default 10 000 ms).
 * - Lag is measured as wall-clock round-trip time to the replica; a value of
 *   -1 means the replica is unreachable.
 * - The module integrates with the existing `src/api/metrics/prometheus.ts`
 *   for all metric updates and never registers its own prom-client metrics.
 *
 * PCI-DSS / SOC2 alignment:
 * - No billing data is read or written by the monitor.
 * - No secrets are logged; connection URLs are read from environment only.
 */

import pg from 'pg';
import { Redis } from 'ioredis';
import { getEnv } from '../config/env.js';
import {
  setReplicationLagMs,
  setRegionAvailability,
  setReplicationIsPrimary,
} from '../api/metrics/prometheus.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type RegionStatus = 'healthy' | 'degraded' | 'unavailable';

export interface ReplicaHealth {
  region: string;
  status: RegionStatus;
  lagMs: number;
  checkedAt: Date;
  error?: string;
}

export interface ReplicationMonitorOptions {
  /** Override the primary region name (default: env.REGION). */
  primaryRegion?: string;
  /** Override the secondary region names (default: env.SECONDARY_REGIONS split on ','). */
  secondaryRegions?: string[];
  /** Override the poll interval in ms (default: env.REPLICATION_POLL_INTERVAL_MS). */
  pollIntervalMs?: number;
  /** Warn threshold for replication lag ms (default: env.REPLICATION_LAG_WARN_MS). */
  lagWarnMs?: number;
  /** Critical threshold for replication lag ms (default: env.REPLICATION_LAG_CRITICAL_MS). */
  lagCriticalMs?: number;
  /**
   * Injectable probe function for the replica database. Defaults to a
   * real pg.Pool probe. Injected in unit tests to avoid live DB access.
   */
  dbProbe?: (url: string) => Promise<number>;
  /**
   * Injectable probe function for the replica Redis. Defaults to a real
   * ioredis PING probe. Injected in unit tests to avoid live Redis access.
   */
  redisProbe?: (url: string) => Promise<number>;
}

// ---------------------------------------------------------------------------
// Default probe implementations
// ---------------------------------------------------------------------------

/**
 * Measure round-trip time to a PostgreSQL replica using a single lightweight
 * `SELECT 1` query. Returns the elapsed ms, or throws on error.
 */
export async function defaultDbProbe(url: string): Promise<number> {
  const pool = new pg.Pool({ connectionString: url, max: 1, connectionTimeoutMillis: 5000 });
  const start = Date.now();
  try {
    await pool.query('SELECT 1');
    return Date.now() - start;
  } finally {
    await pool.end().catch(() => undefined);
  }
}

/**
 * Measure round-trip time to a Redis replica using a PING command.
 * Returns the elapsed ms, or throws on error.
 */
export async function defaultRedisProbe(url: string): Promise<number> {
  const client = new Redis(url, { maxRetriesPerRequest: 1, lazyConnect: true });
  const start = Date.now();
  try {
    await client.connect();
    await client.ping();
    return Date.now() - start;
  } finally {
    client.disconnect();
  }
}

// ---------------------------------------------------------------------------
// Monitor class
// ---------------------------------------------------------------------------

/**
 * Polls replica health on a fixed interval and exposes the state through
 * Prometheus metrics. A single instance should be created per process;
 * call `start()` after the server is ready and `stop()` during shutdown.
 */
export class ReplicationMonitor {
  private readonly primaryRegion: string;
  private readonly secondaryRegions: string[];
  private readonly pollIntervalMs: number;
  private readonly lagWarnMs: number;
  private readonly lagCriticalMs: number;
  private readonly dbProbe: (url: string) => Promise<number>;
  private readonly redisProbe: (url: string) => Promise<number>;

  private _timer: ReturnType<typeof setInterval> | null = null;
  private readonly _lastHealth = new Map<string, ReplicaHealth>();

  constructor(opts: ReplicationMonitorOptions = {}) {
    const env = getEnv();
    this.primaryRegion = opts.primaryRegion ?? env.REGION;
    this.secondaryRegions =
      opts.secondaryRegions ??
      (env.SECONDARY_REGIONS.trim().length > 0
        ? env.SECONDARY_REGIONS.split(',').map((r) => r.trim()).filter(Boolean)
        : []);
    this.pollIntervalMs = opts.pollIntervalMs ?? env.REPLICATION_POLL_INTERVAL_MS;
    this.lagWarnMs = opts.lagWarnMs ?? env.REPLICATION_LAG_WARN_MS;
    this.lagCriticalMs = opts.lagCriticalMs ?? env.REPLICATION_LAG_CRITICAL_MS;
    this.dbProbe = opts.dbProbe ?? defaultDbProbe;
    this.redisProbe = opts.redisProbe ?? defaultRedisProbe;
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Start the polling loop. Idempotent — calling `start()` twice is safe.
   * An initial poll is executed immediately; subsequent polls run every
   * `pollIntervalMs` milliseconds.
   */
  start(): void {
    if (this._timer !== null) return;

    // Announce primary status immediately.
    const env = getEnv();
    setReplicationIsPrimary(this.primaryRegion, env.IS_PRIMARY_REGION);

    // Run an immediate poll, then schedule the loop.
    void this._poll();
    this._timer = setInterval(() => void this._poll(), this.pollIntervalMs);
    (this._timer as unknown as { unref?: () => void }).unref?.();
  }

  /** Stop the polling loop cleanly. Idempotent. */
  stop(): void {
    if (this._timer !== null) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  /** Snapshot of the last health reading for each secondary region. */
  getLastHealth(): Map<string, ReplicaHealth> {
    return new Map(this._lastHealth);
  }

  /** True if all known secondary regions are healthy. */
  isAllHealthy(): boolean {
    for (const h of this._lastHealth.values()) {
      if (h.status !== 'healthy') return false;
    }
    return true;
  }

  // ---------------------------------------------------------------------------
  // Internal poll
  // ---------------------------------------------------------------------------

  /** Run one probe cycle against all configured replica endpoints. */
  async _poll(): Promise<void> {
    const env = getEnv();
    const replicaDbUrl = env.REPLICA_DATABASE_URL;
    const replicaRedisUrl = env.REPLICA_REDIS_URL;

    if (this.secondaryRegions.length === 0 && !replicaDbUrl && !replicaRedisUrl) {
      // No replicas configured — emit healthy for the primary only.
      setRegionAvailability(this.primaryRegion, 'healthy');
      return;
    }

    // Probe each configured replica. In a single-replica setup, SECONDARY_REGIONS
    // typically has one entry that matches the host behind REPLICA_DATABASE_URL /
    // REPLICA_REDIS_URL.
    const targets =
      this.secondaryRegions.length > 0 ? this.secondaryRegions : ['replica'];

    for (const targetRegion of targets) {
      const health = await this._probeReplica(targetRegion, replicaDbUrl, replicaRedisUrl);
      this._lastHealth.set(targetRegion, health);

      // Push to Prometheus.
      setReplicationLagMs(this.primaryRegion, targetRegion, health.lagMs);
      setRegionAvailability(targetRegion, health.status);
    }

    // The primary region is always healthy from its own perspective.
    setRegionAvailability(this.primaryRegion, 'healthy');
  }

  /** Probe a single replica endpoint and classify its health. */
  private async _probeReplica(
    targetRegion: string,
    replicaDbUrl: string | undefined,
    replicaRedisUrl: string | undefined,
  ): Promise<ReplicaHealth> {
    let lagMs = -1;
    let error: string | undefined;

    try {
      if (replicaDbUrl) {
        lagMs = await this.dbProbe(replicaDbUrl);
      } else if (replicaRedisUrl) {
        lagMs = await this.redisProbe(replicaRedisUrl);
      } else {
        // No URL to probe — report healthy with 0 lag (local mock mode).
        lagMs = 0;
      }
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
      lagMs = -1;
    }

    const status = this._classify(lagMs);
    return { region: targetRegion, status, lagMs, checkedAt: new Date(), error };
  }

  /** Classify a lag value into a health status. */
  private _classify(lagMs: number): RegionStatus {
    if (lagMs < 0) return 'unavailable';
    if (lagMs >= this.lagCriticalMs) return 'unavailable';
    if (lagMs >= this.lagWarnMs) return 'degraded';
    return 'healthy';
  }
}

// ---------------------------------------------------------------------------
// Module-level singleton (for use in the Fastify app lifecycle)
// ---------------------------------------------------------------------------

let _instance: ReplicationMonitor | null = null;

/** Return (and lazily create) the process-level singleton monitor. */
export function getReplicationMonitor(opts?: ReplicationMonitorOptions): ReplicationMonitor {
  if (_instance === null) {
    _instance = new ReplicationMonitor(opts);
  }
  return _instance;
}

/** Replace the singleton. Used in tests to inject a pre-configured instance. */
export function setReplicationMonitor(monitor: ReplicationMonitor): void {
  _instance?.stop();
  _instance = monitor;
}

/** Reset the singleton (for tests). */
export function resetReplicationMonitor(): void {
  _instance?.stop();
  _instance = null;
}
