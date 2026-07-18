import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import promClient from 'prom-client';
import { createWriteStream, type WriteStream, statSync } from 'node:fs';

// ---------------------------------------------------------------------------
// Per-device debug log (rotated at 10 MB, NOT Prometheus)
// ---------------------------------------------------------------------------
const DEVICE_LOG_PATH = '/var/log/device-metrics.log';
const DEVICE_LOG_MAX_BYTES = 10 * 1024 * 1024; // 10 MB

let _deviceLogStream: WriteStream | null = null;
let _deviceLogBytes = 0;

function getDeviceLogStream(): WriteStream {
  if (_deviceLogStream === null) {
    try {
      _deviceLogBytes = statSync(DEVICE_LOG_PATH).size;
    } catch {
      _deviceLogBytes = 0;
    }
    _deviceLogStream = createWriteStream(DEVICE_LOG_PATH, { flags: 'a' });
  }
  return _deviceLogStream;
}

/** Write a per-device debug entry. Rotates the file when it exceeds 10 MB. */
export function logDeviceMetric(deviceId: string, fields: Record<string, unknown>): void {
  if (_deviceLogBytes >= DEVICE_LOG_MAX_BYTES) {
    // Rotate: close current stream and start fresh (overwrite).
    _deviceLogStream?.end();
    _deviceLogStream = createWriteStream(DEVICE_LOG_PATH, { flags: 'w' });
    _deviceLogBytes = 0;
  }
  const line =
    JSON.stringify({ level: 'debug', time: Date.now(), device_id: deviceId, ...fields }) + '\n';
  getDeviceLogStream().write(line);
  _deviceLogBytes += Buffer.byteLength(line);
}

// ---------------------------------------------------------------------------
// Cardinality guard
// ---------------------------------------------------------------------------
const MAX_SERIES = 100_000;

/**
 * Register a metric only if the current series count for that metric name is
 * below MAX_SERIES. If the guard trips, emits a HighCardinalityWarning log and
 * returns null instead of throwing.
 */
export async function registerMetric<T extends promClient.Metric>(
  factory: () => T,
  name: string,
): Promise<T | null> {
  const all = await promClient.register.getMetricsAsJSON();
  const seriesCount = all
    .filter((m) => m.name === name)
    .reduce((sum, m) => {
      const v = m as { values?: unknown[] };
      return sum + (v.values?.length ?? 0);
    }, 0);
  if (seriesCount > MAX_SERIES) {
    console.error(
      JSON.stringify({
        level: 'warn',
        event: 'HighCardinalityWarning',
        metric: name,
        series: seriesCount,
        limit: MAX_SERIES,
      }),
    );
    return null;
  }
  return factory();
}

// ---------------------------------------------------------------------------
// Aggregator: pre-aggregate per-device increments into low-cardinality counters
// ---------------------------------------------------------------------------
interface AggregateEntry {
  ingestion: Map<string, number>; // status -> count
  bufferBytes: Map<string, number>; // aggregateKey -> bytes
}

// aggregateKey = `${tenant_id}:${device_tier}:${region}`
const _aggregates = new Map<string, AggregateEntry>();

function getEntry(aggregateKey: string): AggregateEntry {
  let entry = _aggregates.get(aggregateKey);
  if (entry === undefined) {
    entry = { ingestion: new Map(), bufferBytes: new Map() };
    _aggregates.set(aggregateKey, entry);
  }
  return entry;
}

/** Buffer an ingestion increment for the next flush cycle. */
export function bufferIngestionIncrement(
  tenantId: string,
  deviceTier: string,
  region: string,
  status: string,
): void {
  const key = `${tenantId}:${deviceTier}:${region}`;
  const entry = getEntry(key);
  entry.ingestion.set(status, (entry.ingestion.get(status) ?? 0) + 1);
}

/** Buffer a connection-buffer-bytes update for the next flush cycle. */
export function bufferConnectionBufferBytes(
  tenantId: string,
  deviceTier: string,
  region: string,
  bytes: number,
): void {
  const key = `${tenantId}:${deviceTier}:${region}`;
  const entry = getEntry(key);
  entry.bufferBytes.set(key, bytes);
}

/** Flush buffered aggregates into Prometheus counters/gauges. Called every 60 s. */
export function flushAggregates(): void {
  for (const [key, entry] of _aggregates) {
    const [tenantId = 'unknown', deviceTier = 'unknown', region = 'unknown'] = key.split(':');
    for (const [status, count] of entry.ingestion) {
      ingestionCounter.inc({ tenant_id: tenantId, device_tier: deviceTier, region, status }, count);
    }
    for (const [, bytes] of entry.bufferBytes) {
      connectionBufferBytes.set({ tenant_id: tenantId, device_tier: deviceTier, region }, bytes);
    }
  }
  _aggregates.clear();
}

let _flushInterval: ReturnType<typeof setInterval> | null = null;

/** Start the 60-second aggregate flush loop. Idempotent. */
export function startAggregateFlush(): void {
  if (_flushInterval !== null) return;
  _flushInterval = setInterval(flushAggregates, 60_000);
  (_flushInterval as { unref?: () => void }).unref?.();
}

/** Stop the flush loop (for tests / clean shutdown). */
export function stopAggregateFlush(): void {
  if (_flushInterval !== null) {
    clearInterval(_flushInterval);
    _flushInterval = null;
  }
}

// `collectDefaultMetrics` registers a fixed set of process/runtime metrics on the
// supplied registry. Calling it more than once against the same registry throws
// "A metric with the name … has already been registered", so guard the call.
const DEFAULT_METRICS_INIT_FLAG = '__prom_default_metrics_initialized__';
type RegistryWithFlag = promClient.Registry & {
  [DEFAULT_METRICS_INIT_FLAG]?: boolean;
};
const registerRef = promClient.register as RegistryWithFlag;
if (registerRef[DEFAULT_METRICS_INIT_FLAG] !== true) {
  promClient.collectDefaultMetrics({ register: promClient.register });
  registerRef[DEFAULT_METRICS_INIT_FLAG] = true;
}

export const httpRequestDuration: promClient.Histogram = new promClient.Histogram({
  name: 'http_request_duration_ms',
  help: 'Duration of HTTP requests in ms',
  labelNames: ['method', 'route', 'status_code'],
  buckets: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
});

// device_id removed — use aggregate labels to bound cardinality.
// Per-device detail is available in the device-metrics debug log.
export const ingestionCounter: promClient.Counter = new promClient.Counter({
  name: 'ingestion_packets_total',
  help: 'Total number of ingested telemetry packets',
  labelNames: ['tenant_id', 'device_tier', 'region', 'status'],
});

export const blockchainTxCounter: promClient.Counter = new promClient.Counter({
  name: 'blockchain_transactions_total',
  help: 'Total Soroban transactions submitted',
  labelNames: ['status'],
});

// Billing-tier config hot-reload observability (issue #63). Incremented when a
// batch observes the active config version change mid-processing, so the batch
// is re-processed under the new version. Labelled by the start/end version so
// transitions are traceable.
export const configTransitionEvents: promClient.Gauge = new promClient.Gauge({
  name: 'config_transition_events',
  help: 'Billing-tier config version transitions detected mid-batch, by start/end version',
  labelNames: ['start_version', 'end_version'],
});

export function incrementConfigTransitionEvents(
  startVersion: string | number,
  endVersion: string | number,
): void {
  configTransitionEvents.inc({ start_version: startVersion, end_version: endVersion });
}

// Rate-limiter observability (issue #50). Every decision is served from
// centralized Redis state (the token bucket is a server-side Lua script), so
// the limiter is pod-agnostic by construction. This counter makes that visible
// and lets us watch Redis load as HPA scales replicas.
//
// Note: the issue also proposed `rate_limiter_local_hits` for an in-process
// cache layer. That cache is intentionally NOT implemented — a per-pod cache
// would re-introduce the cross-pod state drift this issue exists to prevent —
// so there is no local-hits counter to report.
export const rateLimiterRedisHits: promClient.Counter = new promClient.Counter({
  name: 'rate_limiter_redis_hits_total',
  help: 'Rate-limiter decisions resolved against centralized Redis state, by outcome',
  labelNames: ['decision'],
});

export const circuitBreakerState: promClient.Gauge = new promClient.Gauge({
  name: 'circuit_breaker_state',
  help: 'Current circuit breaker state (0=closed, 1=half-open, 2=open)',
  labelNames: ['client'],
});

export const circuitBreakerQueueDepth: promClient.Gauge = new promClient.Gauge({
  name: 'circuit_breaker_queue_depth',
  help: 'Current number of requests queued in the circuit breaker',
  labelNames: ['client'],
});

export const noncePoolDepth: promClient.Gauge = new promClient.Gauge({
  name: 'nonce_pool_active_count',
  help: 'Active nonce reservations in the pool',
});

export const lockEventListenerCount: promClient.Gauge = new promClient.Gauge({
  name: 'lock_event_listener_count',
  help: 'Number of per-lock event listeners registered on AdvisoryLockManager',
});

export const ingestionQueueDepth: promClient.Gauge = new promClient.Gauge({
  name: 'ingestion_queue_depth',
  help: 'Current ingestion task queue depth',
});

export const eventLoopLag: promClient.Gauge = new promClient.Gauge({
  name: 'node_event_loop_lag_ms',
  help: 'Current event loop lag in ms',
});

// device_id removed — use aggregate labels to bound cardinality.
export const connectionBufferBytes: promClient.Gauge = new promClient.Gauge({
  name: 'connection_buffer_bytes',
  help: 'Partial telemetry reassembly buffer size, aggregated by tenant/tier/region',
  labelNames: ['tenant_id', 'device_tier', 'region'],
});

// Required GC pause buckets per issue #19: 1, 5, 10, 25, 50, 100, 250, 500 ms
export const GC_PAUSE_BUCKETS_MS = [1, 5, 10, 25, 50, 100, 250, 500] as const;

export const gcPauseDuration: promClient.Histogram = new promClient.Histogram({
  name: 'node_gc_pause_duration_ms',
  help: 'Garbage collection pause duration in ms',
  buckets: [...GC_PAUSE_BUCKETS_MS],
});

export const tenantPoolActiveConnections: promClient.Gauge = new promClient.Gauge({
  name: 'tenant_pool_active_connections',
  help: 'Active database connections per tenant sub-pool',
  labelNames: ['tenant_id'],
});

export const tenantPoolQueueDepth: promClient.Gauge = new promClient.Gauge({
  name: 'tenant_pool_queue_depth',
  help: 'Pending fair-queue requests waiting for a tenant connection',
});

export const globalPoolUtilization: promClient.Gauge = new promClient.Gauge({
  name: 'global_pool_utilization',
  help: 'Ratio of active connections to global pool maximum',
});

export const tenantPoolWaitDuration: promClient.Histogram = new promClient.Histogram({
  name: 'tenant_pool_wait_duration_ms',
  help: 'Time spent waiting for a tenant-scoped connection',
  labelNames: ['tenant_id', 'result'],
  buckets: [1, 5, 10, 25, 50, 100, 250, 500],
});

export const tenantPoolRejections: promClient.Counter = new promClient.Counter({
  name: 'tenant_pool_rejections_total',
  help: 'Connections rejected due to pool contention timeout',
  labelNames: ['tenant_id'],
});

// --- Pool metrics (per pg.Pool) ---------------------------------------------------
// Required by issue #19 for real-time monitoring of pool exhaustion.

export const pgPoolConnectionsTotal: promClient.Gauge = new promClient.Gauge({
  name: 'pg_pool_connections_total',
  help: 'Total connections in the PostgreSQL pool (created + idle + active)',
  labelNames: ['pool'],
});

export const pgPoolConnectionsIdle: promClient.Gauge = new promClient.Gauge({
  name: 'pg_pool_connections_idle',
  help: 'Idle connections currently available in the PostgreSQL pool',
  labelNames: ['pool'],
});

export const pgPoolConnectionsActive: promClient.Gauge = new promClient.Gauge({
  name: 'pg_pool_connections_active',
  help: 'Active (in-use) connections in the PostgreSQL pool (total - idle)',
  labelNames: ['pool'],
});

export const pgPoolConnectionsWaiting: promClient.Gauge = new promClient.Gauge({
  name: 'pg_pool_connections_waiting',
  help: 'Clients currently waiting for an available connection in the PostgreSQL pool',
  labelNames: ['pool'],
});

// --- Ledger synchronizer metrics -------------------------------------------------
// Required by issue #19 for monitoring ledger sync lag.

export const ledgerSyncLag: promClient.Gauge = new promClient.Gauge({
  name: 'ledger_sync_lag',
  help: 'Number of ledgers the synchronizer is behind the latest polled sequence',
  labelNames: ['sync_id'],
});

export const ledgerLastSyncedSequence: promClient.Gauge = new promClient.Gauge({
  name: 'ledger_last_synced_sequence',
  help: 'Most recent ledger sequence successfully persisted by the synchronizer',
  labelNames: ['sync_id'],
});

export const ledgerLatestPolledSequence: promClient.Gauge = new promClient.Gauge({
  name: 'ledger_latest_polled_sequence',
  help: 'Latest ledger sequence observed from RPC by the synchronizer poll loop',
  labelNames: ['sync_id'],
});

export const ledgerSyncPollErrors: promClient.Counter = new promClient.Counter({
  name: 'ledger_sync_poll_errors_total',
  help: 'Number of failed RPC polls by the ledger synchronizer',
  labelNames: ['sync_id', 'phase'],
});

// Ledger event-bus continuity (issue #48). Before this change, cross-process
// ledger notifications used Redis pub/sub, which silently dropped every message
// published during a sentinel failover (no subscribers on the new leader). The
// bus now uses durable Redis Streams + consumer groups, but we still track any
// sequence discontinuity observed at consume time so a regression is visible.
// The counter is incremented by the number of missing sequences each time a gap
// is detected; the legacy `redis_pubsub_messages_lost_total` name is retained so
// existing dashboards/alerts continue to work.
export const redisPubsubMessagesLost: promClient.Counter = new promClient.Counter({
  name: 'redis_pubsub_messages_lost_total',
  help: 'Ledger events detected as missing via consumer-group sequence-gap detection',
  labelNames: ['stream'],
});

// Setters -----------------------------------------------------------------------

export function setTenantPoolActiveConnections(tenantId: string, count: number): void {
  tenantPoolActiveConnections.set({ tenant_id: tenantId }, count);
}

export function setTenantPoolQueueDepth(depth: number): void {
  tenantPoolQueueDepth.set(depth);
}

export function setGlobalPoolUtilization(ratio: number): void {
  globalPoolUtilization.set(ratio);
}

export function recordTenantPoolGrant(tenantId: string, waitMs: number): void {
  tenantPoolWaitDuration.observe({ tenant_id: tenantId, result: 'granted' }, waitMs);
}

export function recordTenantPoolRejection(tenantId: string, waitMs: number): void {
  tenantPoolWaitDuration.observe({ tenant_id: tenantId, result: 'rejected' }, waitMs);
  tenantPoolRejections.inc({ tenant_id: tenantId });
}

export function recordRateLimiterRedisHit(decision: 'allowed' | 'denied'): void {
  rateLimiterRedisHits.inc({ decision });
}

export function recordGcPause(durationMs: number): void {
  if (Number.isFinite(durationMs) && durationMs > 0) {
    gcPauseDuration.observe(durationMs);
  }
}

export function setConnectionBufferBytes(
  tenantId: string,
  deviceTier: string,
  region: string,
  bytes: number,
): void {
  connectionBufferBytes.set({ tenant_id: tenantId, device_tier: deviceTier, region }, bytes);
}

export interface PoolSizeMetrics {
  total: number;
  idle: number;
  active: number;
  waiting: number;
}

export function setPgPoolConnections(poolName: string, metrics: PoolSizeMetrics): void {
  pgPoolConnectionsTotal.set({ pool: poolName }, metrics.total);
  pgPoolConnectionsIdle.set({ pool: poolName }, metrics.idle);
  pgPoolConnectionsActive.set({ pool: poolName }, metrics.active);
  pgPoolConnectionsWaiting.set({ pool: poolName }, metrics.waiting);
}

export interface LedgerSyncMetrics {
  syncId: string;
  lag: number;
  lastSyncedSequence: number | null;
  latestPolledSequence: number | null;
}

export function setLedgerSyncMetrics(metrics: LedgerSyncMetrics): void {
  const labels = { sync_id: metrics.syncId };
  ledgerSyncLag.set(labels, Math.max(0, metrics.lag));
  if (metrics.lastSyncedSequence !== null) {
    ledgerLastSyncedSequence.set(labels, metrics.lastSyncedSequence);
  }
  if (metrics.latestPolledSequence !== null) {
    ledgerLatestPolledSequence.set(labels, metrics.latestPolledSequence);
  }
}

export function recordLedgerSyncPollError(syncId: string, phase: 'poll' | 'fetch'): void {
  ledgerSyncPollErrors.inc({ sync_id: syncId, phase });
}

export function recordRedisPubsubMessagesLost(stream: string, count: number): void {
  if (Number.isFinite(count) && count > 0) {
    redisPubsubMessagesLost.inc({ stream }, count);
  }
}

// --- SSE (Server-Sent Events) connection metrics (issue #68) ---------------------
// Tracks backpressure behaviour on the admin SSE stream: active connections,
// dropped events when per-client queues are full, and successfully delivered events.

export const sseConnectionsActive: promClient.Gauge = new promClient.Gauge({
  name: 'sse_connections_active',
  help: 'Number of active SSE client connections to the admin event stream',
});

export const sseEventsDroppedTotal: promClient.Counter = new promClient.Counter({
  name: 'sse_events_dropped_total',
  help: 'SSE events dropped due to full per-client queue or closed connections',
  labelNames: ['reason'],
});

export const sseEventsSentTotal: promClient.Counter = new promClient.Counter({
  name: 'sse_events_sent_total',
  help: 'SSE events successfully written to client connections',
});

export const sseQueueDepth: promClient.Gauge = new promClient.Gauge({
  name: 'sse_queue_depth',
  help: 'Current event queue depth per SSE client',
  labelNames: ['client_id'],
});

export function setSseConnectionsActive(count: number): void {
  sseConnectionsActive.set(count);
}

export function incrementSseEventsDropped(reason: 'queue_full' | 'connection_closed'): void {
  sseEventsDroppedTotal.inc({ reason });
}

export function incrementSseEventsSent(): void {
  sseEventsSentTotal.inc();
}

export function setSseQueueDepth(clientId: string, depth: number): void {
  sseQueueDepth.set({ client_id: clientId }, depth);
}

// --- Multi-region replication metrics (issue #88) ----------------------------
// Tracks replication lag, region availability, failover state, and recovery
// success so existing Prometheus/Grafana dashboards can observe DR activity.

/**
 * Current replication lag in milliseconds between primary and each replica.
 * Label `source_region` is the primary; `target_region` is the replica.
 * A value of -1 indicates the replica is unreachable.
 */
export const replicationLagMs: promClient.Gauge = new promClient.Gauge({
  name: 'replication_lag_ms',
  help: 'Replication lag in ms between primary and replica region (-1 = unreachable)',
  labelNames: ['source_region', 'target_region'],
});

/**
 * Region availability (1 = healthy, 0 = degraded, -1 = unavailable).
 */
export const regionAvailability: promClient.Gauge = new promClient.Gauge({
  name: 'region_availability',
  help: 'Region availability: 1=healthy, 0=degraded, -1=unavailable',
  labelNames: ['region'],
});

/**
 * Failover state: 1 if this instance is currently acting as primary, 0 if secondary.
 * Transitions increment `replication_failover_events_total`.
 */
export const replicationIsPrimary: promClient.Gauge = new promClient.Gauge({
  name: 'replication_is_primary',
  help: '1 if this instance is currently the primary region, 0 if secondary/standby',
  labelNames: ['region'],
});

/**
 * Total number of failover events (planned or emergency) triggered.
 */
export const replicationFailoverEventsTotal: promClient.Counter = new promClient.Counter({
  name: 'replication_failover_events_total',
  help: 'Total failover events triggered, by type (planned | emergency) and direction',
  labelNames: ['type', 'from_region', 'to_region'],
});

/**
 * Total number of successful recovery verifications after failover.
 */
export const replicationRecoverySuccessTotal: promClient.Counter = new promClient.Counter({
  name: 'replication_recovery_success_total',
  help: 'Successful DR recovery verifications after a failover event',
  labelNames: ['region'],
});

/**
 * Total number of failed recovery verifications after failover.
 */
export const replicationRecoveryFailureTotal: promClient.Counter = new promClient.Counter({
  name: 'replication_recovery_failure_total',
  help: 'Failed DR recovery verifications after a failover event',
  labelNames: ['region', 'reason'],
});

/**
 * Total number of billing transactions replicated to secondary regions.
 * Used to verify no data loss after failover.
 */
export const replicatedBillingTransactionsTotal: promClient.Counter = new promClient.Counter({
  name: 'replicated_billing_transactions_total',
  help: 'Billing transactions replicated to secondary regions',
  labelNames: ['source_region', 'target_region', 'status'],
});

// Setters --------------------------------------------------------------------

export function setReplicationLagMs(
  sourceRegion: string,
  targetRegion: string,
  lagMs: number,
): void {
  replicationLagMs.set({ source_region: sourceRegion, target_region: targetRegion }, lagMs);
}

export function setRegionAvailability(
  region: string,
  status: 'healthy' | 'degraded' | 'unavailable',
): void {
  const val = status === 'healthy' ? 1 : status === 'degraded' ? 0 : -1;
  regionAvailability.set({ region }, val);
}

export function setReplicationIsPrimary(region: string, isPrimary: boolean): void {
  replicationIsPrimary.set({ region }, isPrimary ? 1 : 0);
}

export function recordFailoverEvent(
  type: 'planned' | 'emergency',
  fromRegion: string,
  toRegion: string,
): void {
  replicationFailoverEventsTotal.inc({ type, from_region: fromRegion, to_region: toRegion });
}

export function recordRecoverySuccess(region: string): void {
  replicationRecoverySuccessTotal.inc({ region });
}

export function recordRecoveryFailure(region: string, reason: string): void {
  replicationRecoveryFailureTotal.inc({ region, reason });
}

export function recordReplicatedTransaction(
  sourceRegion: string,
  targetRegion: string,
  status: 'ok' | 'failed',
): void {
  replicatedBillingTransactionsTotal.inc({
    source_region: sourceRegion,
    target_region: targetRegion,
    status,
  });
}

// Metrics endpoint -------------------------------------------------------------

export function getMetricsRegistry(): promClient.Registry {
  return promClient.register;
}

export function getMetricsContentType(): string {
  return promClient.register.contentType;
}

export function getMetrics(): Promise<string> {
  return promClient.register.metrics();
}

/**
 * Register the `GET /metrics` endpoint that returns Prometheus text format.
 *
 * The handler is intentionally cheap: it merely stringifies the in-memory
 * counter/gauge/histogram snapshot, with no I/O. The endpoint is expected to
 * respond in well under the 10ms budget required by issue #19 even at
 * 10k scrapes/min (≈166/s).
 */
export function registerMetricsRoute(app: FastifyInstance, path = '/metrics'): void {
  app.get(path, async (_request: FastifyRequest, reply: FastifyReply) => {
    const body = await getMetrics();
    void reply.header('Content-Type', getMetricsContentType());
    void reply.header('Cache-Control', 'no-store');
    return reply.send(body);
  });
}
