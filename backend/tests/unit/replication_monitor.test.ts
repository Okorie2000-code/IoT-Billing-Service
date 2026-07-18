/**
 * Unit tests for ReplicationMonitor (issue #88).
 *
 * All tests use injected dbProbe / redisProbe functions so no real database or
 * Redis connection is required. Prometheus metric objects are imported directly
 * so we can read their current values and assert behaviour without standing up
 * a Fastify server.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import promClient from 'prom-client';
import {
  ReplicationMonitor,
  getReplicationMonitor,
  setReplicationMonitor,
  resetReplicationMonitor,
  defaultDbProbe,
  defaultRedisProbe,
  type ReplicationMonitorOptions,
} from '../../src/replication/replication_monitor.js';
import {
  replicationLagMs,
  regionAvailability,
  replicationIsPrimary,
} from '../../src/api/metrics/prometheus.js';

// ---------------------------------------------------------------------------
// Helper: read the current value of a labelled gauge
// ---------------------------------------------------------------------------
async function readGauge(
  gauge: promClient.Gauge,
  labels: Record<string, string>,
): Promise<number | undefined> {
  const data = await gauge.get();
  const entry = data.values.find((v) =>
    Object.entries(labels).every(([k, val]) => v.labels[k] === val),
  );
  return entry?.value;
}

// ---------------------------------------------------------------------------
// Shared env mock.
// REPLICA_DATABASE_URL is set to a dummy value so _poll() calls the injected
// dbProbe function rather than short-circuiting to "no URL" mode (lagMs = 0).
// Tests that need redis-only behaviour override this inline.
// vi.hoisted() is required because vi.mock factories are hoisted to the top of
// the file before any variable declarations.
// ---------------------------------------------------------------------------
const mockEnvBase = {
  REGION: 'us-east-1',
  SECONDARY_REGIONS: '',
  REPLICATION_POLL_INTERVAL_MS: 10_000,
  REPLICATION_LAG_WARN_MS: 5_000,
  REPLICATION_LAG_CRITICAL_MS: 30_000,
  IS_PRIMARY_REGION: true,
  REPLICA_DATABASE_URL: 'postgres://replica:5432/test',
  REPLICA_REDIS_URL: undefined as string | undefined,
};

const { mockGetEnv } = vi.hoisted(() => ({
  mockGetEnv: vi.fn(() => ({
    REGION: 'us-east-1',
    SECONDARY_REGIONS: '',
    REPLICATION_POLL_INTERVAL_MS: 10_000,
    REPLICATION_LAG_WARN_MS: 5_000,
    REPLICATION_LAG_CRITICAL_MS: 30_000,
    IS_PRIMARY_REGION: true,
    REPLICA_DATABASE_URL: 'postgres://replica:5432/test',
    REPLICA_REDIS_URL: undefined as string | undefined,
  })),
}));

vi.mock('../../src/config/env.js', () => ({
  getEnv: mockGetEnv,
}));

// Fast probes injected in tests — resolved immediately without I/O.
const fastDbProbe = vi.fn((_url: string): Promise<number> => Promise.resolve(50));
const fastRedisProbe = vi.fn((_url: string): Promise<number> => Promise.resolve(20));

function makeOpts(overrides: Partial<ReplicationMonitorOptions> = {}): ReplicationMonitorOptions {
  return {
    pollIntervalMs: 60_000, // long interval so tests control ticks manually
    dbProbe: fastDbProbe,
    redisProbe: fastRedisProbe,
    ...overrides,
  };
}

describe('ReplicationMonitor — constructor defaults', () => {
  afterEach(() => {
    vi.useRealTimers();
    resetReplicationMonitor();
    mockGetEnv.mockReturnValue({ ...mockEnvBase });
  });

  it('reads primary region from env', () => {
    const monitor = new ReplicationMonitor(makeOpts({ primaryRegion: undefined }));
    expect(monitor).toBeInstanceOf(ReplicationMonitor);
  });

  it('starts and stops without error when no secondary regions are configured', async () => {
    // No replica URL so the no-op path fires for primary-only.
    mockGetEnv.mockReturnValue({
      ...mockEnvBase,
      SECONDARY_REGIONS: '',
      REPLICA_DATABASE_URL: undefined,
    });
    const monitor = new ReplicationMonitor(makeOpts({ secondaryRegions: [] }));
    monitor.start();
    await monitor._poll();
    monitor.stop();

    const val = await readGauge(regionAvailability, { region: 'us-east-1' });
    expect(val).toBe(1); // healthy
  });

  it('isAllHealthy() returns true when no replicas are tracked', () => {
    const monitor = new ReplicationMonitor(makeOpts({ secondaryRegions: [] }));
    expect(monitor.isAllHealthy()).toBe(true);
  });
});

describe('ReplicationMonitor — single secondary region', () => {
  let monitor: ReplicationMonitor;

  beforeEach(() => {
    replicationLagMs.reset();
    regionAvailability.reset();
    replicationIsPrimary.reset();
    fastDbProbe.mockClear();
    fastRedisProbe.mockClear();
    fastDbProbe.mockResolvedValue(50);
    fastRedisProbe.mockResolvedValue(20);
    mockGetEnv.mockReturnValue({ ...mockEnvBase });
  });

  afterEach(() => {
    monitor?.stop();
    vi.useRealTimers();
    resetReplicationMonitor();
  });

  it('sets replication_lag_ms gauge after a successful probe', async () => {
    fastDbProbe.mockResolvedValue(120);
    monitor = new ReplicationMonitor(
      makeOpts({
        primaryRegion: 'us-east-1',
        secondaryRegions: ['eu-west-1'],
        dbProbe: fastDbProbe,
      }),
    );

    await monitor._poll();

    const lag = await readGauge(replicationLagMs, {
      source_region: 'us-east-1',
      target_region: 'eu-west-1',
    });
    expect(lag).toBe(120);
  });

  it('marks region_availability as healthy (1) when lag is below warn threshold', async () => {
    fastDbProbe.mockResolvedValue(100); // < 5000ms warn threshold
    monitor = new ReplicationMonitor(
      makeOpts({ primaryRegion: 'us-east-1', secondaryRegions: ['eu-west-1'] }),
    );

    await monitor._poll();

    const avail = await readGauge(regionAvailability, { region: 'eu-west-1' });
    expect(avail).toBe(1);
  });

  it('marks region_availability as degraded (0) when lag is between warn and critical', async () => {
    fastDbProbe.mockResolvedValue(10_000); // > 5000ms warn, < 30000ms critical
    monitor = new ReplicationMonitor(
      makeOpts({
        primaryRegion: 'us-east-1',
        secondaryRegions: ['eu-west-1'],
        lagWarnMs: 5_000,
        lagCriticalMs: 30_000,
      }),
    );

    await monitor._poll();

    const avail = await readGauge(regionAvailability, { region: 'eu-west-1' });
    expect(avail).toBe(0);
  });

  it('marks region_availability as unavailable (-1) when lag meets or exceeds critical threshold', async () => {
    fastDbProbe.mockResolvedValue(30_000); // == 30000ms critical
    monitor = new ReplicationMonitor(
      makeOpts({
        primaryRegion: 'us-east-1',
        secondaryRegions: ['eu-west-1'],
        lagWarnMs: 5_000,
        lagCriticalMs: 30_000,
      }),
    );

    await monitor._poll();

    const avail = await readGauge(regionAvailability, { region: 'eu-west-1' });
    expect(avail).toBe(-1);
  });

  it('marks region_availability as unavailable (-1) and lag as -1 when probe throws', async () => {
    fastDbProbe.mockRejectedValue(new Error('connection refused'));
    monitor = new ReplicationMonitor(
      makeOpts({ primaryRegion: 'us-east-1', secondaryRegions: ['eu-west-1'] }),
    );

    await monitor._poll();

    const avail = await readGauge(regionAvailability, { region: 'eu-west-1' });
    expect(avail).toBe(-1);

    const lag = await readGauge(replicationLagMs, {
      source_region: 'us-east-1',
      target_region: 'eu-west-1',
    });
    expect(lag).toBe(-1);
  });

  it('primary region is always reported as healthy after _poll()', async () => {
    fastDbProbe.mockRejectedValue(new Error('offline'));
    monitor = new ReplicationMonitor(
      makeOpts({ primaryRegion: 'us-east-1', secondaryRegions: ['eu-west-1'] }),
    );

    await monitor._poll();

    const primaryAvail = await readGauge(regionAvailability, { region: 'us-east-1' });
    expect(primaryAvail).toBe(1); // primary is always healthy from its own perspective
  });

  it('sets replication_is_primary gauge on start()', async () => {
    monitor = new ReplicationMonitor(
      makeOpts({ primaryRegion: 'us-east-1', secondaryRegions: [] }),
    );
    monitor.start();
    // Give the immediate _poll() promise a tick to complete.
    await Promise.resolve();

    const isPrimary = await readGauge(replicationIsPrimary, { region: 'us-east-1' });
    expect(isPrimary).toBe(1);
  });

  it('records the health snapshot in getLastHealth() after a poll', async () => {
    fastDbProbe.mockResolvedValue(75);
    monitor = new ReplicationMonitor(
      makeOpts({ primaryRegion: 'us-east-1', secondaryRegions: ['eu-west-1'] }),
    );

    await monitor._poll();

    const map = monitor.getLastHealth();
    expect(map.size).toBe(1);
    const h = map.get('eu-west-1');
    expect(h).toBeDefined();
    expect(h!.lagMs).toBe(75);
    expect(h!.region).toBe('eu-west-1');
    expect(h!.status).toBe('healthy');
    expect(h!.error).toBeUndefined();
  });

  it('records error string in getLastHealth() when probe throws', async () => {
    fastDbProbe.mockRejectedValue(new Error('ETIMEDOUT'));
    monitor = new ReplicationMonitor(
      makeOpts({ primaryRegion: 'us-east-1', secondaryRegions: ['eu-west-1'] }),
    );

    await monitor._poll();

    const h = monitor.getLastHealth().get('eu-west-1');
    expect(h?.status).toBe('unavailable');
    expect(h?.error).toContain('ETIMEDOUT');
  });

  it('isAllHealthy() reflects unhealthy replica state', async () => {
    fastDbProbe.mockRejectedValue(new Error('down'));
    monitor = new ReplicationMonitor(
      makeOpts({ primaryRegion: 'us-east-1', secondaryRegions: ['eu-west-1'] }),
    );

    await monitor._poll();
    expect(monitor.isAllHealthy()).toBe(false);
  });

  it('uses redis probe when no db URL is configured but redis URL is present', async () => {
    // Override env: no db URL, but redis URL is set.
    mockGetEnv.mockReturnValue({
      ...mockEnvBase,
      REPLICA_DATABASE_URL: undefined,
      REPLICA_REDIS_URL: 'redis://replica:6379',
    });

    fastRedisProbe.mockResolvedValue(30);
    monitor = new ReplicationMonitor(
      makeOpts({
        primaryRegion: 'us-east-1',
        secondaryRegions: ['eu-west-1'],
        redisProbe: fastRedisProbe,
        dbProbe: fastDbProbe,
      }),
    );

    await monitor._poll();

    const lag = await readGauge(replicationLagMs, {
      source_region: 'us-east-1',
      target_region: 'eu-west-1',
    });
    expect(lag).toBe(30);
    // db probe must NOT have been called since there is no REPLICA_DATABASE_URL.
    expect(fastDbProbe).not.toHaveBeenCalled();
  });
});

describe('ReplicationMonitor — multiple secondary regions', () => {
  beforeEach(() => {
    replicationLagMs.reset();
    regionAvailability.reset();
    mockGetEnv.mockReturnValue({ ...mockEnvBase });
  });

  afterEach(() => {
    resetReplicationMonitor();
  });

  it('probes all secondary regions independently', async () => {
    let callCount = 0;
    const multiProbe = vi.fn((_url: string): Promise<number> => {
      callCount++;
      return Promise.resolve(callCount * 100);
    });

    const monitor = new ReplicationMonitor(
      makeOpts({
        primaryRegion: 'us-east-1',
        secondaryRegions: ['eu-west-1', 'ap-southeast-1'],
        dbProbe: multiProbe,
      }),
    );

    await monitor._poll();

    const map = monitor.getLastHealth();
    expect(map.size).toBe(2);
    // Both regions should have been polled (one db probe call per region).
    expect(multiProbe).toHaveBeenCalledTimes(2);
    // Each region gets its own health entry.
    expect(map.get('eu-west-1')).toBeDefined();
    expect(map.get('ap-southeast-1')).toBeDefined();

    monitor.stop();
  });
});

describe('ReplicationMonitor — start() / stop() lifecycle', () => {
  beforeEach(() => {
    mockGetEnv.mockReturnValue({ ...mockEnvBase });
  });

  afterEach(() => {
    vi.useRealTimers();
    resetReplicationMonitor();
  });

  it('start() is idempotent — calling it twice creates only one interval', () => {
    vi.useFakeTimers();
    const pollSpy = vi.fn().mockResolvedValue(undefined);
    const monitor = new ReplicationMonitor(
      makeOpts({ secondaryRegions: [], pollIntervalMs: 1_000 }),
    );
    // Replace _poll so we can count invocations.
    monitor._poll = pollSpy;

    monitor.start();
    monitor.start(); // second call must be a no-op

    // Advance by 5 intervals.
    vi.advanceTimersByTime(5_000);

    // 1 immediate call from first start() + 5 timer ticks = 6
    // Second start() must NOT have triggered another immediate call.
    expect(pollSpy).toHaveBeenCalledTimes(6);
    monitor.stop();
  });

  it('stop() is idempotent — calling it twice does not throw', () => {
    const monitor = new ReplicationMonitor(makeOpts({ secondaryRegions: [] }));
    monitor.start();
    expect(() => {
      monitor.stop();
      monitor.stop();
    }).not.toThrow();
  });

  it('stop() prevents further interval ticks', () => {
    vi.useFakeTimers();
    const pollSpy = vi.fn().mockResolvedValue(undefined);
    const monitor = new ReplicationMonitor(
      makeOpts({ secondaryRegions: [], pollIntervalMs: 1_000 }),
    );
    monitor._poll = pollSpy;

    monitor.start();
    vi.advanceTimersByTime(2_000); // 1 immediate + 2 ticks = 3
    monitor.stop();

    vi.advanceTimersByTime(5_000); // should NOT produce more calls
    expect(pollSpy).toHaveBeenCalledTimes(3);
  });
});

describe('ReplicationMonitor — singleton helpers', () => {
  beforeEach(() => {
    mockGetEnv.mockReturnValue({ ...mockEnvBase });
  });

  afterEach(() => {
    resetReplicationMonitor();
  });

  it('getReplicationMonitor() returns the same instance on repeated calls', () => {
    const a = getReplicationMonitor(makeOpts({ secondaryRegions: [] }));
    const b = getReplicationMonitor(); // no opts; must return existing instance
    expect(a).toBe(b);
    a.stop();
  });

  it('setReplicationMonitor() replaces the singleton', () => {
    const original = getReplicationMonitor(makeOpts({ secondaryRegions: [] }));
    const replacement = new ReplicationMonitor(makeOpts({ secondaryRegions: [] }));
    setReplicationMonitor(replacement);
    const fetched = getReplicationMonitor();
    expect(fetched).toBe(replacement);
    expect(fetched).not.toBe(original);
    replacement.stop();
  });

  it('resetReplicationMonitor() clears the singleton so next call creates a fresh one', () => {
    const first = getReplicationMonitor(makeOpts({ secondaryRegions: [] }));
    resetReplicationMonitor();
    const second = getReplicationMonitor(makeOpts({ secondaryRegions: [] }));
    expect(second).not.toBe(first);
    second.stop();
  });
});

describe('ReplicationMonitor — probe injection (defaultDbProbe / defaultRedisProbe shape)', () => {
  it('defaultDbProbe is exported as a function', () => {
    expect(typeof defaultDbProbe).toBe('function');
  });

  it('defaultRedisProbe is exported as a function', () => {
    expect(typeof defaultRedisProbe).toBe('function');
  });
});
