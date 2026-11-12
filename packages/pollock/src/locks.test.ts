// The commands' flows in this process, over the real database and clock: for what depends on
// timing finer than a CLI process takes to start. Bounds that are not under test are generous, so a
// stalled event loop on a loaded runner cannot drop a waiter or lapse a place; tests wait on state
// (who holds, who waits, which hook runs), never on a sleep standing in for it.
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import * as NodeServices from '@effect/platform-node/NodeServices';
import { describe, expect, it } from '@effect/vitest';
import * as ConfigProvider from 'effect/ConfigProvider';
import * as Effect from 'effect/Effect';
import * as Fiber from 'effect/Fiber';
import * as Layer from 'effect/Layer';
import { vi } from 'vitest';
import { Locks } from './locks.ts';
import { Processes } from './processes.ts';
import { Settings } from './settings.ts';
import { makeSignals, Signals } from './signals.ts';
import { Store } from './store.ts';

/**
 * The real layers over a fresh state dir, with millisecond timings, a `resources.json` of
 * `resources`, and the stop signals of `signals` (deliver one with `signals.deliver`).
 */
const withLocks = ({
  resources = {},
  pollInterval = '10ms',
  ticketGrace = '1m',
  signals = makeSignals(),
}: {
  resources?: Record<string, unknown>;
  pollInterval?: string;
  ticketGrace?: string;
  signals?: ReturnType<typeof makeSignals>;
} = {}) => {
  const configDir = mkdtempSync(join(tmpdir(), 'pollock-test-'));
  writeFileSync(join(configDir, 'resources.json'), JSON.stringify(resources));
  return Layer.fresh(
    Locks.layer.pipe(
      Layer.provide(Layer.mergeAll(Store.layer, Processes.layer)),
      Layer.provideMerge(Settings.layer),
      Layer.provide(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown({
            POLLOCK_STATE_DIR: mkdtempSync(join(tmpdir(), 'pollock-test-')),
            POLLOCK_CONFIG_DIR: configDir,
            POLLOCK_POLL_INTERVAL: pollInterval,
            POLLOCK_STALE_AFTER: '1m',
            POLLOCK_TICKET_GRACE: ticketGrace,
          }),
        ),
      ),
      Layer.provide(NodeServices.layer),
      Layer.provide(Layer.succeed(Signals, signals.service)),
    ),
  );
};

// Flows wait on state, which a loaded runner can make slow.
vi.setConfig({ testTimeout: 60_000 });

const acquire = (
  session: string,
  extra: { waitMs?: number; ticket?: string; note?: string } = {},
) =>
  Effect.gen(function* () {
    const locks = yield* Locks;
    return yield* locks.acquire({
      resource: 'vm',
      session,
      note: extra.note,
      ticket: extra.ticket,
      waitMs: extra.waitMs ?? Number.POSITIVE_INFINITY,
    });
  });

const release = (
  session: string | undefined,
  extra: { isForced?: boolean; leaseId?: string } = {},
) =>
  Effect.gen(function* () {
    const locks = yield* Locks;
    return yield* locks.release({
      resource: 'vm',
      session,
      leaseId: extra.leaseId,
      isForced: extra.isForced ?? false,
    });
  });

/** The resource's state as `status` sees it. */
const statusOfVm = Effect.gen(function* () {
  const locks = yield* Locks;
  const { resources } = yield* locks.status({ resource: 'vm' });
  const [vm] = resources;
  if (vm === undefined) throw new Error('status has no vm');
  return vm.snapshot;
});

/** Waits until `check` holds of the resource's state, looking every 10ms. */
const untilVm = (check: (vm: Effect.Success<typeof statusOfVm>) => boolean) =>
  statusOfVm.pipe(
    Effect.map(check),
    Effect.tap(isMet => (isMet ? Effect.void : Effect.sleep('10 millis'))),
    Effect.repeat({ until: isMet => isMet }),
  );

describe('Locks', () => {
  it.live('resumes a timed-out place by its ticket', () =>
    Effect.gen(function* () {
      yield* acquire('a');
      const timedOut = yield* acquire('b', { waitMs: 20 });
      if (timedOut._tag !== 'TimedOut') throw new Error('b was granted');
      expect(timedOut.position).toBe(1);

      // Within the grace period: another session resumes the place, and is served on release.
      const waiting = yield* acquire('b-again', { ticket: timedOut.ticket, waitMs: 20 });
      if (waiting._tag !== 'TimedOut') throw new Error('b-again was granted');
      expect(waiting.ticket).toBe(timedOut.ticket);
      yield* release('a');
      const resumed = yield* acquire('b-again', { ticket: timedOut.ticket });
      expect(resumed).toMatchObject({ _tag: 'Granted', lease: { session: 'b-again' } });
    }).pipe(Effect.provide(withLocks())),
  );

  it.live('loses a kept place once the grace period is over', () =>
    Effect.gen(function* () {
      yield* acquire('a');
      const lapsed = yield* acquire('c', { waitMs: 20 });
      if (lapsed._tag !== 'TimedOut') throw new Error('c was granted');
      // The grace is under test: waiting longer than it is always enough.
      yield* Effect.sleep('300 millis');
      const late = yield* acquire('c', { ticket: lapsed.ticket }).pipe(Effect.flip);
      expect(late).toMatchObject({ _tag: 'TicketGoneError' });
    }).pipe(Effect.provide(withLocks({ ticketGrace: '100ms' }))),
  );

  it.live('times out with each holder, its note and how long it has held', () =>
    Effect.gen(function* () {
      yield* acquire('a', { note: 'driving the simulator' });
      yield* Effect.sleep('50 millis');
      const timedOut = yield* acquire('b', { waitMs: 20 });
      if (timedOut._tag !== 'TimedOut') throw new Error('b was granted');
      const [holder] = timedOut.holders;
      expect(holder).toMatchObject({ session: 'a', note: 'driving the simulator' });
      expect(timedOut.now - (holder?.acquiredAt ?? 0)).toBeGreaterThanOrEqual(50);
    }).pipe(Effect.provide(withLocks())),
  );

  it.live('waits without limit until the holder releases', () =>
    Effect.gen(function* () {
      yield* acquire('a');
      const waiter = yield* Effect.forkChild(acquire('b'));
      yield* untilVm(vm => vm.line.length === 1);
      yield* Effect.sleep('300 millis');
      expect(waiter.pollUnsafe()).toBeUndefined();
      yield* release('a');
      const granted = yield* Fiber.join(waiter);
      expect(granted).toMatchObject({ _tag: 'Granted', lease: { session: 'b' } });
      expect(granted._tag === 'Granted' && granted.waitedMs).toBeGreaterThanOrEqual(300);
    }).pipe(Effect.provide(withLocks())),
  );

  it.live('force-releases another session’s lease, and tells the holder later', () =>
    Effect.gen(function* () {
      const held = yield* acquire('a');
      if (held._tag !== 'Granted') throw new Error('a waited');
      const notMine = yield* release('ops').pipe(Effect.flip);
      expect(notMine).toMatchObject({ _tag: 'NotHeldError' });

      const forced = yield* release(undefined, { isForced: true });
      expect(forced).toMatchObject({ isForced: true, lease: { id: held.lease.id, session: 'a' } });

      const late = yield* release('a').pipe(Effect.flip);
      expect(late).toMatchObject({ _tag: 'NotHeldError' });
      expect(late.message).toContain(`Lease ${held.lease.id} was force-released`);
    }).pipe(Effect.provide(withLocks())),
  );

  it.live('does not grant a waiter once a stop signal arrives, though the slot is free', () => {
    const signals = makeSignals();
    return Effect.gen(function* () {
      yield* acquire('a');
      const waiter = yield* Effect.forkChild(acquire('b'));
      // b gets in line and looks once (in the moment after), then sleeps 10s; the slot frees and the
      // stop arrives meanwhile. The second's settle only lets that first look happen.
      yield* untilVm(vm => vm.line.length === 1);
      yield* Effect.sleep('1 second');
      yield* release('a');
      expect(signals.deliver('SIGTERM')).toBe(true);
      const stopped = yield* Fiber.join(waiter);
      expect(stopped).toMatchObject({ _tag: 'TimedOut', position: 1, holders: [] });
      const notGranted = yield* release('b').pipe(Effect.flip);
      expect(notGranted).toMatchObject({ _tag: 'NotHeldError' });
    }).pipe(Effect.provide(withLocks({ pollInterval: '10s', signals })));
  });

  it.live('gives a session’s two waiting commands the same lease', () =>
    Effect.gen(function* () {
      yield* acquire('a');
      const first = yield* Effect.forkChild(acquire('b'));
      const second = yield* Effect.forkChild(acquire('b'));
      yield* untilVm(vm => vm.line.length === 1);
      yield* release('a');
      const both = [yield* Fiber.join(first), yield* Fiber.join(second)];
      const leases = both.map(acquired => (acquired._tag === 'Granted' ? acquired.lease.id : ''));
      expect(new Set(leases).size).toBe(1);
      expect(
        both.map(acquired => acquired._tag === 'Granted' && acquired.isReused).toSorted(),
      ).toEqual([false, true]);
    }).pipe(Effect.provide(withLocks())),
  );

  it.live('gives up a fresh grant interrupted during up, and keeps a held one', () =>
    Effect.gen(function* () {
      const fresh = yield* Effect.forkChild(acquire('a'));
      yield* untilVm(vm => vm.runningHook === 'up');
      yield* Fiber.interrupt(fresh);
      const gone = yield* release('a').pipe(Effect.flip);
      expect(gone.message).toContain('was given up');
    }).pipe(Effect.provide(withLocks({ resources: { vm: { up: 'sleep 60' } } }))),
  );

  it.live('keeps a held lease when acquiring it again is interrupted during up', () => {
    const slow = join(mkdtempSync(join(tmpdir(), 'pollock-test-')), 'slow');
    return Effect.gen(function* () {
      const held = yield* acquire('a');
      writeFileSync(slow, '');
      const again = yield* Effect.forkChild(acquire('a'));
      yield* untilVm(vm => vm.runningHook === 'up');
      yield* Fiber.interrupt(again);
      const released = yield* release('a');
      expect(released.lease.id).toBe(held._tag === 'Granted' ? held.lease.id : '');
    }).pipe(
      Effect.provide(
        withLocks({ resources: { vm: { up: `if [ -e ${slow} ]; then sleep 60; fi` } } }),
      ),
    );
  });
});
