import type { DatabaseSync } from 'node:sqlite';
import { describe, expect, it } from 'vitest';
import {
  claimHook,
  enqueue,
  expire,
  finishHook,
  giveUpUnreported,
  heartbeatHook,
  keepPlace,
  type Lease,
  leaseToRelease,
  markInUse,
  markReported,
  release,
  type Step,
  snapshot,
  step,
  watchStep,
} from './leases.ts';
import type { ResourceSettings, Timings } from './settings.ts';
import { inTransaction, openDatabase } from './store.ts';

const TIMINGS: Timings = { pollMs: 10, staleMs: 100, ticketGraceMs: 1_000 };
/** Waiters never go stale, so only a kept place's grace matters. */
const PATIENT: Timings = { ...TIMINGS, staleMs: 100_000 };

const YEAR_MS = 365 * 86_400_000;

const exclusive: ResourceSettings = { name: 'vm', capacity: 1, up: undefined, down: undefined };

/** A fresh database and helpers that each run one transaction at an explicit time. */
const world = (resource: ResourceSettings = exclusive, timings: Timings = TIMINGS) => {
  const db = openDatabase(':memory:');
  const tx = <A>(now: number, body: (db: DatabaseSync) => A) =>
    inTransaction(db, () => {
      expire(db, { now, timings });
      return body(db);
    });
  let ids = 0;
  const enqueueAt = (session: string, now: number, ticket?: string) =>
    tx(now, db =>
      enqueue(db, {
        resource: resource.name,
        session,
        note: undefined,
        ticket,
        newTicket: `ticket-${session}`,
        now,
      }),
    );
  /** `session` gets in line at `now`; its ticket. */
  const queue = (session: string, now: number) => {
    const enqueued = enqueueAt(session, now);
    if (enqueued._tag !== 'Queued') {
      throw new Error(`${session} was not queued: ${enqueued._tag}`);
    }
    return enqueued.ticket;
  };
  /** The ticket's session: each test session queues under `ticket-<session>`. */
  const sessionOf = (ticket: string) => ticket.replace(/^ticket-/u, '');
  const lookAt = (ticket: string, now: number): Step =>
    tx(now, db =>
      step(db, { resource, ticket, session: sessionOf(ticket), leaseId: `lease-${++ids}`, now }),
    );
  return {
    db,
    timings,
    tx,
    enqueueAt,
    queue,
    step: lookAt,
    /** `session` gets in line and is granted at once; its lease. */
    /** `session` gets in line and is granted at once, its resource marked in use; its lease. */
    hold: (session: string, now: number) => {
      const stepped = lookAt(queue(session, now), now);
      if (stepped._tag !== 'Granted') {
        throw new Error(`${session} was not granted: ${stepped._tag}`);
      }
      tx(now, db => markInUse(db, { resource, now, timings }));
      return stepped.lease;
    },
    release: (lease: Lease, now: number) =>
      tx(now, db => release(db, { lease, reason: 'released', endedBy: undefined, now })),
    toRelease: (now: number, options: { session?: string; leaseId?: string; isForced?: boolean }) =>
      tx(now, db =>
        leaseToRelease(db, {
          resource: resource.name,
          session: options.session,
          leaseId: options.leaseId,
          isForced: options.isForced ?? false,
        }),
      ),
  };
};

const tagOf = (result: { _tag: string }) => result._tag;

describe('the line', () => {
  it('grants an exclusive resource to one session at a time, first come first served', () => {
    const { hold, queue, step, release } = world();
    const a = hold('a', 0);
    const b = queue('b', 1);
    const c = queue('c', 2);
    expect(step(b, 3)).toMatchObject({ _tag: 'Waiting', position: 1 });
    expect(step(c, 3)).toMatchObject({ _tag: 'Waiting', position: 2 });

    release(a, 4);
    expect(step(c, 5)).toMatchObject({ _tag: 'Waiting', position: 2 });
    expect(step(b, 5)).toMatchObject({ _tag: 'Granted', lease: { session: 'b' } });
    expect(step(c, 6)).toMatchObject({ _tag: 'Waiting', position: 1 });
  });

  it('grants a counted resource up to its capacity', () => {
    const { hold, queue, step } = world({ ...exclusive, capacity: 2 });
    hold('a', 0);
    hold('b', 0);
    expect(tagOf(step(queue('c', 1), 1))).toBe('Waiting');
  });

  it('grants an unlimited resource to everyone', () => {
    const { hold } = world({ ...exclusive, capacity: Number.POSITIVE_INFINITY });
    expect(['a', 'b', 'c', 'd'].map(session => hold(session, 0).id)).toHaveLength(4);
  });

  it('drops a waiter whose command stopped refreshing it, so the next one is served', () => {
    const { hold, queue, step, release } = world();
    const a = hold('a', 0);
    const killed = queue('killed', 1);
    const c = queue('c', 2);
    release(a, 3);
    expect(step(c, 50)).toMatchObject({ _tag: 'Waiting', position: 2 });
    expect(step(c, 101)).toMatchObject({ _tag: 'Granted' });
    expect(tagOf(step(killed, 102))).toBe('TicketGone');
  });

  it('keeps a timed-out place for the grace period, ahead of those behind it', () => {
    const { tx, enqueueAt, hold, queue, step, release, timings } = world(exclusive, PATIENT);
    const a = hold('a', 0);
    const b = queue('b', 1);
    const c = queue('c', 2);
    tx(10, db => keepPlace(db, { ticket: b, now: 10, timings }));
    release(a, 20);
    // b's place is kept though no command refreshes it, and c stays behind it.
    expect(step(c, 500)).toMatchObject({ _tag: 'Waiting', position: 2 });
    // b comes back with its ticket, from another session, and is served.
    expect(enqueueAt('b-elsewhere', 600, b)).toEqual({
      _tag: 'Queued',
      ticket: b,
      isResumed: true,
    });
    expect(step(b, 600)).toMatchObject({ _tag: 'Granted', lease: { session: 'b-elsewhere' } });
  });

  it('loses a kept place once the grace period is over', () => {
    const { tx, enqueueAt, hold, queue, step, timings } = world(exclusive, PATIENT);
    hold('a', 0);
    const b = queue('b', 1);
    const c = queue('c', 2);
    tx(10, db => keepPlace(db, { ticket: b, now: 10, timings }));
    expect(step(c, 1_009)).toMatchObject({ position: 2 });
    expect(step(c, 1_010)).toMatchObject({ position: 1 });
    expect(enqueueAt('b', 1_011, b)).toEqual({ _tag: 'TicketGone', ticket: b });
  });

  it('gives a session in line its own place back instead of queueing it behind itself', () => {
    const { tx, hold, queue, timings } = world();
    hold('a', 0);
    const b = queue('b', 1);
    tx(2, db => keepPlace(db, { ticket: b, now: 2, timings }));
    expect(queue('b', 3)).toBe(b);
  });

  it('gives a session’s second waiting command the lease its first was granted', () => {
    const { hold, queue, step, release } = world();
    const a = hold('a', 0);
    const b = queue('b', 1);
    expect(queue('b', 2)).toBe(b);
    release(a, 3);
    const granted = step(b, 4);
    expect(granted).toMatchObject({ _tag: 'Granted' });
    expect(step(b, 5)).toEqual({
      _tag: 'Held',
      lease: granted._tag === 'Granted' ? granted.lease : undefined,
    });
  });

  it('returns the lease a session already holds when it acquires again', () => {
    const { enqueueAt, hold } = world();
    const a = hold('a', 0);
    expect(enqueueAt('a', 5)).toEqual({ _tag: 'Holder', lease: a });
  });
});

describe('holding', () => {
  it('never ends a lease on its own, however long it is held', () => {
    const { hold, queue, step } = world(exclusive, { ...PATIENT, staleMs: 2 * YEAR_MS });
    const a = hold('a', 0);
    const b = queue('b', 1);
    expect(step(b, YEAR_MS)).toMatchObject({ _tag: 'Waiting', holders: [a] });
  });
});

describe('releasing', () => {
  it('ends the session’s own lease, and refuses one it does not hold', () => {
    const { hold, toRelease } = world({ ...exclusive, capacity: 2 });
    const a = hold('a', 0);
    hold('b', 0);
    expect(toRelease(1, { session: 'a' })).toEqual({ _tag: 'Found', lease: a });
    expect(tagOf(toRelease(1, { session: 'c' }))).toBe('NoLease');
    expect(toRelease(1, { session: 'c', leaseId: a.id })).toEqual({ _tag: 'NotHolder', lease: a });
    expect(tagOf(toRelease(1, { session: 'a', leaseId: 'no-such-lease' }))).toBe('Unknown');
    expect(tagOf(toRelease(1, { session: 'a', leaseId: a.id }))).toBe('Found');
  });

  it('forces the only lease without naming it, and needs --lease among several', () => {
    const { hold, toRelease } = world({ ...exclusive, capacity: 2 });
    expect(tagOf(toRelease(0, { isForced: true }))).toBe('NoLease');
    const a = hold('a', 0);
    expect(toRelease(1, { isForced: true })).toEqual({ _tag: 'Found', lease: a });
    const b = hold('b', 2);
    expect(toRelease(3, { isForced: true })).toEqual({ _tag: 'Ambiguous', leases: [a, b] });
    expect(toRelease(3, { isForced: true, leaseId: b.id })).toEqual({ _tag: 'Found', lease: b });
  });

  it('gives a lease up only until a command reports it', () => {
    const { tx, hold, toRelease } = world({ ...exclusive, capacity: 2 });
    const a = hold('a', 0);
    expect(tx(1, db => giveUpUnreported(db, { lease: a, reason: 'up-failed', now: 1 }))).toBe(true);
    const b = hold('b', 2);
    tx(3, db => markReported(db, { lease: b, now: 3 }));
    expect(tx(4, db => giveUpUnreported(db, { lease: b, reason: 'interrupted', now: 4 }))).toBe(
      false,
    );
    expect(toRelease(5, { session: 'b' })).toEqual({ _tag: 'Found', lease: b });
  });

  it('records only the first ending of a lease', () => {
    const { tx, hold, toRelease } = world();
    const a = hold('a', 0);
    expect(
      tx(5, db => release(db, { lease: a, reason: 'force-released', endedBy: 'ops', now: 5 })),
    ).toBe(true);
    expect(
      tx(6, db => release(db, { lease: a, reason: 'up-failed', endedBy: undefined, now: 6 })),
    ).toBe(false);
    expect(toRelease(7, { session: 'a' })).toMatchObject({ ended: { reason: 'force-released' } });
  });

  it('tells a holder whose lease was force-released who ended it', () => {
    const { tx, hold, toRelease } = world();
    const a = hold('a', 0);
    tx(5, db => release(db, { lease: a, reason: 'force-released', endedBy: 'ops', now: 5 }));
    expect(toRelease(6, { session: 'a' })).toEqual({
      _tag: 'Ended',
      ended: { ...a, reason: 'force-released', endedBy: 'ops', endedAt: 5 },
    });
  });
});

describe('hooks and the idle watcher', () => {
  const withDown: ResourceSettings = {
    ...exclusive,
    down: { command: 'true', timeoutMs: 1_000, idleAfterMs: 50 },
  };

  it('lets one hook run at a time, and frees the slot of one that stopped heartbeating', () => {
    const { tx, timings } = world(withDown);
    const claim = (hook: 'up' | 'down', now: number) =>
      tx(now, db => claimHook(db, { resource: 'vm', hook, token: `${hook}-${now}`, now, timings }));
    expect(claim('down', 0)).toEqual({ _tag: 'Claimed' });
    expect(claim('up', 50)).toEqual({ _tag: 'Busy', hook: 'down' });
    expect(claim('up', 100)).toEqual({ _tag: 'Claimed' });
  });

  it('lets only the claim’s own token refresh or free the slot', () => {
    const { db, tx, timings } = world(withDown);
    const claim = (token: string, now: number) =>
      tx(now, db => claimHook(db, { resource: 'vm', hook: 'up', token, now, timings }));
    const hookOf = () =>
      snapshot(db, { only: 'vm', alsoInclude: [], now: 0, timings })[0]?.state?.hookToken;

    // A command that never got the slot (it was waiting on Busy) frees nothing on its way out.
    expect(claim('first', 0)).toEqual({ _tag: 'Claimed' });
    tx(10, db => finishHook(db, { resource: 'vm', token: 'never-claimed' }));
    expect(claim('second', 20)).toEqual({ _tag: 'Busy', hook: 'up' });

    // After a pause past staleness, a newcomer takes the slot; the old holder's heartbeat and
    // finish no longer touch it.
    expect(claim('newcomer', 200)).toEqual({ _tag: 'Claimed' });
    tx(210, db => heartbeatHook(db, { resource: 'vm', token: 'first', now: 210 }));
    tx(220, db => finishHook(db, { resource: 'vm', token: 'first' }));
    expect(hookOf()).toBe('newcomer');
    expect(claim('third', 250)).toEqual({ _tag: 'Busy', hook: 'up' });
    tx(260, db => finishHook(db, { resource: 'vm', token: 'newcomer' }));
    expect(claim('third', 270)).toEqual({ _tag: 'Claimed' });
  });

  it('reports a hook as running only while its claim is refreshed', () => {
    const { db, tx, timings } = world(withDown);
    tx(0, db => claimHook(db, { resource: 'vm', hook: 'down', token: 't', now: 0, timings }));
    const running = (now: number) =>
      snapshot(db, { only: 'vm', alsoInclude: [], now, timings })[0]?.runningHook;
    expect(running(50)).toBe('down');
    expect(running(100)).toBeNull();
  });

  it('runs down once the resource has been idle for idleAfter, not while anyone is in line', () => {
    const { tx, hold, queue, step, release, timings } = world(withDown);
    const look = (now: number, pid = 1) =>
      tx(now, db => watchStep(db, { resource: withDown, pid, token: `t${now}`, now, timings }));
    const a = hold('a', 0);
    expect(look(1)).toBe('wait');
    const b = queue('b', 2);
    tx(3, db => keepPlace(db, { ticket: b, now: 3, timings }));
    release(a, 4);
    // Nobody holds it, but b's place is kept.
    expect(look(10)).toBe('wait');
    expect(look(500)).toBe('wait');
    expect(look(1_002)).toBe('wait');
    expect(look(1_003)).toBe('wait');
    // Idle since 1003 (b's place lapsed): down is due 50 later, and only once.
    expect(look(1_052)).toBe('wait');
    expect(look(1_053)).toBe('down');
    expect(look(1_054)).toBe('exit');
    expect(step(b, 1_054)._tag).toBe('TicketGone');
  });

  it('leaves the work to a watcher that is already alive', () => {
    const { tx, hold, timings } = world(withDown);
    hold('a', 0);
    const look = (now: number, pid: number) =>
      tx(now, db => watchStep(db, { resource: withDown, pid, token: `t${now}`, now, timings }));
    expect(look(1, 1)).toBe('wait');
    expect(look(2, 2)).toBe('exit');
    expect(look(200, 2)).toBe('wait');
  });
});
