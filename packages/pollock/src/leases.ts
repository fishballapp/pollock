// The lock table: every read and write of leases, the line and hook state. Each function runs
// inside a transaction the caller opened (`Store.transact`), takes "now" as an argument and does
// no other I/O, so the rules here are tested against a real database without a clock.
import type { DatabaseSync, SQLInputValue } from 'node:sqlite';
import * as Schema from 'effect/Schema';
import type { ResourceSettings, Timings } from './settings.ts';

const DAY_MS = 86_400_000;

const LeaseSchema = Schema.Struct({
  id: Schema.String,
  resource: Schema.String,
  /** The one session holding it. */
  session: Schema.String,
  note: Schema.NullOr(Schema.String),
  acquiredAt: Schema.Int,
});
export type Lease = typeof LeaseSchema.Type;

const EndReasonSchema = Schema.Literals(['released', 'force-released', 'up-failed', 'interrupted']);
export type EndReason = typeof EndReasonSchema.Type;

const EndedLeaseSchema = Schema.Struct({
  ...LeaseSchema.fields,
  reason: EndReasonSchema,
  /** The session that force-released it, when one is known. */
  endedBy: Schema.NullOr(Schema.String),
  endedAt: Schema.Int,
});
export type EndedLease = typeof EndedLeaseSchema.Type;

const WaiterSchema = Schema.Struct({
  ticket: Schema.String,
  resource: Schema.String,
  session: Schema.String,
  note: Schema.NullOr(Schema.String),
  enqueuedAt: Schema.Int,
  seenAt: Schema.Int,
  keptUntil: Schema.NullOr(Schema.Int),
});
export type Waiter = typeof WaiterSchema.Type;

const ResourceStateSchema = Schema.Struct({
  resource: Schema.String,
  hook: Schema.NullOr(Schema.Literals(['up', 'down'])),
  hookToken: Schema.NullOr(Schema.String),
  hookSeenAt: Schema.NullOr(Schema.Int),
  needsDown: Schema.Int,
  idleSince: Schema.NullOr(Schema.Int),
  watcherPid: Schema.NullOr(Schema.Int),
  watcherSeenAt: Schema.NullOr(Schema.Int),
  lastDownAt: Schema.NullOr(Schema.Int),
  lastDownExit: Schema.NullOr(Schema.Int),
  lastDownOutput: Schema.NullOr(Schema.String),
});
export type ResourceState = typeof ResourceStateSchema.Type;

const readLeases = Schema.decodeUnknownSync(Schema.Array(LeaseSchema));
const readEnded = Schema.decodeUnknownSync(Schema.Array(EndedLeaseSchema));
const readWaiters = Schema.decodeUnknownSync(Schema.Array(WaiterSchema));
const readStates = Schema.decodeUnknownSync(Schema.Array(ResourceStateSchema));
const readNames = Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ name: Schema.String })));

const LEASE_COLUMNS = 'id, resource, session, note, acquired_at AS acquiredAt';
const ENDED_COLUMNS = `${LEASE_COLUMNS}, reason, ended_by AS endedBy, ended_at AS endedAt`;
const WAITER_COLUMNS = `ticket, resource, session, note, enqueued_at AS enqueuedAt,
  seen_at AS seenAt, kept_until AS keptUntil`;
const STATE_COLUMNS = `resource, hook, hook_token AS hookToken, hook_seen_at AS hookSeenAt, needs_down AS needsDown,
  idle_since AS idleSince, watcher_pid AS watcherPid, watcher_seen_at AS watcherSeenAt,
  last_down_at AS lastDownAt, last_down_exit AS lastDownExit, last_down_output AS lastDownOutput`;

type Params = Record<string, SQLInputValue>;

const leasesWhere = (db: DatabaseSync, where: string, params: Params): Lease[] => [
  ...readLeases(
    db
      .prepare(`SELECT ${LEASE_COLUMNS} FROM lease WHERE ${where} ORDER BY acquired_at, id`)
      .all(params),
  ),
];

const leasesOf = (db: DatabaseSync, resource: string) =>
  leasesWhere(db, 'resource = $resource', { resource });

/** The line for `resource`, first in line first. */
const waitersOf = (db: DatabaseSync, resource: string) =>
  readWaiters(
    db
      .prepare(`SELECT ${WAITER_COLUMNS} FROM waiter WHERE resource = $resource ORDER BY seq`)
      .all({ resource }),
  );

const stateOf = (db: DatabaseSync, resource: string): ResourceState | undefined =>
  readStates(
    db
      .prepare(`SELECT ${STATE_COLUMNS} FROM resource_state WHERE resource = $resource`)
      .all({ resource }),
  )[0];

/** The row for `resource` in `resource_state`, created if missing. */
const ensureState = (db: DatabaseSync, resource: string): ResourceState => {
  db.prepare('INSERT OR IGNORE INTO resource_state (resource) VALUES ($resource)').run({
    resource,
  });
  const state = stateOf(db, resource);
  if (state === undefined) throw new Error(`resource_state row for ${resource} vanished`);
  return state;
};

const isFresh = (seenAt: number | null, { now, timings }: { now: number; timings: Timings }) =>
  seenAt !== null && seenAt + timings.staleMs > now;

/** A random id of lowercase letters and digits, easy to pass on a command line. */
export const randomId = (length = 10): string =>
  Array.from(
    crypto.getRandomValues(new Uint8Array(length)),
    byte => 'abcdefghijklmnopqrstuvwxyz0123456789'[byte % 36],
  ).join('');

// ─── Expiry ──────────────────────────────────────────────────────────────────────────────────────

/**
 * Drops what has lapsed by `now`: places in line no waiting command refreshed (or whose kept place
 * ran out), and old records of ended leases. Leases never lapse: only a release ends one.
 */
export const expire = (db: DatabaseSync, { now, timings }: { now: number; timings: Timings }) => {
  db.prepare(
    `DELETE FROM waiter
     WHERE (kept_until IS NULL AND seen_at + $staleMs <= $now) OR kept_until <= $now`,
  ).run({ now, staleMs: timings.staleMs });
  db.prepare('DELETE FROM ended_lease WHERE ended_at <= $cutoff').run({ cutoff: now - DAY_MS });
};

// ─── Releasing ───────────────────────────────────────────────────────────────────────────────────

export type ReleaseLookup =
  | { readonly _tag: 'Found'; readonly lease: Lease }
  /** The lease named, or this session's last one on the resource, has ended. */
  | { readonly _tag: 'Ended'; readonly ended: EndedLease }
  | { readonly _tag: 'Unknown'; readonly id: string }
  | { readonly _tag: 'OtherResource'; readonly lease: Lease }
  /** The lease named is held by another session, and the release is not forced. */
  | { readonly _tag: 'NotHolder'; readonly lease: Lease }
  | { readonly _tag: 'NoLease' }
  /** A forced release without `--lease` on a resource with several leases. */
  | { readonly _tag: 'Ambiguous'; readonly leases: readonly Lease[] };

const endedWhere = (db: DatabaseSync, where: string, params: Params): EndedLease | undefined =>
  readEnded(
    db
      .prepare(
        `SELECT ${ENDED_COLUMNS} FROM ended_lease WHERE ${where} ORDER BY ended_at DESC LIMIT 1`,
      )
      .all(params),
  )[0];

/** The live lease `id` on `resource`, or why there is none. */
export const findLease = (db: DatabaseSync, { resource, id }: { resource: string; id: string }) => {
  const [lease] = leasesWhere(db, 'id = $id', { id });
  if (lease !== undefined) {
    return lease.resource === resource
      ? ({ _tag: 'Found', lease } as const)
      : ({ _tag: 'OtherResource', lease } as const);
  }
  const ended = endedWhere(db, 'id = $id', { id });
  return ended === undefined
    ? ({ _tag: 'Unknown', id } as const)
    : ({ _tag: 'Ended', ended } as const);
};

/**
 * The lease a release would end. Unforced: `session`'s own (`leaseId` if given). Forced: `leaseId`,
 * else the resource's only lease, whoever holds it.
 */
export const leaseToRelease = (
  db: DatabaseSync,
  {
    resource,
    session,
    leaseId,
    isForced,
  }: {
    resource: string;
    session: string | undefined;
    leaseId: string | undefined;
    isForced: boolean;
  },
): ReleaseLookup => {
  if (leaseId !== undefined) {
    const found = findLease(db, { resource, id: leaseId });
    if (found._tag !== 'Found' || isForced || found.lease.session === session) return found;
    return { _tag: 'NotHolder', lease: found.lease };
  }
  const leases = leasesOf(db, resource);
  if (isForced) {
    const [only, ...others] = leases;
    if (only === undefined) return { _tag: 'NoLease' };
    return others.length === 0 ? { _tag: 'Found', lease: only } : { _tag: 'Ambiguous', leases };
  }
  const own = leases.find(lease => lease.session === session);
  if (own !== undefined) return { _tag: 'Found', lease: own };
  const ended = endedWhere(db, 'resource = $resource AND session = $session', {
    resource,
    session: session ?? null,
  });
  return ended === undefined ? { _tag: 'NoLease' } : { _tag: 'Ended', ended };
};

/**
 * Ends `lease` and records how. Only a live lease: one already ended keeps the record of how it
 * ended (a force-release is not overwritten by a later failure). False when it had already ended.
 */
export const release = (
  db: DatabaseSync,
  {
    lease,
    reason,
    endedBy,
    now,
  }: { lease: Lease; reason: EndReason; endedBy: string | undefined; now: number },
): boolean => {
  const { changes } = db.prepare('DELETE FROM lease WHERE id = $id').run({ id: lease.id });
  if (changes === 0) return false;
  db.prepare(
    `INSERT OR REPLACE INTO ended_lease
       (id, resource, session, note, acquired_at, reason, ended_by, ended_at)
     VALUES ($id, $resource, $session, $note, $acquiredAt, $reason, $endedBy, $now)`,
  ).run({ ...lease, reason, endedBy: endedBy ?? null, now });
  return true;
};

// ─── The line ────────────────────────────────────────────────────────────────────────────────────

/** Records that a command has reported `lease` to its session: no give-up may undo it now. */
export const markReported = (db: DatabaseSync, { lease, now }: { lease: Lease; now: number }) =>
  db
    .prepare('UPDATE lease SET reported_at = $now WHERE id = $id AND reported_at IS NULL')
    .run({ id: lease.id, now });

/** Whether a command has reported `lease` yet (a live one; see `findLease`). */
export const isReported = (db: DatabaseSync, { lease }: { lease: Lease }): boolean =>
  db.prepare('SELECT 1 FROM lease WHERE id = $id AND reported_at IS NOT NULL').get({
    id: lease.id,
  }) !== undefined;

/**
 * Gives `lease` up, as `reason`, only while no command has reported it: once one has, its session
 * was told it holds the lease, and a sibling command's failure must not take it away. False when
 * the lease was reported or had already ended.
 */
export const giveUpUnreported = (
  db: DatabaseSync,
  { lease, reason, now }: { lease: Lease; reason: EndReason; now: number },
): boolean => !isReported(db, { lease }) && release(db, { lease, reason, endedBy: undefined, now });

/** The lease `session` holds on `resource`, if any. */
export const heldBy = (
  db: DatabaseSync,
  { resource, session }: { resource: string; session: string },
): Lease | undefined => leasesOf(db, resource).find(lease => lease.session === session);

export type Enqueued =
  /** The session already holds a lease on the resource: acquiring again returns it. */
  | { readonly _tag: 'Holder'; readonly lease: Lease }
  /** In line under `ticket`: a new place, or the place this session (or ticket) already held. */
  | { readonly _tag: 'Queued'; readonly ticket: string; readonly isResumed: boolean }
  /** `--ticket` named a place that is gone. */
  | { readonly _tag: 'TicketGone'; readonly ticket: string };

/**
 * Puts `session` in line for `resource`. A session already in line (its wait ran out) takes its
 * place back rather than queueing behind itself.
 */
export const enqueue = (
  db: DatabaseSync,
  {
    resource,
    session,
    note,
    ticket,
    newTicket,
    now,
  }: {
    resource: string;
    session: string;
    note: string | undefined;
    /** The ticket from `--ticket`, if given. */
    ticket: string | undefined;
    /** The ticket to use for a new place. */
    newTicket: string;
    now: number;
  },
): Enqueued => {
  const held = heldBy(db, { resource, session });
  if (held !== undefined) return { _tag: 'Holder', lease: held };
  const line = waitersOf(db, resource);
  const place =
    ticket === undefined
      ? line.find(waiter => waiter.session === session)
      : line.find(waiter => waiter.ticket === ticket);
  if (ticket !== undefined && place === undefined) return { _tag: 'TicketGone', ticket };
  if (place !== undefined) {
    db.prepare(
      `UPDATE waiter SET session = $session, note = COALESCE($note, note), seen_at = $now,
         kept_until = NULL
       WHERE ticket = $ticket`,
    ).run({ session, note: note ?? null, now, ticket: place.ticket });
    return { _tag: 'Queued', ticket: place.ticket, isResumed: true };
  }
  db.prepare(
    `INSERT INTO waiter (ticket, resource, session, note, enqueued_at, seen_at)
     VALUES ($ticket, $resource, $session, $note, $now, $now)`,
  ).run({ ticket: newTicket, resource, session, note: note ?? null, now });
  return { _tag: 'Queued', ticket: newTicket, isResumed: false };
};

export type Step =
  | { readonly _tag: 'Granted'; readonly lease: Lease }
  | { readonly _tag: 'Waiting'; readonly position: number; readonly holders: readonly Lease[] }
  /** The place is gone, but the session holds a lease: another of its commands was granted it. */
  | { readonly _tag: 'Held'; readonly lease: Lease }
  | { readonly _tag: 'TicketGone' };

/** Where `ticket` stands in the line for `resource`, and who holds it; undefined if gone. */
export const placeOf = (
  db: DatabaseSync,
  { resource, ticket }: { resource: string; ticket: string },
) => {
  const position = waitersOf(db, resource).findIndex(waiter => waiter.ticket === ticket);
  if (position === -1) return undefined;
  return { position: position + 1, holders: leasesOf(db, resource) };
};

/** Why a place in line is gone: its session was granted a lease meanwhile, or it lapsed. */
export const goneFromLine = (
  db: DatabaseSync,
  { resource, session }: { resource: string; session: string },
): Step => {
  const held = heldBy(db, { resource, session });
  return held === undefined ? { _tag: 'TicketGone' } : { _tag: 'Held', lease: held };
};

/**
 * One look at the line by the command waiting on `ticket` for `session`: refreshes its place, and
 * grants it a lease if a slot is free for its position. Places kept by a timed-out `acquire` count.
 */
export const step = (
  db: DatabaseSync,
  {
    resource,
    ticket,
    session,
    leaseId,
    now,
  }: { resource: ResourceSettings; ticket: string; session: string; leaseId: string; now: number },
): Step => {
  const line = waitersOf(db, resource.name);
  const position = line.findIndex(waiter => waiter.ticket === ticket);
  const waiter = line[position];
  if (waiter === undefined) return goneFromLine(db, { resource: resource.name, session });
  db.prepare('UPDATE waiter SET seen_at = $now, kept_until = NULL WHERE ticket = $ticket').run({
    ticket,
    now,
  });
  const holders = leasesOf(db, resource.name);
  if (position >= resource.capacity - holders.length) {
    return { _tag: 'Waiting', position: position + 1, holders };
  }
  db.prepare('DELETE FROM waiter WHERE ticket = $ticket').run({ ticket });
  const lease: Lease = {
    id: leaseId,
    resource: resource.name,
    session: waiter.session,
    note: waiter.note,
    acquiredAt: now,
  };
  db.prepare(
    `INSERT INTO lease (id, resource, session, note, acquired_at)
     VALUES ($id, $resource, $session, $note, $acquiredAt)`,
  ).run(lease);
  return { _tag: 'Granted', lease };
};

/** Keeps `ticket`'s place for the grace period after its command stops waiting. */
export const keepPlace = (
  db: DatabaseSync,
  { ticket, now, timings }: { ticket: string; now: number; timings: Timings },
) => {
  const keptUntil = now + timings.ticketGraceMs;
  db.prepare('UPDATE waiter SET kept_until = $keptUntil WHERE ticket = $ticket').run({
    ticket,
    keptUntil,
  });
  return keptUntil;
};

// ─── Hooks and the idle watcher ──────────────────────────────────────────────────────────────────

/**
 * Notes that `resource` is in use, so the watcher owes it a `down`. True when the resource has
 * `down` and no watcher is alive to run it.
 */
export const markInUse = (
  db: DatabaseSync,
  { resource, now, timings }: { resource: ResourceSettings; now: number; timings: Timings },
): boolean => {
  if (resource.down === undefined) return false;
  const state = ensureState(db, resource.name);
  db.prepare(
    'UPDATE resource_state SET needs_down = 1, idle_since = NULL WHERE resource = $resource',
  ).run({ resource: resource.name });
  return !isFresh(state.watcherSeenAt, { now, timings });
};

export type HookClaim =
  | { readonly _tag: 'Claimed' }
  | { readonly _tag: 'Busy'; readonly hook: 'up' | 'down' };

/** The hook running on `resource`, if its process is still refreshing its claim. */
const runningHook = (
  state: ResourceState | undefined,
  { now, timings }: { now: number; timings: Timings },
) => (state?.hook != null && isFresh(state.hookSeenAt, { now, timings }) ? state.hook : null);

/**
 * Takes the one hook slot of `resource` for `hook` under `token`, unless a live process holds it.
 * Only the holder of `token` refreshes or frees the claim afterwards.
 */
export const claimHook = (
  db: DatabaseSync,
  {
    resource,
    hook,
    token,
    now,
    timings,
  }: { resource: string; hook: 'up' | 'down'; token: string; now: number; timings: Timings },
): HookClaim => {
  const running = runningHook(ensureState(db, resource), { now, timings });
  if (running !== null) return { _tag: 'Busy', hook: running };
  db.prepare(
    `UPDATE resource_state SET hook = $hook, hook_token = $token, hook_seen_at = $now
     WHERE resource = $resource`,
  ).run({ resource, hook, token, now });
  return { _tag: 'Claimed' };
};

/** Shows the hook claimed under `token` is still alive; does nothing once another holds the slot. */
export const heartbeatHook = (
  db: DatabaseSync,
  { resource, token, now }: { resource: string; token: string; now: number },
) =>
  db
    .prepare(
      `UPDATE resource_state SET hook_seen_at = $now
       WHERE resource = $resource AND hook_token = $token`,
    )
    .run({ resource, token, now });

/** Frees the hook slot claimed under `token`; does nothing if it is not (or no longer) ours. */
export const finishHook = (
  db: DatabaseSync,
  { resource, token }: { resource: string; token: string },
) =>
  db
    .prepare(
      `UPDATE resource_state SET hook = NULL, hook_token = NULL, hook_seen_at = NULL
       WHERE resource = $resource AND hook_token = $token`,
    )
    .run({ resource, token });

/** Shows the watcher of `resource` is still alive while it runs `down`. */
export const heartbeatWatcher = (
  db: DatabaseSync,
  { resource, now }: { resource: string; now: number },
) =>
  db
    .prepare('UPDATE resource_state SET watcher_seen_at = $now WHERE resource = $resource')
    .run({ resource, now });

export type WatchStep = 'exit' | 'wait' | 'down';

/**
 * One look by the watcher of `resource` (process `pid`). It exits if another watcher is alive, or
 * once nothing is owed; claims the hook slot for `down` under `token` once the resource has been
 * idle (no holders, no one in line) for `idleAfter`.
 */
export const watchStep = (
  db: DatabaseSync,
  {
    resource,
    pid,
    token,
    now,
    timings,
  }: { resource: ResourceSettings; pid: number; token: string; now: number; timings: Timings },
): WatchStep => {
  const { down } = resource;
  if (down === undefined) return 'exit';
  const state = ensureState(db, resource.name);
  const params = { resource: resource.name, now, pid };
  if (state.watcherPid !== pid && isFresh(state.watcherSeenAt, { now, timings })) return 'exit';
  db.prepare(
    'UPDATE resource_state SET watcher_pid = $pid, watcher_seen_at = $now WHERE resource = $resource',
  ).run(params);
  const isInUse = leasesOf(db, resource.name).length > 0 || waitersOf(db, resource.name).length > 0;
  if (isInUse) {
    db.prepare('UPDATE resource_state SET idle_since = NULL WHERE resource = $resource').run({
      resource: resource.name,
    });
    return 'wait';
  }
  if (state.needsDown === 0) {
    db.prepare(
      'UPDATE resource_state SET watcher_pid = NULL, watcher_seen_at = NULL WHERE resource = $resource',
    ).run({ resource: resource.name });
    return 'exit';
  }
  if (state.idleSince === null) {
    db.prepare('UPDATE resource_state SET idle_since = $now WHERE resource = $resource').run({
      resource: resource.name,
      now,
    });
    return 'wait';
  }
  if (now - state.idleSince < down.idleAfterMs) return 'wait';
  const claim = claimHook(db, { resource: resource.name, hook: 'down', token, now, timings });
  if (claim._tag === 'Busy') {
    return 'wait';
  }
  db.prepare('UPDATE resource_state SET needs_down = 0 WHERE resource = $resource').run({
    resource: resource.name,
  });
  return 'down';
};

/**
 * Undoes what the watcher `pid` holds, when it is stopped: its registration, so the next grant
 * starts a watcher at once, and the slot of a `down` it was running under `downToken`, which stays
 * owed so the next watcher runs it.
 */
export const leaveWatch = (
  db: DatabaseSync,
  { resource, pid, downToken }: { resource: string; pid: number; downToken: string | undefined },
) => {
  db.prepare(
    `UPDATE resource_state SET watcher_pid = NULL, watcher_seen_at = NULL
     WHERE resource = $resource AND watcher_pid = $pid`,
  ).run({ resource, pid });
  if (downToken === undefined) return;
  db.prepare(
    'UPDATE resource_state SET needs_down = 1 WHERE resource = $resource AND hook_token = $token',
  ).run({ resource, token: downToken });
  finishHook(db, { resource, token: downToken });
};

/** Records how `down` went and frees the hook slot. */
export const finishDown = (
  db: DatabaseSync,
  {
    resource,
    token,
    now,
    exitCode,
    output,
  }: { resource: string; token: string; now: number; exitCode: number; output: string },
) => {
  finishHook(db, { resource, token });
  db.prepare(
    `UPDATE resource_state SET idle_since = NULL, last_down_at = $now, last_down_exit = $exitCode,
       last_down_output = $output
     WHERE resource = $resource`,
  ).run({ resource, now, exitCode, output });
};

// ─── Status ──────────────────────────────────────────────────────────────────────────────────────

export type ResourceSnapshot = {
  readonly resource: string;
  readonly leases: readonly Lease[];
  readonly line: readonly Waiter[];
  readonly state: ResourceState | undefined;
  /** The hook running now; a claim nobody refreshes any more is not running. */
  readonly runningHook: 'up' | 'down' | null;
};

/** Every resource with state, plus `alsoInclude` (the configured ones), sorted by name. */
export const snapshot = (
  db: DatabaseSync,
  {
    only,
    alsoInclude,
    now,
    timings,
  }: { only: string | undefined; alsoInclude: readonly string[]; now: number; timings: Timings },
): ResourceSnapshot[] => {
  const known = readNames(
    db
      .prepare(
        `SELECT resource AS name FROM lease UNION SELECT resource FROM waiter
         UNION SELECT resource FROM resource_state`,
      )
      .all(),
  ).map(({ name }) => name);
  const names = only === undefined ? new Set([...known, ...alsoInclude]) : new Set([only]);
  return [...names].toSorted().map(resource => {
    const state = stateOf(db, resource);
    return {
      resource,
      leases: leasesOf(db, resource),
      line: waitersOf(db, resource),
      state,
      runningHook: runningHook(state, { now, timings }),
    };
  });
};
