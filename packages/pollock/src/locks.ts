// What each command does to the lock table, end to end: queueing, granting, running hooks,
// releasing, and the idle watcher. Every look at the table first drops what has lapsed (`expire`),
// so no background process is needed for it.
import type { DatabaseSync } from 'node:sqlite';
import * as Clock from 'effect/Clock';
import * as Console from 'effect/Console';
import * as Context from 'effect/Context';
import * as Deferred from 'effect/Deferred';
import * as Duration from 'effect/Duration';
import * as Effect from 'effect/Effect';
import * as Layer from 'effect/Layer';
import * as Path from 'effect/Path';
import * as Ref from 'effect/Ref';
import * as Schema from 'effect/Schema';
import { formatDuration } from './duration.ts';
import { EXIT_CODES } from './failures.ts';
import {
  claimHook,
  type EndedLease,
  enqueue,
  expire,
  findLease,
  finishDown,
  finishHook,
  giveUpUnreported,
  goneFromLine,
  heartbeatHook,
  heartbeatWatcher,
  keepPlace,
  type Lease,
  leaseToRelease,
  leaveWatch,
  markInUse,
  markReported,
  placeOf,
  type ReleaseLookup,
  type ResourceSnapshot,
  randomId,
  release,
  snapshot,
  step,
  watchStep,
} from './leases.ts';
import { describeHolders } from './output.ts';
import { Processes } from './processes.ts';
import { type Hook, type ResourceSettings, Settings, UsageError } from './settings.ts';
import { Signals } from './signals.ts';
import { Store } from './store.ts';

/** There is no lease to release: none held, not this session's, or already ended. */
export class NotHeldError extends Schema.TaggedError<NotHeldError>()('NotHeldError', {
  message: Schema.String,
}) {
  readonly exitCode = EXIT_CODES.notHeld;
}

/** `--ticket` named a place in line that is gone. */
export class TicketGoneError extends Schema.TaggedError<TicketGoneError>()('TicketGoneError', {
  message: Schema.String,
}) {
  readonly exitCode = EXIT_CODES.notHeld;
}

/** The resource's `up` hook failed or timed out. A fresh grant was undone; a held lease is kept. */
export class UpFailedError extends Schema.TaggedError<UpFailedError>()('UpFailedError', {
  message: Schema.String,
  output: Schema.String,
}) {
  readonly exitCode = EXIT_CODES.upFailed;
}

export type Acquired =
  | {
      readonly _tag: 'Granted';
      readonly lease: Lease;
      readonly waitedMs: number;
      /** The session already held this lease: acquiring again returned it. */
      readonly isReused: boolean;
      readonly now: number;
    }
  | {
      readonly _tag: 'TimedOut';
      readonly ticket: string;
      readonly position: number;
      readonly keptUntil: number;
      readonly holders: readonly Lease[];
      readonly now: number;
    };

export type Released = { readonly lease: Lease; readonly isForced: boolean; readonly now: number };

const at = (ms: number) => Temporal.Instant.fromEpochMilliseconds(ms).toString();

/** How an ended lease ended, for its holder. */
const describeEnd = ({ id, reason, endedBy, endedAt }: EndedLease): string => {
  if (reason === 'force-released') {
    return `Lease ${id} was force-released${endedBy === null ? '' : ` by ${endedBy}`} at ${at(endedAt)}.`;
  }
  if (reason === 'up-failed') {
    return `Lease ${id} was undone at ${at(endedAt)}: its up hook failed.`;
  }
  if (reason === 'interrupted') {
    return `Lease ${id} was given up at ${at(endedAt)}: the acquire granted it was stopped before reporting it.`;
  }
  return `Lease ${id} was already released at ${at(endedAt)}.`;
};

/** Why there is no lease to release, as the error that says what to do. */
const lookupError = (
  lookup: Exclude<ReleaseLookup, { readonly _tag: 'Found' }>,
  { resource, session }: { resource: string; session: string | undefined },
): NotHeldError | UsageError => {
  switch (lookup._tag) {
    case 'Ended':
      return new NotHeldError({ message: `${describeEnd(lookup.ended)} Nothing to release.` });
    case 'Unknown':
      return new NotHeldError({
        message: `Lease ${lookup.id} does not exist (or ended over a day ago). Nothing to release.`,
      });
    case 'OtherResource':
      return new UsageError({
        message: `Lease ${lookup.lease.id} is for ${lookup.lease.resource}, not ${resource}.`,
      });
    case 'NotHolder':
      return new NotHeldError({
        message: `Lease ${lookup.lease.id} is held by ${lookup.lease.session}, not ${session}. To end it anyway, run \`pollock ${resource} release --force --lease ${lookup.lease.id}\`.`,
      });
    case 'NoLease':
      return new NotHeldError({
        message:
          session === undefined
            ? `Nobody holds ${resource}. Nothing to release.`
            : `Session ${session} holds no lease on ${resource}. Nothing to release.`,
      });
    case 'Ambiguous':
      return new UsageError({
        message: `${resource} has ${lookup.leases.length} leases: ${lookup.leases.map(lease => `${lease.id} (${lease.session})`).join(', ')}. Pass --lease <id> to say which to end.`,
      });
  }
};

export class Locks extends Context.Service<
  Locks,
  {
    readonly acquire: (options: {
      readonly resource: string;
      readonly session: string;
      readonly note: string | undefined;
      readonly ticket: string | undefined;
      /** How long to wait in line; `Infinity` waits until granted. */
      readonly waitMs: number;
    }) => Effect.Effect<Acquired, UsageError | TicketGoneError | UpFailedError | NotHeldError>;
    /**
     * Ends the session's lease on the resource; forced, ends another session's. Returns it as it
     * was.
     */
    readonly release: (options: {
      readonly resource: string;
      readonly session: string | undefined;
      readonly leaseId: string | undefined;
      readonly isForced: boolean;
    }) => Effect.Effect<Released, UsageError | NotHeldError>;
    readonly status: (options: { readonly resource: string | undefined }) => Effect.Effect<{
      readonly now: number;
      readonly resources: readonly {
        readonly snapshot: ResourceSnapshot;
        readonly settings: ResourceSettings;
      }[];
    }>;
    /** The idle watcher of `resource`: runs `down` once it is idle, then exits. */
    readonly watch: (resource: string) => Effect.Effect<void>;
  }
>()('pollock/Locks') {
  static readonly layer = Layer.effect(
    Locks,
    Effect.gen(function* () {
      const settings = yield* Settings;
      const { timings } = settings;
      const store = yield* Store;
      const processes = yield* Processes;
      const signals = yield* Signals;
      const path = yield* Path.Path;

      /** One transaction that first drops whatever has lapsed. */
      const transact = Effect.fnUntraced(function* <A>(body: (db: DatabaseSync, now: number) => A) {
        const now = yield* Clock.currentTimeMillis;
        return yield* store.transact(db => {
          expire(db, { now, timings });
          return body(db, now);
        });
      });

      const say = (line: string) => Console.error(`pollock: ${line}`);

      /**
       * Says `line` when `key` differs from the last one said, so progress prints when the state
       * changes (the position, the holders), not on every look or every second a holder ages.
       */
      const sayOnChange = (said: Ref.Ref<string>, key: string, line: string) =>
        Effect.gen(function* () {
          if ((yield* Ref.getAndSet(said, key)) !== key) yield* say(line);
        });

      const hookLog = (resource: string, hook: 'up' | 'down') =>
        path.join(settings.stateDir, 'hooks', `${resource}.${hook}.log`);

      const runHook = (
        {
          resource,
          hook,
          kind,
          token,
        }: { resource: string; hook: Hook; kind: 'up' | 'down'; token: string },
        heartbeat?: (db: DatabaseSync, now: number) => void,
      ) =>
        processes.runHook({
          hook,
          logFile: hookLog(resource, kind),
          env: { POLLOCK_RESOURCE: resource },
          heartbeatMs: timings.pollMs,
          onHeartbeat: Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;
            yield* store.transact(db => {
              heartbeatHook(db, { resource, token, now });
              heartbeat?.(db, now);
            });
          }),
        });

      /**
       * Readies a granted lease's resource, as on every grant: owes it a `down` (starting a watcher
       * if none is alive), then takes the hook slot, which waits out a running `down`, and runs `up`.
       * Ends in one transaction that frees the slot, confirms the lease is still live, and marks it
       * reported, or, when `up` failed, undoes it unless a command has already reported it.
       */
      const setUp = Effect.fnUntraced(function* (resource: ResourceSettings, lease: Lease) {
        const needsWatcher = yield* transact((db, now) =>
          markInUse(db, { resource, now, timings }),
        );
        if (needsWatcher) yield* processes.startWatcher(resource.name);
        const { up } = resource;
        const token = randomId();
        const said = yield* Ref.make('');
        const outcome = yield* Effect.gen(function* () {
          if (up === undefined && resource.down === undefined) return undefined;
          yield* transact((db, now) =>
            claimHook(db, { resource: resource.name, hook: 'up', token, now, timings }),
          ).pipe(
            Effect.tap(claim =>
              claim._tag === 'Busy'
                ? sayOnChange(
                    said,
                    claim.hook,
                    `${resource.name}: waiting for its ${claim.hook} hook to finish`,
                  ).pipe(Effect.andThen(Effect.sleep(Duration.millis(timings.pollMs))))
                : Effect.void,
            ),
            Effect.repeat({ until: claim => claim._tag === 'Claimed' }),
          );
          if (up === undefined) return undefined;
          yield* say(`${resource.name}: running up`);
          return yield* runHook({ resource: resource.name, hook: up, kind: 'up', token });
        }).pipe(
          // Frees the slot only if this command holds it: a claim is the token's alone.
          Effect.onInterrupt(() =>
            transact(db => finishHook(db, { resource: resource.name, token })),
          ),
        );
        const isUp = outcome === undefined || outcome.exit === 0;
        const settled = yield* transact((db, now) => {
          finishHook(db, { resource: resource.name, token });
          const found = findLease(db, { resource: resource.name, id: lease.id });
          if (found._tag !== 'Found') return { found, isKept: false };
          if (isUp) {
            markReported(db, { lease, now });
            return { found, isKept: true };
          }
          return {
            found,
            isKept: !giveUpUnreported(db, { lease, reason: 'up-failed', now }),
          };
        });
        // A lease gone by now ended under this command (a force-release): that is the news.
        if (settled.found._tag !== 'Found') {
          const { found } = settled;
          return yield* new NotHeldError({
            message: `${found._tag === 'Ended' ? describeEnd(found.ended) : `Lease ${lease.id} is gone.`} It ended while ${resource.name} was being readied; acquire again if you still need it.`,
          });
        }
        if (up === undefined || outcome === undefined || outcome.exit === 0) return;
        const how =
          outcome.exit === 'timeout'
            ? `timed out after ${formatDuration(up.timeoutMs)}`
            : `exited with ${outcome.exit}`;
        const log = hookLog(resource.name, 'up');
        return yield* new UpFailedError({
          message: settled.isKept
            ? `up for ${resource.name} ${how}. Your lease ${lease.id} is kept: fix the resource or its up command (output below, full log in ${log}) and acquire again, or release the lease if you are done.`
            : `up for ${resource.name} ${how}, so the lease was undone. Fix the resource or its up command (output below, full log in ${log}), then acquire again.`,
          output: outcome.output,
        });
      });

      /** Waits in line: the granted (or already held) lease, or the place kept on giving up. */
      const waitInLine = Effect.fnUntraced(function* ({
        resource,
        session,
        note,
        ticket,
        deadline,
        unreported,
      }: {
        resource: ResourceSettings;
        session: string;
        note: string | undefined;
        ticket: string | undefined;
        deadline: number;
        unreported: Ref.Ref<Lease | undefined>;
      }) {
        const name = resource.name;
        const enqueued = yield* transact((db, now) =>
          enqueue(db, { resource: name, session, note, ticket, newTicket: randomId(), now }),
        );
        if (enqueued._tag === 'Holder') return { _tag: 'Held', lease: enqueued.lease } as const;
        if (enqueued._tag === 'TicketGone') return { _tag: 'TicketGone', isNamed: true } as const;
        const said = yield* Ref.make('');
        const leaseId = randomId();
        // Gives up without being granted: keeps the place and says where it stands.
        const giveUp = (db: DatabaseSync, now: number) => {
          const place = placeOf(db, { resource: name, ticket: enqueued.ticket });
          if (place === undefined) return goneFromLine(db, { resource: name, session });
          return {
            _tag: 'TimedOut',
            ticket: enqueued.ticket,
            ...place,
            keptUntil: keepPlace(db, { ticket: enqueued.ticket, now, timings }),
            now,
          } as const;
        };
        // One look at the line. Once the wait runs out or a stop signal arrives, it gives up
        // instead; a stopped command is never granted, since nothing would report the lease.
        const look = Effect.gen(function* () {
          const looked = yield* transact((db, now) => {
            // Read inside the transaction, so a stop decides this look and no grant can follow it.
            if (Deferred.isDoneUnsafe(signals.received)) return giveUp(db, now);
            const stepped = step(db, { resource, ticket: enqueued.ticket, session, leaseId, now });
            if (stepped._tag !== 'Waiting' || now < deadline) return stepped;
            return giveUp(db, now);
          });
          if (looked._tag === 'Granted') yield* Ref.set(unreported, looked.lease);
          return looked;
        });
        const outcome = yield* look.pipe(
          Effect.tap(looked =>
            looked._tag === 'Waiting'
              ? Clock.currentTimeMillis.pipe(
                  Effect.flatMap(now =>
                    sayOnChange(
                      said,
                      [looked.position, ...looked.holders.map(lease => lease.id)].join(' '),
                      `${name}: waiting, number ${looked.position} in line. ${describeHolders(looked.holders, now)}`,
                    ),
                  ),
                  Effect.andThen(
                    Effect.raceFirst(
                      Effect.sleep(Duration.millis(timings.pollMs)),
                      Effect.asVoid(Deferred.await(signals.received)),
                    ),
                  ),
                )
              : Effect.void,
          ),
          Effect.repeat({
            until: (looked): looked is Exclude<typeof looked, { _tag: 'Waiting' }> =>
              looked._tag !== 'Waiting',
          }),
        );
        return outcome._tag === 'TicketGone'
          ? ({ _tag: 'TicketGone', isNamed: false } as const)
          : outcome;
      });

      return Locks.of({
        acquire: Effect.fn('Locks.acquire')(function* ({
          resource: name,
          session,
          note,
          ticket,
          waitMs,
        }) {
          const resource = settings.resource(name);
          const startedAt = yield* Clock.currentTimeMillis;
          // A fresh grant not yet reported: an interrupt before it is reported gives it up, since
          // nobody would know to release it. A lease the session already held stays held.
          const unreported = yield* Ref.make<Lease | undefined>(undefined);
          const giveUpGrant = Ref.get(unreported).pipe(
            Effect.flatMap(lease =>
              lease === undefined
                ? Effect.void
                : transact((db, now) => {
                    giveUpUnreported(db, { lease, reason: 'interrupted', now });
                  }),
            ),
          );
          return yield* Effect.gen(function* () {
            // Signals are handled from getting in line to giving up, so a harness stopping the
            // command keeps its place; the place's output is printed by the caller.
            const waited = yield* signals.handledWhile(
              waitInLine({
                resource,
                session,
                note,
                ticket,
                deadline: startedAt + waitMs,
                unreported,
              }),
            );
            if (waited._tag === 'TimedOut') return waited;
            if (waited._tag === 'TicketGone') {
              return yield* new TicketGoneError({
                message: waited.isNamed
                  ? `Ticket ${ticket} is gone: a place is kept ${formatDuration(timings.ticketGraceMs)} after its wait runs out. Run \`pollock ${name} acquire\` again, without --ticket, to join the end of the line.`
                  : `This command's place in line for ${name} was dropped: it was not refreshed for ${formatDuration(timings.staleMs)} (was the machine asleep?). Run \`pollock ${name} acquire\` again.`,
              });
            }
            // A stop signal that arrived as the lease was granted still stops the command.
            if (yield* Deferred.isDone(signals.received)) return yield* Effect.interrupt;
            const isReused = waited._tag === 'Held';
            yield* setUp(resource, waited.lease);
            const now = yield* Clock.currentTimeMillis;
            return {
              _tag: 'Granted',
              lease: waited.lease,
              waitedMs: isReused ? 0 : now - startedAt,
              isReused,
              now,
            } as const;
          }).pipe(Effect.onInterrupt(() => giveUpGrant));
        }),

        release: Effect.fn('Locks.release')(function* ({ resource, session, leaseId, isForced }) {
          const found = yield* transact((db, now) => {
            const lookup = leaseToRelease(db, { resource, session, leaseId, isForced });
            if (lookup._tag !== 'Found') return lookup;
            const isTakenFromAnother = lookup.lease.session !== session;
            release(db, {
              lease: lookup.lease,
              reason: isTakenFromAnother ? 'force-released' : 'released',
              endedBy: isTakenFromAnother ? session : undefined,
              now,
            });
            return { ...lookup, isForced: isTakenFromAnother, now };
          });
          if (found._tag !== 'Found') return yield* lookupError(found, { resource, session });
          return { lease: found.lease, isForced: found.isForced, now: found.now };
        }),

        status: Effect.fn('Locks.status')(function* ({ resource }) {
          const { snapshots, now } = yield* transact((db, now) => ({
            snapshots: snapshot(db, {
              only: resource,
              alsoInclude: settings.configured.map(({ name }) => name),
              now,
              timings,
            }),
            now,
          }));
          return {
            now,
            resources: snapshots.map(found => ({
              snapshot: found,
              settings: settings.resource(found.resource),
            })),
          };
        }),

        watch: Effect.fn('Locks.watch')(function* (name) {
          const resource = settings.resource(name);
          const { down } = resource;
          if (down === undefined) return;
          const pid = globalThis.process.pid;
          const intervalMs = Math.max(
            timings.pollMs,
            Math.min(down.idleAfterMs / 10, timings.staleMs / 3),
          );
          // The token this watcher's latest look offered, which holds the slot while it runs `down`;
          // the cleanup below acts on the slot only if the token still holds it.
          const runningDown = yield* Ref.make<string | undefined>(undefined);
          const runDown = Effect.fnUntraced(function* (token: string) {
            const outcome = yield* runHook(
              { resource: name, hook: down, kind: 'down', token },
              (db, now) => heartbeatWatcher(db, { resource: name, now }),
            );
            yield* transact((db, now) =>
              finishDown(db, {
                resource: name,
                token,
                now,
                exitCode: outcome.exit === 'timeout' ? -1 : outcome.exit,
                output: outcome.output,
              }),
            );
          });
          // Each look offers a fresh token, which becomes the slot's if this look claims it.
          const look = Effect.gen(function* () {
            const token = randomId();
            yield* Ref.set(runningDown, token);
            const looked = yield* transact((db, now) =>
              watchStep(db, { resource, pid, token, now, timings }),
            );
            if (looked === 'down') yield* runDown(token);
            if (looked === 'wait') yield* Effect.sleep(Duration.millis(intervalMs));
            return looked;
          });
          yield* look.pipe(
            Effect.repeat({ until: looked => looked === 'exit' }),
            // Stopped (SIGTERM, SIGHUP): hand back what this watcher holds now, rather than leave
            // claims that look alive until they go stale, with a `down` owed and nobody to run it.
            Effect.onInterrupt(() =>
              Ref.get(runningDown).pipe(
                Effect.flatMap(downToken =>
                  transact(db => leaveWatch(db, { resource: name, pid, downToken })),
                ),
              ),
            ),
          );
        }),
      });
    }),
  );
}
