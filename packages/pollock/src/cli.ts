import * as Config from 'effect/Config';
import * as Console from 'effect/Console';
import * as Argument from 'effect/cli/Argument';
import * as Command from 'effect/cli/Command';
import * as Flag from 'effect/cli/Flag';
import * as Effect from 'effect/Effect';
import * as Layer from 'effect/Layer';
import * as Option from 'effect/Option';
import * as Schema from 'effect/Schema';
import { formatDuration, parseDuration } from './duration.ts';
import { EXIT_CODES, report } from './failures.ts';
import { type Acquired, Locks } from './locks.ts';
import { describeHolders, formatStatus, leaseView, statusView } from './output.ts';
import { Processes } from './processes.ts';
import { resolveSession, SESSION_VARIABLES } from './session.ts';
import { RESOURCE_NAME_PATTERN, Settings, UsageError } from './settings.ts';
import { Signals } from './signals.ts';
import { Store } from './store.ts';

/** `acquire --wait` ran out: the place in line is kept for `--ticket`. */
class WaitTimedOutError extends Schema.TaggedError<WaitTimedOutError>()('WaitTimedOutError', {
  message: Schema.String,
}) {
  readonly exitCode = EXIT_CODES.waitTimedOut;
}

/** How long `acquire` waits without `--wait`: long enough for real work, short enough to notice a stuck holder. */
export const DEFAULT_WAIT = '30m';

/** Keeps every output a few kilobytes at most, so one write puts it in the pipe whole. */
const MAX_NOTE_LENGTH = 200;

/** `--wait`: a duration, or `none` to wait until granted. */
export const parseWait = (text: string): Option.Option<number> =>
  text === 'none' ? Option.some(Number.POSITIVE_INFINITY) : parseDuration(text);

const print = (value: unknown) => Console.log(JSON.stringify(value, null, 2));

const iso = (ms: number) => Temporal.Instant.fromEpochMilliseconds(ms).toString();

/** The session from `--session` or a harness variable; undefined when none names one. */
const findSession = Effect.fnUntraced(function* (flag: Option.Option<string>) {
  const variables = yield* Effect.forEach(
    SESSION_VARIABLES,
    name =>
      Config.String(name).pipe(
        Config.option,
        Effect.orDie,
        Effect.map(value => [name, Option.getOrUndefined(value)] as const),
      ),
    { concurrency: 1 },
  );
  return resolveSession({ flag: Option.getOrUndefined(flag), env: Object.fromEntries(variables) });
});

/** The session a command acts for; a usage error when none is named. */
const requireSession = Effect.fnUntraced(function* (flag: Option.Option<string>) {
  const session = yield* findSession(flag);
  if (session === undefined) {
    return yield* new UsageError({
      message: `No session id. Pass --session <id> or set POLLOCK_SESSION (also read: ${SESSION_VARIABLES.slice(1).join(', ')}).`,
    });
  }
  return session;
});

const resourceArgument = Argument.String('resource').pipe(
  Argument.filterMap(
    name => (RESOURCE_NAME_PATTERN.test(name) ? Option.some(name) : Option.none()),
    name =>
      `Expected a resource name of letters, digits, ".", "_" and "-" (up to 64), got "${name}"`,
  ),
  Argument.withDescription('The resource: any name, the same for every agent that shares it'),
);

const sessionFlag = Flag.String('session').pipe(
  Flag.withDescription(
    `The session acting. Default: $POLLOCK_SESSION, else ${SESSION_VARIABLES.slice(1).join(', ')}`,
  ),
  Flag.optional,
);

/** Prints what `acquire` came to: the lease, or the kept place with who holds the resource. */
const printAcquired = Effect.fnUntraced(function* (resource: string, acquired: Acquired) {
  if (acquired._tag === 'Granted') {
    return yield* print({
      status: 'granted',
      ...leaseView(acquired.lease, acquired.now),
      waited: formatDuration(acquired.waitedMs),
      reused: acquired.isReused,
    });
  }
  const { ticket, position, keptUntil, holders, now } = acquired;
  yield* print({
    status: 'waiting',
    resource,
    ticket,
    position,
    keptUntil: iso(keptUntil),
    holders: holders.map(lease => leaseView(lease, now)),
  });
  const forceRelease =
    holders.length === 1
      ? `pollock ${resource} release --force`
      : `pollock ${resource} release --force --lease <id>`;
  return yield* new WaitTimedOutError({
    message: `Still waiting for ${resource}: number ${position} in line. ${describeHolders(holders, now)} Run \`pollock ${resource} acquire --ticket ${ticket}\` within ${formatDuration(keptUntil - now)} to keep waiting in your place. If a holder looks abandoned, tell the user; they can free it with \`${forceRelease}\`.`,
  });
});

const acquireCommand = Command.make(
  'acquire',
  {
    resource: resourceArgument,
    waitMs: Flag.String('wait').pipe(
      Flag.filterMap(
        parseWait,
        text => `Expected a duration like 90s, 10m or 1h30m, or none, got "${text}"`,
      ),
      Flag.withDescription(
        `Give up after this long, keeping the place in line for --ticket (exit 3); none waits until granted. Default: ${DEFAULT_WAIT}`,
      ),
      Flag.withDefault(Option.getOrThrow(parseWait(DEFAULT_WAIT))),
    ),
    note: Flag.String('note').pipe(
      Flag.filterMap(
        note => (note.length <= MAX_NOTE_LENGTH ? Option.some(note) : Option.none()),
        note => `Expected a note of at most ${MAX_NOTE_LENGTH} characters, got ${note.length}`,
      ),
      Flag.withDescription(
        `What the lease is for, shown to everyone waiting and in status; at most ${MAX_NOTE_LENGTH} characters`,
      ),
      Flag.optional,
    ),
    ticket: Flag.String('ticket').pipe(
      Flag.withDescription('Resume the place in line a timed-out acquire printed'),
      Flag.optional,
    ),
    session: sessionFlag,
  },
  ({ resource, waitMs, note, ticket, session }) =>
    report(
      Effect.gen(function* () {
        const locks = yield* Locks;
        const signals = yield* Signals;
        const options = {
          resource,
          session: yield* requireSession(session),
          note: Option.getOrUndefined(note),
          ticket: Option.getOrUndefined(ticket),
          waitMs,
        };
        // Once acquire returns, its result is printed before anything can interrupt: a lease
        // granted but never reported would stay held with nobody knowing. A stop signal during
        // the print is ignored; acquire itself gives up a fresh grant it is interrupted before.
        return yield* Effect.uninterruptibleMask(restore =>
          Effect.gen(function* () {
            const acquired = yield* restore(locks.acquire(options));
            return yield* signals.handledWhile(printAcquired(resource, acquired));
          }),
        );
      }),
    ),
).pipe(
  Command.withDescription(
    'Wait in line for the resource, then print the lease. The lease is held until released: release it when done',
  ),
);

const releaseCommand = Command.make(
  'release',
  {
    resource: resourceArgument,
    isForced: Flag.Boolean('force').pipe(
      Flag.withDescription(
        'End a lease another session holds, when its holder is gone. With several leases on the resource, name one with --lease',
      ),
      Flag.withDefault(false),
    ),
    lease: Flag.String('lease').pipe(
      Flag.withDescription(
        'The lease to end. Default: this session’s (with --force: the only one)',
      ),
      Flag.optional,
    ),
    session: sessionFlag,
  },
  ({ resource, isForced, lease, session }) =>
    report(
      Effect.gen(function* () {
        const locks = yield* Locks;
        const released = yield* locks.release({
          resource,
          // A person freeing a stuck lock from a terminal has no session; only --force allows that.
          session: isForced ? yield* findSession(session) : yield* requireSession(session),
          leaseId: Option.getOrUndefined(lease),
          isForced,
        });
        const view = leaseView(released.lease, released.now);
        if (released.isForced) {
          yield* Console.error(
            `pollock: force-released lease ${view.lease} on ${resource}, held by ${view.session} for ${view.heldFor}.`,
          );
        }
        return yield* print({ released: true, forced: released.isForced, ...view });
      }),
    ),
).pipe(
  Command.withDescription(
    'End this session’s lease and let the next in line in; --force ends a lease another session holds',
  ),
);

const statusCommand = Command.make(
  'status',
  {
    resource: resourceArgument.pipe(Argument.optional),
    isJson: Flag.Boolean('json').pipe(
      Flag.withDescription('Print JSON instead of a table'),
      Flag.withDefault(false),
    ),
  },
  ({ resource, isJson }) =>
    report(
      Effect.gen(function* () {
        const locks = yield* Locks;
        const { now, resources } = yield* locks.status({
          resource: Option.getOrUndefined(resource),
        });
        if (isJson) {
          return yield* print({
            resources: resources.map(resource => statusView(resource, now)),
          });
        }
        return yield* Console.log(formatStatus(resources, now));
      }),
    ),
).pipe(
  Command.withDescription(
    'Who holds each resource and for how long, and who waits; one resource, or all',
  ),
);

// Started detached by a grant on a resource with `down`; not for people.
const watchCommand = Command.make('watch', { resource: resourceArgument }, ({ resource }) =>
  Effect.gen(function* () {
    const locks = yield* Locks;
    yield* locks.watch(resource);
  }),
).pipe(Command.unlisted);

const commands = [acquireCommand, releaseCommand, statusCommand, watchCommand] as const;

const COMMAND_NAMES = new Set<string>(commands.map(command => command.name));

/**
 * People write the resource first (`pollock my-vm acquire`); effect/cli opens a subcommand only
 * from the first word, so the two swap before parsing (`pollock acquire my-vm`).
 */
export const toCommandFirst = (args: readonly string[]): string[] => {
  const [resource, command, ...rest] = args;
  if (resource === undefined || command === undefined) return [...args];
  if (resource.startsWith('-') || COMMAND_NAMES.has(resource) || !COMMAND_NAMES.has(command)) {
    return [...args];
  }
  return [command, resource, ...rest];
};

const LocksLive = Locks.layer.pipe(
  Layer.provide(Layer.mergeAll(Store.layer, Processes.layer)),
  Layer.provideMerge(Settings.layer),
);

export const cli = Command.make('pollock').pipe(
  Command.withDescription(
    `Let coding agents on one machine queue for a shared resource. Usage: pollock <resource> <command>. A lease is held until released. Every command prints one JSON object on stdout; a failure is one JSON line on stderr. Exit codes: ${EXIT_CODES.ok} ok, ${EXIT_CODES.failure} unexpected failure, ${EXIT_CODES.usage} usage or config, ${EXIT_CODES.waitTimedOut} wait timed out (place kept), ${EXIT_CODES.notHeld} nothing to release or ticket gone, ${EXIT_CODES.upFailed} up failed`,
  ),
  Command.withSubcommands(commands),
  // Inside the tree, so `--help` and `--version` never read the config or open the database.
  Command.provide(LocksLive),
);
