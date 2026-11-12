// Where the state lives, how often things poll, and what each resource is: the optional
// `resources.json` decoded into settings with every default filled in.
import { homedir } from 'node:os';
import * as Config from 'effect/Config';
import * as Context from 'effect/Context';
import * as Effect from 'effect/Effect';
import * as FileSystem from 'effect/FileSystem';
import * as Layer from 'effect/Layer';
import * as Option from 'effect/Option';
import * as Path from 'effect/Path';
import * as Schema from 'effect/Schema';
import { DurationSchema, parseDuration } from './duration.ts';
import { EXIT_CODES, report } from './failures.ts';

/** The command was used wrongly, or the config file is not one. */
export class UsageError extends Schema.TaggedError<UsageError>()('UsageError', {
  message: Schema.String,
}) {
  readonly exitCode = EXIT_CODES.usage;
}

const MINUTE = 60_000;

const DEFAULTS = {
  hookTimeoutMs: 5 * MINUTE,
  idleAfterMs: 10 * MINUTE,
};

/** Letters, digits, `.`, `_` and `-`, starting with a letter or digit. */
export const RESOURCE_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/u;

const ResourceFileSchema = Schema.Struct({
  capacity: Schema.optionalKey(
    Schema.Union([Schema.Int.check(Schema.isGreaterThan(0)), Schema.Literal('unlimited')]),
  ),
  up: Schema.optionalKey(Schema.String),
  upTimeout: Schema.optionalKey(DurationSchema),
  down: Schema.optionalKey(Schema.String),
  downTimeout: Schema.optionalKey(DurationSchema),
  idleAfter: Schema.optionalKey(DurationSchema),
}).check(
  Schema.makeFilter(
    config =>
      config.down !== undefined ||
      (config.idleAfter === undefined && config.downTimeout === undefined),
    { message: 'idleAfter and downTimeout need down' },
  ),
  Schema.makeFilter(config => config.up !== undefined || config.upTimeout === undefined, {
    message: 'upTimeout needs up',
  }),
);

const ResourcesFileSchema = Schema.fromJsonString(
  Schema.Record(Schema.String.check(Schema.isPattern(RESOURCE_NAME_PATTERN)), ResourceFileSchema),
);

/** A shell command run for a resource, killed with its process group at the timeout. */
export type Hook = { readonly command: string; readonly timeoutMs: number };

export type ResourceSettings = {
  readonly name: string;
  /** How many leases may be held at once; `Infinity` for unlimited. */
  readonly capacity: number;
  readonly up: Hook | undefined;
  readonly down: (Hook & { readonly idleAfterMs: number }) | undefined;
};

/** How often waiting commands poll, and how long state lives without anyone refreshing it. */
export type Timings = {
  /** Between two looks at the database by a waiting command or the watcher. */
  readonly pollMs: number;
  /** A waiter, a running hook or a watcher not refreshed for this long is gone. */
  readonly staleMs: number;
  /** How long a timed-out `acquire` keeps its place in line for `--ticket`. */
  readonly ticketGraceMs: number;
};

const toSettings = (name: string, file: typeof ResourceFileSchema.Type): ResourceSettings => ({
  name,
  capacity: file.capacity === 'unlimited' ? Number.POSITIVE_INFINITY : (file.capacity ?? 1),
  up:
    file.up === undefined
      ? undefined
      : { command: file.up, timeoutMs: file.upTimeout ?? DEFAULTS.hookTimeoutMs },
  down:
    file.down === undefined
      ? undefined
      : {
          command: file.down,
          timeoutMs: file.downTimeout ?? DEFAULTS.hookTimeoutMs,
          idleAfterMs: file.idleAfter ?? DEFAULTS.idleAfterMs,
        },
});

/** An environment variable, with an empty one read as unset. */
const variable = (name: string) =>
  Config.String(name).pipe(
    Config.option,
    Effect.orDie,
    Effect.map(Option.filter(value => value !== '')),
  );

const durationConfig = Effect.fnUntraced(function* (
  name: string,
  { fallbackMs, isZeroAllowed }: { fallbackMs: number; isZeroAllowed: boolean },
) {
  const text = yield* variable(name);
  if (Option.isNone(text)) return fallbackMs;
  const ms = parseDuration(text.value);
  if (Option.isNone(ms) || (ms.value === 0 && !isZeroAllowed)) {
    return yield* new UsageError({
      message: `${name} is not a${isZeroAllowed ? '' : ' non-zero'} duration like 1s or 250ms.`,
    });
  }
  return ms.value;
});

/** `$<name>`, else `$<xdgName>/pollock`, else `~/<xdgFallback>/pollock`; empty is unset. */
const directoryConfig = Effect.fnUntraced(function* (
  name: string,
  xdgName: string,
  xdgFallback: string,
) {
  const path = yield* Path.Path;
  const own = yield* variable(name);
  if (Option.isSome(own)) return own.value;
  const xdg = yield* variable(xdgName);
  return path.join(
    Option.getOrElse(xdg, () => path.join(homedir(), xdgFallback)),
    'pollock',
  );
});

const readResources = Effect.fn('Settings.readResources')(function* (file: string) {
  const fs = yield* FileSystem.FileSystem;
  const text = yield* fs.readFileString(file).pipe(
    Effect.asSome,
    Effect.catchReason('PlatformError', 'NotFound', () => Effect.succeedNone),
    Effect.orDie,
  );
  if (Option.isNone(text)) return new Map<string, ResourceSettings>();
  const decoded = yield* Schema.decodeEffect(ResourcesFileSchema)(text.value, {
    onExcessProperty: 'error',
  }).pipe(
    Effect.mapError(
      error => new UsageError({ message: `${file} is not a valid config: ${error.message}` }),
    ),
  );
  return new Map(Object.entries(decoded).map(([name, config]) => [name, toSettings(name, config)]));
});

const resolve = Effect.gen(function* () {
  const path = yield* Path.Path;
  const stateDir = yield* directoryConfig('POLLOCK_STATE_DIR', 'XDG_STATE_HOME', '.local/state');
  const configDir = yield* directoryConfig('POLLOCK_CONFIG_DIR', 'XDG_CONFIG_HOME', '.config');
  const configFile = path.join(configDir, 'resources.json');
  const resources = yield* readResources(configFile);
  const timings: Timings = {
    pollMs: yield* durationConfig('POLLOCK_POLL_INTERVAL', {
      fallbackMs: 1_000,
      isZeroAllowed: false,
    }),
    staleMs: yield* durationConfig('POLLOCK_STALE_AFTER', {
      fallbackMs: 15_000,
      isZeroAllowed: false,
    }),
    ticketGraceMs: yield* durationConfig('POLLOCK_TICKET_GRACE', {
      fallbackMs: 2 * MINUTE,
      isZeroAllowed: true,
    }),
  };
  return {
    stateDir,
    databaseFile: path.join(stateDir, 'pollock.db'),
    configFile,
    timings,
    /** Every resource the config file names. */
    configured: [...resources.values()],
    /** A resource's settings; one the config does not name is exclusive with the defaults. */
    resource: (name: string): ResourceSettings => resources.get(name) ?? toSettings(name, {}),
  };
});

export class Settings extends Context.Service<Settings, Effect.Success<typeof resolve>>()(
  'pollock/Settings',
) {
  static readonly layer = Layer.effect(Settings, report(resolve));
}
