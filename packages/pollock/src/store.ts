// The one SQLite database every pollock process on the machine shares. Each state change is one
// `BEGIN IMMEDIATE` transaction: SQLite lets one writer in at a time and makes the others wait (up
// to the busy timeout), which is what makes acquiring atomic without a daemon.
import { DatabaseSync } from 'node:sqlite';
import * as Context from 'effect/Context';
import * as Duration from 'effect/Duration';
import * as Effect from 'effect/Effect';
import * as FileSystem from 'effect/FileSystem';
import * as Layer from 'effect/Layer';
import * as Predicate from 'effect/Predicate';
import * as Schedule from 'effect/Schedule';
import * as Schema from 'effect/Schema';
import { Settings } from './settings.ts';

/** How long a command waits for another one's transaction before failing. */
const BUSY_TIMEOUT_MS = 10_000;

const MIGRATIONS = [
  `
  -- A lease has one holder, and lives until it is released.
  CREATE TABLE lease (
    id TEXT PRIMARY KEY,
    resource TEXT NOT NULL,
    session TEXT NOT NULL,
    note TEXT,
    acquired_at INTEGER NOT NULL,
    -- Set when a command finishes readying it and reports it: until then, a give-up undoes it.
    reported_at INTEGER
  ) STRICT;
  CREATE INDEX lease_resource ON lease (resource);

  -- How a lease ended and who ended it, so its holder's late release can say so. Pruned after a
  -- day.
  CREATE TABLE ended_lease (
    id TEXT PRIMARY KEY,
    resource TEXT NOT NULL,
    session TEXT NOT NULL,
    note TEXT,
    acquired_at INTEGER NOT NULL,
    reason TEXT NOT NULL,
    ended_by TEXT,
    ended_at INTEGER NOT NULL
  ) STRICT;
  CREATE INDEX ended_lease_holder ON ended_lease (resource, session);

  -- The line, first come first served by seq. kept_until is set while no command waits on the
  -- ticket (its --wait ran out); otherwise the waiting command refreshes seen_at.
  CREATE TABLE waiter (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket TEXT NOT NULL UNIQUE,
    resource TEXT NOT NULL,
    session TEXT NOT NULL,
    note TEXT,
    enqueued_at INTEGER NOT NULL,
    seen_at INTEGER NOT NULL,
    kept_until INTEGER
  ) STRICT;
  CREATE INDEX waiter_resource ON waiter (resource, seq);

  -- Per resource with hooks: the hook running now (one at a time, owned by its claim's token),
  -- and the idle watcher.
  CREATE TABLE resource_state (
    resource TEXT PRIMARY KEY,
    hook TEXT,
    hook_token TEXT,
    hook_seen_at INTEGER,
    needs_down INTEGER NOT NULL DEFAULT 0,
    idle_since INTEGER,
    watcher_pid INTEGER,
    watcher_seen_at INTEGER,
    last_down_at INTEGER,
    last_down_exit INTEGER,
    last_down_output TEXT
  ) STRICT;
  `,
];

/** Runs `body` as one `BEGIN IMMEDIATE` transaction, rolled back if it throws. */
export const inTransaction = <A>(db: DatabaseSync, body: (db: DatabaseSync) => A): A => {
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = body(db);
    db.exec('COMMIT');
    return result;
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
};

const readVersion = Schema.decodeUnknownSync(Schema.Struct({ user_version: Schema.Int }));

/**
 * SQLite answers "database is locked" (SQLITE_BUSY, SQLITE_LOCKED) at once, without waiting out the
 * busy timeout, in a few WAL cases: a connection opening while the last one is cleaning up on close,
 * or while another recovers after a crash. Processes come and go all the time here, so the first
 * is routine. Such a statement changed nothing, and its transaction rolled back.
 */
export const isBusy = (error: unknown): boolean =>
  Predicate.hasProperty(error, 'errcode') &&
  Predicate.isNumber(error.errcode) &&
  [SQLITE_BUSY, SQLITE_LOCKED].includes(error.errcode & 0xff);

const SQLITE_BUSY = 5;
const SQLITE_LOCKED = 6;

/** How often, and how long, a step that met a busy database is tried again. */
const BUSY_RETRY = { interval: Duration.millis(25), times: BUSY_TIMEOUT_MS / 25 };

/** A database step threw; `isBusy` when it may be tried again. */
class StepFailed extends Schema.TaggedError<StepFailed>()('StepFailed', {
  cause: Schema.Defect(),
  isBusy: Schema.Boolean,
}) {}

/** Runs `step`, again from the start whenever the database answers busy, for up to 10s. */
export const whileBusy = <A>(step: () => A): Effect.Effect<A> =>
  Effect.try({ try: step, catch: cause => new StepFailed({ cause, isBusy: isBusy(cause) }) }).pipe(
    Effect.retry({
      while: failed => failed.isBusy,
      times: BUSY_RETRY.times,
      schedule: Schedule.spaced(BUSY_RETRY.interval),
    }),
    Effect.catch(failed => Effect.die(failed.cause)),
  );

const connect = (file: string) =>
  new DatabaseSync(file, { timeout: BUSY_TIMEOUT_MS, enableForeignKeyConstraints: true });

/** Puts `db` in WAL mode and migrates it. Safe to repeat: each step is idempotent. */
const prepare = (db: DatabaseSync) => {
  db.exec('PRAGMA journal_mode = WAL');
  // In WAL mode this cannot corrupt the database; a power cut may lose the last transaction,
  // which costs at most a lease or a place in line. It spares an fsync on every state change.
  db.exec('PRAGMA synchronous = NORMAL');
  inTransaction(db, () => {
    const { user_version: version } = readVersion(db.prepare('PRAGMA user_version').get());
    for (const sql of MIGRATIONS.slice(version)) db.exec(sql);
    db.exec(`PRAGMA user_version = ${MIGRATIONS.length}`);
  });
};

/** Opens and migrates a database nothing else uses: `:memory:`, for the table's tests. */
export const openDatabase = (file: string): DatabaseSync => {
  const db = connect(file);
  prepare(db);
  return db;
};

export class Store extends Context.Service<
  Store,
  {
    /**
     * Runs `body` as one transaction, again from the start if the database answered busy; a
     * failure inside it (busy for 10s) is a defect.
     */
    readonly transact: <A>(body: (db: DatabaseSync) => A) => Effect.Effect<A>;
  }
>()('pollock/Store') {
  static readonly layer = Layer.effect(
    Store,
    Effect.gen(function* () {
      const { stateDir, databaseFile } = yield* Settings;
      const fs = yield* FileSystem.FileSystem;
      yield* fs.makeDirectory(stateDir, { recursive: true }).pipe(Effect.orDie);
      const db = yield* Effect.acquireRelease(
        Effect.sync(() => connect(databaseFile)),
        db => Effect.sync(() => db.close()),
      );
      yield* whileBusy(() => prepare(db));
      return Store.of({ transact: body => whileBusy(() => inTransaction(db, body)) });
    }),
  );
}
