// The built CLI as agents use it: real processes against one real database in a temp dir.
//
// A process may take seconds to start on a loaded CI runner, so no assertion may depend on one
// starting within a short bound. Bounds that are not under test are generous (a place goes stale
// after 10s, a kept place lasts a minute); a test that is about a bound sets a short one for its own
// processes and waits on the state it changes. A hook that must still be running when something
// arrives waits for a gate file the test creates, rather than for a time.
import { type ChildProcess, execFile, spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync, type SQLInputValue } from 'node:sqlite';
import { promisify } from 'node:util';
import * as Option from 'effect/Option';
import { build } from 'tsdown';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { parseDuration } from './duration.ts';
import { isBusy } from './store.ts';

// Every command is a real process, and each may take seconds to start on a loaded runner.
vi.setConfig({ testTimeout: 60_000 });

const PACKAGE = join(import.meta.dirname, '..');

/** The bundled bin, as published: a third of the start-up cost of running from source. */
let bin: string;

beforeAll(async () => {
  const outDir = await mkdtemp(join(tmpdir(), 'pollock-bin-'));
  await build({
    cwd: PACKAGE,
    config: join(PACKAGE, 'tsdown.config.ts'),
    outDir,
    logLevel: 'silent',
  });
  bin = join(outDir, 'bin.mjs');
}, 60_000);

type Outcome = { code: number | null; stdout: string; stderr: string };

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'pollock-test-'));
});

/** The pids of every process running this run's bin: commands still going, and watchers. */
const binProcesses = async () => {
  // pgrep exits 1 when nothing matches.
  const { stdout } = await promisify(execFile)('pgrep', ['-f', bin]).catch(() => ({ stdout: '' }));
  return stdout.split('\n').filter(Boolean).map(Number);
};

afterEach(async () => {
  // Watchers are detached and outlive the test: one waits for ever when a lease is left held, and
  // one started by a release stands by for a while. A hook such a process was running is in its
  // own process group and survives it; `waitFor` ends those once the directory is gone.
  for (const pid of await binProcesses()) {
    try {
      process.kill(pid, 'SIGKILL');
    } catch {
      // Already gone.
    }
  }
  await rm(dir, { recursive: true, force: true });
});

const testEnv = () => {
  // A harness running these tests sets session variables of its own; none may leak in.
  const inherited = Object.fromEntries(
    Object.entries(process.env).filter(
      ([name]) => !/^(POLLOCK_|T3CODE_|CLAUDE_|CODEX_)/u.test(name),
    ),
  );
  return {
    ...inherited,
    POLLOCK_STATE_DIR: join(dir, 'state'),
    POLLOCK_CONFIG_DIR: dir,
    POLLOCK_POLL_INTERVAL: '20ms',
    POLLOCK_STALE_AFTER: '10s',
    POLLOCK_TICKET_GRACE: '1m',
  };
};

const configure = (resources: Record<string, unknown>) =>
  writeFile(join(dir, 'resources.json'), JSON.stringify(resources));

/** Starts `pollock <args>` in the background. */
const start = (args: readonly string[], env: Record<string, string> = {}) => {
  const child = spawn(process.execPath, [bin, ...args], {
    env: { ...testEnv(), ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const output = { stdout: '', stderr: '' };
  child.stdout.on('data', chunk => {
    output.stdout += chunk;
  });
  child.stderr.on('data', chunk => {
    output.stderr += chunk;
  });
  const done = new Promise<Outcome>(resolve =>
    child.on('close', code => resolve({ code, ...output })),
  );
  return { child, done, output };
};

/** Runs `pollock <args>` to the end. */
const pollock = (...args: string[]) => start(args).done;

/**
 * A file a hook waits for (`waitFor(gate)`) until the test creates it (`open(gate)`). The wait also
 * ends when the test's directory is removed, so no hook outlives its test.
 */
const gate = (name: string) => join(dir, name);
const waitFor = (path: string) =>
  `while [ ! -e ${path} ] && [ -d ${dir} ]; do sleep 0.05; done; [ -d ${dir} ] || exit 1`;
const open = (path: string) => writeFile(path, '');

const json = (outcome: Outcome) => JSON.parse(outcome.stdout);

/**
 * Resolves once `check` holds, polling every 20ms; fails after `timeoutMs`. A read that finds the
 * database busy (CLI processes write it all the time) counts as "not yet".
 */
const until = async (check: () => Promise<boolean>, timeoutMs = 20_000) => {
  const deadline = Date.now() + timeoutMs;
  const holds = () =>
    check().catch(error => {
      if (isBusy(error)) return false;
      throw error;
    });
  while (!(await holds())) {
    if (Date.now() > deadline) throw new Error('condition not met in time');
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};

/**
 * Reads the CLI's database directly: polling through `status` would spawn a process a look. It
 * opens read-write, never writing, with the CLI's busy timeout: CLI processes write all the time,
 * and a read-only connection cannot recreate the WAL index the last writer removed on closing.
 */
const readDatabase = (sql: string, ...params: SQLInputValue[]) => {
  const db = new DatabaseSync(join(dir, 'state', 'pollock.db'), { timeout: 10_000 });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
};

/** The number one row's `count` column holds. */
const count = (sql: string, resource: string) => Number(readDatabase(sql, resource)[0]?.count);

const queueLength = (resource: string) =>
  count('SELECT count(*) AS count FROM waiter WHERE resource = ?', resource);

const holderCount = (resource: string) =>
  count('SELECT count(*) AS count FROM lease WHERE resource = ?', resource);

const isRunning = (child: ChildProcess) => child.exitCode === null && child.signalCode === null;

/** The JSON error line a failed command printed last on stderr. */
const errorOf = (outcome: Outcome) =>
  JSON.parse(outcome.stderr.trim().split('\n').at(-1) ?? '').error;

describe('acquire', () => {
  it('never grants more than the capacity to acquires started at the same moment', async () => {
    await configure({ pool: { capacity: 2 } });
    const outcomes = await Promise.all(
      ['a', 'b', 'c', 'd'].flatMap(session => [
        pollock('vm', 'acquire', '--session', session, '--wait', '300ms'),
        pollock('pool', 'acquire', '--session', session, '--wait', '300ms'),
      ]),
    );
    const granted = (resource: string) =>
      outcomes.filter(outcome => outcome.code === 0 && json(outcome).resource === resource);
    expect(granted('vm')).toHaveLength(1);
    expect(granted('pool')).toHaveLength(2);
    expect(outcomes.filter(outcome => outcome.code === 3)).toHaveLength(5);
  });

  it('starts many processes at once on an empty state directory', async () => {
    await configure({ pool: { capacity: 'unlimited' } });
    const sessions = Array.from({ length: 20 }, (_, index) => `s${index}`);
    const outcomes = await Promise.all([
      ...sessions.map(session => pollock('pool', 'acquire', '--session', session)),
      ...sessions.map(() => pollock('status', '--json')),
    ]);
    expect(outcomes.filter(outcome => outcome.code !== 0).map(({ stderr }) => stderr)).toEqual([]);
    expect(holderCount('pool')).toBe(20);
  });

  it('serves waiters first come first served, and skips one that was killed', async () => {
    // The killed waiter is dropped once its place goes stale; that bound is under test here, so
    // these processes use a short one (each live waiter refreshes its place every 20ms).
    const staleSoon = { POLLOCK_STALE_AFTER: '2s' };
    expect((await pollock('vm', 'acquire', '--session', 'a')).code).toBe(0);
    const b = start(['vm', 'acquire', '--session', 'b'], staleSoon);
    await until(async () => queueLength('vm') === 1);
    const killed = start(['vm', 'acquire', '--session', 'killed'], staleSoon);
    await until(async () => queueLength('vm') === 2);
    const c = start(['vm', 'acquire', '--session', 'c'], staleSoon);
    await until(async () => queueLength('vm') === 3);
    killed.child.kill('SIGKILL');

    await pollock('vm', 'release', '--session', 'a');
    const grantedB = await b.done;
    expect(grantedB.code).toBe(0);
    // Held since it was granted, not since it started waiting.
    const { heldFor, waited } = json(grantedB);
    expect(Option.getOrThrow(parseDuration(heldFor))).toBeLessThan(
      Option.getOrThrow(parseDuration(waited)),
    );
    expect(isRunning(c.child)).toBe(true);
    expect(b.output.stderr).toContain('number 1 in line. Held by a for');

    await pollock('vm', 'release', '--session', 'b');
    expect((await c.done).code).toBe(0);
  });

  it('times out with a ticket, naming each holder, its note and how long it has held', async () => {
    await pollock('vm', 'acquire', '--session', 'a', '--note', 'driving the simulator');
    const timedOut = await pollock('vm', 'acquire', '--session', 'b', '--wait', '1500ms');
    expect(timedOut.code).toBe(3);
    const { status, ticket, position, holders } = json(timedOut);
    expect({ status, position }).toEqual({ status: 'waiting', position: 1 });
    expect(holders).toEqual([
      expect.objectContaining({
        session: 'a',
        note: 'driving the simulator',
        heldFor: expect.any(String),
      }),
    ]);
    const { message } = errorOf(timedOut);
    expect(message).toContain('Held by a ("driving the simulator") for ');
    expect(message).toContain(`--ticket ${ticket}`);
    expect(message).toMatch(
      /tell the user; they can free it with `pollock vm release --force`\.$/u,
    );
    // Said once, though the holder aged by more than a second while it waited.
    expect(timedOut.stderr.match(/waiting, number 1 in line/gu)).toHaveLength(1);
  });

  it('keeps the place when a signal stops the wait, ahead of whoever queued since', async () => {
    await pollock('vm', 'acquire', '--session', 'a');
    const b = start(['vm', 'acquire', '--session', 'b']);
    await until(async () => queueLength('vm') === 1);
    const c = start(['vm', 'acquire', '--session', 'c']);
    await until(async () => queueLength('vm') === 2);

    // A harness's timeout: SIGTERM to a foreground command.
    b.child.kill('SIGTERM');
    const stopped = await b.done;
    expect(stopped.code).toBe(3);
    const { ticket } = json(stopped);
    expect(json(stopped)).toMatchObject({ status: 'waiting', position: 1 });
    expect(errorOf(stopped).message).toContain(`--ticket ${ticket}`);

    // The session's next acquire takes the same place, still ahead of c, and is served first.
    const again = start(['vm', 'acquire', '--session', 'b']);
    await pollock('vm', 'release', '--session', 'a');
    expect(json(await again.done)).toMatchObject({ status: 'granted', session: 'b' });
    expect(isRunning(c.child)).toBe(true);
    c.child.kill('SIGKILL');
  });

  it.each(['SIGINT', 'SIGHUP'] as const)('keeps the place on %s too', async signal => {
    await pollock('vm', 'acquire', '--session', 'a');
    const b = start(['vm', 'acquire', '--session', 'b']);
    await until(async () => queueLength('vm') === 1);
    b.child.kill(signal);
    expect((await b.done).code).toBe(3);
    expect(queueLength('vm')).toBe(1);
  });

  it('waits 30 minutes unless told otherwise', async () => {
    const help = await pollock('acquire', '--help');
    expect(help.stdout).toMatch(/none waits until granted\. Default: 30m/u);
  });
});

describe('release', () => {
  it('ends the holder’s lease, refuses others, and lets anyone force it', async () => {
    await configure({ pool: { capacity: 2 } });
    const a = json(await pollock('pool', 'acquire', '--session', 'a'));
    const b = json(await pollock('pool', 'acquire', '--session', 'b'));

    const notMine = await pollock('pool', 'release', '--session', 'c');
    expect({ code: notMine.code, stdout: notMine.stdout }).toEqual({ code: 4, stdout: '' });

    const ambiguous = await pollock('pool', 'release', '--force');
    expect(ambiguous.code).toBe(2);
    expect(errorOf(ambiguous).message).toContain(`${a.lease} (a), ${b.lease} (b)`);

    // A person at a terminal, with no session, frees a's lease.
    const forced = await pollock('pool', 'release', '--force', '--lease', a.lease);
    expect(forced.code).toBe(0);
    expect(json(forced)).toMatchObject({
      released: true,
      forced: true,
      lease: a.lease,
      session: 'a',
    });
    expect(forced.stderr).toContain(`force-released lease ${a.lease} on pool, held by a for`);

    const late = await pollock('pool', 'release', '--session', 'a');
    expect(late.code).toBe(4);
    expect(errorOf(late).message).toContain(`Lease ${a.lease} was force-released`);

    const own = await pollock('pool', 'release', '--session', 'b');
    expect(json(own)).toMatchObject({ released: true, forced: false, lease: b.lease });
    expect(holderCount('pool')).toBe(0);
  });
});

describe('hooks', () => {
  it('undoes the grant when a signal stops up', async () => {
    await configure({ vm: { up: waitFor(gate('never')) } });
    const a = start(['vm', 'acquire', '--session', 'a']);
    await until(async () => a.output.stderr.includes('running up'));
    a.child.kill('SIGTERM');
    expect((await a.done).code).toBe(130);
    expect(holderCount('vm')).toBe(0);
  });

  it('waits out a running down before granting a resource that has no up', async () => {
    const log = join(dir, 'hooks.log');
    const downDone = gate('down-done');
    await configure({
      vm: {
        down: `echo "down start" >> ${log}; ${waitFor(downDone)}; echo "down end" >> ${log}`,
        idleAfter: '50ms',
      },
    });
    const lines = async () => (await readFile(log, 'utf8').catch(() => '')).trim().split('\n');
    await pollock('vm', 'acquire', '--session', 'a');
    await pollock('vm', 'release', '--session', 'a');
    await until(async () => (await lines()).includes('down start'));
    const b = start(['vm', 'acquire', '--session', 'b']);
    await until(async () => b.output.stderr.includes('waiting for its down hook to finish'));
    expect(isRunning(b.child)).toBe(true);
    await open(downDone);
    expect((await b.done).code).toBe(0);
    expect(await lines()).toEqual(['down start', 'down end']);
  });

  it('kills an up that ignores SIGTERM when its acquire is stopped', async () => {
    await configure({ vm: { up: `trap '' TERM; ${waitFor(gate('never'))}` } });
    const a = start(['vm', 'acquire', '--session', 'a']);
    await until(async () => a.output.stderr.includes('running up'));
    const stoppedAt = Date.now();
    a.child.kill('SIGTERM');
    expect((await a.done).code).toBe(130);
    // Killed 2s after SIGTERM; the hook alone would never end.
    expect(Date.now() - stoppedAt).toBeLessThan(20_000);
    expect(holderCount('vm')).toBe(0);
  });

  it('keeps a held lease when up fails on acquiring it again', async () => {
    const broken = join(dir, 'broken');
    await configure({ vm: { up: `test ! -e ${broken}` } });
    const held = json(await pollock('vm', 'acquire', '--session', 'a'));
    await writeFile(broken, '');
    const again = await pollock('vm', 'acquire', '--session', 'a');
    expect(again.code).toBe(5);
    expect(errorOf(again).message).toContain(`Your lease ${held.lease} is kept`);
    expect(holderCount('vm')).toBe(1);
  });

  it('fails an acquire whose lease was force-released while up ran', async () => {
    const upDone = gate('up-done');
    await configure({ vm: { up: waitFor(upDone) } });
    const a = start(['vm', 'acquire', '--session', 'a']);
    await until(async () => a.output.stderr.includes('running up'));
    await pollock('vm', 'release', '--force');
    await open(upDone);
    const outcome = await a.done;
    expect(outcome.code).toBe(4);
    expect(outcome.stdout).toBe('');
    expect(errorOf(outcome).message).toContain('was force-released');
  });

  it('starts a watcher again when the holder re-acquires after the watcher died', async () => {
    // Staleness is under test: these processes (and the watchers they start) use a short bound.
    const staleSoon = { POLLOCK_STALE_AFTER: '1s' };
    await configure({ vm: { down: 'true', idleAfter: '1h' } });
    const watcher = (column: string) =>
      count(`SELECT ${column} AS count FROM resource_state WHERE resource = ?`, 'vm');
    const watcherPid = () => watcher('watcher_pid');
    expect((await start(['vm', 'acquire', '--session', 'a'], staleSoon).done).code).toBe(0);
    await until(async () => watcherPid() > 0);
    const first = watcherPid();
    process.kill(first, 'SIGKILL');
    // Its last heartbeat goes stale, as after a reboot.
    await until(async () => Date.now() - watcher('watcher_seen_at') > 1_500);
    expect((await start(['vm', 'acquire', '--session', 'a'], staleSoon).done).code).toBe(0);
    await until(async () => watcherPid() > 0 && watcherPid() !== first);
    process.kill(watcherPid(), 'SIGKILL');
  });

  it('never lets a sibling command revoke a lease another one reported', async () => {
    const downDone = gate('down-done');
    await configure({
      vm: {
        up: 'if [ -n "$FAIL_UP" ]; then exit 1; fi',
        down: waitFor(downDone),
        idleAfter: '50ms',
      },
    });
    const isDownRunning = () =>
      count(
        "SELECT count(*) AS count FROM resource_state WHERE resource = ? AND hook = 'down'",
        'vm',
      ) === 1;
    await pollock('vm', 'acquire', '--session', 'x');
    await pollock('vm', 'release', '--session', 'x');
    await until(async () => isDownRunning());

    // A is granted a fresh lease, finds the slot busy and looks again only 8s later; B, the same
    // session, looks every 20ms, so it takes the slot within moments of down ending (the gate
    // opens a few seconds into A's 8s at most) and reports the lease. A then runs up and fails.
    const a = start(['vm', 'acquire', '--session', 's'], {
      FAIL_UP: '1',
      POLLOCK_POLL_INTERVAL: '8s',
      POLLOCK_STALE_AFTER: '1m',
    });
    await until(async () => a.output.stderr.includes('waiting for its down hook to finish'));
    const pendingB = start(['vm', 'acquire', '--session', 's']);
    await until(async () => pendingB.output.stderr.includes('waiting for its down hook to finish'));
    await open(downDone);
    const b = await pendingB.done;
    expect(b.code).toBe(0);
    expect(json(b)).toMatchObject({ status: 'granted', reused: true });

    // A's up then fails, but the lease B reported stays held.
    const failed = await a.done;
    expect(failed.code).toBe(5);
    expect(errorOf(failed).message).toContain(`Your lease ${json(b).lease} is kept`);
    expect(holderCount('vm')).toBe(1);
  });

  it('reports how a held lease ended when it was force-released while up failed', async () => {
    const broken = gate('broken');
    const upDone = gate('up-done');
    await configure({ vm: { up: `if [ -e ${broken} ]; then ${waitFor(upDone)}; exit 1; fi` } });
    await pollock('vm', 'acquire', '--session', 'a');
    await open(broken);
    const again = start(['vm', 'acquire', '--session', 'a']);
    await until(async () => again.output.stderr.includes('running up'));
    await pollock('vm', 'release', '--force');
    await open(upDone);
    const outcome = await again.done;
    expect(outcome.code).toBe(4);
    expect(errorOf(outcome).message).toContain('was force-released');
  });

  it('hands back a stopped watcher’s claims, so the next watcher runs the down', async () => {
    const log = join(dir, 'down.log');
    await configure({
      vm: { down: `echo down >> ${log}; ${waitFor(gate('never'))}`, idleAfter: '50ms' },
    });
    const downs = async () =>
      (await readFile(log, 'utf8').catch(() => '')).split('\n').filter(Boolean).length;
    const watcherPid = () =>
      count('SELECT watcher_pid AS count FROM resource_state WHERE resource = ?', 'vm');
    await pollock('vm', 'acquire', '--session', 'a');
    await pollock('vm', 'release', '--session', 'a');
    await until(async () => (await downs()) === 1);

    // The release started a second watcher. It takes over: at once if it had not looked yet,
    // else when it looks again after standing by for the stale window (10s).
    const stopped = watcherPid();
    process.kill(stopped, 'SIGTERM');
    await until(async () => (await downs()) === 2, 40_000);
    expect(watcherPid()).not.toBe(stopped);
  });

  it('runs down after a force-release though the holder’s processes and the watcher were killed', async () => {
    // Staleness is under test: the killed watcher looks alive for this long.
    const staleSoon = { POLLOCK_STALE_AFTER: '2s' };
    const log = join(dir, 'down.log');
    const upDone = gate('up-done');
    await configure({
      vm: { up: waitFor(upDone), down: `echo down >> ${log}`, idleAfter: '50ms' },
    });
    const downs = async () =>
      (await readFile(log, 'utf8').catch(() => '')).split('\n').filter(Boolean).length;

    // A harness kills everything its agent started: the acquire (still in up) and the watcher.
    const holder = start(['vm', 'acquire', '--session', 'holder'], staleSoon);
    await until(async () => holder.output.stderr.includes('running up'));
    const watcherPid = () =>
      count('SELECT watcher_pid AS count FROM resource_state WHERE resource = ?', 'vm');
    await until(async () => watcherPid() > 0);
    process.kill(watcherPid(), 'SIGKILL');
    holder.child.kill('SIGKILL');
    await holder.done;
    await open(upDone);
    expect(await binProcesses()).toEqual([]);
    expect(holderCount('vm')).toBe(1);

    // Another session frees the lease at once, while the dead watcher still looks alive.
    const forced = await start(['vm', 'release', '--force', '--session', 'other'], staleSoon).done;
    expect(json(forced)).toMatchObject({ released: true, forced: true, session: 'holder' });
    await until(async () => (await downs()) === 1);
  });

  it('starts a watcher from status when the one that owed a down was killed', async () => {
    const staleSoon = { POLLOCK_STALE_AFTER: '1s' };
    const log = join(dir, 'down.log');
    await configure({ vm: { down: `echo down >> ${log}`, idleAfter: '4s' } });
    const watcherPid = () =>
      count('SELECT watcher_pid AS count FROM resource_state WHERE resource = ?', 'vm');
    const status = async () =>
      json(await start(['vm', 'status', '--json'], staleSoon).done).resources[0].pendingDown;

    expect(await status()).toBeNull();
    expect((await start(['vm', 'acquire', '--session', 'a'], staleSoon).done).code).toBe(0);
    await until(async () => watcherPid() > 0);
    expect(await status()).toEqual({ at: null, watcherRestarted: false });
    // Released, with a watcher standing by beside the first: both are killed.
    expect((await start(['vm', 'release', '--session', 'a'], staleSoon).done).code).toBe(0);
    for (const pid of await binProcesses()) process.kill(pid, 'SIGKILL');

    // The resource is idle and up, with nothing left to shut it down, until a command looks. The
    // dead watcher's heartbeat is still fresh; its pid gives it away.
    expect(await status()).toEqual({ at: expect.any(String), watcherRestarted: true });
    const table = await start(['vm', 'status'], staleSoon).done;
    expect(table.stdout).toMatch(/\n {2}down (in \S+|was due \S+ ago)/u);

    // Killed again: a release with nothing to release is a command that looks, too.
    for (const pid of await binProcesses()) process.kill(pid, 'SIGKILL');
    expect((await start(['vm', 'release', '--session', 'a'], staleSoon).done).code).toBe(4);
    await until(async () => existsSync(log));
    expect(await status()).toBeNull();
  });

  it('undoes the grant when up fails, and says why', async () => {
    await configure({ vm: { up: 'echo booting; echo "no such vm" >&2; exit 3' } });
    const outcome = await pollock('vm', 'acquire', '--session', 'a');
    expect(outcome.code).toBe(5);
    expect(errorOf(outcome)).toMatchObject({
      code: 'UpFailedError',
      output: 'booting\nno such vm\n',
    });
    expect(holderCount('vm')).toBe(0);
  });

  it('runs down once the resource is idle, and never alongside up', async () => {
    const log = join(dir, 'hooks.log');
    const downDone = gate('down-done');
    const hook = (name: string, wait = '') =>
      `echo "${name} start" >> ${log}; ${wait}echo "${name} end" >> ${log}`;
    await configure({
      vm: { up: hook('up'), down: hook('down', `${waitFor(downDone)}; `), idleAfter: '100ms' },
    });
    const lines = async () => (await readFile(log, 'utf8').catch(() => '')).trim().split('\n');

    await pollock('vm', 'acquire', '--session', 'a');
    await pollock('vm', 'release', '--session', 'a');
    // The watcher runs down; an acquire arriving while it runs waits for it, then runs up.
    await until(async () => (await lines()).includes('down start'));
    const b = start(['vm', 'acquire', '--session', 'b']);
    await until(async () => b.output.stderr.includes('waiting for its down hook to finish'));
    await open(downDone);
    expect((await b.done).code).toBe(0);
    expect(await lines()).toEqual([
      'up start',
      'up end',
      'down start',
      'down end',
      'up start',
      'up end',
    ]);

    await pollock('vm', 'release', '--session', 'b');
    await until(async () => (await lines()).length === 8);
    const status = json(await pollock('vm', 'status', '--json')).resources[0];
    expect(status.lastDown).toMatchObject({ exitCode: 0 });
  });
});

describe('the command line', () => {
  it.each([
    ['an unknown flag', ['vm', 'acquire', '--bogus']],
    ['a bad --wait', ['vm', 'acquire', '--wait', 'soon']],
    ['a bad resource name', ['bad/name', 'acquire']],
    ['a flag without its value', ['vm', 'release', '--lease']],
    ['an unknown command', ['vm', 'frobnicate']],
  ])('reports %s as one JSON line on stderr, exit 2, nothing on stdout', async (_, args) => {
    const outcome = await pollock(...args);
    expect({ code: outcome.code, stdout: outcome.stdout }).toEqual({ code: 2, stdout: '' });
    expect(outcome.stderr.trim().split('\n')).toHaveLength(1);
    expect(errorOf(outcome)).toMatchObject({ code: 'UsageError', exitCode: 2 });
  });

  it('refuses a note over 200 characters', async () => {
    const outcome = await pollock('vm', 'acquire', '--session', 'a', '--note', 'x'.repeat(201));
    expect({ code: outcome.code, stdout: outcome.stdout }).toEqual({ code: 2, stdout: '' });
    expect(errorOf(outcome).message).toContain('at most 200 characters');
    expect((await pollock('vm', 'acquire', '--session', 'a', '--note', 'x'.repeat(200))).code).toBe(
      0,
    );
  });

  it('prints help when asked', async () => {
    const help = await pollock('vm', 'acquire', '--help');
    expect(help.code).toBe(0);
    expect(help.stdout).toContain('--wait');
  });

  it('reads an empty directory variable as unset, and refuses a zero poll interval', async () => {
    const env = { POLLOCK_STATE_DIR: '', XDG_STATE_HOME: join(dir, 'xdg') };
    expect((await start(['vm', 'acquire', '--session', 'a'], env).done).code).toBe(0);
    await readFile(join(dir, 'xdg', 'pollock', 'pollock.db'));
    const zero = await start(['status'], { POLLOCK_POLL_INTERVAL: '0s' }).done;
    expect(zero.code).toBe(2);
    expect(errorOf(zero).message).toContain('POLLOCK_POLL_INTERVAL');
  });

  it('reads the resource first, and the session from the environment', async () => {
    const outcome = await pollock('vm', 'acquire');
    expect(outcome.code).toBe(2);
    expect(outcome.stderr).toContain('No session id');
  });

  it('fails loudly on a config file it cannot read', async () => {
    await configure({ vm: { ttl: '10m' } });
    const outcome = await pollock('vm', 'status');
    expect(outcome.code).toBe(2);
    expect(outcome.stderr).toContain('resources.json is not a valid config');
  });

  it('prints every resource in use or configured', async () => {
    await configure({ idle: { capacity: 'unlimited' } });
    await pollock('vm', 'acquire', '--session', 'a', '--note', 'driving the simulator');
    const table = await pollock('status');
    expect(table.stdout).toContain('idle  capacity unlimited, 0 held, 0 waiting');
    expect(table.stdout).toMatch(/held for \S+ by a \("driving the simulator"\)/u);
  });

  it('keeps the status --json shape the README promises', async () => {
    await pollock('vm', 'acquire', '--session', 'a', '--note', 'driving the simulator');
    expect((await pollock('vm', 'acquire', '--session', 'b', '--wait', '10ms')).code).toBe(3);
    const { resources } = json(await pollock('status', '--json'));
    expect(resources).toEqual([
      {
        resource: 'vm',
        capacity: 1,
        holders: [
          {
            lease: expect.any(String),
            resource: 'vm',
            session: 'a',
            note: 'driving the simulator',
            acquiredAt: expect.any(String),
            heldFor: expect.any(String),
          },
        ],
        queue: [
          {
            position: 1,
            session: 'b',
            note: null,
            waitingSince: expect.any(String),
            keptUntil: expect.any(String),
          },
        ],
        hookRunning: null,
        pendingDown: null,
        lastDown: null,
      },
    ]);
  });
});
