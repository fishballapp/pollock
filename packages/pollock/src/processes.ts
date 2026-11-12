// Everything pollock does to other processes: running hooks with a timeout, and starting the
// idle watcher detached.
import * as Context from 'effect/Context';
import * as Duration from 'effect/Duration';
import * as Effect from 'effect/Effect';
import * as Fiber from 'effect/Fiber';
import * as FileSystem from 'effect/FileSystem';
import * as Layer from 'effect/Layer';
import * as Option from 'effect/Option';
import * as Path from 'effect/Path';
import * as ChildProcess from 'effect/process/ChildProcess';
import * as ChildProcessSpawner from 'effect/process/ChildProcessSpawner';
import * as Schedule from 'effect/Schedule';
import type { Hook } from './settings.ts';

/**
 * How long a hook's process group gets after SIGTERM before SIGKILL, on a timeout and when the
 * command running it is interrupted: a hook that ignores SIGTERM must not hold its command open.
 */
const HOOK_KILL_GRACE = Duration.seconds(2);

/** How much of a hook's output a failure carries. */
const OUTPUT_TAIL_BYTES = 4_000;

export type HookOutcome = {
  /** The hook's exit code, or `timeout` when it ran past its limit and was killed. */
  readonly exit: number | 'timeout';
  /** The end of what it printed, stdout and stderr together. */
  readonly output: string;
};

export class Processes extends Context.Service<
  Processes,
  {
    /** Runs `hook` through `/bin/sh -c`, `onHeartbeat` every `heartbeatMs` while it runs. */
    readonly runHook: (options: {
      readonly hook: Hook;
      /** Where the hook's output goes, overwritten each run. */
      readonly logFile: string;
      readonly env: Record<string, string>;
      readonly heartbeatMs: number;
      readonly onHeartbeat: Effect.Effect<void>;
    }) => Effect.Effect<HookOutcome>;
    /** Starts `pollock watch <resource>` detached, outliving this command. */
    readonly startWatcher: (resource: string) => Effect.Effect<void>;
  }
>()('pollock/Processes') {
  static readonly layer = Layer.effect(
    Processes,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      const withHeartbeat = <A, E, R>(
        effect: Effect.Effect<A, E, R>,
        { heartbeatMs, onHeartbeat }: { heartbeatMs: number; onHeartbeat: Effect.Effect<void> },
      ) =>
        Effect.gen(function* () {
          const heartbeat = yield* onHeartbeat.pipe(
            Effect.repeat(Schedule.spaced(Duration.millis(heartbeatMs))),
            Effect.forkChild,
          );
          return yield* effect.pipe(Effect.ensuring(Fiber.interrupt(heartbeat)));
        });

      return Processes.of({
        runHook: Effect.fn('Processes.runHook')(function* ({
          hook,
          logFile,
          env,
          heartbeatMs,
          onHeartbeat,
        }) {
          yield* fs.makeDirectory(path.dirname(logFile), { recursive: true }).pipe(Effect.orDie);
          const exit = yield* Effect.scoped(
            Effect.gen(function* () {
              // The shell sends the hook's output to the log rather than a pipe: a process the
              // hook leaves running (a VM, a container) keeps its stdout, and a pipe it holds open
              // would hold this command open too. Its own process group, so a timeout kills
              // everything the hook started.
              const handle = yield* spawner.spawn(
                ChildProcess.make(
                  '/bin/sh',
                  ['-c', 'exec >"$1" 2>&1; eval "$2"', 'sh', logFile, hook.command],
                  {
                    env,
                    extendEnv: true,
                    detached: true,
                    stdin: 'ignore',
                    stdout: 'ignore',
                    stderr: 'ignore',
                    // Also used when the scope closes on an interrupt.
                    forceKillAfter: HOOK_KILL_GRACE,
                  },
                ),
              );
              const exit = yield* withHeartbeat(
                handle.exitCode.pipe(
                  Effect.map(Number),
                  // Killed by a signal: a failure, as a shell would report it.
                  Effect.orElseSucceed(() => 1),
                  Effect.timeoutOption(Duration.millis(hook.timeoutMs)),
                ),
                { heartbeatMs, onHeartbeat },
              );
              if (Option.isNone(exit)) {
                yield* handle.kill({ forceKillAfter: HOOK_KILL_GRACE }).pipe(Effect.ignore);
                return 'timeout' as const;
              }
              // What a successful hook started stays in its process group and must outlive it.
              if (exit.value === 0) yield* Effect.asVoid(handle.unref);
              return exit.value;
            }),
          ).pipe(Effect.orDie);
          const output = yield* fs.readFileString(logFile).pipe(Effect.orElseSucceed(() => ''));
          return { exit, output: output.slice(-OUTPUT_TAIL_BYTES) };
        }),

        startWatcher: Effect.fn('Processes.startWatcher')(function* (resource) {
          const [node, script] = process.argv;
          if (node === undefined || script === undefined) {
            return yield* Effect.die(new Error('process.argv has no script to start the watcher'));
          }
          yield* Effect.scoped(
            Effect.gen(function* () {
              const handle = yield* spawner.spawn(
                ChildProcess.make(
                  process.execPath,
                  [...process.execArgv, script, 'watch', resource],
                  {
                    detached: true,
                    stdin: 'ignore',
                    stdout: 'ignore',
                    stderr: 'ignore',
                  },
                ),
              );
              yield* Effect.asVoid(handle.unref);
            }),
          ).pipe(Effect.orDie);
        }),
      });
    }),
  );
}
