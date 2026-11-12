// What a signal does to a running command. Normally it interrupts the command. While `acquire`
// waits in line it ends the wait instead, as if `--wait` ran out: agent harnesses kill a
// foreground command at their own timeout, and that must not cost the agent its place in line.
import * as Context from 'effect/Context';
import * as Deferred from 'effect/Deferred';
import * as Effect from 'effect/Effect';
import * as Runtime from 'effect/Runtime';

/** The signals a harness or a terminal sends to stop a command. SIGKILL cannot be handled. */
export const STOP_SIGNALS = ['SIGINT', 'SIGTERM', 'SIGHUP'] as const;
export type StopSignal = (typeof STOP_SIGNALS)[number];

export class Signals extends Context.Service<
  Signals,
  {
    /** Runs `effect` with stop signals delivered to `received` instead of interrupting it. */
    readonly handledWhile: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
    /** Completed by the first stop signal that arrives while one is handled. */
    readonly received: Deferred.Deferred<StopSignal>;
  }
>()('pollock/Signals') {}

/** The service, and the hook a process's main loop calls when a signal arrives. */
export const makeSignals = () => {
  const received = Deferred.makeUnsafe<StopSignal>();
  // How many `handledWhile` regions are running; a counter rather than a flag, so they nest.
  let handlers = 0;
  return {
    service: Signals.of({
      received,
      handledWhile: effect =>
        Effect.acquireUseRelease(
          Effect.sync(() => {
            handlers += 1;
          }),
          () => effect,
          () =>
            Effect.sync(() => {
              handlers -= 1;
            }),
        ),
    }),
    /** Hands `signal` to a running `handledWhile`; false when none is running. */
    deliver: (signal: StopSignal): boolean => {
      if (handlers === 0) return false;
      Deferred.doneUnsafe(received, Effect.succeed(signal));
      return true;
    },
  };
};

/**
 * Runs the CLI as the process's main program, as `NodeRuntime.runMain` does, but a stop signal
 * goes to `signals` first and interrupts the program only when nothing handles it. SIGHUP counts
 * too: a closed terminal or a harness may send it.
 */
export const runMain = (signals: ReturnType<typeof makeSignals>) =>
  Runtime.makeRunMain(({ fiber, teardown }) => {
    let isSignalled = false;
    const onSignal = (signal: StopSignal) => {
      isSignalled = true;
      if (!signals.deliver(signal)) fiber.interruptUnsafe(fiber.id);
    };
    for (const signal of STOP_SIGNALS) process.on(signal, onSignal);
    fiber.addObserver(exit => {
      for (const signal of STOP_SIGNALS) process.removeListener(signal, onSignal);
      teardown(exit, code => {
        if (isSignalled || code !== 0) process.exit(code);
      });
    });
  });
