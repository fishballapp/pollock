// How a failure reaches the caller: one JSON line on stderr that says what to do next, and an exit
// code that says what kind of failure it was. The caller is usually an agent, so both are stable.
import * as Console from 'effect/Console';
import * as Effect from 'effect/Effect';
import * as Runtime from 'effect/Runtime';
import * as Schema from 'effect/Schema';

/** What each exit code means. Stable: scripts, hooks and agents branch on them. */
export const EXIT_CODES = {
  ok: 0,
  failure: 1,
  usage: 2,
  waitTimedOut: 3,
  notHeld: 4,
  upFailed: 5,
} as const;

/** Already printed. Carries only the exit code out of the program. */
export class Reported extends Schema.TaggedError<Reported>()('Reported', {
  exitCode: Schema.Int,
}) {
  readonly [Runtime.errorExitCode]: number = this.exitCode;
  readonly [Runtime.errorReported] = false;
}

/** A failure a command reports: its tag is the `code`, its class says the exit code. */
type Reportable = {
  readonly _tag: string;
  readonly message: string;
  readonly exitCode: number;
  readonly output?: string;
};

/** Prints a failure as `{"error":{code,message,exitCode}}` on stderr and exits with its code. */
export const report = <A, E extends Reportable, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, Reported, R> =>
  Effect.catch(
    effect,
    Effect.fnUntraced(function* ({ _tag, message, exitCode, output }) {
      yield* Console.error(
        JSON.stringify({
          error: { code: _tag, message, exitCode, ...(output === undefined ? {} : { output }) },
        }),
      );
      return yield* new Reported({ exitCode });
    }),
  );
