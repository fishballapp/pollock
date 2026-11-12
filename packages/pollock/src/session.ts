/**
 * Who is asking: the session a command acts for. Resolved from `--session` first, then from
 * variables that agent harnesses set in their shells.
 */

/** Checked in order after `--session`; the first one set names the session. */
export const SESSION_VARIABLES = [
  'POLLOCK_SESSION',
  'T3CODE_THREAD_ID',
  'CLAUDE_CODE_SESSION_ID',
  'CODEX_THREAD_ID',
] as const;

type Env = Readonly<Record<string, string | undefined>>;

const firstSet = (env: Env, names: readonly string[]): string | undefined =>
  names.map(name => env[name]?.trim()).find(value => value !== undefined && value !== '');

/** The session from `--session` or the environment; undefined when nothing names one. */
export const resolveSession = ({
  flag,
  env,
}: {
  flag: string | undefined;
  env: Env;
}): string | undefined => flag ?? firstSet(env, SESSION_VARIABLES);
