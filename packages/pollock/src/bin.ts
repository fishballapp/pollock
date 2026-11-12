#!/usr/bin/env node
import * as NodeServices from '@effect/platform-node/NodeServices';
import * as Console from 'effect/Console';
import * as CliConfig from 'effect/cli/CliConfig';
import * as CliOutput from 'effect/cli/CliOutput';
import * as Command from 'effect/cli/Command';
import * as GlobalFlag from 'effect/cli/GlobalFlag';
import * as Effect from 'effect/Effect';
import * as Layer from 'effect/Layer';
import * as Logger from 'effect/Logger';
import packageJson from '../package.json' with { type: 'json' };
import { cli, toCommandFirst } from './cli.ts';
import { EXIT_CODES, Reported } from './failures.ts';
import { makeSignals, runMain, Signals } from './signals.ts';

const formatter = CliOutput.defaultFormatter();
const signals = makeSignals();
const args = toCommandFirst(process.argv.slice(2));

// effect/cli prints the help page with every parse error. Agents need a usage error to be one JSON
// line on stderr and nothing on stdout, so the help page renders only when it was asked for: with
// --help or -h, or with no arguments at all. Otherwise it renders as an empty string, which the
// console below drops (no command prints an empty line).
const isHelpAsked = args.length === 0 || args.includes('--help') || args.includes('-h');

const usageLine = (messages: readonly string[]) =>
  JSON.stringify({
    error: {
      code: 'UsageError',
      message: `${messages.join('. ')}. Run \`pollock --help\` for usage.`,
      exitCode: EXIT_CODES.usage,
    },
  });

/** The default console, but a lone empty line (the help page not asked for) is not printed. */
const WithoutBlankHelp = Layer.effect(
  Console.Console,
  Console.consoleWith(base =>
    Effect.succeed<Console.Console>({
      ...base,
      log: (...values) => {
        if (values.length === 1 && values[0] === '') return;
        base.log(...values);
      },
    }),
  ),
);

Command.runWith(cli, { version: packageJson.version })(args).pipe(
  // effect/cli has printed the help and the parse error; the exit code says it was a usage error.
  Effect.catchTag('ShowHelp', ({ errors }) =>
    errors.length === 0 ? Effect.void : Effect.fail(new Reported({ exitCode: EXIT_CODES.usage })),
  ),
  Effect.provide(
    Layer.mergeAll(
      CliConfig.layer({ builtIns: [GlobalFlag.Help, GlobalFlag.Version] }),
      CliOutput.layer({
        ...formatter,
        formatHelpDoc: doc => (isHelpAsked ? formatter.formatHelpDoc(doc) : ''),
        formatError: error => usageLine([error.message]),
        formatErrors: errors => usageLine(errors.map(error => error.message)),
        formatVersion: (_name, version) => version,
      }),
      WithoutBlankHelp,
      // stdout is the command's JSON; logs go to stderr.
      Layer.succeed(Logger.LogToStderr, true),
      Layer.succeed(Signals, signals.service),
    ).pipe(Layer.provideMerge(NodeServices.layer)),
  ),
  runMain(signals),
);
