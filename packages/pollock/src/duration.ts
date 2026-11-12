import * as Option from 'effect/Option';
import * as Schema from 'effect/Schema';
import * as SchemaTransformation from 'effect/SchemaTransformation';

const UNIT_MS = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 } as const;

/** One or more `<integer><unit>` parts, largest unit first by convention: `90s`, `1h30m`, `250ms`. */
const DURATION_PATTERN = /^(?:\d+(?:ms|s|m|h|d))+$/u;
const PART_PATTERN = /(\d+)(ms|s|m|h|d)/gu;

const isUnit = (unit: string): unit is keyof typeof UNIT_MS => Object.hasOwn(UNIT_MS, unit);

/** `10m` → 600000. None for anything that is not a duration. */
export const parseDuration = (text: string): Option.Option<number> => {
  if (!DURATION_PATTERN.test(text)) return Option.none();
  return Option.some(
    text.matchAll(PART_PATTERN).reduce((total, [, amount, unit]) => {
      if (amount === undefined || unit === undefined || !isUnit(unit)) {
        throw new Error(`parseDuration: "${text}" matched the pattern but not its parts`);
      }
      return total + Number(amount) * UNIT_MS[unit];
    }, 0),
  );
};

/** 600000 → `10m`, 90500 → `1m30s500ms`. */
export const formatDuration = (ms: number): string => {
  if (ms <= 0) return '0s';
  const parts = (['d', 'h', 'm', 's', 'ms'] as const).reduce<{ rest: number; text: string }>(
    ({ rest, text }, unit) => {
      const amount = Math.floor(rest / UNIT_MS[unit]);
      return amount === 0
        ? { rest, text }
        : { rest: rest - amount * UNIT_MS[unit], text: `${text}${amount}${unit}` };
    },
    { rest: Math.round(ms), text: '' },
  );
  return parts.text;
};

/** A duration written as text (`10m`), decoded to milliseconds. */
export const DurationSchema = Schema.String.pipe(
  Schema.check(Schema.isPattern(DURATION_PATTERN)),
  Schema.annotate({ description: 'A duration such as 90s, 10m, 1h30m or 250ms' }),
  Schema.decodeTo(
    Schema.Int,
    SchemaTransformation.transform({
      decode: text =>
        Option.getOrThrowWith(parseDuration(text), () => new Error(`not a duration: ${text}`)),
      encode: formatDuration,
    }),
  ),
);
