import { describe, expect, it } from '@effect/vitest';
import * as Effect from 'effect/Effect';
import * as Exit from 'effect/Exit';
import { isBusy, whileBusy } from './store.ts';

/** What node:sqlite throws for "database is locked". */
const busy = () => Object.assign(new Error('database is locked'), { errcode: 5 });

describe('whileBusy', () => {
  it('reads SQLITE_BUSY and SQLITE_LOCKED, extended codes included, as busy', () => {
    expect([5, 6, 261, 517].map(errcode => isBusy({ errcode }))).toEqual([true, true, true, true]);
    expect([1, 11, 19].map(errcode => isBusy({ errcode }))).toEqual([false, false, false]);
    expect(isBusy(new Error('database is locked'))).toBe(false);
  });

  it.live('runs a step again from the start while the database answers busy', () =>
    Effect.gen(function* () {
      let attempts = 0;
      const result = yield* whileBusy(() => {
        attempts += 1;
        if (attempts < 3) throw busy();
        return 'done';
      });
      expect({ result, attempts }).toEqual({ result: 'done', attempts: 3 });
    }),
  );

  it.live('fails at once on anything else', () =>
    Effect.gen(function* () {
      let attempts = 0;
      const exit = yield* Effect.exit(
        whileBusy(() => {
          attempts += 1;
          throw Object.assign(new Error('constraint failed'), { errcode: 19 });
        }),
      );
      expect({ isFailure: Exit.isFailure(exit), attempts }).toEqual({
        isFailure: true,
        attempts: 1,
      });
    }),
  );
});
