// What commands print. stdout carries one JSON object per command (`status` prints a table unless
// `--json`), so an agent can parse it; anything said along the way goes to stderr.
import { formatDuration } from './duration.ts';
import type { Lease, ResourceSnapshot, Waiter } from './leases.ts';
import type { ResourceSettings } from './settings.ts';

const iso = (ms: number) => Temporal.Instant.fromEpochMilliseconds(ms).toString();

const clockTime = (ms: number) =>
  Temporal.Instant.fromEpochMilliseconds(ms)
    .toZonedDateTimeISO(Temporal.Now.timeZoneId())
    .toPlainTime()
    .toString({ smallestUnit: 'second' });

/** A duration for people: whole seconds once it is over one. */
const roughly = (ms: number) => formatDuration(ms < 1_000 ? ms : Math.round(ms / 1_000) * 1_000);

const capacityView = (capacity: number) => (Number.isFinite(capacity) ? capacity : 'unlimited');

/** A lease as printed, with how long it has been held by `now`. */
export const leaseView = (lease: Lease, now: number) => ({
  lease: lease.id,
  resource: lease.resource,
  session: lease.session,
  note: lease.note,
  acquiredAt: iso(lease.acquiredAt),
  heldFor: roughly(now - lease.acquiredAt),
});

const waiterView = (waiter: Waiter, index: number) => ({
  position: index + 1,
  session: waiter.session,
  note: waiter.note,
  waitingSince: iso(waiter.enqueuedAt),
  // Set while the place is kept for `acquire --ticket` and no command is waiting on it.
  keptUntil: waiter.keptUntil === null ? null : iso(waiter.keptUntil),
});

export const statusView = (
  { resource, leases, line, state, runningHook }: ResourceSnapshot,
  settings: ResourceSettings,
  now: number,
) => ({
  resource,
  capacity: capacityView(settings.capacity),
  holders: leases.map(lease => leaseView(lease, now)),
  queue: line.map(waiterView),
  hookRunning: runningHook,
  lastDown:
    state?.lastDownAt == null
      ? null
      : { at: iso(state.lastDownAt), exitCode: state.lastDownExit, output: state.lastDownOutput },
});

const noteOf = (lease: Lease) => (lease.note === null ? '' : ` ("${lease.note}")`);

/** The holders in one line: who, for what, and for how long. */
export const describeHolders = (holders: readonly Lease[], now: number) => {
  if (holders.length === 0) {
    return 'Nobody holds it; the places ahead are kept for sessions whose wait ran out.';
  }
  return `Held by ${holders
    .map(
      lease =>
        `${lease.session}${noteOf(lease)} for ${roughly(now - lease.acquiredAt)} (lease ${lease.id})`,
    )
    .join('; ')}.`;
};

/** The human `status`: a block per resource, each holder led by how long it has held. */
export const formatStatus = (
  snapshots: readonly { snapshot: ResourceSnapshot; settings: ResourceSettings }[],
  now: number,
): string => {
  if (snapshots.length === 0) return 'No resources in use or configured.';
  return snapshots
    .map(({ snapshot: { resource, leases, line, state, runningHook }, settings }) => {
      const header = `${resource}  capacity ${capacityView(settings.capacity)}, ${leases.length} held, ${line.length} waiting${runningHook === null ? '' : `, running ${runningHook}`}`;
      const leaseLines = leases.map(
        lease =>
          `  held for ${roughly(now - lease.acquiredAt)} by ${lease.session}${noteOf(lease)}  since ${clockTime(lease.acquiredAt)}, lease ${lease.id}`,
      );
      const queueLines = line.map(
        (waiter, index) =>
          `  ${index + 1}. ${waiter.session}  waiting ${roughly(now - waiter.enqueuedAt)}${waiter.keptUntil === null ? '' : `  (place kept ${roughly(waiter.keptUntil - now)} more)`}${waiter.note === null ? '' : `  "${waiter.note}"`}`,
      );
      const downLine =
        state?.lastDownAt == null
          ? []
          : [`  last down ${roughly(now - state.lastDownAt)} ago, exit ${state.lastDownExit}`];
      return [header, ...leaseLines, ...queueLines, ...downLine].join('\n');
    })
    .join('\n\n');
};
