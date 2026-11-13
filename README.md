# pollock

**A lock queue for coding agents: they take turns with a shared resource on one machine.**

Run several coding agents side by side and some things can serve only one of them at a time: a VM
whose GUI an agent drives, a device, a test account. Two agents clicking in one VM ruin each
other's work, and neither can see the other. pollock gives each resource a line: an agent
acquires a **lease**, works, and releases it; the next in line gets it.

- **A lease is held until it is released.** It belongs to the agent's session, not to a process:
  `acquire` returns as soon as the lease is granted, and the agent keeps it across its later
  commands, turns and helpers. Nothing times out from under an agent that is still working.
- **A stuck lock is loud, and one command frees it.** A waiter that gives up says who holds the
  resource, for what and for how long; `status` shows the same; `release --force` ends it.
- **No daemon.** State is one SQLite file; every change is one transaction. Waiting is a loop
  inside the waiting command, so it costs an agent no tokens.
- **Every command prints one JSON object**, and fails with one line that says what to do next and
  a documented exit code.
- **Optional `up` and `down` hooks** start a resource on demand and shut it down once nobody has
  used it for a while.

It assumes no particular agent: it reads the session ids some harnesses export, and takes
`--session` from anything else.

## Install

```sh
npm i -g @fsh.ba/pollock
```

Node 26 or later (it uses the built-in `node:sqlite`), on macOS or Linux. It installs no
dependencies.

## Quick start

```sh
# Get in line for the VM as a background command, and carry on with other work. When it exits
# with the lease (JSON on stdout), switch to the VM.
pollock my-vm acquire --wait none --note "testing the login flow"

# ... drive the VM ...

# Done: the next in line gets it.
pollock my-vm release

# Who holds what, for how long, and who is waiting.
pollock status
```

A resource is any name (letters, digits, `.`, `_`, `-`). With no config, it is exclusive: one lease
at a time.

## Sessions

Every command acts for a **session**: the agent conversation or task, which outlives any one shell
command. pollock takes the first of:

1. `--session <id>`
2. `$POLLOCK_SESSION`
3. a variable an agent harness sets: `T3CODE_THREAD_ID`, `CLAUDE_CODE_SESSION_ID`,
   `CODEX_THREAD_ID`

and fails with exit 2 if none is set. Set `POLLOCK_SESSION` once in the agent's environment for
any other harness. A lease has exactly one holder: the session that acquired it.

## Commands

The resource comes first: `pollock <resource> <command>`.

### `acquire`

```
pollock <resource> acquire [--wait <duration>] [--note <text>] [--ticket <ticket>] [--session <id>]
```

Gets in line and waits, first come first served, until a lease is granted; runs the resource's
`up` hook; prints the lease. While waiting it says on stderr who holds the resource and where it
stands in line, whenever that changes.

**Run it in the background where the harness can.** An agent that waits in the foreground can do
nothing else until the command returns or the tool's command timeout kills it. Where the harness
has background commands that report back when they exit (Claude Code does: it re-invokes the
agent when a background command exits), start `acquire --wait none` as one, carry on with other
work, and start on the resource as soon as the command exits with the lease. `--wait none` is
right there: the harness's own limit on background commands ends the wait if it must, and a
waiter stopped that way keeps its place (below).

**Otherwise, wait in the foreground with `--wait` shorter than the tool's command timeout**, so
`acquire` returns on its own and says who holds the resource. On exit 3, run
`acquire --ticket <ticket>` (or `acquire` again from the same session) to keep waiting in the
same place.

The lease starts when it is granted, not when the agent gets to it. Switch to the resource
promptly: others are waiting behind you.

- `--wait <duration>`: give up after this long; **30m** by default, `none` to wait until granted.
  On giving up it exits 3, prints `{"status": "waiting", "ticket": …, "holders": […]}`, and keeps
  the place in line for 2 minutes: `acquire --ticket <ticket>` resumes it. Re-running `acquire`
  from the same session also takes its place back. A kept place still counts in line: a free slot
  can wait up to those 2 minutes for the session ahead to come back. That is the price of first
  come, first served.
- A waiter stopped by a signal (SIGTERM, SIGINT or SIGHUP) gives up the same way: exit 3, the
  ticket, and its place kept for 2 minutes. So when a harness stops a command at its own timeout,
  the agent's next `acquire` is still in its place. A process killed with SIGKILL cannot do this:
  its place is dropped once it goes 15 seconds unrefreshed.
- `--note <text>`: what the lease is for, shown to everyone waiting and in `status`; at most 200
  characters (more is a usage error, exit 2).

A session that already holds a lease on the resource gets that lease back (`"reused": true`), and
`up` runs again; if that `up` fails, the lease is kept and `acquire` exits 5 saying so. Two
`acquire`s from one session waiting at once share one place in line, and both return the one
lease (the second with `"reused": true`). Durations are written `90s`, `10m`, `1h30m`, `250ms`.

### `release`

```
pollock <resource> release [--force] [--lease <lease>] [--session <id>]
```

Ends this session's lease on the resource and lets the next in line in.

- `--force`: end a lease another session holds, when its holder is gone. With one lease on the
  resource it needs nothing else (no session either: a person can run it from a terminal); with
  several, name the lease with `--lease <id>`, else it fails listing them. The output says whose
  lease it ended. If the former holder later runs `release`, it is told its lease was
  force-released, and by whom.

### `status`

```
pollock [<resource>] status [--json]
```

Every resource in use or configured (or just the one named): who holds it, **for how long**, and
for what, then the line, then the `down` it is still owed. A table by default; JSON with `--json`.
The `--json` shape is stable, for scripts and harness hooks to read: `{"resources": [{"resource",
"capacity", "holders": [{"lease", "resource", "session", "note", "acquiredAt", "heldFor"}],
"queue": [{"position", "session", "note", "waitingSince", "keptUntil"}], "hookRunning",
"pendingDown", "lastDown"}]}`. Fields may be added; none is renamed or removed without a major
version.

```
macos-vm  capacity 1, 1 held, 1 waiting
  held for 2h14m by builder-3 ("release build")  since 09:02:11, lease 2bq8r0c1zt
  1. docs-9  waiting 41s  "screenshots for the docs"
  down once it has been idle for 20m

linux-box  capacity unlimited, 0 held, 0 waiting
  down in 12m
  last down 3h2m ago, exit 0
```

`pendingDown` is the shutdown a resource with `down` is still owed: `null` when there is none (it
has not been used since its last `down`, or `down` is running now: see `hookRunning`), otherwise
`{"at", "watcherRestarted"}`. `at` is when `down` is due, `null` while the resource is held or
someone is in line. So a resource that is idle and still up always shows when it will go down.
`watcherRestarted` is true when this `status` found no watcher alive to run it and started one
([below](#the-watcher)); the table says so too.

## If a holder never releases

A lease never ends on its own, so an agent that dies (or forgets) leaves the resource held. That
fails closed: other agents wait, visibly, rather than barge in on work that may still be going on.

- A waiter whose `--wait` runs out exits 3 with the holders, their notes and how long they have
  held, and the command that frees the resource. An agent should tell its user rather than force
  it itself: the user knows whether the holder is still working.
- `pollock status` shows the same.
- `pollock <resource> release --force` frees it (`--lease <id>` when several hold it).

## Output and exit codes

Every command prints one JSON object on stdout when it succeeds (`status` prints a table unless
`--json`). A failure prints one JSON line on stderr and nothing on stdout; a usage error (an
unknown flag, a bad value, a missing argument) too, with exit 2. `--help` prints the help.

```json
{"error":{"code":"NotHeldError","message":"Session docs-9 holds no lease on my-vm. Nothing to release.","exitCode":4}}
```

| Exit | Meaning |
| --- | --- |
| 0 | Done. |
| 1 | Unexpected failure (a bug, or the database stayed locked for 10 seconds). |
| 2 | Usage error: a bad flag, no session id, `--force` without `--lease` among several leases, an invalid config file. |
| 3 | `acquire --wait` ran out. stdout has the ticket and the holders; the place is kept for 2 minutes. |
| 4 | Nothing to release (no lease held, another session's without `--force`, already ended), `--ticket` named a place that is gone, or the lease ended (was force-released) while `up` ran. |
| 5 | The resource's `up` hook failed or timed out; the error carries its output. A fresh grant was undone; a lease already reported to the session is kept. |

## Configuration

Resources need no configuration. To change one, add it to
`$XDG_CONFIG_HOME/pollock/resources.json` (default `~/.config/pollock/resources.json`):

```json
{
  "macos-vm": {
    "up": "my-vm-tool start --wait macos-vm",
    "upTimeout": "5m",
    "down": "my-vm-tool stop macos-vm",
    "idleAfter": "20m"
  },
  "linux-box": {
    "capacity": "unlimited",
    "up": "podman start linux-box",
    "down": "podman stop linux-box",
    "idleAfter": "30m"
  },
  "gpu": { "capacity": 2 }
}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `capacity` | `1` | How many leases may be held at once: a positive integer, or `"unlimited"`. 1 is an exclusive lock. |
| `up` | none | A shell command run on every grant, before `acquire` returns. |
| `upTimeout` | `5m` | `up` is killed (with everything it started) past this. |
| `down` | none | A shell command run once the resource has had no holders and nobody in line for `idleAfter`. |
| `idleAfter` | `10m` | How long the resource must be idle before `down`. |
| `downTimeout` | `5m` | `down` is killed past this. |

A file that is not valid JSON, or has an unknown field or a bad value, fails every command with
exit 2: a typo never silently turns a resource back into the default.

### The `up` and `down` hooks

Both run through `/bin/sh -c`, with `POLLOCK_RESOURCE` set to the resource's name.

- **`up` must be idempotent and must check the resource itself.** It runs on every grant, even
  when the resource is already running, because pollock cannot trust a stored "it is up":
  the host may have rebooted, or someone may have stopped it by hand. "Start it unless it is
  running, then wait until it answers" is the shape.
- If `up` exits non-zero or times out, `acquire` exits 5 with its output. A fresh grant is undone;
  a lease already reported to the session (by this `acquire` or another of its own) is kept, and
  one that ended meanwhile (force-released) is reported as ended, exit 4. A hook that ignores SIGTERM is killed 2 seconds after
  it, on a timeout and when its `acquire` is stopped.
- `up` and `down` of one resource never run at the same time (nor do two `up`s): an `acquire`
  that arrives while `down` runs waits for it to finish, then runs `up`. A resource with `down`
  but no `up` likewise grants no lease until a running `down` has finished. One exception: after
  the machine sleeps longer than 15 seconds mid-hook, another command may take the hook's slot and
  run its own hook beside it, which is why hooks must be idempotent.
- Each hook's output goes to `<state dir>/hooks/<resource>.<up|down>.log`, replaced on each run.
  A process a successful hook leaves running keeps running.

### The watcher

`down` needs something alive after everyone has left. So a resource with `down` gets one small
watcher process, detached from the agent and from the command that started it. It runs `down` once
the resource has been idle for `idleAfter`, counted from the release that left it idle, records
how it went (shown in `status`), and exits. Resources without `down` start no watcher.

`down` does not depend on any process a holder started. A grant starts the watcher, and so does
every `release`, forced or not, from any session. So when
a holder's processes are all gone (a harness that stops an agent may kill everything its commands
started, the watcher included) and someone frees its lease with `release --force`, that release's
watcher runs `down`.

What pollock cannot do without a daemon is notice a watcher killed later, while the resource sits
idle with nobody running a command. Then `down` waits for the next pollock command that looks at
the resource: `status` (for that resource or for all), `acquire`, or `release` (one with nothing
to release too). It starts a new watcher, which runs an overdue `down` at once. A reboot kills the
watcher too; the next `up` brings the resource back regardless.

## Worked example: an owner holds the VM while a helper works in it

An agent (the owner) needs a VM for a UI test and hands the clicking to a helper agent. The lease
is the owner's; the helper runs no pollock command at all.

```sh
# The owner, session "owner-7", gets in line as a background command and works on other things.
# When the command exits with the lease, it switches to the VM.
$ pollock macos-vm acquire --wait none --note "checkout flow UI test"
pollock: macos-vm: waiting, number 1 in line. Held by builder-3 ("release build") for 4m (lease 2bq8r0c1zt).
pollock: macos-vm: running up
{"status": "granted", "lease": "7f3k2q9xa1", "session": "owner-7", …}

# The owner starts the helper on the VM and ends its turn. The lease stays held across the
# owner's turns, however long the helper takes.

# Anyone can look; a third agent sees who has the VM and for what.
$ pollock status
macos-vm  capacity 1, 1 held, 1 waiting
  held for 12m by owner-7 ("checkout flow UI test")  since 14:20:05, lease 7f3k2q9xa1
  1. docs-9  waiting 41s  "screenshots for the docs"

# The helper reports back; the owner releases, and docs-9 gets the VM.
$ pollock macos-vm release
{"released": true, "forced": false, "lease": "7f3k2q9xa1", "session": "owner-7", …}
```

## State and environment

| Variable | Default | Meaning |
| --- | --- | --- |
| `POLLOCK_STATE_DIR` | `$XDG_STATE_HOME/pollock` (`~/.local/state/pollock`) | Where the database and hook logs live. |
| `POLLOCK_CONFIG_DIR` | `$XDG_CONFIG_HOME/pollock` (`~/.config/pollock`) | Where `resources.json` is read from. |
| `POLLOCK_SESSION` | | The session id, when the harness exports none of the variables above. |
| `POLLOCK_POLL_INTERVAL` | `1s` | How often a waiting command, a running hook and the watcher look at the database. |
| `POLLOCK_STALE_AFTER` | `15s` | A place in line (or a hook, or a watcher) not refreshed for this long is dropped, so a killed waiter never blocks the line. |
| `POLLOCK_TICKET_GRACE` | `2m` | How long a timed-out `acquire` keeps its place. |

The last three exist mostly for tests; the defaults suit agents.

Every agent sharing a resource must use the same state directory: pollock coordinates one
machine (one user's agents), through one file. An agent whose commands run in a sandbox that only
lets it write its workspace needs `POLLOCK_STATE_DIR` pointed at a directory the sandbox can
write, the same one for every agent sharing the resource.

## Licence

MIT
