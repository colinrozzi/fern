# Coverage policy

CI enforces a **line-coverage ratchet** (`cargo llvm-cov --fail-under-lines N`
in `.github/workflows/ci.yml`). The floor only moves up: any PR that adds
tests should raise it to the newly measured value (rounded down).

Current floor: **97** (measured over the tree *excluding* `src/mux.rs`).

## Carve-out: `src/mux.rs`

The terminal multiplexer (`fern mux`) is excluded from the ratchet via
`--ignore-filename-regex 'src/mux\.rs'`. It's a crossterm raw-mode render loop
driven by keystrokes and a live broadcast feed — exercisable only under a PTY
harness, which it doesn't have yet. Rather than drag the whole-tree floor down
to accommodate untested UI, the bar stays on the covered core and the mux is
quarantined. This is a **temporary** waiver, not residue: when the PTY harness
lands (drive `fern mux` under a pty, assert on `capture-pane`-style frames),
drop the ignore and fold mux back under the ratchet.

The floor stepped down 98 → 97 because of code that only the mux exercises but
that lives *outside* `mux.rs`: the `Cmd::Mux` dispatch arm in `main.rs` (launches
the TUI) and the mux-only transport helpers in `client.rs`
(`open_subscription`, `open_attach`) — none reachable without running the TUI.
They pull the *non-mux* total to ~97.8%. Everything else (and the documented
residue below) is still covered. When the mux PTY harness lands it exercises
these too; re-raise toward 100 then.

## How we got here

The drive from 49% → 98% (PRs #10–#14) followed three rules:

1. **Dead code is deleted, not excluded.** Unreachable arms were removed or
   made structurally impossible (e.g. the lexer emits fd+op as one token so
   the parser can't see a malformed pair; `open_for_write` takes a bool so
   the input-redirect case can't be expressed).
2. **Defensive paths get fault-injection tests**, not waivers: daemons are
   SIGKILLed mid-attach, broadcast buffers are shrunk and flooded past,
   clients run against impostor daemons that reply garbage or hang up.
3. **Coverage must be deterministic.** A line that's only reachable by
   winning a process-teardown race would make the ratchet itself flaky; we
   document it here instead of writing a timing-dependent test.

## The documented residue (~75 lines, ~2%)

| Group | Where | Why it stays uncovered |
|---|---|---|
| Panic catcher | `daemon.rs` (eval-task-panicked arm) | Only fires if eval panics — a bug by definition. Testing it means injecting a panic through a backdoor. |
| `accept()` error break | `daemon.rs` | Kernel refusing `accept` (fd exhaustion). Not constructible in a test without harming the host. |
| Broadcast `Closed` arms | `daemon.rs` | The event sender lives in `DaemonState` and is never dropped while connections are served; only daemon teardown closes it, and the process exits instead. |
| Thread-teardown breaks | `client.rs` stdin pump, `eval.rs` PTY reader/writer threads | Reachable only by racing channel-drop against a blocking read/write during process exit. Timing-dependent. |
| Mid-stream I/O failure tails | `client.rs`, `daemon.rs` send/forward error arms | Require the peer to vanish between two adjacent writes; covered behaviorally by the SIGKILL tests, but which arm catches it is scheduling-dependent. |
| Stdin-EOF during raw attach | `client.rs` | A PTY slave only sees EOF when every master handle closes — but the test must hold a master to read the output it asserts on. |
| Defensive protocol skips | `client.rs` `_ => {}` arms | Frames a real daemon never sends in that state; the impostor-daemon tests cover the adjacent error arms. |

If a future change makes one of these reachable deterministically, test it
and strike the row.
