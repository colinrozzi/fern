# fern

A small experiment in **shared shell sessions** — both you and a collaborator (human or AI) attach to the same long-running daemon, submit commands, and watch the same output land in the same tree.

It's modeled after a Jupyter notebook, not a `tmux` window: every command is a discrete *cell* that produces structured output (`stdout`, `stderr`, exit code, end-state). Cells form a **tree** — you can branch off any prior cell and run a different command from that point, with independent state going forward. Named **branches** track the tips of those lines of work, and a cell's **I/O mode** (pipes vs. a real terminal) is just inherited environment that flows down the tree.

## Status

A working prototype. Not packaged, not stable, not optimized — built as a design exercise. Probably full of rough edges. Have fun.

## Install

Requires Rust (provided via the bundled Nix flake) and a Unix host (Linux or macOS — uses unix sockets and PTYs).

```bash
nix develop --command cargo build --release
# binary at ./target/release/fern
```

Without Nix: install Rust 1.85+ and run `cargo build --release` (or `cargo install --path .`).

## Quick start

In one terminal, start the daemon:

```bash
fern daemon
# daemon listening on $XDG_RUNTIME_DIR/fern.sock
```

In a second terminal, submit some commands:

```bash
fern run 'echo "hello $USER"'
fern run 'cd /tmp'
fern run pwd                          # → /tmp (state inherited)
fern run 'echo a b c | wc -w'         # pipelines work
```

Watch everything that happens across all clients, or dump the tree:

```bash
fern watch
fern tree
# #0 ecda6f2 (root) [system] exit 0
#   #1 bc1479a echo "hello $USER" [colin] exit 0
#     #2 4ac0f74 cd /tmp [colin] exit 0
#       #3 76445d3 pwd [colin] exit 0
#         #4 a76878d echo a b c | wc -w [colin] exit 0
```

Each cell shows a short prefix of its **content hash** (SHA-256 over its parent hash, recipe, output, exit code, and end-state) — the tree is a Merkle DAG rooted at a cell that bakes in the machine's identity.

## Branches

A branch is a **named, mutable pointer to a cell** — like a git ref over the cell tree. The daemon owns them, so every client sees the same branches. The tree starts with `main` at the root, and your "current branch" (where the next `fern run` lands) is shared across clients on the host.

```bash
fern branch                           # list branches (* = current)
# * main        #4   a76878d
fern branch new experiment --at 1     # new branch off cell #1
fern switch experiment                # next runs land here
fern run 'echo on a branch'           # fast-forwards `experiment`
fern branch rename experiment exp2
fern branch rm exp2
```

A submit **fast-forwards** the current branch when its parent is the branch tip; pointing at a *historical* cell instead **forks** a new `fork-<uuid>` branch:

```bash
fern run --parent 1 'echo branching from the past'
# [fern] forked off 'main' → new branch 'fork-1a2b3c4d'
```

Because a branch can point at a still-running cell (one that has no content hash yet), in-flight work is always referenceable.

## I/O mode (pipes vs. a terminal)

There's no global "interactive" switch. A cell's I/O mode is just **inherited environment**: set `FERN_IO=tty` and external commands from there on spawn under a PTY (so `isatty`-aware programs see a real terminal); unset it and they run under pipes. Like `cd`/`export`, the setting propagates to descendants, is branch-local, and is content-addressed.

```bash
fern run 'tty'                        # not a tty   (default: pipes)
fern run 'export FERN_IO=tty'         # flip this branch to terminal mode
fern run 'tty'                        # /dev/ttys003   (captured)
fern run 'unset FERN_IO'              # back to pipes
```

PTY output is captured into the cell like any other, so terminal cells are replayable and hashed over their real output.

To interact with a terminal program, run it **detached** on a `FERN_IO=tty` branch, then drive it:

```bash
fern run --detach 'vim notes.txt'     # attachable terminal cell at the tip
fern attach main                      # raw bidirectional terminal; Ctrl+] detaches
fern send main 'some input'           # or inject one line non-interactively (scriptable)
fern kill 7                           # terminate a running cell
```

`attach` and `send` take a **branch name or a cell id** — a branch resolves to its current tip.

## The cockpit

`fern attach [branch]` is one unified cockpit whose behavior follows the branch tip:

- a **finished tip** gives a cooked prompt — each line you type extends the branch (with `:branches` / `:switch` / `:tree` / `:at` / `:quit`);
- a **live terminal tip** drops you straight into the raw PTY.

On a pipe branch your commands run inline and stream; on a `FERN_IO=tty` branch each command is launched detached and you drive it raw, returning to the prompt when it exits. `Ctrl+]` leaves the cockpit (the cell keeps running); `:quit` does too.

```bash
fern attach            # cockpit on the current branch
fern attach experiment # …or a named one
(on main) > echo hello
hello
[#1 exit 0 2ms]
(on main) > :switch experiment
```

`fern repl` is an alias for `fern attach` on the current branch.

## The model

The whole thing rests on one design call: a cell is a discrete process spawn, not a screen-grid coordinate. That choice eliminates the rough edges of `tmux`-style sharing (prompt scraping, byte-timing, no exit codes) and lights up the things you'd actually want from a shared session:

- **Structured results**: every cell produces `{stdout, stderr, exit_code, duration, end_state}`.
- **Inheritable state**: `cd`, `export`, and the I/O mode (`FERN_IO`) propagate to children via a `State { cwd, env }` struct, not by piping bytes into a long-lived bash.
- **First-class branching**: state lives in the tree, so forking is just `--parent N`. Two siblings get fully independent state.
- **Named branches**: server-owned refs track the tips of your lines of work; fast-forward or fork by where you point.
- **Content-addressed**: each finished cell hashes its lineage + recipe + output + end-state, so identical work dedups and every cell encodes "this happened, on this machine, in this state".
- **Multi-client**: the daemon owns one tree; any number of clients submit and subscribe to the same source of truth.
- **Streaming**: output flows as `OutputChunk` events as bytes arrive; no buffering until completion.
- **One cell type, two I/O modes**: pipes by default (clean, split, captured); a PTY when the inherited environment asks for one (terminal-native, attachable).

## Layout

Flat modules in `src/`:

| File | What it owns |
|---|---|
| `tree.rs` | `State`, `Process`, `Cell`, `Tree`, branches, content hashing |
| `parse.rs` | Lexer + AST + recursive-descent parser (quoting, vars, pipes, redirs, `&&`/`\|\|`/`;`) |
| `eval.rs` | Async streaming evaluator (`tokio::process`, builtins, pipelines, redirects, `FERN_IO=tty` PTY spawn) |
| `wire.rs` | Protocol types (`Request`, `Response`, `CellEvent`, `CellSnapshot`, `BranchSnapshot`) |
| `daemon.rs` | Unix-socket server, the cell tree + branches, broadcast, inline/detached execution, PTY attach |
| `client.rs` | Daemon RPCs + CLI verbs (`run`/`watch`/`tree`/`branch`/`switch`/`attach`/`send`/`kill`) + the cockpit |
| `main.rs` | `clap` CLI |

## Limitations

This is a prototype. Known gaps:

- No persistence — daemon restart loses the tree and its branches.
- The parser doesn't yet handle `$(...)` command substitution, glob, background `&`, control flow (`if`/`while`/`for`), heredocs, arrays, or arithmetic.
- `FERN_IO=tty` applies to single external commands; pipelines and redirected commands stay pipe-mode. PTY output is a single (merged) terminal stream — no stdout/stderr split for terminal cells.
- Only **detached** tty cells are attachable; an inline `fern run` on a tty branch captures output but has no stdin.
- Multiple clients attaching to the same cell will interleave their inputs.

## License

MIT. See [LICENSE](LICENSE).
