# fern

A small experiment in **shared shell sessions** — both you and a collaborator (human or AI) attach to the same long-running daemon, submit commands, and watch the same output land in the same tree.

It's modeled after a Jupyter notebook, not a `tmux` window: every command is a discrete *cell* that produces structured output (`stdout`, `stderr`, exit code, end-state). Cells form a **tree** — you can branch off any prior cell and run a different command from that point, with independent state going forward.

## Status

A working prototype. Not packaged, not stable, not optimized — built in one sitting as a design exercise. Probably full of rough edges. Have fun.

## Install

Requires Rust (provided via the bundled Nix flake) and Linux (uses unix sockets and PTYs).

```bash
nix develop --command cargo build --release
# binary at ./target/release/fern
```

Without Nix: install Rust 1.85+ and run `cargo build --release`.

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

Watch everything that happens across all clients:

```bash
fern watch
```

See the tree:

```bash
fern tree
# #0 (root) [system] exit 0
#   #1 echo "hello $USER" [colin] exit 0
#     #2 cd /tmp [colin] exit 0
#       #3 pwd [colin] exit 0
#         #4 echo a b c | wc -w [colin] exit 0
```

Branch off an earlier cell:

```bash
fern run --parent 1 'echo branching from #1'
fern tree
# ... shows two children under #1
```

Run a long-running cell (e.g. a dev server) in the background:

```bash
fern run --detach 'bash -c "for i in 1..10; do echo tick-$i; sleep 0.5; done"'
# detached: cell #6 running
fern kill 6                            # terminate it
```

Interactive cells (PTY-backed — `sudo`, password prompts, etc.):

```bash
fern run --interactive 'bash -c "read -p \"name: \" name; echo hi-$name"'
# interactive: cell #7 running; attach with `fern attach 7`
fern attach 7
# raw-mode terminal; type response, Ctrl+] to detach without killing
```

Or use the interactive REPL:

```bash
fern repl
shsh repl — type :help for commands, :quit to exit
(at #0) > echo hello
hello
[#1 exit 0 2ms]
(at #1) > :tree
...
(at #1) > :cd 0          # move cursor back to root
(at #0) > ...            # next command branches off root
```

## The model

The whole thing rests on one design call: a cell is a discrete process spawn, not a screen-grid coordinate. That choice eliminates the rough edges of `tmux`-style sharing (prompt scraping, byte-timing, no exit codes) and lights up the things you'd actually want from a shared session:

- **Structured results**: every cell produces `{stdout, stderr, exit_code, duration, end_state}`.
- **Inheritable state**: `cd` and `export` propagate to children via a `State { cwd, env }` struct, not by piping bytes into a long-lived bash.
- **First-class branching**: state lives in the tree, so forking is just `--parent N`. Two siblings get fully independent state.
- **Multi-client**: the daemon owns one tree; any number of clients submit and subscribe to the same source of truth.
- **Streaming**: output flows as `OutputChunk` events as bytes arrive; no buffering until completion.
- **Three execution modes**: inline (block until exit), detached (return immediately, run in background), interactive (PTY-backed + bidirectional attach).

## Layout

Flat modules in `src/`:

| File | What it owns |
|---|---|
| `tree.rs` | `State`, `Process`, `Cell`, `Tree` |
| `parse.rs` | Lexer + AST + recursive-descent parser (quoting, vars, pipes, redirs, `&&`/`\|\|`/`;`) |
| `eval.rs` | Async streaming evaluator (`tokio::process`, builtins, pipelines, redirects) |
| `wire.rs` | Protocol types (`Request`, `Response`, `CellEvent`, `CellSnapshot`) |
| `daemon.rs` | Unix-socket server, the cell tree, broadcast, three execution paths (inline / detached / interactive PTY via `portable-pty`) |
| `client.rs` | Daemon RPCs + the `run`/`watch`/`tree`/`kill`/`attach` CLI verbs |
| `repl.rs` | Interactive REPL on top of the client API (no in-process tree) |
| `main.rs` | `clap` CLI |

## Limitations

This is a prototype. Known gaps:

- Interactive cells run as `bash -c <source>` rather than through the in-house parser (so no cell-aware builtins inside them).
- Interactive cell output isn't captured into the tree's stored `stdout`/`stderr` (it streams to broadcast but isn't replayable from `fern tree`).
- No persistence — daemon restart loses the tree.
- The parser doesn't yet handle `$(...)` command substitution, glob, background `&`, control flow (`if`/`while`/`for`), heredocs, arrays, or arithmetic.
- Multiple clients attaching to the same interactive cell will interleave their inputs.

## License

MIT. See [LICENSE](LICENSE).
