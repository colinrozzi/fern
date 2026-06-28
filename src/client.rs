//! Client: low-level daemon RPCs + the CLI verbs that print on top of them.
//!
//! Low-level API (used by both CLI and REPL):
//!   * `submit_streaming` — submits a command, invokes a callback for each
//!     OutputChunk as it arrives, returns the final CellSnapshot
//!   * `submit` — wrapper that discards chunks (for callers that only want the snap)
//!   * `fetch_tree` — one-shot tree dump
//!   * `read_cursor` / `write_cursor` — shared XDG cursor file

use anyhow::{Context, Result, anyhow};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

use crate::tree::{CellId, DEFAULT_BRANCH};
use crate::wire::{
    self, BranchSnapshot, CellEvent, CellSnapshot, Request, Response, Stream, TreeSnapshot,
    socket_path,
};

// ---------- Current branch (shared across all clients on this host) -----
//
// The "cursor" is now the name of the branch the next `fern run` extends.
// It lives in a shared XDG file so all clients on this host agree on it, and
// only changes via `fern switch` — running a command advances the branch's
// tip on the daemon, not this file.

fn current_branch_path() -> PathBuf {
    let base = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(base).join("fern-branch")
}

pub fn read_current_branch() -> String {
    std::fs::read_to_string(current_branch_path())
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_BRANCH.to_string())
}

pub fn write_current_branch(name: &str) {
    let _ = std::fs::write(current_branch_path(), name);
}

/// Resolve the branch an implicit (no `--branch`) command should land on.
/// The daemon's tree is ephemeral but the current-branch file isn't, so after
/// a daemon restart it can name a branch that no longer exists — in that case
/// warn and fall back to the default branch instead of failing every command.
/// An *explicitly* requested branch is never redirected; only the cursor heals.
async fn resolve_current_branch() -> Result<String> {
    let name = read_current_branch();
    // The default branch always exists (seeded at root, undeletable) — skip
    // the round-trip in the common case.
    if name == DEFAULT_BRANCH {
        return Ok(name);
    }
    let branches = fetch_branches().await?;
    if branches.iter().any(|b| b.name == name) {
        return Ok(name);
    }
    eprintln!(
        "[fern] current branch '{name}' no longer exists (daemon restarted?) — \
         falling back to '{DEFAULT_BRANCH}'"
    );
    write_current_branch(DEFAULT_BRANCH);
    Ok(DEFAULT_BRANCH.to_string())
}

// ---------- Low-level RPCs ---------------------------------------------

async fn connect() -> Result<UnixStream> {
    let path = socket_path();
    UnixStream::connect(&path)
        .await
        .with_context(|| format!("connect {} (is the daemon running?)", path.display()))
}

async fn send_req(stream: &mut UnixStream, req: &Request) -> Result<()> {
    let mut line = serde_json::to_string(req)?;
    line.push('\n');
    stream.write_all(line.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// Open a persistent broadcast subscription. The returned stream yields
/// newline-delimited `Response::Event` lines for every cell on every branch —
/// the feed a multi-pane client follows so cells landing on a pane's branch
/// (from any client) show up live.
pub(crate) async fn open_subscription() -> Result<UnixStream> {
    let mut stream = connect().await?;
    send_req(&mut stream, &Request::Subscribe).await?;
    Ok(stream)
}

/// Submit a command; invoke `on_chunk(stream, data)` for each OutputChunk as
/// it streams in; return the final CellSnapshot when Completed.
pub async fn submit_streaming<F>(
    branch: Option<String>,
    parent: Option<CellId>,
    who: Option<String>,
    source: String,
    mut on_chunk: F,
) -> Result<CellSnapshot>
where
    F: FnMut(Stream, &str),
{
    let who = who.unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "?".into()));
    let branch = match branch {
        Some(b) => b,
        None => resolve_current_branch().await?,
    };

    let mut stream = connect().await?;
    send_req(
        &mut stream,
        &Request::Submit {
            branch: branch.clone(),
            parent,
            source: source.clone(),
            who: who.clone(),
            detach: false,
        },
    )
    .await?;

    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();
    let mut stdout = String::new();
    let mut stderr = String::new();
    let mut cell_parent: Option<CellId> = None;

    while let Some(line) = lines.next_line().await? {
        let resp: Response = serde_json::from_str(&line)?;
        match resp {
            Response::Event(CellEvent::Started {
                parent: p,
                branch: landed,
                ..
            }) => {
                cell_parent = p;
                report_fork(&branch, &landed);
            }
            Response::Event(CellEvent::OutputChunk {
                stream: s, data, ..
            }) => {
                on_chunk(s, &data);
                match s {
                    Stream::Stdout => stdout.push_str(&data),
                    Stream::Stderr => stderr.push_str(&data),
                }
            }
            Response::Event(CellEvent::Completed {
                id,
                exit_code,
                duration_ms,
                hash,
            }) => {
                return Ok(CellSnapshot {
                    id,
                    parent: cell_parent,
                    submitter: who,
                    source,
                    exit_code: Some(exit_code),
                    duration_ms,
                    stdout,
                    stderr,
                    hash,
                });
            }
            Response::Error { message } => return Err(anyhow!(message)),
            _ => {}
        }
    }
    Err(anyhow!("connection closed before Completed event"))
}

/// Tell the user when a submission forked instead of fast-forwarding.
fn report_fork(requested: &str, landed: &str) {
    if requested != landed {
        eprintln!("[fern] forked off '{requested}' → new branch '{landed}'");
    }
}

/// Submit a detached (long-running) cell. Returns the cell id as soon as the
/// daemon sends Started; the cell runs in the background. Use `watch` to see
/// output stream, `kill` to terminate it.
pub async fn submit_detached(
    branch: Option<String>,
    parent: Option<CellId>,
    who: Option<String>,
    source: String,
) -> Result<CellId> {
    let who = who.unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "?".into()));
    let branch = match branch {
        Some(b) => b,
        None => resolve_current_branch().await?,
    };

    let mut stream = connect().await?;
    send_req(
        &mut stream,
        &Request::Submit {
            branch: branch.clone(),
            parent,
            source,
            who,
            detach: true,
        },
    )
    .await?;

    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();
    while let Some(line) = lines.next_line().await? {
        let resp: Response = serde_json::from_str(&line)?;
        match resp {
            Response::Event(CellEvent::Started {
                id, branch: landed, ..
            }) => {
                report_fork(&branch, &landed);
                return Ok(id);
            }
            Response::Error { message } => return Err(anyhow!(message)),
            _ => {}
        }
    }
    Err(anyhow!("connection closed before Started event"))
}

/// Resolve a target string to a cell id: a branch name → its current tip,
/// otherwise a numeric cell id. Branch names win over numeric lookups.
async fn resolve_target(target: &str) -> Result<CellId> {
    let branches = fetch_branches().await?;
    if let Some(b) = branches.iter().find(|b| b.name == target) {
        return Ok(b.tip);
    }
    target
        .parse::<CellId>()
        .map_err(|_| anyhow!("no branch '{target}' and not a cell id"))
}

enum AttachOutcome {
    /// The user pressed Ctrl+] — the cell keeps running; leave the cockpit.
    Detached,
    /// The cell finished (or we streamed it to completion).
    Completed,
}

/// The process's single stdin reader. Stdin reads block and cannot be
/// cancelled — a reader task abandoned mid-read swallows whatever the user
/// types next. So the cockpit owns exactly ONE pump for its whole lifetime
/// and consumes it either as cooked lines (at the prompt) or raw chunks
/// (while driving a PTY cell); leftover bytes carry over between modes.
struct StdinFeed {
    rx: mpsc::Receiver<Vec<u8>>,
    buf: Vec<u8>,
}

impl StdinFeed {
    fn spawn() -> Self {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(64);
        tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut stdin = std::io::stdin();
            let mut buf = [0u8; 1024];
            loop {
                match stdin.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tx.blocking_send(buf[..n].to_vec()).is_err() {
                            break;
                        }
                    }
                }
            }
        });
        Self {
            rx,
            buf: Vec::new(),
        }
    }

    /// Next raw chunk (for raw-mode follow). Buffered bytes drain first.
    /// None = stdin closed. Cancel-safe.
    async fn chunk(&mut self) -> Option<Vec<u8>> {
        if !self.buf.is_empty() {
            return Some(std::mem::take(&mut self.buf));
        }
        self.rx.recv().await
    }

    /// Next cooked line, without the trailing newline. None = stdin closed
    /// (a final unterminated line is returned first). Cancel-safe.
    async fn line(&mut self) -> Option<String> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let rest = self.buf.split_off(pos + 1);
                let mut line = std::mem::replace(&mut self.buf, rest);
                line.truncate(pos);
                return Some(
                    String::from_utf8_lossy(&line)
                        .trim_end_matches('\r')
                        .to_string(),
                );
            }
            match self.rx.recv().await {
                Some(bytes) => self.buf.extend_from_slice(&bytes),
                None if self.buf.is_empty() => return None,
                None => {
                    let line = std::mem::take(&mut self.buf);
                    return Some(String::from_utf8_lossy(&line).into_owned());
                }
            }
        }
    }
}

/// Connect to a running cell. If it's a PTY cell, run a raw bidirectional
/// terminal (Ctrl+] detaches); otherwise stream its output until it completes.
/// Reads input from the cockpit's shared stdin feed — no private reader, so
/// no bytes are ever stranded in an abandoned read when the cell completes.
async fn follow(id: CellId, stdin: &mut StdinFeed) -> Result<AttachOutcome> {
    use crossterm::terminal::{disable_raw_mode, enable_raw_mode};

    let mut stream = connect().await?;
    send_req(&mut stream, &Request::Attach { id }).await?;
    let (rd, mut wr) = stream.into_split();
    let mut lines = BufReader::new(rd).lines();

    let first = lines
        .next_line()
        .await?
        .ok_or_else(|| anyhow!("daemon closed connection"))?;
    match serde_json::from_str::<Response>(&first)? {
        Response::Ok => {}
        // Not a PTY cell (a pipe cell / builtin / already gone): just show output.
        Response::Error { .. } => {
            drop(wr);
            return stream_until_done(id).await;
        }
        other => return Err(anyhow!("expected Ok, got {other:?}")),
    }

    enable_raw_mode().context("enable raw mode")?;
    let _guard = RawModeGuard;
    eprintln!("[fern] driving cell #{id}. Ctrl+] to detach (leaves the cockpit).\r");

    let mut stdin_open = true;
    let outcome = loop {
        use std::io::Write;
        // Daemon events render in both cases; stdin forwards only while open
        // (EOF — e.g. piped input exhausted — just stops the input side).
        let event = tokio::select! {
            ev = lines.next_line() => Some(ev?),
            bytes = stdin.chunk(), if stdin_open => {
                match bytes {
                    None => {
                        stdin_open = false;
                    }
                    Some(bytes) if bytes.contains(&0x1d) => {
                        eprintln!("\r\n[fern] detached (cell still running)\r");
                        break AttachOutcome::Detached;
                    }
                    Some(bytes) => {
                        let req = Request::Input {
                            id,
                            data: String::from_utf8_lossy(&bytes).into_owned(),
                        };
                        let mut line = serde_json::to_string(&req)?;
                        line.push('\n');
                        if wr.write_all(line.as_bytes()).await.is_err() {
                            break AttachOutcome::Completed;
                        }
                        let _ = wr.flush().await;
                    }
                }
                None
            }
        };
        let Some(event) = event else { continue };
        let Some(line) = event else {
            break AttachOutcome::Completed; // daemon closed
        };
        match serde_json::from_str::<Response>(&line)? {
            Response::Event(CellEvent::OutputChunk { data, .. }) => {
                let mut stdout = std::io::stdout();
                stdout.write_all(data.as_bytes()).ok();
                stdout.flush().ok();
            }
            Response::Event(CellEvent::Completed { exit_code, .. }) => {
                eprintln!("\r\n[fern] cell exited with code {exit_code}\r");
                break AttachOutcome::Completed;
            }
            Response::Error { message } => {
                eprintln!("\r\n[fern] error: {message}\r");
                break AttachOutcome::Completed;
            }
            _ => {}
        }
    };
    disable_raw_mode().ok();
    Ok(outcome)
}

/// Fetch one cell's snapshot by id.
async fn fetch_cell(id: CellId) -> Result<CellSnapshot> {
    let mut stream = connect().await?;
    send_req(&mut stream, &Request::GetCell { id }).await?;
    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();
    if let Some(line) = lines.next_line().await? {
        return match serde_json::from_str::<Response>(&line)? {
            Response::Cell(c) => Ok(c),
            Response::Error { message } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected: {other:?}")),
        };
    }
    Err(anyhow!("connection closed before Cell response"))
}

/// Stream a (non-PTY) running cell's output until it completes. Used when the
/// tip isn't an attachable terminal — e.g. a running pipe cell started elsewhere.
async fn stream_until_done(id: CellId) -> Result<AttachOutcome> {
    use std::io::Write;
    let mut stream = connect().await?;
    send_req(&mut stream, &Request::Subscribe).await?;
    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();

    // Check AFTER subscribing (so nothing can slip through the gap): a fast
    // cell may already be done, and its events will never come again — replay
    // the stored result instead of waiting forever.
    if let Ok(cell) = fetch_cell(id).await
        && let Some(exit_code) = cell.exit_code
    {
        print!("{}", cell.stdout);
        eprint!("{}", cell.stderr);
        std::io::stdout().flush().ok();
        println!("[#{id} exit {exit_code} {}ms]", cell.duration_ms);
        return Ok(AttachOutcome::Completed);
    }

    while let Some(line) = lines.next_line().await? {
        match serde_json::from_str::<Response>(&line)? {
            Response::Event(CellEvent::OutputChunk {
                id: i,
                stream: s,
                data,
            }) if i == id => match s {
                Stream::Stdout => {
                    print!("{data}");
                    std::io::stdout().flush().ok();
                }
                Stream::Stderr => {
                    eprint!("{data}");
                    std::io::stderr().flush().ok();
                }
            },
            Response::Event(CellEvent::Completed {
                id: i,
                exit_code,
                duration_ms,
                ..
            }) if i == id => {
                println!("[#{i} exit {exit_code} {duration_ms}ms]");
                return Ok(AttachOutcome::Completed);
            }
            _ => {}
        }
    }
    Ok(AttachOutcome::Completed)
}

async fn branch_tip_state(name: &str) -> Result<Option<(CellId, bool)>> {
    let branches = fetch_branches().await?;
    Ok(branches
        .iter()
        .find(|b| b.name == name)
        .map(|b| (b.tip, b.running)))
}

async fn branch_is_tty(name: &str) -> Result<bool> {
    let branches = fetch_branches().await?;
    Ok(branches
        .iter()
        .find(|b| b.name == name)
        .map(|b| b.tty)
        .unwrap_or(false))
}

/// Live feed of other clients' activity, rendered into the cockpit while we
/// sit at the prompt. Holds a persistent Subscribe connection so cells that
/// land on our branch from elsewhere show up as they happen, instead of the
/// prompt being blind to everyone else. `mine` suppresses echo of cells this
/// cockpit submitted itself (their output already rendered inline).
struct CockpitFeed {
    lines: tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    /// Keeps the connection's write side open; the daemon never reads it.
    _wr: tokio::net::unix::OwnedWriteHalf,
    mine: std::collections::HashSet<CellId>,
    /// Foreign cells on our branch currently being rendered.
    foreign: std::collections::HashSet<CellId>,
    /// Whether the last rendered chunk ended with a newline (so the status
    /// line never lands mid-line).
    at_line_start: bool,
}

impl CockpitFeed {
    async fn open() -> Result<Self> {
        let mut stream = connect().await?;
        send_req(&mut stream, &Request::Subscribe).await?;
        let (rd, wr) = stream.into_split();
        Ok(Self {
            lines: BufReader::new(rd).lines(),
            _wr: wr,
            mine: std::collections::HashSet::new(),
            foreign: std::collections::HashSet::new(),
            at_line_start: true,
        })
    }

    /// Next broadcast event. Errors when the daemon goes away.
    async fn next(&mut self) -> Result<CellEvent> {
        while let Some(line) = self.lines.next_line().await? {
            if let Ok(Response::Event(ev)) = serde_json::from_str::<Response>(&line) {
                return Ok(ev);
            }
        }
        Err(anyhow!("daemon closed the event stream"))
    }

    /// Render a foreign event if it belongs on `branch`. `at_prompt` tracks
    /// whether the prompt line is currently showing — the first render of a
    /// burst clears it, and the caller restores it once activity settles.
    fn render(&mut self, branch: &str, ev: CellEvent, at_prompt: &mut bool) {
        use std::io::Write;
        let clear_prompt = |at_prompt: &mut bool| {
            if *at_prompt {
                print!("\r\x1b[K");
                *at_prompt = false;
            }
        };
        match ev {
            CellEvent::Started {
                id,
                source,
                who,
                branch: b,
                ..
            } => {
                if self.mine.contains(&id) || b != branch {
                    return;
                }
                self.foreign.insert(id);
                clear_prompt(at_prompt);
                println!("[#{id} {who} on {b}] $ {source}");
                self.at_line_start = true;
            }
            CellEvent::OutputChunk { id, stream, data } => {
                if !self.foreign.contains(&id) {
                    return;
                }
                clear_prompt(at_prompt);
                match stream {
                    Stream::Stdout => print!("{data}"),
                    Stream::Stderr => eprint!("{data}"),
                }
                std::io::stdout().flush().ok();
                if let Some(c) = data.chars().last() {
                    self.at_line_start = c == '\n';
                }
            }
            CellEvent::Completed {
                id,
                exit_code,
                duration_ms,
                ..
            } => {
                if !self.foreign.remove(&id) {
                    return;
                }
                clear_prompt(at_prompt);
                if !self.at_line_start {
                    println!();
                }
                println!("[#{id} exit {exit_code} {duration_ms}ms]");
                self.at_line_start = true;
            }
        }
    }
}

/// The unified cockpit: attach to a branch and work on it. A finished tip gives
/// a cooked prompt (each line extends the branch); a live PTY tip drops you into
/// the raw terminal. On a `FERN_IO=tty` branch, commands you type are launched
/// detached and you drive them raw; on a pipe branch they run inline + stream.
/// While parked at the prompt, cells other clients land on this branch render
/// live above it.
pub async fn cockpit(target: Option<String>) -> Result<()> {
    use std::io::Write;

    // An explicit target must exist; the implicit cursor heals itself if the
    // branch it names is gone (e.g. after a daemon restart).
    let mut branch = match target {
        Some(t) => {
            let branches = fetch_branches().await?;
            if !branches.iter().any(|b| b.name == t) {
                return Err(anyhow!(
                    "no such branch '{t}' (create it with `fern branch new {t}`)"
                ));
            }
            t
        }
        None => resolve_current_branch().await?,
    };
    write_current_branch(&branch);
    println!("fern — attached to '{branch}'. :help for commands, :quit to leave.");

    let mut stdin = StdinFeed::spawn();
    let mut feed = CockpitFeed::open().await?;

    loop {
        // If the tip is running, follow it (raw if it's a terminal) first.
        if let Some((tip, true)) = branch_tip_state(&branch).await? {
            match follow(tip, &mut stdin).await? {
                AttachOutcome::Detached => return Ok(()),
                AttachOutcome::Completed => {}
            }
            continue;
        }

        print!("(on {branch}) > ");
        std::io::stdout().flush().ok();
        // Wait for a line of input, rendering other clients' activity on this
        // branch as it streams in. Both arms are cancel-safe.
        let mut at_prompt = true;
        let line = loop {
            tokio::select! {
                l = stdin.line() => break l,
                ev = feed.next() => {
                    feed.render(&branch, ev?, &mut at_prompt);
                    if !at_prompt && feed.foreign.is_empty() {
                        print!("(on {branch}) > ");
                        std::io::stdout().flush().ok();
                        at_prompt = true;
                    }
                }
            }
        };
        let Some(line) = line else {
            break;
        };
        let t = line.trim().to_string();
        if t.is_empty() {
            continue;
        }
        if let Some(rest) = t.strip_prefix(':') {
            let before = branch.clone();
            match cockpit_meta(&mut branch, rest).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(e) => println!("error: {e}"),
            }
            if branch != before {
                // Don't keep streaming cells from the branch we just left.
                feed.foreign.clear();
            }
            continue;
        }

        let who = std::env::var("USER").ok();
        // A tty branch hands the raw terminal to each program that spawns one;
        // lines that are purely builtins (cd/export/…) spawn nothing, so run
        // them inline and stream. The builtin set is owned by the evaluator.
        if branch_is_tty(&branch).await? && !crate::eval::is_pure_builtin_line(&t) {
            match submit_detached(Some(branch.clone()), None, who, t).await {
                Ok(id) => {
                    feed.mine.insert(id);
                    match follow(id, &mut stdin).await {
                        Ok(AttachOutcome::Detached) => return Ok(()),
                        Ok(AttachOutcome::Completed) => {}
                        Err(e) => println!("error: {e}"),
                    }
                }
                Err(e) => println!("error: {e}"),
            }
        } else {
            match submit_streaming(Some(branch.clone()), None, who, t, |s, data| match s {
                Stream::Stdout => {
                    print!("{data}");
                    std::io::stdout().flush().ok();
                }
                Stream::Stderr => {
                    eprint!("{data}");
                    std::io::stderr().flush().ok();
                }
            })
            .await
            {
                Ok(snap) => {
                    feed.mine.insert(snap.id);
                    let last = snap
                        .stderr
                        .chars()
                        .last()
                        .or_else(|| snap.stdout.chars().last());
                    if matches!(last, Some(c) if c != '\n') {
                        println!();
                    }
                    // A snapshot from submit_streaming always has an exit code
                    // (it's built from the Completed event).
                    let code = snap.exit_code.unwrap_or(-1);
                    println!("[#{} exit {code} {}ms]", snap.id, snap.duration_ms);
                }
                Err(e) => println!("error: {e}"),
            }
        }
    }
    Ok(())
}

/// Handle a `:meta` command in the cockpit. Returns Ok(false) to quit.
async fn cockpit_meta(branch: &mut String, line: &str) -> Result<bool> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    match parts.first().copied().unwrap_or("") {
        "quit" | "q" | "exit" => return Ok(false),
        "help" | "h" | "" => {
            println!(":branches          list branches");
            println!(":switch <name>     switch the current branch");
            println!(":tree              show the cell tree");
            println!(":at                show the current branch");
            println!(":quit / :q         leave the cockpit");
        }
        "at" => println!("on branch '{branch}'"),
        "branches" => {
            let branches = fetch_branches().await?;
            render_branches(&branches, branch);
        }
        "tree" => {
            let snap = fetch_tree().await?;
            render_tree(&snap);
        }
        "switch" => {
            let name = parts
                .get(1)
                .ok_or_else(|| anyhow!(":switch needs a branch name"))?;
            let branches = fetch_branches().await?;
            if !branches.iter().any(|b| b.name == *name) {
                return Err(anyhow!("no such branch '{name}'"));
            }
            *branch = name.to_string();
            write_current_branch(name);
            println!("switched to '{name}'");
        }
        other => return Err(anyhow!("unknown command :{other} (try :help)")),
    }
    Ok(true)
}

struct RawModeGuard;
impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// Send one line of input to a running PTY cell (branch tip or cell id) without
/// taking over the terminal. Scriptable, and the way a non-interactive client
/// (e.g. an agent) feeds a waiting cell. A trailing newline is appended.
pub async fn send(target: String, data: String) -> Result<()> {
    let id = resolve_target(&target).await?;

    let mut stream = connect().await?;
    send_req(&mut stream, &Request::Attach { id }).await?;
    let (rd, mut wr) = stream.into_split();
    let mut lines = BufReader::new(rd).lines();

    let first = lines
        .next_line()
        .await?
        .ok_or_else(|| anyhow!("daemon closed connection"))?;
    match serde_json::from_str::<Response>(&first)? {
        Response::Ok => {}
        Response::Error { message } => return Err(anyhow!(message)),
        other => return Err(anyhow!("expected Ok, got {other:?}")),
    }

    let mut payload = data;
    payload.push('\n');
    let req = Request::Input { id, data: payload };
    let mut line = serde_json::to_string(&req)?;
    line.push('\n');
    wr.write_all(line.as_bytes()).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn kill(id: CellId) -> Result<()> {
    let mut stream = connect().await?;
    send_req(&mut stream, &Request::Kill { id }).await?;
    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();
    if let Some(line) = lines.next_line().await? {
        let resp: Response = serde_json::from_str(&line)?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error { message } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected: {other:?}")),
        }
    } else {
        Err(anyhow!("connection closed before reply"))
    }
}

pub async fn fetch_tree() -> Result<TreeSnapshot> {
    let mut stream = connect().await?;
    send_req(&mut stream, &Request::GetTree).await?;
    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();
    if let Some(line) = lines.next_line().await? {
        let resp: Response = serde_json::from_str(&line)?;
        match resp {
            Response::Tree(snap) => return Ok(snap),
            Response::Error { message } => return Err(anyhow!(message)),
            other => return Err(anyhow!("unexpected: {other:?}")),
        }
    }
    Err(anyhow!("connection closed before Tree response"))
}

pub async fn fetch_branches() -> Result<Vec<BranchSnapshot>> {
    let mut stream = connect().await?;
    send_req(&mut stream, &Request::ListBranches).await?;
    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();
    if let Some(line) = lines.next_line().await? {
        let resp: Response = serde_json::from_str(&line)?;
        match resp {
            Response::Branches { branches } => return Ok(branches),
            Response::Error { message } => return Err(anyhow!(message)),
            other => return Err(anyhow!("unexpected: {other:?}")),
        }
    }
    Err(anyhow!("connection closed before Branches response"))
}

/// Send a request that expects a bare `Ok`/`Error` reply.
pub(crate) async fn send_expect_ok(req: &Request) -> Result<()> {
    let mut stream = connect().await?;
    send_req(&mut stream, req).await?;
    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();
    if let Some(line) = lines.next_line().await? {
        let resp: Response = serde_json::from_str(&line)?;
        return match resp {
            Response::Ok => Ok(()),
            Response::Error { message } => Err(anyhow!(message)),
            other => Err(anyhow!("unexpected: {other:?}")),
        };
    }
    Err(anyhow!("connection closed before reply"))
}

// ---------- CLI verbs ---------------------------------------------------

pub async fn run(
    branch: Option<String>,
    parent: Option<CellId>,
    who: Option<String>,
    source: String,
) -> Result<i32> {
    let snap = submit_streaming(branch, parent, who, source, |stream, data| match stream {
        Stream::Stdout => {
            use std::io::Write;
            print!("{data}");
            std::io::stdout().flush().ok();
        }
        Stream::Stderr => {
            use std::io::Write;
            eprint!("{data}");
            std::io::stderr().flush().ok();
        }
    })
    .await?;
    // Make sure we don't end on the middle of a line.
    ensure_trailing_newline(&snap.stdout, &snap.stderr);
    Ok(snap.exit_code.unwrap_or(0))
}

pub async fn watch() -> Result<()> {
    let mut stream = connect().await?;
    send_req(&mut stream, &Request::Subscribe).await?;
    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();

    while let Some(line) = lines.next_line().await? {
        let resp: Response = serde_json::from_str(&line)?;
        match resp {
            Response::Event(ev) => render(&ev),
            Response::Error { message } => eprintln!("error: {message}"),
            _ => {}
        }
    }
    Ok(())
}

pub async fn tree() -> Result<()> {
    let snap = fetch_tree().await?;
    render_tree(&snap);
    Ok(())
}

// ---------- Branch verbs ------------------------------------------------

pub async fn branch_list() -> Result<()> {
    let branches = fetch_branches().await?;
    render_branches(&branches, &read_current_branch());
    Ok(())
}

pub async fn branch_new(name: String, at: Option<CellId>) -> Result<()> {
    // Default the new branch's base to the current branch's tip.
    let at = match at {
        Some(id) => id,
        None => {
            let current = read_current_branch();
            fetch_branches()
                .await?
                .into_iter()
                .find(|b| b.name == current)
                .map(|b| b.tip)
                .ok_or_else(|| anyhow!("current branch '{current}' not found; pass --at <id>"))?
        }
    };
    send_expect_ok(&Request::CreateBranch {
        name: name.clone(),
        at,
    })
    .await?;
    println!("created branch '{name}' at #{at}");
    Ok(())
}

pub async fn branch_rm(name: String) -> Result<()> {
    send_expect_ok(&Request::DeleteBranch { name: name.clone() }).await?;
    // If we deleted the branch we were on, fall back to the default.
    if read_current_branch() == name {
        write_current_branch(DEFAULT_BRANCH);
        println!("deleted branch '{name}' (switched to '{DEFAULT_BRANCH}')");
    } else {
        println!("deleted branch '{name}'");
    }
    Ok(())
}

pub async fn branch_rename(from: String, to: String) -> Result<()> {
    send_expect_ok(&Request::RenameBranch {
        from: from.clone(),
        to: to.clone(),
    })
    .await?;
    if read_current_branch() == from {
        write_current_branch(&to);
    }
    println!("renamed branch '{from}' → '{to}'");
    Ok(())
}

pub async fn switch(name: String) -> Result<()> {
    let branches = fetch_branches().await?;
    if !branches.iter().any(|b| b.name == name) {
        return Err(anyhow!(
            "no such branch '{name}' (use `fern branch new {name}` to create it)"
        ));
    }
    write_current_branch(&name);
    println!("switched to branch '{name}'");
    Ok(())
}

// ---------- Rendering helpers ------------------------------------------

fn ensure_trailing_newline(stdout: &str, stderr: &str) {
    let last = stderr.chars().last().or_else(|| stdout.chars().last());
    if matches!(last, Some(c) if c != '\n') {
        println!();
    }
}

fn render(ev: &CellEvent) {
    match ev {
        CellEvent::Started {
            id,
            parent,
            source,
            who,
            branch,
        } => {
            let parent = parent
                .map(|p| format!("from #{p}"))
                .unwrap_or_else(|| "root".into());
            println!("\n[#{id} {who} {parent} on {branch}] $ {source}");
        }
        CellEvent::OutputChunk { stream, data, .. } => match stream {
            Stream::Stdout => {
                use std::io::Write;
                print!("{data}");
                std::io::stdout().flush().ok();
            }
            Stream::Stderr => {
                use std::io::Write;
                eprint!("{data}");
                std::io::stderr().flush().ok();
            }
        },
        CellEvent::Completed {
            id,
            exit_code,
            duration_ms,
            hash,
        } => {
            let short = hash
                .as_deref()
                .map(|h| format!(" {}", &h[..h.len().min(7)]))
                .unwrap_or_default();
            println!("[#{id}{short}] exit {exit_code} ({duration_ms}ms)");
        }
    }
}

fn render_branches(branches: &[BranchSnapshot], current: &str) {
    // Never empty: the default branch is seeded at the root and undeletable.
    for b in branches {
        let marker = if b.name == current { "*" } else { " " };
        let state = if b.running {
            "running".to_string()
        } else {
            b.tip_hash
                .as_deref()
                .map(|h| h[..h.len().min(7)].to_string())
                .unwrap_or_else(|| "-".to_string())
        };
        // Flag terminal-mode branches so it's clear where the next command
        // would run under a PTY.
        let mode = if b.tty { " (tty)" } else { "" };
        println!("{marker} {:<24} #{:<4} {state}{mode}", b.name, b.tip);
    }
}

fn render_tree(snap: &wire::TreeSnapshot) {
    use std::collections::HashMap;
    let mut children: HashMap<Option<CellId>, Vec<&wire::CellSnapshot>> = HashMap::new();
    for c in &snap.cells {
        children.entry(c.parent).or_default().push(c);
    }
    fn walk(
        children: &std::collections::HashMap<Option<CellId>, Vec<&wire::CellSnapshot>>,
        parent: Option<CellId>,
        depth: usize,
    ) {
        if let Some(cs) = children.get(&parent) {
            for c in cs {
                let indent = "  ".repeat(depth);
                let src = if c.source.is_empty() {
                    "(root)".to_string()
                } else {
                    c.source.clone()
                };
                let status = match c.exit_code {
                    Some(code) => format!("exit {code}"),
                    None => "running".to_string(),
                };
                let short_hash = c
                    .hash
                    .as_deref()
                    .map(|h| format!(" {}", &h[..h.len().min(7)]))
                    .unwrap_or_default();
                println!(
                    "{indent}#{}{short_hash} {} [{}] {status}",
                    c.id, src, c.submitter
                );
                walk(children, Some(c.id), depth + 1);
            }
        }
    }
    walk(&children, None, 0);
}
