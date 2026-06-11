//! Streaming evaluator: parsed Stmt + State → events on a channel + (new State, exit_code).
//!
//! Stdout/stderr from child processes are forwarded to the caller via an
//! `mpsc::Sender<CellEvent>` as `OutputChunk` events as bytes arrive — nothing
//! is buffered to a final blob.
//!
//! Builtins (`cd`, `export`, `unset`) update state and emit no chunks.
//! Regular commands spawn a process; state is unchanged.
//! Pipelines spawn each stage, chain stdin/stdout, capture stderr of every
//! stage and stdout of only the last stage.
//! Redirects on a command apply to that command's spawn.

use anyhow::{Result, anyhow};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command as TCmd;
use tokio::sync::mpsc;

use crate::parse::{self, Command, RedirOp, Redirect, Segment, Stmt, Word};
use crate::tree::{CellId, Output, Process, State};
use crate::wire::{CellEvent, Stream};

/// Top-level entry: parse and evaluate one source line, streaming chunks to
/// `events` and returning the post-run state + exit code.
pub async fn eval_line(
    state: &State,
    line: &str,
    cell_id: CellId,
    events: mpsc::Sender<CellEvent>,
    pty_reg: Option<mpsc::UnboundedSender<PtyRegistration>>,
) -> Result<(State, i32)> {
    let Some(stmt) = parse::parse(line)? else {
        return Ok((state.clone(), 0));
    };
    eval_stmt(state, &stmt, cell_id, &events, pty_reg.as_ref()).await
}

/// How a running PTY cell exposes its controls. `exec_under_pty` sends one of
/// these up to the daemon (when a registrar is provided) so `attach`/`kill` can
/// reach a still-running terminal cell: write to `input`, terminate via `killer`.
pub struct PtyRegistration {
    pub input: mpsc::UnboundedSender<Vec<u8>>,
    pub killer: Box<dyn portable_pty::ChildKiller + Send + Sync>,
}

#[async_recursion::async_recursion]
async fn eval_stmt(
    state: &State,
    stmt: &Stmt,
    cell_id: CellId,
    events: &mpsc::Sender<CellEvent>,
    pty_reg: Option<&mpsc::UnboundedSender<PtyRegistration>>,
) -> Result<(State, i32)> {
    match stmt {
        Stmt::Cmd(c) => eval_command(state, c, cell_id, events, pty_reg).await,
        // Pipelines stay pipe-mode (a multi-stage pipeline isn't a single
        // foreground terminal program), so they don't take a PTY registrar.
        Stmt::Pipe(cmds) => eval_pipeline(state, cmds, cell_id, events).await,
        Stmt::Seq(l, r) => {
            let (s1, _e1) = eval_stmt(state, l, cell_id, events, pty_reg).await?;
            eval_stmt(&s1, r, cell_id, events, pty_reg).await
        }
        Stmt::AndIf(l, r) => {
            let (s1, e1) = eval_stmt(state, l, cell_id, events, pty_reg).await?;
            if e1 == 0 {
                eval_stmt(&s1, r, cell_id, events, pty_reg).await
            } else {
                Ok((s1, e1))
            }
        }
        Stmt::OrIf(l, r) => {
            let (s1, e1) = eval_stmt(state, l, cell_id, events, pty_reg).await?;
            if e1 != 0 {
                eval_stmt(&s1, r, cell_id, events, pty_reg).await
            } else {
                Ok((s1, e1))
            }
        }
    }
}

async fn eval_command(
    state: &State,
    c: &Command,
    cell_id: CellId,
    events: &mpsc::Sender<CellEvent>,
    pty_reg: Option<&mpsc::UnboundedSender<PtyRegistration>>,
) -> Result<(State, i32)> {
    let argv: Vec<String> = c.words.iter().map(|w| expand_word(w, state)).collect();
    if argv.is_empty() {
        return Ok((state.clone(), 0));
    }

    let name = argv[0].as_str();
    if !BUILTINS.contains(&name) {
        // External command. I/O mode is inherited environment: a cell whose
        // state carries FERN_IO=tty runs external commands under a PTY (so
        // isatty-aware programs see a terminal). Redirects fall back to pipes.
        if tty_mode(state) && c.redirects.is_empty() {
            let exit = exec_under_pty(&argv, state, cell_id, events, pty_reg).await?;
            return Ok((state.clone(), exit));
        }
        let proc = Process::from_state(state, argv);
        let exit = exec_with_redirects(&proc, &c.redirects, state, cell_id, events).await?;
        return Ok((state.clone(), exit));
    }

    match name {
        "cd" => {
            if !c.redirects.is_empty() {
                return Err(anyhow!("redirects on builtin not yet supported"));
            }
            let target = if argv.len() < 2 {
                state
                    .env
                    .get("HOME")
                    .map(PathBuf::from)
                    .ok_or_else(|| anyhow!("cd: HOME not set"))?
            } else {
                let p = PathBuf::from(&argv[1]);
                if p.is_absolute() {
                    p
                } else {
                    state.cwd.join(p)
                }
            };
            let canon = target
                .canonicalize()
                .map_err(|e| anyhow!("cd {}: {e}", target.display()))?;
            let mut ns = state.clone();
            ns.cwd = canon;
            Ok((ns, 0))
        }
        "export" => {
            let mut ns = state.clone();
            for arg in &argv[1..] {
                if let Some(eq) = arg.find('=') {
                    ns.env.insert(arg[..eq].into(), arg[eq + 1..].into());
                }
            }
            Ok((ns, 0))
        }
        "unset" => {
            let mut ns = state.clone();
            for arg in &argv[1..] {
                ns.env.remove(arg);
            }
            Ok((ns, 0))
        }
        _ => unreachable!("non-builtin `{name}` reached builtin dispatch"),
    }
}

/// The shell builtins handled in-process by the evaluator. This is the single
/// source of truth: `eval_command` dispatches external-vs-builtin off this list,
/// and the client consults it (via [`is_pure_builtin_line`]) to decide whether a
/// line needs a terminal. Add a builtin here *and* in the match above.
pub const BUILTINS: &[&str] = &["cd", "export", "unset"];

/// True iff `source` parses and every command in it is a builtin — i.e. running
/// it spawns no external program, so on a `FERN_IO=tty` branch it needs no
/// terminal and can run inline. Conservative: a parse failure, a pipeline, or a
/// leading word that isn't a literal builtin all yield `false`.
pub fn is_pure_builtin_line(source: &str) -> bool {
    matches!(parse::parse(source), Ok(Some(stmt)) if stmt_is_pure_builtin(&stmt))
}

fn stmt_is_pure_builtin(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Cmd(c) => command_is_builtin(c),
        // A pipeline always runs its stages as external processes.
        Stmt::Pipe(_) => false,
        Stmt::AndIf(l, r) | Stmt::OrIf(l, r) | Stmt::Seq(l, r) => {
            stmt_is_pure_builtin(l) && stmt_is_pure_builtin(r)
        }
    }
}

fn command_is_builtin(c: &Command) -> bool {
    match c.words.first() {
        // An empty command (e.g. a bare assignment we don't model) is a no-op.
        None => true,
        // Only a single-literal leading word can be statically classified; a
        // word built from a variable could expand to anything, so treat it as
        // external (the safe choice — it gets the terminal if the branch is tty).
        Some(w) => matches!(
            w.segments.as_slice(),
            [Segment::Literal(name)] if BUILTINS.contains(&name.as_str())
        ),
    }
}

/// True when the inherited environment asks for a terminal (`FERN_IO=tty`).
/// Mode is configuration carried by the branch, not a per-cell flag.
pub fn tty_mode(state: &State) -> bool {
    state.env.get("FERN_IO").map(|v| v == "tty").unwrap_or(false)
}

async fn eval_pipeline(
    state: &State,
    cmds: &[Command],
    cell_id: CellId,
    events: &mpsc::Sender<CellEvent>,
) -> Result<(State, i32)> {
    if cmds.len() == 1 {
        return eval_command(state, &cmds[0], cell_id, events, None).await;
    }

    let mut prev_stdout: Option<std::process::Stdio> = None;
    let mut children: Vec<tokio::process::Child> = Vec::with_capacity(cmds.len());
    let mut stream_tasks: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::new();

    for (i, cmd) in cmds.iter().enumerate() {
        let argv: Vec<String> = cmd.words.iter().map(|w| expand_word(w, state)).collect();
        if argv.is_empty() {
            return Err(anyhow!("empty command in pipeline"));
        }
        let mut p = TCmd::new(&argv[0]);
        p.args(&argv[1..]);
        p.current_dir(&state.cwd);
        p.env_clear();
        p.envs(&state.env);

        match prev_stdout.take() {
            Some(s) => {
                p.stdin(s);
            }
            None => {
                p.stdin(Stdio::null());
            }
        }

        let is_last = i + 1 == cmds.len();
        if !is_last {
            p.stdout(Stdio::piped());
        } else {
            apply_last_stdout_redirect(&mut p, &cmd.redirects, state).await?;
        }
        p.stderr(Stdio::piped());
        // Kill the child if its future is dropped (e.g. detached cell aborted
        // via `fern kill`, or client disconnected mid-eval). Otherwise children
        // would orphan and stream-readers would hang on still-live pipes.
        p.kill_on_drop(true);

        let mut child = p
            .spawn()
            .map_err(|e| anyhow!("spawn {}: {e}", argv[0]))?;

        // Stream stderr of every stage.
        if let Some(err) = child.stderr.take() {
            let tx = events.clone();
            stream_tasks.push(tokio::spawn(stream_pipe(
                err,
                Stream::Stderr,
                cell_id,
                tx,
            )));
        }

        if !is_last {
            // Hand off stdout to the next stage as its stdin.
            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| anyhow!("no stdout to pipe"))?;
            let stdio: Stdio = stdout.try_into().map_err(|e| anyhow!("stdout→stdio: {e}"))?;
            prev_stdout = Some(stdio);
        } else if let Some(out) = child.stdout.take() {
            let tx = events.clone();
            stream_tasks.push(tokio::spawn(stream_pipe(out, Stream::Stdout, cell_id, tx)));
        }

        children.push(child);
    }

    // Wait for the last command's exit; then drain the rest.
    let mut exit_code = 0;
    let n = children.len();
    for (i, mut child) in children.into_iter().enumerate() {
        let status = child.wait().await?;
        if i + 1 == n {
            exit_code = status.code().unwrap_or(-1);
        }
    }
    for t in stream_tasks {
        let _ = t.await;
    }
    Ok((state.clone(), exit_code))
}

async fn apply_last_stdout_redirect(
    p: &mut TCmd,
    redirects: &[Redirect],
    state: &State,
) -> Result<()> {
    let mut handled = false;
    for r in redirects {
        if r.fd == 1 && matches!(r.op, RedirOp::Out | RedirOp::Append) {
            let path = expand_word(&r.target, state);
            let f = open_for_write(&path, r.op).await?;
            p.stdout(Stdio::from(f.into_std().await));
            handled = true;
        }
    }
    if !handled {
        p.stdout(Stdio::piped());
    }
    Ok(())
}

async fn exec_with_redirects(
    proc: &Process,
    redirects: &[Redirect],
    state: &State,
    cell_id: CellId,
    events: &mpsc::Sender<CellEvent>,
) -> Result<i32> {
    let mut p = TCmd::new(&proc.argv[0]);
    p.args(&proc.argv[1..]);
    p.current_dir(&proc.cwd);
    p.env_clear();
    p.envs(&proc.env);
    p.kill_on_drop(true);

    let mut stdin_set = false;
    let mut stdout_to_file = false;
    let mut stderr_to_file = false;

    for r in redirects {
        let path = expand_word(&r.target, state);
        match (r.fd, r.op) {
            (0, RedirOp::In) => {
                let f = File::open(&path)
                    .await
                    .map_err(|e| anyhow!("< {}: {e}", path))?;
                p.stdin(Stdio::from(f.into_std().await));
                stdin_set = true;
            }
            (1, op @ (RedirOp::Out | RedirOp::Append)) => {
                let f = open_for_write(&path, op).await?;
                p.stdout(Stdio::from(f.into_std().await));
                stdout_to_file = true;
            }
            (2, op @ (RedirOp::Out | RedirOp::Append)) => {
                let f = open_for_write(&path, op).await?;
                p.stderr(Stdio::from(f.into_std().await));
                stderr_to_file = true;
            }
            (fd, op) => return Err(anyhow!("unsupported redirect: fd={fd}, op={op:?}")),
        }
    }
    if !stdin_set {
        p.stdin(Stdio::null());
    }
    if !stdout_to_file {
        p.stdout(Stdio::piped());
    }
    if !stderr_to_file {
        p.stderr(Stdio::piped());
    }

    let mut child = p
        .spawn()
        .map_err(|e| anyhow!("spawn {}: {e}", proc.argv[0]))?;

    let stdout_task = if !stdout_to_file {
        let out = child.stdout.take().unwrap();
        let tx = events.clone();
        Some(tokio::spawn(stream_pipe(out, Stream::Stdout, cell_id, tx)))
    } else {
        None
    };
    let stderr_task = if !stderr_to_file {
        let err = child.stderr.take().unwrap();
        let tx = events.clone();
        Some(tokio::spawn(stream_pipe(err, Stream::Stderr, cell_id, tx)))
    } else {
        None
    };

    let status = child.wait().await?;
    if let Some(t) = stdout_task {
        let _ = t.await;
    }
    if let Some(t) = stderr_task {
        let _ = t.await;
    }
    Ok(status.code().unwrap_or(-1))
}

/// Run a single external command under a fresh PTY, streaming its (terminal)
/// output as OutputChunk events. The caller accumulates those chunks into the
/// cell's result, so PTY output is captured and content-addressed like any
/// other cell. State is unchanged (a subprocess's cwd/env die with it).
///
/// When `pty_reg` is provided, the cell's stdin is wired to an input channel
/// and an input sender + child killer are registered with the daemon, so a
/// client can `attach`/`send`/`kill` the running terminal. Without it (the
/// inline path) the command runs to completion with no stdin — a program that
/// reads input will block.
async fn exec_under_pty(
    argv: &[String],
    state: &State,
    cell_id: CellId,
    events: &mpsc::Sender<CellEvent>,
    pty_reg: Option<&mpsc::UnboundedSender<PtyRegistration>>,
) -> Result<i32> {
    use portable_pty::{CommandBuilder, PtySize, native_pty_system};

    let pair = native_pty_system().openpty(PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    })?;

    let mut cmd = CommandBuilder::new(&argv[0]);
    for a in &argv[1..] {
        cmd.arg(a);
    }
    cmd.cwd(state.cwd.clone());
    for (k, v) in &state.env {
        cmd.env(k, v);
    }

    let mut child = pair
        .slave
        .spawn_command(cmd)
        .map_err(|e| anyhow!("spawn {}: {e}", argv[0]))?;
    let reader = pair.master.try_clone_reader()?;

    // If a registrar is present, expose input + kill controls for this cell.
    if let Some(reg) = pty_reg {
        let writer = pair.master.take_writer()?;
        let (input_tx, mut input_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        std::thread::spawn(move || {
            use std::io::Write;
            let mut writer = writer;
            while let Some(bytes) = input_rx.blocking_recv() {
                if writer.write_all(&bytes).is_err() {
                    break;
                }
                let _ = writer.flush();
            }
        });
        let _ = reg.send(PtyRegistration {
            input: input_tx,
            killer: child.clone_killer(),
        });
    }

    drop(pair.slave); // so the master sees EOF once the child exits

    // Reader thread: PTY master → OutputChunk events (captured by the caller).
    let tx = events.clone();
    let reader_thread = std::thread::spawn(move || {
        use std::io::Read;
        let mut reader = reader;
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let data = String::from_utf8_lossy(&buf[..n]).into_owned();
                    let _ = tx.blocking_send(CellEvent::OutputChunk {
                        id: cell_id,
                        stream: Stream::Stdout,
                        data,
                    });
                }
            }
        }
    });

    let status = tokio::task::spawn_blocking(move || child.wait())
        .await
        .map_err(|e| anyhow!("pty wait join: {e}"))?
        .map_err(|e| anyhow!("pty wait: {e}"))?;
    let _ = reader_thread.join();
    drop(pair.master);

    Ok(status.exit_code() as i32)
}

async fn open_for_write(path: &str, op: RedirOp) -> Result<File> {
    match op {
        RedirOp::Out => File::create(path)
            .await
            .map_err(|e| anyhow!("> {}: {e}", path)),
        RedirOp::Append => OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await
            .map_err(|e| anyhow!(">> {}: {e}", path)),
        RedirOp::In => unreachable!(),
    }
}

async fn stream_pipe<R: AsyncRead + Unpin>(
    mut rd: R,
    stream: Stream,
    cell_id: CellId,
    events: mpsc::Sender<CellEvent>,
) -> Result<()> {
    let mut buf = [0u8; 8192];
    loop {
        let n = rd.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        let data = String::from_utf8_lossy(&buf[..n]).into_owned();
        let ev = CellEvent::OutputChunk {
            id: cell_id,
            stream,
            data,
        };
        if events.send(ev).await.is_err() {
            return Ok(());
        }
    }
}

fn expand_word(w: &Word, state: &State) -> String {
    let mut out = String::new();
    for seg in &w.segments {
        match seg {
            Segment::Literal(s) => out.push_str(s),
            Segment::Var(name) => {
                if let Some(v) = state.env.get(name) {
                    out.push_str(v);
                }
            }
        }
    }
    out
}

// ---------- Helpers for tests + non-streaming callers --------------------

/// Run `eval_line` to completion and collect all chunks into an `Output`.
/// Convenient for tests and any other caller that doesn't need streaming.
pub async fn eval_line_collect(state: &State, line: &str) -> Result<(State, Output)> {
    let (tx, mut rx) = mpsc::channel::<CellEvent>(1024);
    let drain = async move {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        while let Some(ev) = rx.recv().await {
            if let CellEvent::OutputChunk { stream, data, .. } = ev {
                match stream {
                    Stream::Stdout => stdout.extend_from_slice(data.as_bytes()),
                    Stream::Stderr => stderr.extend_from_slice(data.as_bytes()),
                }
            }
        }
        (stdout, stderr)
    };
    let (res, (stdout, stderr)) = tokio::join!(eval_line(state, line, 0, tx, None), drain);
    let (new_state, exit_code) = res?;
    Ok((
        new_state,
        Output {
            exit_code,
            stdout,
            stderr,
        },
    ))
}

// ---------- Tests --------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> State {
        State::baseline().unwrap()
    }

    fn out_str(o: &Output) -> String {
        String::from_utf8_lossy(&o.stdout).into_owned()
    }

    #[tokio::test]
    async fn simple_command() {
        let (_s, o) = eval_line_collect(&st(), "echo hello").await.unwrap();
        assert_eq!(out_str(&o).trim(), "hello");
        assert_eq!(o.exit_code, 0);
    }

    #[test]
    fn pure_builtin_classification() {
        // Pure builtins — no external program, so no terminal needed.
        assert!(is_pure_builtin_line("cd /tmp"));
        assert!(is_pure_builtin_line("export FOO=bar"));
        assert!(is_pure_builtin_line("unset FOO"));
        // Builtins chained still spawn nothing external.
        assert!(is_pure_builtin_line("cd /tmp && export FERN_IO=tty"));
        assert!(is_pure_builtin_line("cd /a; cd /b; unset X"));

        // Anything that runs an external program is not pure-builtin.
        assert!(!is_pure_builtin_line("ls"));
        assert!(!is_pure_builtin_line("vim"));
        assert!(!is_pure_builtin_line("cd /tmp && ls")); // one external stage is enough
        assert!(!is_pure_builtin_line("echo hi | cat")); // pipelines run external
        // A var-led command can expand to anything → treated as external (safe).
        assert!(!is_pure_builtin_line("$EDITOR"));
        // Empty / unparseable lines aren't builtins.
        assert!(!is_pure_builtin_line(""));
    }

    #[tokio::test]
    async fn var_expansion_bare() {
        let mut s = st();
        s.env.insert("X".into(), "world".into());
        let (_s, o) = eval_line_collect(&s, "echo hi-$X").await.unwrap();
        assert_eq!(out_str(&o).trim(), "hi-world");
    }

    #[tokio::test]
    async fn var_expansion_in_quotes() {
        let mut s = st();
        s.env.insert("X".into(), "world".into());
        let (_s, o) = eval_line_collect(&s, r#"echo "hi $X""#).await.unwrap();
        assert_eq!(out_str(&o).trim(), "hi world");
    }

    #[tokio::test]
    async fn single_quotes_no_expansion() {
        let mut s = st();
        s.env.insert("X".into(), "world".into());
        let (_s, o) = eval_line_collect(&s, "echo 'hi $X'").await.unwrap();
        assert_eq!(out_str(&o).trim(), "hi $X");
    }

    #[tokio::test]
    async fn pipeline() {
        let (_s, o) = eval_line_collect(&st(), "printf 'a\\nb\\nc\\n' | wc -l")
            .await
            .unwrap();
        assert_eq!(out_str(&o).trim(), "3");
    }

    #[tokio::test]
    async fn and_short_circuits() {
        let (_s, o) = eval_line_collect(&st(), "false && echo nope").await.unwrap();
        assert_ne!(o.exit_code, 0);
        assert!(out_str(&o).is_empty());
    }

    #[tokio::test]
    async fn or_runs_on_failure() {
        let (_s, o) = eval_line_collect(&st(), "false || echo yes").await.unwrap();
        assert_eq!(o.exit_code, 0);
        assert_eq!(out_str(&o).trim(), "yes");
    }

    #[tokio::test]
    async fn seq_runs_both() {
        let (_s, o) = eval_line_collect(&st(), "echo a; echo b").await.unwrap();
        assert_eq!(out_str(&o).trim(), "a\nb");
    }

    #[tokio::test]
    async fn cd_builtin() {
        let (s, _o) = eval_line_collect(&st(), "cd /tmp").await.unwrap();
        // `cd` canonicalizes, so on macOS /tmp resolves to /private/tmp.
        assert_eq!(s.cwd, std::fs::canonicalize("/tmp").unwrap());
    }

    #[tokio::test]
    async fn export_builtin() {
        let (s, _o) = eval_line_collect(&st(), "export FOO=bar").await.unwrap();
        assert_eq!(s.env.get("FOO").map(String::as_str), Some("bar"));
    }

    #[tokio::test]
    async fn redirect_out_then_in() {
        let tmp = std::env::temp_dir().join(format!("fern-test-{}", std::process::id()));
        let path = tmp.to_str().unwrap();
        let line_out = format!("echo redirected > {path}");
        let (_s, o) = eval_line_collect(&st(), &line_out).await.unwrap();
        assert_eq!(o.exit_code, 0);
        assert!(
            o.stdout.is_empty(),
            "redirected stdout should not be captured"
        );
        let read = std::fs::read_to_string(path).unwrap();
        assert_eq!(read.trim(), "redirected");

        let line_in = format!("cat < {path}");
        let (_s, o) = eval_line_collect(&st(), &line_in).await.unwrap();
        assert_eq!(out_str(&o).trim(), "redirected");

        std::fs::remove_file(path).ok();
    }

    #[tokio::test]
    async fn pipe_mode_is_not_a_tty() {
        // Default (no FERN_IO): external commands run under pipes.
        let (_s, o) = eval_line_collect(&st(), "tty").await.unwrap();
        assert_ne!(o.exit_code, 0);
        assert!(
            out_str(&o).to_lowercase().contains("not a tty"),
            "got: {:?}",
            out_str(&o)
        );
    }

    #[tokio::test]
    async fn tty_mode_gives_a_terminal() {
        // FERN_IO=tty in the inherited env makes the command see a real terminal.
        let mut s = st();
        s.env.insert("FERN_IO".into(), "tty".into());
        let (_s, o) = eval_line_collect(&s, "tty").await.unwrap();
        assert_eq!(o.exit_code, 0, "tty should report a terminal; got: {o:?}");
        assert!(out_str(&o).contains("/dev/"), "got: {:?}", out_str(&o));
    }

    #[tokio::test]
    async fn pipeline_state_unchanged() {
        let s = st();
        let cwd_before = s.cwd.clone();
        let (s2, _o) = eval_line_collect(&s, "echo a | cat").await.unwrap();
        assert_eq!(s2.cwd, cwd_before);
    }
}
