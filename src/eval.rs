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
) -> Result<(State, i32)> {
    let Some(stmt) = parse::parse(line)? else {
        return Ok((state.clone(), 0));
    };
    eval_stmt(state, &stmt, cell_id, &events).await
}

#[async_recursion::async_recursion]
async fn eval_stmt(
    state: &State,
    stmt: &Stmt,
    cell_id: CellId,
    events: &mpsc::Sender<CellEvent>,
) -> Result<(State, i32)> {
    match stmt {
        Stmt::Cmd(c) => eval_command(state, c, cell_id, events).await,
        Stmt::Pipe(cmds) => eval_pipeline(state, cmds, cell_id, events).await,
        Stmt::Seq(l, r) => {
            let (s1, _e1) = eval_stmt(state, l, cell_id, events).await?;
            eval_stmt(&s1, r, cell_id, events).await
        }
        Stmt::AndIf(l, r) => {
            let (s1, e1) = eval_stmt(state, l, cell_id, events).await?;
            if e1 == 0 {
                eval_stmt(&s1, r, cell_id, events).await
            } else {
                Ok((s1, e1))
            }
        }
        Stmt::OrIf(l, r) => {
            let (s1, e1) = eval_stmt(state, l, cell_id, events).await?;
            if e1 != 0 {
                eval_stmt(&s1, r, cell_id, events).await
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
) -> Result<(State, i32)> {
    let argv: Vec<String> = c.words.iter().map(|w| expand_word(w, state)).collect();
    if argv.is_empty() {
        return Ok((state.clone(), 0));
    }

    match argv[0].as_str() {
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
        _ => {
            let proc = Process::from_state(state, argv);
            let exit = exec_with_redirects(&proc, &c.redirects, state, cell_id, events).await?;
            Ok((state.clone(), exit))
        }
    }
}

async fn eval_pipeline(
    state: &State,
    cmds: &[Command],
    cell_id: CellId,
    events: &mpsc::Sender<CellEvent>,
) -> Result<(State, i32)> {
    if cmds.len() == 1 {
        return eval_command(state, &cmds[0], cell_id, events).await;
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
        // via `shsh kill`, or client disconnected mid-eval). Otherwise children
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
    let (res, (stdout, stderr)) = tokio::join!(eval_line(state, line, 0, tx), drain);
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
        assert_eq!(s.cwd, PathBuf::from("/tmp"));
    }

    #[tokio::test]
    async fn export_builtin() {
        let (s, _o) = eval_line_collect(&st(), "export FOO=bar").await.unwrap();
        assert_eq!(s.env.get("FOO").map(String::as_str), Some("bar"));
    }

    #[tokio::test]
    async fn redirect_out_then_in() {
        let tmp = std::env::temp_dir().join(format!("shsh-test-{}", std::process::id()));
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
    async fn pipeline_state_unchanged() {
        let s = st();
        let cwd_before = s.cwd.clone();
        let (s2, _o) = eval_line_collect(&s, "echo a | cat").await.unwrap();
        assert_eq!(s2.cwd, cwd_before);
    }
}
