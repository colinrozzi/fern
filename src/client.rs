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
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::tree::CellId;
use crate::wire::{self, CellEvent, CellSnapshot, Request, Response, Stream, TreeSnapshot, socket_path};

// ---------- Cursor (shared across all clients on this host) ------------

fn cursor_path() -> PathBuf {
    let base = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".into());
    PathBuf::from(base).join("fern-cursor")
}

pub fn read_cursor() -> CellId {
    std::fs::read_to_string(cursor_path())
        .ok()
        .and_then(|s| s.trim().parse::<CellId>().ok())
        .unwrap_or(0)
}

pub fn write_cursor(id: CellId) {
    let _ = std::fs::write(cursor_path(), id.to_string());
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

/// Submit a command; invoke `on_chunk(stream, data)` for each OutputChunk as
/// it streams in; return the final CellSnapshot when Completed.
pub async fn submit_streaming<F>(
    parent: Option<CellId>,
    who: Option<String>,
    source: String,
    mut on_chunk: F,
) -> Result<CellSnapshot>
where
    F: FnMut(Stream, &str),
{
    let who = who.unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "?".into()));
    let parent = parent.unwrap_or_else(read_cursor);

    let mut stream = connect().await?;
    send_req(
        &mut stream,
        &Request::Submit {
            parent,
            source: source.clone(),
            who: who.clone(),
            detach: false,
            interactive: false,
        },
    )
    .await?;

    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();
    let mut stdout = String::new();
    let mut stderr = String::new();

    while let Some(line) = lines.next_line().await? {
        let resp: Response = serde_json::from_str(&line)?;
        match resp {
            Response::Event(CellEvent::Started { id, .. }) => {
                write_cursor(id);
            }
            Response::Event(CellEvent::OutputChunk { stream: s, data, .. }) => {
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
            }) => {
                return Ok(CellSnapshot {
                    id,
                    parent: Some(parent),
                    submitter: who,
                    source,
                    exit_code: Some(exit_code),
                    duration_ms,
                    stdout,
                    stderr,
                });
            }
            Response::Error { message } => return Err(anyhow!(message)),
            _ => {}
        }
    }
    Err(anyhow!("connection closed before Completed event"))
}

/// Submit a detached (long-running) cell. Returns the cell id as soon as the
/// daemon sends Started; the cell runs in the background. Use `watch` to see
/// output stream, `kill` to terminate it.
pub async fn submit_detached(
    parent: Option<CellId>,
    who: Option<String>,
    source: String,
    interactive: bool,
) -> Result<CellId> {
    let who = who.unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "?".into()));
    let parent = parent.unwrap_or_else(read_cursor);

    let mut stream = connect().await?;
    send_req(
        &mut stream,
        &Request::Submit {
            parent,
            source,
            who,
            detach: true,
            interactive,
        },
    )
    .await?;

    let (rd, _wr) = stream.split();
    let mut lines = BufReader::new(rd).lines();
    while let Some(line) = lines.next_line().await? {
        let resp: Response = serde_json::from_str(&line)?;
        match resp {
            Response::Event(CellEvent::Started { id, .. }) => {
                write_cursor(id);
                return Ok(id);
            }
            Response::Error { message } => return Err(anyhow!(message)),
            _ => {}
        }
    }
    Err(anyhow!("connection closed before Started event"))
}

/// Attach to a running interactive cell. Puts the terminal in raw mode so
/// keys flow directly to the cell's PTY (Ctrl+]) detaches without killing.
pub async fn attach(id: CellId) -> Result<()> {
    use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
    use tokio::io::AsyncReadExt;

    let mut stream = connect().await?;
    send_req(&mut stream, &Request::Attach { id }).await?;

    let (rd, mut wr) = stream.into_split();
    let mut lines = BufReader::new(rd).lines();

    // Read the daemon's first response — Ok means attached, Error otherwise.
    let first = lines
        .next_line()
        .await?
        .ok_or_else(|| anyhow!("daemon closed connection"))?;
    let first_resp: Response = serde_json::from_str(&first)?;
    match first_resp {
        Response::Ok => {}
        Response::Error { message } => return Err(anyhow!(message)),
        other => return Err(anyhow!("expected Ok, got {other:?}")),
    }

    enable_raw_mode().context("enable raw mode")?;
    // Make sure we always restore cooked mode on exit.
    let _guard = RawModeGuard;

    eprintln!("[fern] attached to cell #{id}. Ctrl+] to detach.\r");

    // Reader: stdin → daemon Input requests. Detach on Ctrl+] (0x1d).
    let detach_signal = Arc::new(tokio::sync::Notify::new());
    let detach_signal_recv = detach_signal.clone();

    let stdin_task = tokio::spawn(async move {
        let mut stdin = tokio::io::stdin();
        let mut buf = [0u8; 1024];
        loop {
            let n = match stdin.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => break,
            };
            // Detach on Ctrl+] (GS, 0x1d)
            if buf[..n].contains(&0x1d) {
                detach_signal.notify_one();
                break;
            }
            let payload = String::from_utf8_lossy(&buf[..n]).into_owned();
            let req = Request::Input { id, data: payload };
            let mut line = serde_json::to_string(&req).unwrap();
            line.push('\n');
            if wr.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            let _ = wr.flush().await;
        }
    });

    // Writer: daemon events → stdout (raw bytes, no Started/Completed framing).
    let event_task = async {
        use std::io::Write;
        let mut stdout = std::io::stdout();
        while let Some(line) = lines.next_line().await? {
            let resp: Response = serde_json::from_str(&line)?;
            match resp {
                Response::Event(CellEvent::OutputChunk { data, .. }) => {
                    stdout.write_all(data.as_bytes()).ok();
                    stdout.flush().ok();
                }
                Response::Event(CellEvent::Completed { exit_code, .. }) => {
                    eprintln!("\r\n[fern] cell exited with code {exit_code}\r");
                    return Ok::<_, anyhow::Error>(());
                }
                Response::Error { message } => {
                    eprintln!("\r\n[fern] error: {message}\r");
                    return Ok(());
                }
                _ => {}
            }
        }
        Ok(())
    };

    tokio::select! {
        _ = event_task => {},
        _ = detach_signal_recv.notified() => {
            eprintln!("\r\n[fern] detached (cell still running)\r");
        }
    }
    stdin_task.abort();
    disable_raw_mode().ok();
    Ok(())
}

struct RawModeGuard;
impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
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

/// Non-streaming wrapper around `submit_streaming` for callers that just want
/// the final snapshot.
#[allow(dead_code)]
pub async fn submit(
    parent: Option<CellId>,
    who: Option<String>,
    source: String,
) -> Result<CellSnapshot> {
    submit_streaming(parent, who, source, |_, _| {}).await
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

// ---------- CLI verbs ---------------------------------------------------

pub async fn run(parent: Option<CellId>, who: Option<String>, source: String) -> Result<i32> {
    let snap = submit_streaming(parent, who, source, |stream, data| match stream {
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

// ---------- Rendering helpers ------------------------------------------

fn ensure_trailing_newline(stdout: &str, stderr: &str) {
    let last = stderr
        .chars()
        .last()
        .or_else(|| stdout.chars().last());
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
        } => {
            let parent = parent
                .map(|p| format!("from #{p}"))
                .unwrap_or_else(|| "root".into());
            println!("\n[#{id} {who} {parent}] $ {source}");
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
        } => {
            println!("[#{id}] exit {exit_code} ({duration_ms}ms)");
        }
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
                println!("{indent}#{} {} [{}] {status}", c.id, src, c.submitter);
                walk(children, Some(c.id), depth + 1);
            }
        }
    }
    walk(&children, None, 0);
}
