//! Daemon: owns the cell tree, broadcasts CellEvents, serves requests over a
//! unix socket. Supports inline, detached, and interactive (PTY-attached) cells.

use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::AbortHandle;

use crate::tree::{Cell, CellId, CellResult, State, Tree};
use crate::wire::{CellEvent, CellSnapshot, Request, Response, Stream, TreeSnapshot, socket_path};

struct DaemonState {
    tree: Mutex<Tree>,
    events: broadcast::Sender<CellEvent>,
    active: Mutex<HashMap<CellId, Arc<ActiveCell>>>,
}

struct ActiveCell {
    /// Aborts the supervising task. For non-interactive cells this kills the
    /// eval; for interactive cells the supervisor exits naturally once the
    /// child does (or we kill the child explicitly).
    abort: AbortHandle,
    /// PTY-specific bits. None for non-interactive cells.
    pty: Option<PtyHandle>,
}

struct PtyHandle {
    /// Sender for bytes to write to the PTY's slave stdin.
    input: mpsc::UnboundedSender<Vec<u8>>,
    /// Child handle for explicit kill. Wrapped in std::sync::Mutex so we can
    /// hold it across the blocking try_wait/kill calls.
    child: std::sync::Mutex<Box<dyn portable_pty::Child + Send + Sync>>,
}

pub async fn run() -> Result<()> {
    let path = socket_path();
    let _ = std::fs::remove_file(&path);
    let listener =
        UnixListener::bind(&path).with_context(|| format!("bind {}", path.display()))?;
    eprintln!("shsh daemon listening on {}", path.display());

    let (events, _) = broadcast::channel(4096);
    let state = Arc::new(DaemonState {
        tree: Mutex::new(Tree::new(State::baseline()?)),
        events,
        active: Mutex::new(HashMap::new()),
    });

    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, state).await {
                eprintln!("conn error: {e:#}");
            }
        });
    }
}

async fn handle_conn(stream: UnixStream, state: Arc<DaemonState>) -> Result<()> {
    let (rd, mut wr) = stream.into_split();
    let mut lines = BufReader::new(rd).lines();

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            continue;
        }
        let req: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                send(
                    &mut wr,
                    &Response::Error {
                        message: format!("bad request: {e}"),
                    },
                )
                .await?;
                continue;
            }
        };
        match req {
            Request::Submit {
                parent,
                source,
                who,
                detach,
                interactive,
            } => {
                handle_submit(&state, parent, source, who, detach, interactive, &mut wr).await?;
            }
            Request::Subscribe => {
                handle_subscribe(&state, &mut wr).await?;
                break;
            }
            Request::GetTree => {
                let t = state.tree.lock().await;
                let snapshot = snapshot_tree(&t);
                send(&mut wr, &Response::Tree(snapshot)).await?;
            }
            Request::GetCell { id } => {
                let t = state.tree.lock().await;
                match t.get(id) {
                    Some(c) => send(&mut wr, &Response::Cell(snapshot_cell(c))).await?,
                    None => {
                        send(
                            &mut wr,
                            &Response::Error {
                                message: format!("no such cell #{id}"),
                            },
                        )
                        .await?
                    }
                }
            }
            Request::Kill { id } => handle_kill(&state, id, &mut wr).await?,
            Request::Attach { id } => {
                handle_attach(&state, id, &mut lines, &mut wr).await?;
                break;
            }
            Request::Input { .. } => {
                // Input is only valid inside an attach. Ignore at top level.
                send(
                    &mut wr,
                    &Response::Error {
                        message: "Input received outside Attach session".into(),
                    },
                )
                .await?;
            }
        }
    }
    Ok(())
}

// ---------- Submit (inline / detached / interactive+detached) ----------

async fn handle_submit(
    state: &Arc<DaemonState>,
    parent: CellId,
    source: String,
    who: String,
    detach: bool,
    interactive: bool,
    wr: &mut tokio::net::unix::OwnedWriteHalf,
) -> Result<()> {
    if interactive && !detach {
        send(
            wr,
            &Response::Error {
                message: "--interactive currently requires --detach (attach from another client)"
                    .into(),
            },
        )
        .await?;
        return Ok(());
    }

    let (cell_id, parent_state) = {
        let mut t = state.tree.lock().await;
        let parent_state = match t.get(parent) {
            Some(c) => c
                .result
                .as_ref()
                .ok_or_else(|| anyhow!("parent #{parent} hasn't finished — can't branch yet"))?
                .end_state
                .clone(),
            None => {
                send(
                    wr,
                    &Response::Error {
                        message: format!("no such parent cell #{parent}"),
                    },
                )
                .await?;
                return Ok(());
            }
        };
        let id = t.reserve_id();
        (id, parent_state)
    };

    let started_event = CellEvent::Started {
        id: cell_id,
        parent: Some(parent),
        source: source.clone(),
        who: who.clone(),
    };
    let _ = state.events.send(started_event.clone());
    send(wr, &Response::Event(started_event)).await?;

    if interactive {
        spawn_interactive_detached(state.clone(), parent, cell_id, parent_state, source, who)
            .await?;
        return Ok(());
    }

    if detach {
        // Insert placeholder cell with no result yet.
        {
            let mut t = state.tree.lock().await;
            t.insert_cell(
                parent,
                Cell {
                    id: cell_id,
                    parent: Some(parent),
                    submitter: who.clone(),
                    source: source.clone(),
                    result: None,
                },
            );
        }
        let state_clone = state.clone();
        tokio::spawn(async move {
            run_detached(state_clone, cell_id, parent_state, source).await;
        });
        return Ok(());
    }

    // Inline non-interactive path.
    run_inline(state, parent, cell_id, parent_state, source, who, wr).await
}

async fn run_inline(
    state: &Arc<DaemonState>,
    parent: CellId,
    cell_id: CellId,
    parent_state: State,
    source: String,
    who: String,
    wr: &mut tokio::net::unix::OwnedWriteHalf,
) -> Result<()> {
    let (chunk_tx, mut chunk_rx) = mpsc::channel::<CellEvent>(128);
    let src = source.clone();
    let started = std::time::Instant::now();

    let eval_state = parent_state.clone();
    let eval_fut = async move { crate::eval::eval_line(&eval_state, &src, cell_id, chunk_tx).await };

    let events_broadcast = state.events.clone();
    let forward_fut = async {
        let mut stdout = String::new();
        let mut stderr = String::new();
        while let Some(ev) = chunk_rx.recv().await {
            if let CellEvent::OutputChunk { stream, data, .. } = &ev {
                match stream {
                    Stream::Stdout => stdout.push_str(data),
                    Stream::Stderr => stderr.push_str(data),
                }
            }
            let _ = events_broadcast.send(ev.clone());
            send(wr, &Response::Event(ev)).await?;
        }
        Ok::<_, anyhow::Error>((stdout, stderr))
    };

    let (eval_result, fwd_result) = tokio::join!(eval_fut, forward_fut);
    let mut error_msg: Option<String> = None;
    let (new_state, exit_code) = match eval_result {
        Ok(r) => r,
        Err(e) => {
            error_msg = Some(format!("{e}\n"));
            (parent_state.clone(), 2)
        }
    };
    let (stdout, mut stderr) = fwd_result?;
    if let Some(msg) = error_msg {
        // Broadcast a chunk so watchers see why the cell died too.
        let _ = state.events.send(CellEvent::OutputChunk {
            id: cell_id,
            stream: Stream::Stderr,
            data: msg.clone(),
        });
        let _ = send(
            wr,
            &Response::Event(CellEvent::OutputChunk {
                id: cell_id,
                stream: Stream::Stderr,
                data: msg.clone(),
            }),
        )
        .await;
        stderr.push_str(&msg);
    }
    let duration = started.elapsed();

    let cell = Cell {
        id: cell_id,
        parent: Some(parent),
        submitter: who,
        source,
        result: Some(CellResult {
            exit_code,
            stdout: stdout.into_bytes(),
            stderr: stderr.into_bytes(),
            duration,
            end_state: new_state,
        }),
    };
    {
        let mut t = state.tree.lock().await;
        t.insert_cell(parent, cell);
    }
    let completed = CellEvent::Completed {
        id: cell_id,
        exit_code,
        duration_ms: duration.as_millis() as u64,
    };
    let _ = state.events.send(completed.clone());
    send(wr, &Response::Event(completed)).await?;
    Ok(())
}

async fn run_detached(
    state: Arc<DaemonState>,
    cell_id: CellId,
    parent_state: State,
    source: String,
) {
    let (chunk_tx, mut chunk_rx) = mpsc::channel::<CellEvent>(128);
    let started = std::time::Instant::now();

    let eval_state = parent_state.clone();
    let src = source.clone();
    let eval_handle =
        tokio::spawn(async move { crate::eval::eval_line(&eval_state, &src, cell_id, chunk_tx).await });

    state.active.lock().await.insert(
        cell_id,
        Arc::new(ActiveCell {
            abort: eval_handle.abort_handle(),
            pty: None,
        }),
    );

    let mut stdout = String::new();
    let mut stderr = String::new();
    while let Some(ev) = chunk_rx.recv().await {
        if let CellEvent::OutputChunk { stream, data, .. } = &ev {
            match stream {
                Stream::Stdout => stdout.push_str(data),
                Stream::Stderr => stderr.push_str(data),
            }
        }
        let _ = state.events.send(ev);
    }

    let join = eval_handle.await;
    let (new_state, exit_code) = match join {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            let msg = format!("{e}\n");
            let _ = state.events.send(CellEvent::OutputChunk {
                id: cell_id,
                stream: Stream::Stderr,
                data: msg.clone(),
            });
            stderr.push_str(&msg);
            (parent_state.clone(), 2)
        }
        Err(je) if je.is_cancelled() => {
            stderr.push_str("[killed by shsh kill]\n");
            (parent_state.clone(), -1)
        }
        Err(e) => {
            eprintln!("detached cell #{cell_id} panicked: {e}");
            (parent_state.clone(), 2)
        }
    };
    let duration = started.elapsed();

    {
        let mut t = state.tree.lock().await;
        t.set_cell_result(
            cell_id,
            CellResult {
                exit_code,
                stdout: stdout.into_bytes(),
                stderr: stderr.into_bytes(),
                duration,
                end_state: new_state,
            },
        );
    }
    state.active.lock().await.remove(&cell_id);

    let _ = state.events.send(CellEvent::Completed {
        id: cell_id,
        exit_code,
        duration_ms: duration.as_millis() as u64,
    });
}

// ---------- Interactive (PTY-backed, detached) -------------------------

async fn spawn_interactive_detached(
    state: Arc<DaemonState>,
    parent: CellId,
    cell_id: CellId,
    parent_state: State,
    source: String,
    who: String,
) -> Result<()> {
    // Insert placeholder cell.
    {
        let mut t = state.tree.lock().await;
        t.insert_cell(
            parent,
            Cell {
                id: cell_id,
                parent: Some(parent),
                submitter: who,
                source: source.clone(),
                result: None,
            },
        );
    }

    let started = std::time::Instant::now();
    let pty_setup = setup_pty(cell_id, &parent_state, &source, state.events.clone());
    let (input_tx, child, eof_rx) = match pty_setup {
        Ok(x) => x,
        Err(e) => {
            let mut t = state.tree.lock().await;
            t.set_cell_result(
                cell_id,
                CellResult {
                    exit_code: 2,
                    stdout: vec![],
                    stderr: format!("pty setup: {e}\n").into_bytes(),
                    duration: Duration::ZERO,
                    end_state: parent_state.clone(),
                },
            );
            let _ = state.events.send(CellEvent::Completed {
                id: cell_id,
                exit_code: 2,
                duration_ms: 0,
            });
            return Ok(());
        }
    };

    let pty_handle = PtyHandle {
        input: input_tx,
        child: std::sync::Mutex::new(child),
    };

    // Supervisor: waits for PTY EOF, then reaps exit, updates tree, broadcasts Completed.
    let state_clone = state.clone();
    let parent_state_clone = parent_state.clone();
    let supervisor = tokio::spawn(async move {
        let _ = eof_rx.await; // PTY EOF — child has exited (or was killed)

        // Reap the child exit code via try_wait on the child stored in active.
        // We need to look up the active cell again to find the child handle.
        let exit_code = {
            let active_map = state_clone.active.lock().await;
            let ac = active_map.get(&cell_id);
            ac.and_then(|ac| ac.pty.as_ref())
                .and_then(|pty| {
                    let mut child = pty.child.lock().ok()?;
                    child.try_wait().ok().flatten().map(|s| s.exit_code() as i32)
                })
                .unwrap_or(-1)
        };
        let duration = started.elapsed();

        {
            let mut t = state_clone.tree.lock().await;
            t.set_cell_result(
                cell_id,
                CellResult {
                    exit_code,
                    stdout: vec![], // interactive output isn't captured (v1 limitation)
                    stderr: vec![],
                    duration,
                    end_state: parent_state_clone,
                },
            );
        }
        state_clone.active.lock().await.remove(&cell_id);

        let _ = state_clone.events.send(CellEvent::Completed {
            id: cell_id,
            exit_code,
            duration_ms: duration.as_millis() as u64,
        });
    });

    state.active.lock().await.insert(
        cell_id,
        Arc::new(ActiveCell {
            abort: supervisor.abort_handle(),
            pty: Some(pty_handle),
        }),
    );

    Ok(())
}

/// Open a PTY, spawn `bash -c <source>` on the slave, wire up reader/writer
/// threads, and return:
///   - input_tx: pump bytes here to write to the PTY's stdin
///   - child: portable-pty Child handle (for kill)
///   - eof_rx: fires when the PTY reader sees EOF (i.e. child has exited)
fn setup_pty(
    cell_id: CellId,
    parent_state: &State,
    source: &str,
    events: broadcast::Sender<CellEvent>,
) -> Result<(
    mpsc::UnboundedSender<Vec<u8>>,
    Box<dyn portable_pty::Child + Send + Sync>,
    tokio::sync::oneshot::Receiver<()>,
)> {
    let pty_system = portable_pty::native_pty_system();
    let pair = pty_system.openpty(portable_pty::PtySize {
        rows: 24,
        cols: 80,
        pixel_width: 0,
        pixel_height: 0,
    })?;

    let mut cmd = portable_pty::CommandBuilder::new("bash");
    cmd.arg("-c");
    cmd.arg(source);
    cmd.cwd(parent_state.cwd.clone());
    for (k, v) in &parent_state.env {
        cmd.env(k, v);
    }

    let child = pair
        .slave
        .spawn_command(cmd)
        .map_err(|e| anyhow!("spawn bash: {e}"))?;
    drop(pair.slave);

    let master_reader = pair.master.try_clone_reader()?;
    let master_writer = pair.master.take_writer()?;

    let (input_tx, mut input_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (eof_tx, eof_rx) = tokio::sync::oneshot::channel::<()>();

    // Reader thread: PTY → broadcast events; signal EOF on close.
    std::thread::Builder::new()
        .name(format!("shsh-pty-r-{cell_id}"))
        .spawn(move || {
            use std::io::Read;
            let mut reader = master_reader;
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let data = String::from_utf8_lossy(&buf[..n]).into_owned();
                        let _ = events.send(CellEvent::OutputChunk {
                            id: cell_id,
                            stream: Stream::Stdout,
                            data,
                        });
                    }
                }
            }
            let _ = eof_tx.send(());
        })?;

    // Writer thread: input channel → PTY stdin.
    std::thread::Builder::new()
        .name(format!("shsh-pty-w-{cell_id}"))
        .spawn(move || {
            use std::io::Write;
            let mut writer = master_writer;
            while let Some(bytes) = input_rx.blocking_recv() {
                if writer.write_all(&bytes).is_err() {
                    break;
                }
                let _ = writer.flush();
            }
        })?;

    Ok((input_tx, child, eof_rx))
}

// ---------- Kill --------------------------------------------------------

async fn handle_kill(
    state: &Arc<DaemonState>,
    id: CellId,
    wr: &mut tokio::net::unix::OwnedWriteHalf,
) -> Result<()> {
    let active = state.active.lock().await.get(&id).cloned();
    match active {
        Some(ac) => {
            if let Some(pty) = &ac.pty {
                // Interactive cell: signal the child. Supervisor will reap.
                if let Ok(mut child) = pty.child.lock() {
                    let _ = child.kill();
                }
            } else {
                // Non-interactive: abort the eval task.
                ac.abort.abort();
            }
            send(wr, &Response::Ok).await?;
        }
        None => {
            send(
                wr,
                &Response::Error {
                    message: format!("cell #{id} is not running"),
                },
            )
            .await?
        }
    }
    Ok(())
}

// ---------- Attach ------------------------------------------------------

async fn handle_attach(
    state: &Arc<DaemonState>,
    id: CellId,
    rd_lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    wr: &mut tokio::net::unix::OwnedWriteHalf,
) -> Result<()> {
    let active = state.active.lock().await.get(&id).cloned();
    let Some(ac) = active else {
        send(
            wr,
            &Response::Error {
                message: format!("cell #{id} is not running (or not interactive)"),
            },
        )
        .await?;
        return Ok(());
    };
    let Some(pty) = &ac.pty else {
        send(
            wr,
            &Response::Error {
                message: format!("cell #{id} is not interactive"),
            },
        )
        .await?;
        return Ok(());
    };
    let pty_input = pty.input.clone();

    send(wr, &Response::Ok).await?;

    let mut event_rx = state.events.subscribe();
    loop {
        tokio::select! {
            ev_result = event_rx.recv() => {
                match ev_result {
                    Ok(ev) => {
                        let matches = match &ev {
                            CellEvent::OutputChunk { id: i, .. } => *i == id,
                            CellEvent::Completed { id: i, .. } => *i == id,
                            _ => false,
                        };
                        if !matches { continue; }
                        let is_done = matches!(ev, CellEvent::Completed { .. });
                        send(wr, &Response::Event(ev)).await?;
                        if is_done {
                            return Ok(());
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            line_result = rd_lines.next_line() => {
                let line = match line_result? {
                    Some(l) => l,
                    None => return Ok(()),
                };
                if line.is_empty() { continue; }
                if let Ok(req) = serde_json::from_str::<Request>(&line) {
                    if let Request::Input { id: req_id, data } = req {
                        if req_id == id {
                            let _ = pty_input.send(data.into_bytes());
                        }
                    }
                }
            }
        }
    }
}

// ---------- Subscribe + helpers ----------------------------------------

async fn handle_subscribe(
    state: &Arc<DaemonState>,
    wr: &mut tokio::net::unix::OwnedWriteHalf,
) -> Result<()> {
    let mut rx = state.events.subscribe();
    loop {
        match rx.recv().await {
            Ok(ev) => send(wr, &Response::Event(ev)).await?,
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
    Ok(())
}

async fn send(w: &mut tokio::net::unix::OwnedWriteHalf, resp: &Response) -> Result<()> {
    let mut line = serde_json::to_string(resp)?;
    line.push('\n');
    w.write_all(line.as_bytes()).await?;
    w.flush().await?;
    Ok(())
}

fn snapshot_cell(c: &Cell) -> CellSnapshot {
    let r = c.result.as_ref();
    CellSnapshot {
        id: c.id,
        parent: c.parent,
        submitter: c.submitter.clone(),
        source: c.source.clone(),
        exit_code: r.map(|r| r.exit_code),
        duration_ms: r.map(|r| r.duration.as_millis() as u64).unwrap_or(0),
        stdout: r
            .map(|r| String::from_utf8_lossy(&r.stdout).into_owned())
            .unwrap_or_default(),
        stderr: r
            .map(|r| String::from_utf8_lossy(&r.stderr).into_owned())
            .unwrap_or_default(),
    }
}

fn snapshot_tree(t: &Tree) -> TreeSnapshot {
    let mut cells: Vec<CellSnapshot> = Vec::new();
    let mut stack = vec![0u64];
    while let Some(id) = stack.pop() {
        if let Some(c) = t.get(id) {
            cells.push(snapshot_cell(c));
            for &child in t.children_of(id).iter().rev() {
                stack.push(child);
            }
        }
    }
    cells.sort_by_key(|c| c.id);
    TreeSnapshot { cells }
}
