//! Daemon: owns the cell tree, broadcasts CellEvents, serves requests over a
//! unix socket. Supports inline, detached, and interactive (PTY-attached) cells.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::AbortHandle;

use crate::eval::PtyRegistration;
use crate::tree::{Cell, CellId, CellResult, State, SystemInfo, Tree};
use crate::wire::{
    BranchSnapshot, CellEvent, CellSnapshot, Request, Response, Stream, TreeSnapshot, socket_path,
};

struct DaemonState {
    tree: Mutex<Tree>,
    events: broadcast::Sender<CellEvent>,
    active: Mutex<HashMap<CellId, Arc<ActiveCell>>>,
}

struct ActiveCell {
    /// Aborts the eval task. Used to kill pipe-mode cells; PTY cells are killed
    /// via their `killer` instead (aborting the task wouldn't reap the child).
    abort: AbortHandle,
    /// Whether this cell runs in tty mode (inherited `FERN_IO=tty`). Only tty
    /// cells ever spawn a PTY, so only they are attachable — for anything else
    /// `attach` fails fast and the client falls back to plain streaming.
    tty: bool,
    /// The running PTY's controls, populated once the cell spawns a terminal
    /// (a `FERN_IO=tty` external command). `ready` fires when this is set.
    pty: tokio::sync::Mutex<Option<PtyHandle>>,
    /// Notified when `pty` is populated, or when `finished` flips — so an
    /// attacher waits deterministically instead of polling.
    ready: tokio::sync::Notify,
    /// Set just before the cell is finalized, so a waiter wakes and gives up
    /// rather than waiting for a PTY that will never come (e.g. a tty branch
    /// running a pipeline, or a spawn that failed).
    finished: std::sync::atomic::AtomicBool,
}

struct PtyHandle {
    /// Sender for bytes to write to the PTY's stdin.
    input: mpsc::UnboundedSender<Vec<u8>>,
    /// Kills the PTY child. Wrapped in a std Mutex so `kill` can take `&mut`.
    killer: std::sync::Mutex<Box<dyn portable_pty::ChildKiller + Send + Sync>>,
}

pub async fn run() -> Result<()> {
    let path = socket_path();
    let _ = std::fs::remove_file(&path);
    let listener =
        UnixListener::bind(&path).with_context(|| format!("bind {}", path.display()))?;

    let sysinfo = SystemInfo::collect();
    eprintln!(
        "fern daemon listening on {} (host={} fern v{})",
        path.display(),
        sysinfo.hostname,
        sysinfo.fern_version
    );

    let (events, _) = broadcast::channel(4096);
    let state = Arc::new(DaemonState {
        tree: Mutex::new(Tree::new(State::baseline()?, sysinfo)),
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
                branch,
                parent,
                source,
                who,
                detach,
            } => {
                handle_submit(&state, branch, parent, source, who, detach, &mut wr).await?;
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
            Request::ListBranches => {
                let t = state.tree.lock().await;
                send(
                    &mut wr,
                    &Response::Branches {
                        branches: snapshot_branches(&t),
                    },
                )
                .await?;
            }
            Request::CreateBranch { name, at } => {
                let mut t = state.tree.lock().await;
                if t.branch_exists(&name) {
                    send(
                        &mut wr,
                        &Response::Error {
                            message: format!("branch '{name}' already exists"),
                        },
                    )
                    .await?;
                } else if t.get(at).is_none() {
                    send(
                        &mut wr,
                        &Response::Error {
                            message: format!("no such cell #{at}"),
                        },
                    )
                    .await?;
                } else {
                    t.set_branch(name, at);
                    send(&mut wr, &Response::Ok).await?;
                }
            }
            Request::DeleteBranch { name } => {
                if name == crate::tree::DEFAULT_BRANCH {
                    send(
                        &mut wr,
                        &Response::Error {
                            message: format!("refusing to delete the default branch '{name}'"),
                        },
                    )
                    .await?;
                } else {
                    let mut t = state.tree.lock().await;
                    if t.delete_branch(&name) {
                        send(&mut wr, &Response::Ok).await?;
                    } else {
                        send(
                            &mut wr,
                            &Response::Error {
                                message: format!("no such branch '{name}'"),
                            },
                        )
                        .await?;
                    }
                }
            }
            Request::RenameBranch { from, to } => {
                let mut t = state.tree.lock().await;
                if !t.branch_exists(&from) {
                    send(
                        &mut wr,
                        &Response::Error {
                            message: format!("no such branch '{from}'"),
                        },
                    )
                    .await?;
                } else if t.branch_exists(&to) {
                    send(
                        &mut wr,
                        &Response::Error {
                            message: format!("branch '{to}' already exists"),
                        },
                    )
                    .await?;
                } else {
                    t.rename_branch(&from, &to);
                    send(&mut wr, &Response::Ok).await?;
                }
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
    branch: String,
    parent: Option<CellId>,
    source: String,
    who: String,
    detach: bool,
    wr: &mut tokio::net::unix::OwnedWriteHalf,
) -> Result<()> {
    // Resolve parent + branch, reserve the id, advance-or-fork the branch, and
    // insert the placeholder cell — all under one lock, so the tip atomically
    // points at the new (still-running) cell the instant it exists.
    let (cell_id, parent_cell, parent_state, landed_branch) = {
        let mut t = state.tree.lock().await;
        let tip = t.branch_tip(&branch);
        let parent_cell = match parent.or(tip) {
            Some(p) => p,
            None => {
                send(
                    wr,
                    &Response::Error {
                        message: format!("no such branch '{branch}' and no --parent given"),
                    },
                )
                .await?;
                return Ok(());
            }
        };
        let parent_state = match t.get(parent_cell) {
            Some(c) => match c.result.as_ref() {
                Some(r) => r.end_state.clone(),
                None => {
                    send(
                        wr,
                        &Response::Error {
                            message: format!(
                                "parent #{parent_cell} hasn't finished — can't branch yet"
                            ),
                        },
                    )
                    .await?;
                    return Ok(());
                }
            },
            None => {
                send(
                    wr,
                    &Response::Error {
                        message: format!("no such parent cell #{parent_cell}"),
                    },
                )
                .await?;
                return Ok(());
            }
        };
        let id = t.reserve_id();
        // Fast-forward the branch when we're extending its current tip;
        // otherwise the parent is historical, so fork onto a new branch.
        let landed = if tip == Some(parent_cell) {
            t.set_branch(&branch, id);
            branch.clone()
        } else {
            let name = gen_fork_name(&t);
            t.set_branch(&name, id);
            name
        };
        t.insert_cell(
            parent_cell,
            Cell {
                id,
                parent: Some(parent_cell),
                submitter: who.clone(),
                source: source.clone(),
                hash: None,
                result: None,
            },
        );
        (id, parent_cell, parent_state, landed)
    };

    // For detached cells, set up and register the ActiveCell *before* announcing
    // Started, so any client that races in on the event is guaranteed to find
    // the active entry (and can wait on it for the PTY to register).
    if detach {
        start_detached(state.clone(), cell_id, parent_state.clone(), source.clone()).await;
    }

    let started_event = CellEvent::Started {
        id: cell_id,
        parent: Some(parent_cell),
        source: source.clone(),
        who: who.clone(),
        branch: landed_branch,
    };
    let _ = state.events.send(started_event.clone());
    send(wr, &Response::Event(started_event)).await?;

    if detach {
        return Ok(());
    }

    // Inline path. (Inline cells aren't attachable; a `FERN_IO=tty` command run
    // inline gets captured terminal output but no stdin — use --detach + attach
    // for interactive programs.)
    run_inline(state, cell_id, parent_state, source, wr).await
}

/// Pick a unique `fork-<uuid>` branch name not already in use.
fn gen_fork_name(t: &Tree) -> String {
    loop {
        let name = format!("fork-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        if !t.branch_exists(&name) {
            return name;
        }
    }
}

async fn run_inline(
    state: &Arc<DaemonState>,
    cell_id: CellId,
    parent_state: State,
    source: String,
    wr: &mut tokio::net::unix::OwnedWriteHalf,
) -> Result<()> {
    let (chunk_tx, mut chunk_rx) = mpsc::channel::<CellEvent>(128);
    let src = source.clone();
    let started = std::time::Instant::now();

    let eval_state = parent_state.clone();
    let eval_fut =
        async move { crate::eval::eval_line(&eval_state, &src, cell_id, chunk_tx, None).await };

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

    let hash = {
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
        t.get(cell_id).and_then(|c| c.hash.clone())
    };
    let completed = CellEvent::Completed {
        id: cell_id,
        exit_code,
        duration_ms: duration.as_millis() as u64,
        hash,
    };
    let _ = state.events.send(completed.clone());
    send(wr, &Response::Event(completed)).await?;
    Ok(())
}

/// Launch a detached cell: spawn eval, register the `ActiveCell` (so `attach`
/// can find and wait on it), and run output-draining + finalization in the
/// background. Returns once the `ActiveCell` is registered — the caller can
/// then safely announce `Started`.
async fn start_detached(
    state: Arc<DaemonState>,
    cell_id: CellId,
    parent_state: State,
    source: String,
) {
    let (chunk_tx, mut chunk_rx) = mpsc::channel::<CellEvent>(128);
    let (reg_tx, mut reg_rx) = mpsc::unbounded_channel::<PtyRegistration>();
    let started = std::time::Instant::now();

    let eval_state = parent_state.clone();
    let src = source.clone();
    let eval_handle = tokio::spawn(async move {
        crate::eval::eval_line(&eval_state, &src, cell_id, chunk_tx, Some(reg_tx)).await
    });

    let ac = Arc::new(ActiveCell {
        abort: eval_handle.abort_handle(),
        tty: crate::eval::tty_mode(&parent_state),
        pty: tokio::sync::Mutex::new(None),
        ready: tokio::sync::Notify::new(),
        finished: std::sync::atomic::AtomicBool::new(false),
    });
    state.active.lock().await.insert(cell_id, ac.clone());

    // Fold the PTY controls into the active entry once eval spawns a terminal,
    // waking any attacher waiting on it. The channel closes when eval finishes.
    let reg_ac = ac.clone();
    tokio::spawn(async move {
        while let Some(reg) = reg_rx.recv().await {
            *reg_ac.pty.lock().await = Some(PtyHandle {
                input: reg.input,
                killer: std::sync::Mutex::new(reg.killer),
            });
            reg_ac.ready.notify_waiters();
        }
    });

    // Background: drain output → finalize the cell → broadcast Completed.
    tokio::spawn(async move {
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

        let (new_state, exit_code) = match eval_handle.await {
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
                stderr.push_str("[killed by fern kill]\n");
                (parent_state.clone(), -1)
            }
            Err(e) => {
                eprintln!("detached cell #{cell_id} panicked: {e}");
                (parent_state.clone(), 2)
            }
        };
        let duration = started.elapsed();

        let hash = {
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
            t.get(cell_id).and_then(|c| c.hash.clone())
        };

        // Wake any attacher still waiting for a PTY that will never arrive,
        // then drop the active entry.
        ac.finished.store(true, std::sync::atomic::Ordering::SeqCst);
        ac.ready.notify_waiters();
        state.active.lock().await.remove(&cell_id);

        let _ = state.events.send(CellEvent::Completed {
            id: cell_id,
            exit_code,
            duration_ms: duration.as_millis() as u64,
            hash,
        });
    });
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
            let pty = ac.pty.lock().await;
            if let Some(pty) = pty.as_ref() {
                // PTY cell: kill the child; eval reaps it and records the result.
                if let Ok(mut killer) = pty.killer.lock() {
                    let _ = killer.kill();
                }
            } else {
                // Pipe cell: abort the eval task (kill_on_drop reaps children).
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
    use std::sync::atomic::Ordering;

    let Some(ac) = state.active.lock().await.get(&id).cloned() else {
        send(
            wr,
            &Response::Error {
                message: format!("cell #{id} is not running"),
            },
        )
        .await?;
        return Ok(());
    };
    // Only tty cells ever spawn a PTY; fail fast on anything else so the client
    // falls back to plain streaming instead of waiting.
    if !ac.tty {
        send(
            wr,
            &Response::Error {
                message: format!("cell #{id} is not a terminal cell"),
            },
        )
        .await?;
        return Ok(());
    }

    // A tty cell registers its PTY a moment after Started (once eval spawns the
    // process), so we may briefly arrive first. Wait on `ready` rather than
    // polling: `enable()` arms the notification before we check the slot, so a
    // registration can't slip through the gap; `finished` covers a tty cell that
    // exits without ever spawning a PTY (a pipeline, or a failed spawn).
    let pty_input = {
        let notified = ac.ready.notified();
        tokio::pin!(notified);
        loop {
            notified.as_mut().enable();
            if let Some(pty) = ac.pty.lock().await.as_ref() {
                break Some(pty.input.clone());
            }
            if ac.finished.load(Ordering::SeqCst) {
                break None;
            }
            tokio::select! {
                _ = notified.as_mut() => {}
                // Safety net against a missed wakeup; registration is normally ms.
                _ = tokio::time::sleep(Duration::from_secs(5)) => break None,
            }
            notified.set(ac.ready.notified());
        }
    };
    let Some(pty_input) = pty_input else {
        send(
            wr,
            &Response::Error {
                message: format!("cell #{id} spawned no attachable terminal process"),
            },
        )
        .await?;
        return Ok(());
    };

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
        hash: c.hash.clone(),
    }
}

fn snapshot_branches(t: &Tree) -> Vec<BranchSnapshot> {
    t.branches()
        .map(|(name, tip)| {
            let cell = t.get(tip);
            let running = cell.map(|c| c.result.is_none()).unwrap_or(false);
            let tip_hash = if running {
                None
            } else {
                cell.and_then(|c| c.hash.clone())
            };
            // A branch is in terminal mode when its tip's end-state environment
            // carries FERN_IO=tty (what the next command would inherit).
            let tty = cell
                .and_then(|c| c.result.as_ref())
                .map(|r| r.end_state.env.get("FERN_IO").map(String::as_str) == Some("tty"))
                .unwrap_or(false);
            BranchSnapshot {
                name: name.clone(),
                tip,
                tip_hash,
                running,
                tty,
            }
        })
        .collect()
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
