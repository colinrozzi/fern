//! Mux: a client-local terminal multiplexer over the fern daemon.
//!
//! A **pane is a viewport onto a branch** (a chain of cells), not a single
//! cell. The daemon stays a dumb byte/PTY mux; all the layout, focus, and
//! input-routing live here in the client. A pane's bottom edge follows the tip:
//! a cooked prompt (typing a line submits a cell), a streaming pipe cell, or —
//! after `Ctrl-a e` — a **live terminal program** (vim, top, a shell) rendered
//! into a `vt100` grid and driven raw over its own attach connection, resized
//! to the pane via the daemon's `Resize`.
//!
//! Splitting a pane **forks the branch** (`Split = CreateBranch` at the focused
//! pane's tip), so two side-by-side panes are two independent lineages in the
//! one shared tree — the thing a flat tmux pane can't be.
//!
//! Reuses `client`'s transport (`submit_detached`, `open_subscription`,
//! `open_attach`, `fetch_tree`/`fetch_branches`, `send_expect_ok`); adds only
//! the UI: a layout tree, a select loop, and a crossterm renderer.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Write, stdout};

use anyhow::{Result, anyhow};
use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::style::{
    Attribute, Color, Print, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
};
use crossterm::terminal::{
    Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
    enable_raw_mode, size,
};
use crossterm::{execute, queue};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

use crate::client;
use crate::tree::CellId;
use crate::wire::{CellEvent, CellSnapshot, Request, Response, TreeSnapshot};

// ---------- model ------------------------------------------------------

type PaneId = u32;

/// A pane follows one branch. The tip's kind decides the bottom edge.
struct Pane {
    branch: String,
    scrollback: Vec<CellView>,
    active: Active,
}

impl Pane {
    fn new(branch: String) -> Self {
        Pane {
            branch,
            scrollback: Vec::new(),
            active: Active::Cooked {
                input: String::new(),
            },
        }
    }
}

/// The live bottom edge of a pane.
enum Active {
    /// Tip finished → a cooked prompt; Enter submits a new cell on the branch.
    Cooked { input: String },
    /// A cell is streaming on this branch — ours or another client's. `id` is
    /// `None` for the brief window between our own Enter and its `Started`
    /// confirmation; `resume` is the prompt text to restore once it finishes
    /// (empty for our own submits, the half-typed line for an external cell).
    Running {
        id: Option<CellId>,
        source: String,
        who: String,
        body: String,
        resume: String,
    },
    /// A live terminal program (vim, top, a shell) driven raw. Output bytes feed
    /// a `vt100` grid; keystrokes go to the cell's PTY over a dedicated attach.
    /// `size` is the last `(rows, cols)` we sized the grid+PTY to, so a reflow is
    /// detected and pushed. `resume` restores the prompt text on exit.
    LiveTty {
        id: CellId,
        parser: Box<vt100::Parser>,
        input: mpsc::UnboundedSender<Vec<u8>>,
        resume: String,
        size: (u16, u16),
    },
}

/// A finished cell as the pane renders it (structured scrollback, not flat bytes).
struct CellView {
    source: String,
    who: String,
    body: String,
    exit: Option<i32>,
    duration_ms: u64,
}

impl CellView {
    fn from_snap(s: &CellSnapshot) -> CellView {
        CellView {
            source: s.source.clone(),
            who: s.submitter.clone(),
            body: format!("{}{}", s.stdout, s.stderr),
            exit: s.exit_code,
            duration_ms: s.duration_ms,
        }
    }
}

/// Client-local layout: a binary split tree, exactly like tmux.
enum Layout {
    Leaf(PaneId),
    Split {
        dir: Dir,
        ratio: f32,
        a: Box<Layout>,
        b: Box<Layout>,
    },
}

#[derive(Clone, Copy)]
enum Dir {
    /// Panes side by side (split the width).
    Horizontal,
    /// Panes stacked (split the height).
    Vertical,
}

enum Mode {
    /// Keys go to the focused pane.
    Pane,
    /// The leader key was pressed; the next key is a mux command.
    Prefix,
}

struct Mux {
    panes: BTreeMap<PaneId, Pane>,
    order: Vec<PaneId>,
    layout: Layout,
    focus: PaneId,
    mode: Mode,
    next_id: PaneId,
    size: (u16, u16),
    status: String,
    /// Cell id → the pane rendering it, learned at `Started` (which carries the
    /// branch) so later `OutputChunk`/`Completed` (which carry only the id) route.
    routes: HashMap<CellId, PaneId>,
    /// Cells driven by a live-tty pane over their own attach connection. Their
    /// events also arrive on the shared subscription — ignore those duplicates.
    tty_cells: HashSet<CellId>,
}

/// Messages into the select loop: the broadcast feed, local submit errors, and
/// the dedicated per-cell attach that backs a live-tty pane.
enum MuxMsg {
    /// A cell event from the persistent subscription (any client, any branch).
    Event(CellEvent),
    /// A local submit never reached the daemon.
    SubmitFailed { pane: PaneId, err: String },
    /// Raw PTY output for a live-tty pane (from its attach connection).
    TtyOutput { pane: PaneId, data: Vec<u8> },
    /// A live-tty cell ended (or its attach dropped) — revert the pane to a prompt.
    TtyClosed { pane: PaneId },
}

const LEADER: char = 'a'; // Ctrl-a, like screen.

// ---------- entry point ------------------------------------------------

pub async fn run() -> Result<()> {
    let branch = client::read_current_branch();
    let (cols, rows) = size().unwrap_or((80, 24));

    let mut mux = Mux {
        panes: BTreeMap::from([(0, Pane::new(branch))]),
        order: vec![0],
        layout: Layout::Leaf(0),
        focus: 0,
        mode: Mode::Pane,
        next_id: 1,
        size: (cols, rows),
        status: format!("C-{LEADER} then: e tty · %/\" split · o focus · q quit"),
        routes: HashMap::new(),
        tty_cells: HashSet::new(),
    };

    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<MuxMsg>();
    let (key_tx, mut key_rx) = mpsc::unbounded_channel::<Event>();

    // crossterm reads block; mirror client's stdin pump and run it on a thread.
    std::thread::spawn(move || {
        while let Ok(ev) = crossterm::event::read() {
            if key_tx.send(ev).is_err() {
                break;
            }
        }
    });

    // Passive follow: one persistent subscription feeds every cell event into
    // the loop, so a cell landing on any pane's branch — from this mux or
    // another client — renders live.
    tokio::spawn(subscribe_feed(msg_tx.clone()));

    // Open with the current branch's history rather than a blank pane.
    if let Err(e) = backfill(&mut mux, 0).await {
        mux.status = format!("backfill failed: {e}");
    }

    enable_raw_mode()?;
    let _guard = TermGuard;
    execute!(stdout(), EnterAlternateScreen, Hide)?;

    loop {
        reconcile_tty_sizes(&mut mux);
        render(&mux)?;
        tokio::select! {
            ev = key_rx.recv() => {
                let Some(ev) = ev else { break };
                if handle_event(&mut mux, ev, &msg_tx).await? {
                    break; // quit
                }
            }
            msg = msg_rx.recv() => {
                if let Some(msg) = msg {
                    apply_msg(&mut mux, msg);
                }
            }
        }
    }
    Ok(())
}

/// Restore the terminal no matter how we leave.
struct TermGuard;
impl Drop for TermGuard {
    fn drop(&mut self) {
        let _ = execute!(stdout(), Show, LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

// ---------- input ------------------------------------------------------

/// Returns Ok(true) to quit.
async fn handle_event(
    mux: &mut Mux,
    ev: Event,
    msg_tx: &mpsc::UnboundedSender<MuxMsg>,
) -> Result<bool> {
    match ev {
        Event::Resize(w, h) => mux.size = (w, h),
        Event::Key(k) if k.kind != KeyEventKind::Release => {
            let ctrl = k.modifiers.contains(KeyModifiers::CONTROL);
            match mux.mode {
                Mode::Prefix => {
                    mux.mode = Mode::Pane;
                    return handle_command(mux, k.code, msg_tx).await;
                }
                Mode::Pane if ctrl && k.code == KeyCode::Char(LEADER) => {
                    mux.mode = Mode::Prefix;
                    mux.status = "PREFIX — %/\" split · o focus · q quit".into();
                }
                Mode::Pane => pane_key(mux, k.code, ctrl, msg_tx),
            }
        }
        _ => {}
    }
    Ok(false)
}

/// A key destined for the focused pane: cooked-prompt editing, or — for a
/// live-tty pane — encoded raw and sent to the program's PTY.
fn pane_key(mux: &mut Mux, code: KeyCode, ctrl: bool, msg_tx: &mpsc::UnboundedSender<MuxMsg>) {
    let focus = mux.focus;
    let pane = mux.panes.get_mut(&focus).expect("focus is a live pane");
    let input = match &mut pane.active {
        Active::Cooked { input } => input,
        Active::LiveTty { input, .. } => {
            if let Some(bytes) = encode_key(code, ctrl) {
                let _ = input.send(bytes);
            }
            return;
        }
        _ => return, // a running pipe cell has no cooked prompt to edit
    };
    match code {
        KeyCode::Char(c) if !ctrl => input.push(c),
        KeyCode::Backspace => {
            input.pop();
        }
        KeyCode::Enter => {
            let source = input.trim().to_string();
            input.clear();
            if source.is_empty() {
                return;
            }
            // Lock the prompt now (id unknown until the broadcast `Started`
            // confirms it); all output renders via the subscription, so our own
            // cell and an external one travel the exact same path.
            let branch = pane.branch.clone();
            pane.active = Active::Running {
                id: None,
                source: source.clone(),
                who: whoami(),
                body: String::new(),
                resume: String::new(),
            };
            spawn_submit(focus, branch, source, msg_tx.clone());
        }
        _ => {}
    }
}

fn whoami() -> String {
    std::env::var("USER").unwrap_or_else(|_| "?".into())
}

/// A mux command (the key after the leader).
async fn handle_command(
    mux: &mut Mux,
    code: KeyCode,
    msg_tx: &mpsc::UnboundedSender<MuxMsg>,
) -> Result<bool> {
    match code {
        KeyCode::Char('q') | KeyCode::Char('d') => return Ok(true),
        KeyCode::Char('o') | KeyCode::Tab | KeyCode::Right | KeyCode::Down => mux.cycle_focus(),
        KeyCode::Char('%') => split(mux, Dir::Horizontal).await?,
        KeyCode::Char('"') => split(mux, Dir::Vertical).await?,
        KeyCode::Char('e') => {
            if let Err(e) = enter_tty(mux, msg_tx).await {
                mux.status = format!("tty launch failed: {e}");
                return Ok(false);
            }
        }
        _ => {}
    }
    mux.status = format!("focus: {}", mux.panes[&mux.focus].branch);
    Ok(false)
}

/// Launch a terminal program in the focused pane and drive it raw. The command
/// is the pane's typed prompt (or `$SHELL`). The pane's branch is flipped to
/// tty mode first if needed, so the cell spawns a real PTY; then we attach for
/// bidirectional raw I/O and render its output into a `vt100` grid.
async fn enter_tty(mux: &mut Mux, msg_tx: &mpsc::UnboundedSender<MuxMsg>) -> Result<()> {
    let focus = mux.focus;
    let branch = mux.panes[&focus].branch.clone();
    let cmd = match &mux.panes[&focus].active {
        Active::Cooked { input } if !input.trim().is_empty() => input.trim().to_string(),
        _ => std::env::var("SHELL").unwrap_or_else(|_| "bash".into()),
    };

    // The cell only gets a PTY when its inherited env carries FERN_IO=tty. Flip
    // the branch first (a quick builtin cell) if it isn't already in tty mode.
    let is_tty = client::fetch_branches()
        .await?
        .iter()
        .find(|b| b.name == branch)
        .map(|b| b.tty)
        .unwrap_or(false);
    if !is_tty {
        client::submit_streaming(
            Some(branch.clone()),
            None,
            None,
            "export FERN_IO=tty".into(),
            |_, _| {},
        )
        .await?;
    }

    let id = client::submit_detached(Some(branch), None, None, cmd).await?;
    mux.tty_cells.insert(id);

    let (input_tx, input_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    spawn_attach(id, focus, input_rx, msg_tx.clone());

    let (rows, cols) = mux.pane_inner(focus).unwrap_or((24, 80));
    mux.panes
        .get_mut(&focus)
        .expect("focused pane exists")
        .active = Active::LiveTty {
        id,
        parser: Box::new(vt100::Parser::new(rows, cols, 0)),
        input: input_tx,
        resume: String::new(),
        size: (rows, cols),
    };
    spawn_resize(id, rows, cols);
    Ok(())
}

/// A live-tty pane's dedicated connection: attach output → grid, keystrokes →
/// PTY stdin. Ends (and tells the loop) when the cell completes or drops.
fn spawn_attach(
    id: CellId,
    pane: PaneId,
    mut input_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    tx: mpsc::UnboundedSender<MuxMsg>,
) {
    tokio::spawn(async move {
        let Ok(stream) = client::open_attach(id).await else {
            let _ = tx.send(MuxMsg::TtyClosed { pane });
            return;
        };
        let (rd, mut wr) = stream.into_split();
        let mut lines = BufReader::new(rd).lines();

        // The handshake line: Ok means we're driving a PTY; anything else fails.
        let ok = matches!(
            lines.next_line().await.ok().flatten().as_deref(),
            Some(l) if matches!(serde_json::from_str::<Response>(l), Ok(Response::Ok))
        );
        if !ok {
            let _ = tx.send(MuxMsg::TtyClosed { pane });
            return;
        }

        loop {
            tokio::select! {
                line = lines.next_line() => {
                    let Ok(Some(l)) = line else {
                        let _ = tx.send(MuxMsg::TtyClosed { pane });
                        break;
                    };
                    match serde_json::from_str::<Response>(&l) {
                        Ok(Response::Event(CellEvent::OutputChunk { data, .. })) => {
                            let _ = tx.send(MuxMsg::TtyOutput { pane, data: data.into_bytes() });
                        }
                        Ok(Response::Event(CellEvent::Completed { .. })) => {
                            let _ = tx.send(MuxMsg::TtyClosed { pane });
                            break;
                        }
                        _ => {}
                    }
                }
                bytes = input_rx.recv() => match bytes {
                    Some(b) => {
                        let req = Request::Input { id, data: String::from_utf8_lossy(&b).into_owned() };
                        let Ok(mut s) = serde_json::to_string(&req) else { continue };
                        s.push('\n');
                        if wr.write_all(s.as_bytes()).await.is_err() {
                            break;
                        }
                        let _ = wr.flush().await;
                    }
                    None => break, // the pane dropped its input sender
                },
            }
        }
    });
}

/// Push a resize to the daemon in the background (fire-and-forget).
fn spawn_resize(id: CellId, rows: u16, cols: u16) {
    tokio::spawn(async move {
        let _ = client::send_expect_ok(&Request::Resize { id, rows, cols }).await;
    });
}

/// Encode a keypress into the bytes a terminal program expects on its stdin.
fn encode_key(code: KeyCode, ctrl: bool) -> Option<Vec<u8>> {
    let bytes = match code {
        KeyCode::Char(c) if ctrl && c.is_ascii_alphabetic() => {
            vec![(c.to_ascii_lowercase() as u8) & 0x1f]
        }
        KeyCode::Char(c) => c.to_string().into_bytes(),
        KeyCode::Enter => vec![b'\r'],
        KeyCode::Backspace => vec![0x7f],
        KeyCode::Tab => vec![b'\t'],
        KeyCode::Esc => vec![0x1b],
        KeyCode::Left => b"\x1b[D".to_vec(),
        KeyCode::Right => b"\x1b[C".to_vec(),
        KeyCode::Up => b"\x1b[A".to_vec(),
        KeyCode::Down => b"\x1b[B".to_vec(),
        KeyCode::Home => b"\x1b[H".to_vec(),
        KeyCode::End => b"\x1b[F".to_vec(),
        KeyCode::PageUp => b"\x1b[5~".to_vec(),
        KeyCode::PageDown => b"\x1b[6~".to_vec(),
        KeyCode::Delete => b"\x1b[3~".to_vec(),
        KeyCode::Insert => b"\x1b[2~".to_vec(),
        _ => return None,
    };
    Some(bytes)
}

/// Fire a detached submit on `branch`; its output streams back via the shared
/// subscription, not this connection. Only a submit *failure* needs reporting.
fn spawn_submit(pane: PaneId, branch: String, source: String, tx: mpsc::UnboundedSender<MuxMsg>) {
    tokio::spawn(async move {
        if let Err(e) = client::submit_detached(Some(branch), None, None, source).await {
            let _ = tx.send(MuxMsg::SubmitFailed {
                pane,
                err: e.to_string(),
            });
        }
    });
}

/// The persistent subscription: forward every cell event into the loop.
async fn subscribe_feed(tx: mpsc::UnboundedSender<MuxMsg>) -> Result<()> {
    let stream = client::open_subscription().await?;
    let (rd, _wr) = stream.into_split();
    let mut lines = BufReader::new(rd).lines();
    while let Some(line) = lines.next_line().await? {
        if let Ok(Response::Event(ev)) = serde_json::from_str::<Response>(&line)
            && tx.send(MuxMsg::Event(ev)).is_err()
        {
            break; // mux exited
        }
    }
    Ok(())
}

/// Hydrate a pane from the daemon's current tree so it opens with its branch's
/// history instead of blank. Walks the tip's lineage root→tip into scrollback;
/// if the tip is still running, adopts it as the live region (and routes it) so
/// the subscription keeps streaming it to completion.
async fn backfill(mux: &mut Mux, pid: PaneId) -> Result<()> {
    let branch = mux.panes[&pid].branch.clone();
    let Some(tip) = client::fetch_branches()
        .await?
        .iter()
        .find(|b| b.name == branch)
        .map(|b| b.tip)
    else {
        return Ok(()); // branch not on the daemon (e.g. a stale cursor) — stay blank
    };
    let tree = client::fetch_tree().await?;
    let chain = lineage(&tree, tip);

    let p = mux.panes.get_mut(&pid).expect("pane exists");
    for c in chain {
        if c.parent.is_none() {
            continue; // skip the synthetic [system] root
        }
        if c.id == tip && c.exit_code.is_none() {
            p.active = Active::Running {
                id: Some(c.id),
                source: c.source.clone(),
                who: c.submitter.clone(),
                body: format!("{}{}", c.stdout, c.stderr),
                resume: String::new(),
            };
            mux.routes.insert(c.id, pid);
        } else {
            p.scrollback.push(CellView::from_snap(c));
        }
    }
    Ok(())
}

/// The chain of cells from the root down to `tip`, in render order (root→tip).
fn lineage(tree: &TreeSnapshot, tip: CellId) -> Vec<&CellSnapshot> {
    let by_id: HashMap<CellId, &CellSnapshot> = tree.cells.iter().map(|c| (c.id, c)).collect();
    let mut chain = Vec::new();
    let mut cur = Some(tip);
    while let Some(id) = cur {
        let Some(c) = by_id.get(&id) else { break };
        chain.push(*c);
        cur = c.parent;
    }
    chain.reverse();
    chain
}

fn apply_msg(mux: &mut Mux, msg: MuxMsg) {
    match msg {
        MuxMsg::Event(ev) => apply_event(mux, ev),
        MuxMsg::SubmitFailed { pane, err } => {
            if let Some(p) = mux.panes.get_mut(&pane) {
                p.scrollback.push(CellView {
                    source: "(submit failed)".into(),
                    who: "fern".into(),
                    body: err,
                    exit: Some(-1),
                    duration_ms: 0,
                });
                p.active = Active::Cooked {
                    input: String::new(),
                };
            }
        }
        MuxMsg::TtyOutput { pane, data } => {
            if let Some(p) = mux.panes.get_mut(&pane)
                && let Active::LiveTty { parser, .. } = &mut p.active
            {
                parser.process(&data);
            }
        }
        MuxMsg::TtyClosed { pane } => {
            let ended = mux.panes.get_mut(&pane).and_then(|p| match &mut p.active {
                Active::LiveTty { id, resume, .. } => Some((*id, std::mem::take(resume))),
                _ => None,
            });
            if let Some((id, resume)) = ended {
                mux.tty_cells.remove(&id);
                if let Some(p) = mux.panes.get_mut(&pane) {
                    p.active = Active::Cooked { input: resume };
                }
            }
        }
    }
}

/// Route a broadcast cell event to the pane following its branch. `Started`
/// carries the branch (so we learn id→pane); the rest carry only the id.
fn apply_event(mux: &mut Mux, ev: CellEvent) {
    // Cells owned by a live-tty pane are driven over their own attach — their
    // duplicate broadcast events must not touch pane state here.
    if mux.tty_cells.contains(&event_cell_id(&ev)) {
        return;
    }
    match ev {
        CellEvent::Started {
            id,
            source,
            who,
            branch,
            ..
        } => {
            let Some(pid) = mux.pane_on_branch(&branch) else {
                return; // a cell on a branch no pane is showing
            };
            // Don't disturb a pane that's already driving a live-tty program
            // (e.g. the FERN_IO=tty export cell landing on it).
            if matches!(mux.panes[&pid].active, Active::LiveTty { .. }) {
                return;
            }
            mux.routes.insert(id, pid);
            let p = mux.panes.get_mut(&pid).expect("routed pane exists");
            if matches!(&p.active, Active::Running { id: None, .. }) {
                // our own optimistic submit — just stamp the confirmed id
                if let Active::Running { id: slot, .. } = &mut p.active {
                    *slot = Some(id);
                }
            } else {
                // external cell (or pane idle): flip in, preserving any typed line
                let resume = match &mut p.active {
                    Active::Cooked { input } => std::mem::take(input),
                    _ => String::new(),
                };
                p.active = Active::Running {
                    id: Some(id),
                    source,
                    who,
                    body: String::new(),
                    resume,
                };
            }
        }
        CellEvent::OutputChunk { id, data, .. } => {
            if let Some(&pid) = mux.routes.get(&id)
                && let Some(p) = mux.panes.get_mut(&pid)
                && let Active::Running { body, .. } = &mut p.active
            {
                body.push_str(&data);
            }
        }
        CellEvent::Completed {
            id,
            exit_code,
            duration_ms,
            ..
        } => {
            let Some(pid) = mux.routes.remove(&id) else {
                return;
            };
            let Some(p) = mux.panes.get_mut(&pid) else {
                return;
            };
            if let Active::Running {
                source,
                who,
                body,
                resume,
                ..
            } = &mut p.active
            {
                let view = CellView {
                    source: std::mem::take(source),
                    who: std::mem::take(who),
                    body: std::mem::take(body),
                    exit: Some(exit_code),
                    duration_ms,
                };
                let resume = std::mem::take(resume);
                p.scrollback.push(view);
                p.active = Active::Cooked { input: resume };
            }
        }
    }
}

fn event_cell_id(ev: &CellEvent) -> CellId {
    match ev {
        CellEvent::Started { id, .. }
        | CellEvent::OutputChunk { id, .. }
        | CellEvent::Completed { id, .. } => *id,
    }
}

impl Mux {
    fn cycle_focus(&mut self) {
        if let Some(i) = self.order.iter().position(|&p| p == self.focus) {
            self.focus = self.order[(i + 1) % self.order.len()];
        }
    }

    /// The pane currently showing `branch`, if any. Panes hold distinct
    /// branches (a split forks a new one), so this is unambiguous.
    fn pane_on_branch(&self, branch: &str) -> Option<PaneId> {
        self.order
            .iter()
            .copied()
            .find(|id| self.panes[id].branch == branch)
    }

    /// The current on-screen rectangle for a pane (before the border inset).
    fn pane_rect(&self, pid: PaneId) -> Option<Rect> {
        let (cols, rows) = self.size;
        if cols < 4 || rows < 4 {
            return None;
        }
        let area = Rect {
            x: 0,
            y: 0,
            w: cols,
            h: rows.saturating_sub(1),
        };
        let mut rects = Vec::new();
        layout_rects(&self.layout, area, &mut rects);
        rects.into_iter().find(|(p, _)| *p == pid).map(|(_, r)| r)
    }

    /// A pane's inner `(rows, cols)` — the content area inside its border.
    fn pane_inner(&self, pid: PaneId) -> Option<(u16, u16)> {
        self.pane_rect(pid)
            .map(|r| (r.h.saturating_sub(2), r.w.saturating_sub(2)))
    }
}

/// Keep each live-tty pane's grid and PTY sized to its current rectangle, so a
/// split, focus change, or window resize reflows the running program.
fn reconcile_tty_sizes(mux: &mut Mux) {
    let (cols, rows) = mux.size;
    if cols < 4 || rows < 4 {
        return;
    }
    let area = Rect {
        x: 0,
        y: 0,
        w: cols,
        h: rows.saturating_sub(1),
    };
    let mut rects = Vec::new();
    layout_rects(&mux.layout, area, &mut rects);
    for (pid, r) in rects {
        let inner = (r.h.saturating_sub(2), r.w.saturating_sub(2));
        if inner.0 == 0 || inner.1 == 0 {
            continue;
        }
        if let Some(p) = mux.panes.get_mut(&pid)
            && let Active::LiveTty {
                id, parser, size, ..
            } = &mut p.active
            && *size != inner
        {
            parser.screen_mut().set_size(inner.0, inner.1);
            *size = inner;
            spawn_resize(*id, inner.0, inner.1);
        }
    }
}

/// Split the focused pane: fork a fresh branch at its tip and open a new pane
/// on it. This is the showcase — a pane split that's also a git-style branch.
async fn split(mux: &mut Mux, dir: Dir) -> Result<()> {
    let parent = mux.focus;
    let parent_branch = mux.panes[&parent].branch.clone();

    let branches = client::fetch_branches().await?;
    let tip = branches
        .iter()
        .find(|b| b.name == parent_branch)
        .map(|b| b.tip)
        .ok_or_else(|| anyhow!("branch '{parent_branch}' vanished"))?;

    let id = mux.next_id;
    mux.next_id += 1;
    let name = format!("mux-{id}");
    create_branch(&name, tip).await?;

    mux.panes.insert(id, Pane::new(name));
    mux.order.push(id);
    let mut repl = Some(Layout::Split {
        dir,
        ratio: 0.5,
        a: Box::new(Layout::Leaf(parent)),
        b: Box::new(Layout::Leaf(id)),
    });
    replace_leaf(&mut mux.layout, parent, &mut repl);
    mux.focus = id;

    // The fork shares the parent's lineage up to the split point — show it.
    backfill(mux, id).await?;
    Ok(())
}

async fn create_branch(name: &str, at: CellId) -> Result<()> {
    client::send_expect_ok(&Request::CreateBranch {
        name: name.to_string(),
        at,
    })
    .await
}

/// Swap the `Leaf(target)` node for `repl` in place. The target leaf is unique,
/// so `repl` is taken exactly once.
fn replace_leaf(layout: &mut Layout, target: PaneId, repl: &mut Option<Layout>) -> bool {
    match layout {
        Layout::Leaf(p) if *p == target => {
            if let Some(r) = repl.take() {
                *layout = r;
            }
            true
        }
        Layout::Leaf(_) => false,
        Layout::Split { a, b, .. } => {
            replace_leaf(a, target, repl) || replace_leaf(b, target, repl)
        }
    }
}

// ---------- render -----------------------------------------------------

#[derive(Clone, Copy)]
struct Rect {
    x: u16,
    y: u16,
    w: u16,
    h: u16,
}

fn layout_rects(layout: &Layout, area: Rect, out: &mut Vec<(PaneId, Rect)>) {
    match layout {
        Layout::Leaf(p) => out.push((*p, area)),
        Layout::Split { dir, ratio, a, b } => match dir {
            Dir::Horizontal => {
                let aw = ((area.w as f32) * ratio) as u16;
                layout_rects(a, Rect { w: aw, ..area }, out);
                layout_rects(
                    b,
                    Rect {
                        x: area.x + aw,
                        w: area.w.saturating_sub(aw),
                        ..area
                    },
                    out,
                );
            }
            Dir::Vertical => {
                let ah = ((area.h as f32) * ratio) as u16;
                layout_rects(a, Rect { h: ah, ..area }, out);
                layout_rects(
                    b,
                    Rect {
                        y: area.y + ah,
                        h: area.h.saturating_sub(ah),
                        ..area
                    },
                    out,
                );
            }
        },
    }
}

fn render(mux: &Mux) -> std::io::Result<()> {
    let mut out = stdout();
    let (cols, rows) = mux.size;
    if cols < 4 || rows < 4 {
        return Ok(()); // too small to draw anything sensible
    }
    queue!(out, Clear(ClearType::All))?;

    let area = Rect {
        x: 0,
        y: 0,
        w: cols,
        h: rows.saturating_sub(1), // reserve the last row for the status bar
    };
    let mut rects = Vec::new();
    layout_rects(&mux.layout, area, &mut rects);
    for (pid, r) in rects {
        draw_pane(&mut out, &mux.panes[&pid], r, pid == mux.focus)?;
    }

    // status bar
    let mode = match mux.mode {
        Mode::Pane => "PANE",
        Mode::Prefix => "PREFIX",
    };
    let mut bar = format!(" [{mode}] {}", mux.status);
    bar.truncate(cols as usize);
    let pad = (cols as usize).saturating_sub(bar.chars().count());
    bar.push_str(&" ".repeat(pad));
    queue!(
        out,
        MoveTo(0, rows - 1),
        SetAttribute(Attribute::Reverse),
        Print(bar),
        SetAttribute(Attribute::Reset)
    )?;

    out.flush()
}

fn draw_pane(out: &mut impl Write, pane: &Pane, r: Rect, focused: bool) -> std::io::Result<()> {
    if r.w < 2 || r.h < 2 {
        return Ok(());
    }
    let (tl, tr, bl, br, hz, vt) = if focused {
        ('┏', '┓', '┗', '┛', '━', '┃')
    } else {
        ('┌', '┐', '└', '┘', '─', '│')
    };
    let inner = (r.w - 2) as usize;
    let content_h = (r.h - 2) as usize;

    // top border carries the branch name as a title
    let title: String = format!(" {} ", pane.branch).chars().take(inner).collect();
    let mut top = String::from(tl);
    top.push_str(&title);
    top.extend(std::iter::repeat_n(
        hz,
        inner.saturating_sub(title.chars().count()),
    ));
    top.push(tr);
    queue!(out, MoveTo(r.x, r.y), Print(top))?;

    // side borders + blank interior; content is overlaid below
    for i in 0..content_h {
        queue!(
            out,
            MoveTo(r.x, r.y + 1 + i as u16),
            Print(vt),
            Print(" ".repeat(inner)),
            Print(vt)
        )?;
    }

    match &pane.active {
        Active::LiveTty { parser, .. } => draw_grid(out, parser.screen(), r)?,
        _ => draw_lines(out, &pane_lines(pane), r)?,
    }

    let mut bot = String::from(bl);
    bot.extend(std::iter::repeat_n(hz, inner));
    bot.push(br);
    queue!(out, MoveTo(r.x, r.y + r.h - 1), Print(bot))?;
    Ok(())
}

/// Fill a pane's interior with the tail of its text lines.
fn draw_lines(out: &mut impl Write, lines: &[String], r: Rect) -> std::io::Result<()> {
    let inner = (r.w - 2) as usize;
    let content_h = (r.h - 2) as usize;
    let start = lines.len().saturating_sub(content_h);
    for i in 0..content_h {
        let raw = lines.get(start + i).map(String::as_str).unwrap_or("");
        let text: String = raw.chars().take(inner).collect();
        queue!(out, MoveTo(r.x + 1, r.y + 1 + i as u16), Print(text))?;
    }
    Ok(())
}

/// Render a `vt100` screen grid into a pane's interior, cell by cell with colors
/// and attributes. The cursor is shown as an inverse cell (cheap, no real cursor
/// juggling across panes).
fn draw_grid(out: &mut impl Write, screen: &vt100::Screen, r: Rect) -> std::io::Result<()> {
    let (grid_rows, grid_cols) = screen.size();
    let inner_w = (r.w - 2).min(grid_cols);
    let inner_h = (r.h - 2).min(grid_rows);
    let (cur_row, cur_col) = screen.cursor_position();
    let show_cursor = !screen.hide_cursor();

    for row in 0..inner_h {
        queue!(out, MoveTo(r.x + 1, r.y + 1 + row))?;
        for col in 0..inner_w {
            let cell = screen.cell(row, col);
            let at_cursor = show_cursor && row == cur_row && col == cur_col;
            let (fg, bg, bold, inverse) = match cell {
                Some(c) => (c.fgcolor(), c.bgcolor(), c.bold(), c.inverse() ^ at_cursor),
                None => (
                    vt100::Color::Default,
                    vt100::Color::Default,
                    false,
                    at_cursor,
                ),
            };
            queue!(out, SetForegroundColor(conv_color(fg, Color::Reset)))?;
            queue!(out, SetBackgroundColor(conv_color(bg, Color::Reset)))?;
            if bold {
                queue!(out, SetAttribute(Attribute::Bold))?;
            }
            if inverse {
                queue!(out, SetAttribute(Attribute::Reverse))?;
            }
            let glyph = cell.map(|c| c.contents()).filter(|s| !s.is_empty());
            queue!(out, Print(glyph.unwrap_or(" ")))?;
            queue!(out, SetAttribute(Attribute::Reset), ResetColor)?;
        }
    }
    Ok(())
}

fn conv_color(c: vt100::Color, default: Color) -> Color {
    match c {
        vt100::Color::Default => default,
        vt100::Color::Idx(i) => Color::AnsiValue(i),
        vt100::Color::Rgb(r, g, b) => Color::Rgb { r, g, b },
    }
}

/// Flatten a pane into display lines: structured scrollback blocks, then the
/// live region (streaming output or the cooked prompt).
fn pane_lines(pane: &Pane) -> Vec<String> {
    let mut v = Vec::new();
    for c in &pane.scrollback {
        v.push(format!("❯ {}", c.source));
        v.extend(body_lines(&c.body));
        if let Some(code) = c.exit {
            v.push(format!("  └ {} · exit {code} · {}ms", c.who, c.duration_ms));
        }
        v.push(String::new());
    }
    match &pane.active {
        Active::Running {
            source, who, body, ..
        } => {
            v.push(format!("❯ {source}"));
            v.extend(body_lines(body));
            v.push(format!("  … {who} running"));
        }
        Active::Cooked { input } => v.push(format!("❯ {input}█")),
        // A live-tty pane renders its grid instead of these lines.
        Active::LiveTty { .. } => {}
    }
    v
}

/// Split output into renderable lines. Pipe-mode programs don't address the
/// cursor, so a minimal discipline suffices: `\r` overwrites (keep the last
/// segment), and CSI/control bytes are stripped so they can't corrupt the grid.
/// (A real VT grid arrives with v2's live-tty panes.)
fn body_lines(body: &str) -> Vec<String> {
    body.split('\n')
        .map(|line| {
            let last = line.rsplit('\r').next().unwrap_or(line);
            sanitize(last)
        })
        .collect()
}

fn sanitize(s: &str) -> String {
    let mut out = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\x1b' => {
                // drop a CSI sequence: ESC [ ... <final byte @-~>
                if chars.peek() == Some(&'[') {
                    chars.next();
                    while let Some(&n) = chars.peek() {
                        chars.next();
                        if ('@'..='~').contains(&n) {
                            break;
                        }
                    }
                }
            }
            '\t' => out.push_str("    "),
            c if (c as u32) < 0x20 => {} // other control bytes: drop
            c => out.push(c),
        }
    }
    out
}
