//! Mux: a client-local terminal multiplexer over the fern daemon.
//!
//! A **pane is a viewport onto a branch** (a chain of cells), not a single
//! cell. The daemon stays a dumb byte/PTY mux; all the layout, focus, and
//! input-routing live here in the client. This v1 covers **pipe-mode panes**:
//! each pane shows its branch's finished cells as scrollback plus a cooked
//! prompt; typing a line submits a new cell on that branch. A live-tty tip
//! (vim in a pane) needs the protocol's `Resize` and a real VT grid — that's
//! v2; the `Active` enum has room for it.
//!
//! Splitting a pane **forks the branch** (`Split = CreateBranch` at the focused
//! pane's tip), so two side-by-side panes are two independent lineages in the
//! one shared tree — the thing a flat tmux pane can't be.
//!
//! Reuses `client`'s transport (`submit_streaming`, `fetch_branches`,
//! `send_expect_ok`); adds only the UI: a layout tree, a select loop, and a
//! crossterm renderer.

use std::collections::{BTreeMap, HashMap};
use std::io::{Write, stdout};

use anyhow::{Result, anyhow};
use crossterm::cursor::{Hide, MoveTo, Show};
use crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::style::{Attribute, Print, SetAttribute};
use crossterm::terminal::{
    Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode,
    enable_raw_mode, size,
};
use crossterm::{execute, queue};
use tokio::io::{AsyncBufReadExt, BufReader};
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
    // v2: LiveTty { id: CellId, grid: TermGrid } — needs Resize + a VT emulator.
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
}

/// Messages into the select loop: the broadcast feed, plus local submit errors.
enum MuxMsg {
    /// A cell event from the persistent subscription (any client, any branch).
    Event(CellEvent),
    /// A local submit never reached the daemon.
    SubmitFailed { pane: PaneId, err: String },
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
        status: format!("C-{LEADER} then: %/\" split · o focus · q quit"),
        routes: HashMap::new(),
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
                    return handle_command(mux, k.code).await;
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

/// A key destined for the focused pane (cooked-prompt editing for v1).
fn pane_key(mux: &mut Mux, code: KeyCode, ctrl: bool, msg_tx: &mpsc::UnboundedSender<MuxMsg>) {
    let focus = mux.focus;
    let pane = mux.panes.get_mut(&focus).expect("focus is a live pane");
    let Active::Cooked { input } = &mut pane.active else {
        return; // a running cell has no cooked prompt to edit
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
async fn handle_command(mux: &mut Mux, code: KeyCode) -> Result<bool> {
    match code {
        KeyCode::Char('q') | KeyCode::Char('d') => return Ok(true),
        KeyCode::Char('o') | KeyCode::Tab | KeyCode::Right | KeyCode::Down => mux.cycle_focus(),
        KeyCode::Char('%') => split(mux, Dir::Horizontal).await?,
        KeyCode::Char('"') => split(mux, Dir::Vertical).await?,
        _ => {}
    }
    mux.status = format!("focus: {}", mux.panes[&mux.focus].branch);
    Ok(false)
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
    }
}

/// Route a broadcast cell event to the pane following its branch. `Started`
/// carries the branch (so we learn id→pane); the rest carry only the id.
fn apply_event(mux: &mut Mux, ev: CellEvent) {
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

    // content: the tail of the pane's lines that fits
    let lines = pane_lines(pane);
    let content_h = (r.h - 2) as usize;
    let start = lines.len().saturating_sub(content_h);
    for i in 0..content_h {
        let raw = lines.get(start + i).map(String::as_str).unwrap_or("");
        let mut text: String = raw.chars().take(inner).collect();
        text.extend(std::iter::repeat_n(
            ' ',
            inner.saturating_sub(text.chars().count()),
        ));
        queue!(
            out,
            MoveTo(r.x, r.y + 1 + i as u16),
            Print(vt),
            Print(text),
            Print(vt)
        )?;
    }

    let mut bot = String::from(bl);
    bot.extend(std::iter::repeat_n(hz, inner));
    bot.push(br);
    queue!(out, MoveTo(r.x, r.y + r.h - 1), Print(bot))?;
    Ok(())
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
