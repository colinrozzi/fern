//! Wire protocol between the daemon and its clients.
//!
//! Newline-delimited JSON over a unix socket. Each Submit yields a stream of
//! events: `Started → OutputChunk* → Completed`. For detached submits, the
//! submitting connection only sees `Started` (and immediately disconnects);
//! subsequent chunks + Completed flow only to broadcast subscribers.

use serde::{Deserialize, Serialize};

use crate::tree::CellId;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Request {
    Submit {
        parent: CellId,
        source: String,
        who: String,
        /// If true, daemon returns after the Started event and runs the cell
        /// in the background. Output streams via broadcast; tree shows the
        /// cell with no exit_code until it completes (or is killed).
        #[serde(default)]
        detach: bool,
        /// If true, the cell runs under a PTY (so isatty-aware programs see a
        /// terminal). Required for `fern attach`. v1 requires `detach=true`
        /// alongside this; you attach to the running cell from a separate client.
        #[serde(default)]
        interactive: bool,
    },
    Subscribe,
    GetTree,
    GetCell {
        id: CellId,
    },
    /// Abort a running detached cell.
    Kill {
        id: CellId,
    },
    /// Attach to a running interactive cell. After this request, this
    /// connection becomes bidirectional: incoming Input requests are forwarded
    /// to the cell's PTY stdin; outgoing OutputChunk events for this cell are
    /// streamed back. Detach by closing the connection.
    Attach {
        id: CellId,
    },
    /// Send raw bytes to a cell's PTY stdin. Sent on the connection that
    /// previously issued Attach.
    Input {
        id: CellId,
        data: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Response {
    Event(CellEvent),
    Tree(TreeSnapshot),
    Cell(CellSnapshot),
    Error { message: String },
    Ok,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Stream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum CellEvent {
    Started {
        id: CellId,
        parent: Option<CellId>,
        source: String,
        who: String,
    },
    OutputChunk {
        id: CellId,
        stream: Stream,
        data: String,
    },
    Completed {
        id: CellId,
        exit_code: i32,
        duration_ms: u64,
        /// SHA-256 content hash. Optional for backward compat / partial updates;
        /// the daemon always sets it for cells that produced a result.
        #[serde(default)]
        hash: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellSnapshot {
    pub id: CellId,
    pub parent: Option<CellId>,
    pub submitter: String,
    pub source: String,
    /// `None` while the cell is still running.
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub stdout: String,
    pub stderr: String,
    /// SHA-256 content hash. `None` while running, `Some(hex)` after Completed.
    #[serde(default)]
    pub hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeSnapshot {
    pub cells: Vec<CellSnapshot>,
}

pub fn socket_path() -> std::path::PathBuf {
    let base = std::env::var("XDG_RUNTIME_DIR").unwrap_or_else(|_| "/tmp".to_string());
    std::path::PathBuf::from(base).join("fern.sock")
}
