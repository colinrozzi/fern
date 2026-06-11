//! Append-only persistence for the cell tree.
//!
//! The daemon logs one JSON record per line to `$XDG_DATA_HOME/fern/tree.jsonl`:
//! the root (state + system info) when a store is first created, each cell as
//! it *completes* (running cells are never persisted — daemon death kills them
//! anyway), and branch operations. On startup the log replays into a `Tree`,
//! recomputing every content hash from the persisted data and verifying it
//! against the logged one — the Merkle DAG gives integrity checking for free.
//!
//! Replay is tolerant by design: a torn final line (crash mid-append), a cell
//! whose parent never persisted, or a branch pointing at a missing cell are
//! all warned about and skipped rather than refusing to start.

use anyhow::Result;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::tree::{Cell, CellId, CellResult, State, SystemInfo, Tree};

/// One line in the log.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    /// First record of a store: everything needed to rebuild the root cell
    /// with the same hash (do NOT re-collect at load time — the machine may
    /// have changed; the lineage shouldn't).
    Root {
        state: State,
        sysinfo: SystemInfo,
    },
    Cell {
        id: CellId,
        parent: CellId,
        submitter: String,
        source: String,
        exit_code: i32,
        /// Output as text. Chunks are lossy-UTF-8 at capture time already,
        /// so this round-trips what the tree actually held.
        stdout: String,
        stderr: String,
        duration_ms: u64,
        end_state: State,
        /// Content hash at write time; verified on replay.
        hash: String,
    },
    SetBranch {
        name: String,
        tip: CellId,
    },
    DeleteBranch {
        name: String,
    },
    RenameBranch {
        from: String,
        to: String,
    },
}

/// Build the log record for a completed cell. None for the root or a cell
/// that hasn't finished (neither is ever logged).
pub fn record_of(cell: &Cell) -> Option<Record> {
    let parent = cell.parent?;
    let result = cell.result.as_ref()?;
    let hash = cell.hash.clone()?;
    Some(Record::Cell {
        id: cell.id,
        parent,
        submitter: cell.submitter.clone(),
        source: cell.source.clone(),
        exit_code: result.exit_code,
        stdout: String::from_utf8_lossy(&result.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&result.stderr).into_owned(),
        duration_ms: result.duration.as_millis() as u64,
        end_state: result.end_state.clone(),
        hash,
    })
}

pub struct Store {
    file: std::fs::File,
}

impl Store {
    /// `$XDG_DATA_HOME/fern/tree.jsonl`, falling back to `~/.local/share`.
    pub fn default_path() -> PathBuf {
        Self::default_path_from(
            std::env::var("XDG_DATA_HOME").ok(),
            std::env::var("HOME").ok(),
        )
    }

    /// Pure core of `default_path`, separated so the resolution order is
    /// testable without mutating process env.
    fn default_path_from(xdg_data_home: Option<String>, home: Option<String>) -> PathBuf {
        let base = xdg_data_home
            .map(PathBuf::from)
            .or_else(|| home.map(|h| PathBuf::from(h).join(".local/share")))
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        base.join("fern").join("tree.jsonl")
    }

    /// Open the log (creating parent directories), returning the store ready
    /// for appends plus every record already on disk.
    pub fn open(path: &Path) -> Result<(Self, Vec<Record>)> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut records = Vec::new();
        if let Ok(text) = std::fs::read_to_string(path) {
            for (i, line) in text.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<Record>(line) {
                    Ok(r) => records.push(r),
                    // Most likely a torn write from a crash mid-append.
                    Err(e) => eprintln!("fern store: skipping bad line {}: {e}", i + 1),
                }
            }
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok((Self { file }, records))
    }

    pub fn append(&mut self, rec: &Record) -> Result<()> {
        let mut line = serde_json::to_string(rec)?;
        line.push('\n');
        self.file.write_all(line.as_bytes())?;
        // One fsync per completed cell is cheap at interactive rates and
        // makes the log survive power loss, not just process death.
        let _ = self.file.sync_data();
        Ok(())
    }
}

/// Replay logged records into a Tree. Returns None when the log has no Root
/// record (empty/new store) — the caller starts fresh and writes one.
pub fn replay(records: &[Record]) -> Option<Tree> {
    let mut iter = records.iter();
    let Some(Record::Root { state, sysinfo }) = iter.next() else {
        return None;
    };
    let mut tree = Tree::new(state.clone(), sysinfo.clone());
    for rec in iter {
        match rec {
            Record::Cell {
                id,
                parent,
                submitter,
                source,
                exit_code,
                stdout,
                stderr,
                duration_ms,
                end_state,
                hash,
            } => {
                if tree.get(*id).is_some() {
                    eprintln!("fern store: duplicate cell #{id}; skipping");
                    continue;
                }
                if tree.get(*parent).is_none() {
                    eprintln!("fern store: cell #{id} has missing parent #{parent}; skipping");
                    continue;
                }
                let cell = Cell {
                    id: *id,
                    parent: Some(*parent),
                    submitter: submitter.clone(),
                    source: source.clone(),
                    hash: None,
                    result: Some(CellResult {
                        exit_code: *exit_code,
                        stdout: stdout.clone().into_bytes(),
                        stderr: stderr.clone().into_bytes(),
                        duration: std::time::Duration::from_millis(*duration_ms),
                        end_state: end_state.clone(),
                    }),
                };
                let recomputed = tree.restore_cell(*parent, cell);
                if recomputed.as_deref() != Some(hash.as_str()) {
                    eprintln!(
                        "fern store: hash mismatch for cell #{id}: logged {hash}, recomputed {}",
                        recomputed.as_deref().unwrap_or("none")
                    );
                }
            }
            Record::SetBranch { name, tip } => {
                if tree.get(*tip).is_some() {
                    tree.set_branch(name, *tip);
                } else {
                    eprintln!(
                        "fern store: branch '{name}' points at missing cell #{tip}; skipping"
                    );
                }
            }
            Record::DeleteBranch { name } => {
                tree.delete_branch(name);
            }
            Record::RenameBranch { from, to } => {
                tree.rename_branch(from, to);
            }
            Record::Root { .. } => {
                eprintln!("fern store: unexpected mid-log Root record; ignoring");
            }
        }
    }
    Some(tree)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sys() -> SystemInfo {
        SystemInfo {
            os: "linux".into(),
            arch: "x86_64".into(),
            hostname: "test-host".into(),
            fern_version: "0.0.0-test".into(),
        }
    }

    fn temp_log(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "fern-store-test-{}-{tag}.jsonl",
            std::process::id()
        ))
    }

    /// Build a tree the way the daemon does, persist it, replay it, and check
    /// the Merkle hashes and branches survive the round trip.
    #[test]
    fn round_trip_preserves_hashes_and_branches() {
        let state = State::baseline().unwrap();
        let mut tree = Tree::new(state.clone(), sys());
        let id = tree.reserve_id();
        tree.insert_cell(
            0,
            Cell {
                id,
                parent: Some(0),
                submitter: "test".into(),
                source: "echo hi".into(),
                hash: None,
                result: None,
            },
        );
        tree.set_cell_result(
            id,
            CellResult {
                exit_code: 0,
                stdout: b"hi\n".to_vec(),
                stderr: vec![],
                duration: Duration::from_millis(5),
                end_state: state.clone(),
            },
        );
        tree.set_branch("feature", id);

        let path = temp_log("roundtrip");
        std::fs::remove_file(&path).ok();
        let (mut store, existing) = Store::open(&path).unwrap();
        assert!(existing.is_empty());
        store
            .append(&Record::Root {
                state,
                sysinfo: sys(),
            })
            .unwrap();
        store
            .append(&record_of(tree.get(id).unwrap()).unwrap())
            .unwrap();
        store
            .append(&Record::SetBranch {
                name: "feature".into(),
                tip: id,
            })
            .unwrap();
        drop(store);

        let (_store, records) = Store::open(&path).unwrap();
        let mut restored = replay(&records).unwrap();
        // The replayed cell recomputes to the identical content hash.
        assert_eq!(restored.get(id).unwrap().hash, tree.get(id).unwrap().hash);
        assert_eq!(restored.branch_tip("feature"), Some(id));
        assert_eq!(restored.branch_tip(crate::tree::DEFAULT_BRANCH), Some(0));
        // next_id advanced past the restored cell.
        assert_eq!(restored.reserve_id(), id + 1);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn torn_final_line_is_skipped() {
        let path = temp_log("torn");
        std::fs::remove_file(&path).ok();
        let (mut store, _) = Store::open(&path).unwrap();
        store
            .append(&Record::Root {
                state: State::baseline().unwrap(),
                sysinfo: sys(),
            })
            .unwrap();
        drop(store);
        // Simulate a crash mid-append.
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(b"{\"kind\":\"cell\",\"id\":1,\"par").unwrap();
        drop(f);

        let (_store, records) = Store::open(&path).unwrap();
        assert_eq!(records.len(), 1); // root survived, torn line dropped
        assert!(replay(&records).is_some());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn dangling_branch_and_orphan_cell_are_skipped() {
        let state = State::baseline().unwrap();
        let records = vec![
            Record::Root {
                state: state.clone(),
                sysinfo: sys(),
            },
            // Parent #5 never persisted (was still running at crash).
            Record::Cell {
                id: 6,
                parent: 5,
                submitter: "t".into(),
                source: "echo orphan".into(),
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
                duration_ms: 1,
                end_state: state,
                hash: "deadbeef".into(),
            },
            Record::SetBranch {
                name: "ghost".into(),
                tip: 99,
            },
        ];
        let tree = replay(&records).unwrap();
        assert!(tree.get(6).is_none());
        assert!(!tree.branch_exists("ghost"));
        assert_eq!(tree.branch_tip(crate::tree::DEFAULT_BRANCH), Some(0));
    }

    #[test]
    fn empty_store_replays_to_none() {
        assert!(replay(&[]).is_none());
    }

    #[test]
    fn default_path_resolution_order() {
        // XDG_DATA_HOME wins.
        assert_eq!(
            Store::default_path_from(Some("/xdg".into()), Some("/home/u".into())),
            PathBuf::from("/xdg/fern/tree.jsonl")
        );
        // HOME fallback.
        assert_eq!(
            Store::default_path_from(None, Some("/home/u".into())),
            PathBuf::from("/home/u/.local/share/fern/tree.jsonl")
        );
        // Last-resort /tmp.
        assert_eq!(
            Store::default_path_from(None, None),
            PathBuf::from("/tmp/fern/tree.jsonl")
        );
        // The env-reading wrapper produces the same shape.
        assert!(Store::default_path().ends_with("fern/tree.jsonl"));
    }

    #[test]
    fn replay_applies_branch_ops_and_skips_junk() {
        let state = State::baseline().unwrap();
        let cell = |id: CellId| Record::Cell {
            id,
            parent: 0,
            submitter: "t".into(),
            source: format!("echo {id}"),
            exit_code: 0,
            stdout: String::new(),
            stderr: String::new(),
            duration_ms: 1,
            end_state: state.clone(),
            hash: "ignored".into(), // mismatch only warns; recomputed hash wins
        };
        let records = vec![
            Record::Root {
                state: state.clone(),
                sysinfo: sys(),
            },
            cell(1),
            cell(1), // duplicate id → skipped
            Record::SetBranch {
                name: "work".into(),
                tip: 1,
            },
            Record::RenameBranch {
                from: "work".into(),
                to: "renamed".into(),
            },
            Record::SetBranch {
                name: "doomed".into(),
                tip: 1,
            },
            Record::DeleteBranch {
                name: "doomed".into(),
            },
            // A second Root mid-log is ignored with a warning.
            Record::Root {
                state: state.clone(),
                sysinfo: sys(),
            },
        ];
        let tree = replay(&records).unwrap();
        assert!(tree.get(1).is_some());
        assert_eq!(tree.branch_tip("renamed"), Some(1));
        assert!(!tree.branch_exists("work"));
        assert!(!tree.branch_exists("doomed"));
    }

    #[test]
    fn open_skips_blank_lines() {
        let path = temp_log("blank");
        std::fs::remove_file(&path).ok();
        let (mut store, _) = Store::open(&path).unwrap();
        store
            .append(&Record::Root {
                state: State::baseline().unwrap(),
                sysinfo: sys(),
            })
            .unwrap();
        drop(store);
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(b"\n   \n").unwrap();
        drop(f);
        let (_s, records) = Store::open(&path).unwrap();
        assert_eq!(records.len(), 1);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn open_fails_on_uncreatable_dir() {
        assert!(Store::open(Path::new("/proc/definitely/not/creatable/tree.jsonl")).is_err());
    }
}
