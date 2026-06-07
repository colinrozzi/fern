//! Core types: State, Process, Cell, and the Tree that holds them.
//!
//! Cells form a tree. Each cell takes a parent `State`, evaluates one command
//! line, and produces a new `State` (cwd + env after it ran).

use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::time::Instant;

// ---------- State + Process ---------------------------------------------

/// Inheritable context that flows down the cell tree.
#[derive(Debug, Clone, Default)]
pub struct State {
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
}

impl State {
    /// A reasonable starting state: cwd = current dir, env = minimal baseline.
    pub fn baseline() -> Result<Self> {
        let cwd = std::env::current_dir()?;
        let mut env = BTreeMap::new();
        for key in ["PATH", "HOME", "USER", "LANG", "TERM"] {
            if let Ok(v) = std::env::var(key) {
                env.insert(key.into(), v);
            }
        }
        Ok(Self { cwd, env })
    }
}

/// OS-level spec for one spawn. Derived from a parent `State` + argv.
#[derive(Debug, Clone)]
pub struct Process {
    pub argv: Vec<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
}

impl Process {
    pub fn from_state(parent: &State, argv: Vec<String>) -> Self {
        Self {
            argv,
            cwd: parent.cwd.clone(),
            env: parent.env.clone(),
        }
    }
}

// ---------- Output ------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct Output {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

// ---------- Cell tree ---------------------------------------------------

pub type CellId = u64;

#[derive(Debug, Clone)]
pub struct Cell {
    pub id: CellId,
    pub parent: Option<CellId>,
    pub submitter: String,
    /// The command line as the user typed it. Empty for the root cell.
    pub source: String,
    pub result: Option<CellResult>,
}

#[derive(Debug, Clone)]
pub struct CellResult {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration: std::time::Duration,
    pub end_state: State,
}

#[derive(Debug)]
pub struct Tree {
    cells: HashMap<CellId, Cell>,
    children: HashMap<CellId, Vec<CellId>>,
    next_id: CellId,
}

impl Tree {
    /// Create a tree rooted at a synthetic cell #0 holding the baseline state.
    pub fn new(root_state: State) -> Self {
        let mut cells = HashMap::new();
        cells.insert(
            0,
            Cell {
                id: 0,
                parent: None,
                submitter: "system".into(),
                source: String::new(),
                result: Some(CellResult {
                    exit_code: 0,
                    stdout: vec![],
                    stderr: vec![],
                    duration: std::time::Duration::ZERO,
                    end_state: root_state,
                }),
            },
        );
        let mut children = HashMap::new();
        children.insert(0, vec![]);
        Self {
            cells,
            children,
            next_id: 1,
        }
    }

    pub fn get(&self, id: CellId) -> Option<&Cell> {
        self.cells.get(&id)
    }

    pub fn children_of(&self, id: CellId) -> &[CellId] {
        self.children.get(&id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Allocate an id without inserting a cell. Used by the daemon when it
    /// needs to send the Started event before eval finishes.
    pub fn reserve_id(&mut self) -> CellId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Insert a fully-built cell (with a previously-reserved id) under `parent`.
    pub fn insert_cell(&mut self, parent: CellId, cell: Cell) {
        let id = cell.id;
        self.cells.insert(id, cell);
        self.children.entry(parent).or_default().push(id);
        self.children.entry(id).or_default();
    }

    /// Attach (or replace) the result of a cell previously inserted with `result: None`.
    /// Used for detached cells whose result lands later (or on kill).
    pub fn set_cell_result(&mut self, id: CellId, result: CellResult) {
        if let Some(cell) = self.cells.get_mut(&id) {
            cell.result = Some(result);
        }
    }

    /// Parse and evaluate `source` as a child of `parent`. Returns the new cell id.
    /// Parse and runtime errors become a cell with exit_code 2 and the error
    /// text on stderr, so the tree always advances.
    ///
    /// The daemon doesn't use this — it does reserve_id + streaming eval +
    /// insert_cell so the tree mutex isn't held across eval. Kept for tests
    /// and any future single-threaded caller.
    #[allow(dead_code)]
    pub async fn submit(
        &mut self,
        parent: CellId,
        submitter: String,
        source: String,
    ) -> Result<CellId> {
        let parent_state = self
            .cells
            .get(&parent)
            .ok_or_else(|| anyhow!("no such parent cell {parent}"))?
            .result
            .as_ref()
            .ok_or_else(|| anyhow!("parent cell {parent} has no result yet"))?
            .end_state
            .clone();

        let id = self.next_id;
        self.next_id += 1;
        let started = Instant::now();

        let (new_state, output) = match crate::eval::eval_line_collect(&parent_state, &source).await
        {
            Ok(r) => r,
            Err(e) => (
                parent_state.clone(),
                Output {
                    exit_code: 2,
                    stdout: vec![],
                    stderr: format!("{e}\n").into_bytes(),
                },
            ),
        };

        let result = CellResult {
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
            duration: started.elapsed(),
            end_state: new_state,
        };

        self.cells.insert(
            id,
            Cell {
                id,
                parent: Some(parent),
                submitter,
                source,
                result: Some(result),
            },
        );
        self.children.entry(parent).or_default().push(id);
        self.children.entry(id).or_default();
        Ok(id)
    }
}

// ---------- Tests --------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    async fn run(t: &mut Tree, parent: CellId, line: &str) -> CellId {
        t.submit(parent, "test".into(), line.into()).await.unwrap()
    }

    fn out(t: &Tree, id: CellId) -> String {
        let r = t.get(id).unwrap().result.as_ref().unwrap();
        String::from_utf8_lossy(&r.stdout).into_owned()
    }

    fn cwd(t: &Tree, id: CellId) -> PathBuf {
        t.get(id).unwrap().result.as_ref().unwrap().end_state.cwd.clone()
    }

    fn env_of(t: &Tree, id: CellId, key: &str) -> Option<String> {
        t.get(id)
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .end_state
            .env
            .get(key)
            .cloned()
    }

    #[tokio::test]
    async fn cwd_persists_across_cells() {
        let mut t = Tree::new(State::baseline().unwrap());
        let a = run(&mut t, 0, "cd /tmp").await;
        let b = run(&mut t, a, "pwd").await;
        assert_eq!(cwd(&t, a), PathBuf::from("/tmp"));
        assert_eq!(out(&t, b).trim(), "/tmp");
    }

    #[tokio::test]
    async fn export_persists_to_children() {
        let mut t = Tree::new(State::baseline().unwrap());
        let a = run(&mut t, 0, "export FOO=bar").await;
        let b = run(&mut t, a, "printenv FOO").await;
        assert_eq!(env_of(&t, a, "FOO").as_deref(), Some("bar"));
        assert_eq!(out(&t, b).trim(), "bar");
    }

    #[tokio::test]
    async fn branches_are_independent() {
        let mut t = Tree::new(State::baseline().unwrap());
        let root = run(&mut t, 0, "export FOO=root").await;
        let a1 = run(&mut t, root, "export FOO=branchA").await;
        let a2 = run(&mut t, a1, "printenv FOO").await;
        let b1 = run(&mut t, root, "export FOO=branchB").await;
        let b2 = run(&mut t, b1, "printenv FOO").await;
        assert_eq!(out(&t, a2).trim(), "branchA");
        assert_eq!(out(&t, b2).trim(), "branchB");
    }

    #[tokio::test]
    async fn shell_features_work_in_cells() {
        let mut t = Tree::new(State::baseline().unwrap());
        let a = run(&mut t, 0, "echo hello | wc -c").await;
        assert_eq!(out(&t, a).trim(), "6"); // "hello\n" = 6 bytes
        let b = run(&mut t, 0, "false || echo recovered").await;
        assert_eq!(out(&t, b).trim(), "recovered");
    }
}
