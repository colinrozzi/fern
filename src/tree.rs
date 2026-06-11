//! Core types: State, Process, Cell, and the Tree that holds them.
//!
//! Cells form a Merkle DAG. Each cell takes a parent `State`, evaluates one
//! command line, and produces a new `State` + structured Output. On completion,
//! the cell is content-addressed: `hash(parent_hash, source, submitter, stdout,
//! stderr, exit_code, end_state)`. The root cell's hash baselines the machine
//! via `SystemInfo` (os, arch, hostname, fern version) — so every descendant's
//! hash transitively encodes "this happened on *that* machine in *that* state".

use anyhow::{Result, anyhow};
use sha2::{Digest, Sha256};
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
        // Seed $PWD from the actual cwd (not the inherited env) so the root
        // state is internally consistent; `cd` keeps it in sync from there.
        env.insert("PWD".into(), cwd.to_string_lossy().into_owned());
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

// ---------- SystemInfo (baseline for the root cell's hash) -------------

#[derive(Debug, Clone)]
pub struct SystemInfo {
    pub os: String,
    pub arch: String,
    pub hostname: String,
    pub fern_version: String,
}

impl SystemInfo {
    pub fn collect() -> Self {
        let hostname = std::fs::read_to_string("/etc/hostname")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| std::env::var("HOSTNAME").ok())
            .unwrap_or_else(|| "unknown".into());
        Self {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            hostname,
            fern_version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

// ---------- Cell tree ---------------------------------------------------

pub type CellId = u64;

/// Hex-encoded SHA-256 of the cell's content. Cryptographic identity.
pub type Hash = String;

#[derive(Debug, Clone)]
pub struct Cell {
    pub id: CellId,
    pub parent: Option<CellId>,
    pub submitter: String,
    /// The command line as the user typed it. Empty for the root cell.
    pub source: String,
    pub result: Option<CellResult>,
    /// Content hash. None while the cell is still running; Some after the
    /// result has been recorded.
    pub hash: Option<Hash>,
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
    /// Named, mutable pointers into the tree. A branch is a name → current tip
    /// (a `CellId`). The tip moves forward as work lands on the branch, and may
    /// point at a still-running cell (one with no content hash yet). The default
    /// branch `main` is seeded at the root.
    branches: BTreeMap<String, CellId>,
}

/// The default branch every tree starts with, pointing at the root cell.
pub const DEFAULT_BRANCH: &str = "main";

impl Tree {
    /// Create a tree rooted at a synthetic cell #0. The root's hash bakes in
    /// the SystemInfo so every descendant inherits this machine's lineage.
    pub fn new(root_state: State, sysinfo: SystemInfo) -> Self {
        let root_hash = hash_root(&sysinfo, &root_state);
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
                hash: Some(root_hash),
            },
        );
        let mut children = HashMap::new();
        children.insert(0, vec![]);
        let mut branches = BTreeMap::new();
        branches.insert(DEFAULT_BRANCH.to_string(), 0);
        Self {
            cells,
            children,
            next_id: 1,
            branches,
        }
    }

    pub fn get(&self, id: CellId) -> Option<&Cell> {
        self.cells.get(&id)
    }

    pub fn children_of(&self, id: CellId) -> &[CellId] {
        self.children.get(&id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    // ---------- Branches ------------------------------------------------

    /// Current tip of `name`, if the branch exists.
    pub fn branch_tip(&self, name: &str) -> Option<CellId> {
        self.branches.get(name).copied()
    }

    pub fn branch_exists(&self, name: &str) -> bool {
        self.branches.contains_key(name)
    }

    /// Create or move `name` to point at `id`.
    pub fn set_branch(&mut self, name: impl Into<String>, id: CellId) {
        self.branches.insert(name.into(), id);
    }

    /// Remove a branch. Returns false if it didn't exist. The root branch
    /// can't be deleted via this (callers should guard), but nothing here
    /// prevents it — the daemon enforces policy.
    pub fn delete_branch(&mut self, name: &str) -> bool {
        self.branches.remove(name).is_some()
    }

    /// Rename `from` → `to`, preserving the tip. Returns false if `from`
    /// doesn't exist or `to` already does.
    pub fn rename_branch(&mut self, from: &str, to: &str) -> bool {
        if !self.branches.contains_key(from) || self.branches.contains_key(to) {
            return false;
        }
        if let Some(id) = self.branches.remove(from) {
            self.branches.insert(to.to_string(), id);
            return true;
        }
        false
    }

    /// All branches as (name, tip) pairs, sorted by name.
    pub fn branches(&self) -> impl Iterator<Item = (&String, CellId)> {
        self.branches.iter().map(|(n, &id)| (n, id))
    }

    /// Allocate an id without inserting a cell. Used by the daemon when it
    /// needs to send the Started event before eval finishes.
    pub fn reserve_id(&mut self) -> CellId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Insert a fully-built cell (with a previously-reserved id) under `parent`.
    /// Computes the cell's content hash before insertion.
    pub fn insert_cell(&mut self, parent: CellId, mut cell: Cell) {
        let parent_hash = self.cells.get(&parent).and_then(|p| p.hash.clone());
        cell.hash = Some(hash_cell(&cell, parent_hash.as_deref()));
        let id = cell.id;
        self.cells.insert(id, cell);
        self.children.entry(parent).or_default().push(id);
        self.children.entry(id).or_default();
    }

    /// Attach (or replace) the result of a cell previously inserted with
    /// `result: None`. Computes and stores the content hash.
    pub fn set_cell_result(&mut self, id: CellId, result: CellResult) {
        let parent_hash = self
            .cells
            .get(&id)
            .and_then(|c| c.parent)
            .and_then(|p| self.cells.get(&p))
            .and_then(|p| p.hash.clone());
        if let Some(cell) = self.cells.get_mut(&id) {
            cell.result = Some(result);
            cell.hash = Some(hash_cell(cell, parent_hash.as_deref()));
        }
    }

    /// Parse, evaluate, and insert `source` as a child of `parent`. Returns
    /// the new cell id.
    ///
    /// The daemon doesn't use this — it does reserve_id + streaming eval +
    /// insert_cell so the tree mutex isn't held across eval. Kept for tests.
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

        let id = self.reserve_id();
        let cell = Cell {
            id,
            parent: Some(parent),
            submitter,
            source,
            result: Some(CellResult {
                exit_code: output.exit_code,
                stdout: output.stdout,
                stderr: output.stderr,
                duration: started.elapsed(),
                end_state: new_state,
            }),
            hash: None, // insert_cell fills this in
        };
        self.insert_cell(parent, cell);
        Ok(id)
    }
}

// ---------- Hash functions ---------------------------------------------

fn h_str(h: &mut Sha256, s: &str) {
    h.update((s.len() as u64).to_le_bytes());
    h.update(s.as_bytes());
}

fn h_bytes(h: &mut Sha256, b: &[u8]) {
    h.update((b.len() as u64).to_le_bytes());
    h.update(b);
}

fn h_state(h: &mut Sha256, state: &State) {
    h_str(h, &state.cwd.to_string_lossy());
    h.update((state.env.len() as u64).to_le_bytes());
    for (k, v) in &state.env {
        h_str(h, k);
        h_str(h, v);
    }
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Root cell hash: machine + starting state. Domain-separated from regular
/// cells so a regular cell can't accidentally collide with a root.
fn hash_root(info: &SystemInfo, state: &State) -> Hash {
    let mut h = Sha256::new();
    h.update(b"FERN_ROOT_V1\0");
    h_str(&mut h, &info.os);
    h_str(&mut h, &info.arch);
    h_str(&mut h, &info.hostname);
    h_str(&mut h, &info.fern_version);
    h_state(&mut h, state);
    to_hex(&h.finalize())
}

/// Cell hash: parent_hash + recipe + outputs + end_state. Only meaningful
/// when the cell has a result (still-running cells have no hash).
fn hash_cell(cell: &Cell, parent_hash: Option<&str>) -> Hash {
    let mut h = Sha256::new();
    h.update(b"FERN_CELL_V1\0");
    h_str(&mut h, parent_hash.unwrap_or(""));
    h_str(&mut h, &cell.source);
    h_str(&mut h, &cell.submitter);
    if let Some(r) = &cell.result {
        h_bytes(&mut h, &r.stdout);
        h_bytes(&mut h, &r.stderr);
        h.update(r.exit_code.to_le_bytes());
        h_state(&mut h, &r.end_state);
    }
    to_hex(&h.finalize())
}

// ---------- Tests --------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sys() -> SystemInfo {
        SystemInfo {
            os: "linux".into(),
            arch: "x86_64".into(),
            hostname: "test-host".into(),
            fern_version: "0.0.0".into(),
        }
    }

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
        let mut t = Tree::new(State::baseline().unwrap(), sys());
        let a = run(&mut t, 0, "cd /tmp").await;
        let b = run(&mut t, a, "pwd").await;
        // `cd` canonicalizes, so on macOS /tmp resolves to /private/tmp.
        let tmp = std::fs::canonicalize("/tmp").unwrap();
        assert_eq!(cwd(&t, a), tmp);
        assert_eq!(out(&t, b).trim(), tmp.to_str().unwrap());
    }

    #[tokio::test]
    async fn export_persists_to_children() {
        let mut t = Tree::new(State::baseline().unwrap(), sys());
        let a = run(&mut t, 0, "export FOO=bar").await;
        let b = run(&mut t, a, "printenv FOO").await;
        assert_eq!(env_of(&t, a, "FOO").as_deref(), Some("bar"));
        assert_eq!(out(&t, b).trim(), "bar");
    }

    #[tokio::test]
    async fn branches_are_independent() {
        let mut t = Tree::new(State::baseline().unwrap(), sys());
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
        let mut t = Tree::new(State::baseline().unwrap(), sys());
        let a = run(&mut t, 0, "echo hello | wc -c").await;
        assert_eq!(out(&t, a).trim(), "6"); // "hello\n" = 6 bytes
        let b = run(&mut t, 0, "false || echo recovered").await;
        assert_eq!(out(&t, b).trim(), "recovered");
    }

    // ---------- Hash tests --------------------------------------------

    #[tokio::test]
    async fn cells_get_a_hash_on_completion() {
        let mut t = Tree::new(State::baseline().unwrap(), sys());
        let id = run(&mut t, 0, "echo hi").await;
        let cell = t.get(id).unwrap();
        let hash = cell.hash.as_ref().unwrap();
        assert_eq!(hash.len(), 64, "SHA-256 hex is 64 chars");
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn root_hash_changes_with_machine() {
        let state = State::baseline().unwrap();
        let info_a = SystemInfo {
            os: "linux".into(),
            arch: "x86_64".into(),
            hostname: "machine-a".into(),
            fern_version: "0.0.0".into(),
        };
        let info_b = SystemInfo {
            hostname: "machine-b".into(),
            ..info_a.clone()
        };
        let tree_a = Tree::new(state.clone(), info_a);
        let tree_b = Tree::new(state, info_b);
        assert_ne!(
            tree_a.get(0).unwrap().hash,
            tree_b.get(0).unwrap().hash,
            "different hostnames produce different root hashes"
        );
    }

    #[tokio::test]
    async fn identical_runs_collide_dedup_by_design() {
        // Two `true` cells off the same parent should hash identically:
        // same parent, same recipe, same (empty) output, same end-state.
        let mut t = Tree::new(State::baseline().unwrap(), sys());
        let a = run(&mut t, 0, "true").await;
        let b = run(&mut t, 0, "true").await;
        let ha = t.get(a).unwrap().hash.clone().unwrap();
        let hb = t.get(b).unwrap().hash.clone().unwrap();
        assert_eq!(ha, hb, "identical work hashes identically");
    }

    #[tokio::test]
    async fn different_output_changes_hash() {
        // Two cells with the same recipe but different output should differ.
        // `echo $RANDOM` would do it but we can't easily compare values; use
        // different commands instead.
        let mut t = Tree::new(State::baseline().unwrap(), sys());
        let a = run(&mut t, 0, "echo a").await;
        let b = run(&mut t, 0, "echo b").await;
        assert_ne!(t.get(a).unwrap().hash, t.get(b).unwrap().hash);
    }

    // ---------- Branch tests ------------------------------------------

    #[test]
    fn tree_seeds_default_branch_at_root() {
        let t = Tree::new(State::baseline().unwrap(), sys());
        assert_eq!(t.branch_tip(DEFAULT_BRANCH), Some(0));
        assert!(t.branch_exists(DEFAULT_BRANCH));
    }

    #[test]
    fn set_and_move_branch() {
        let mut t = Tree::new(State::baseline().unwrap(), sys());
        t.set_branch("feature", 0);
        assert_eq!(t.branch_tip("feature"), Some(0));
        t.set_branch("feature", 5); // move it
        assert_eq!(t.branch_tip("feature"), Some(5));
    }

    #[test]
    fn delete_branch_removes_it() {
        let mut t = Tree::new(State::baseline().unwrap(), sys());
        t.set_branch("tmp", 0);
        assert!(t.delete_branch("tmp"));
        assert!(!t.branch_exists("tmp"));
        assert!(!t.delete_branch("tmp")); // already gone
    }

    #[test]
    fn rename_branch_preserves_tip_and_guards() {
        let mut t = Tree::new(State::baseline().unwrap(), sys());
        t.set_branch("old", 3);
        assert!(t.rename_branch("old", "new"));
        assert_eq!(t.branch_tip("new"), Some(3));
        assert!(!t.branch_exists("old"));
        // can't rename a missing branch, or onto an existing one
        assert!(!t.rename_branch("missing", "x"));
        t.set_branch("a", 1);
        t.set_branch("b", 2);
        assert!(!t.rename_branch("a", "b"));
    }

    #[tokio::test]
    async fn hash_chain_propagates() {
        // Child's hash transitively depends on root's hash, so changing the
        // machine baseline changes EVERY descendant's hash.
        let state = State::baseline().unwrap();
        let mut t_a = Tree::new(
            state.clone(),
            SystemInfo {
                hostname: "host-a".into(),
                ..sys()
            },
        );
        let mut t_b = Tree::new(
            state,
            SystemInfo {
                hostname: "host-b".into(),
                ..sys()
            },
        );
        let id_a = run(&mut t_a, 0, "echo same").await;
        let id_b = run(&mut t_b, 0, "echo same").await;
        assert_ne!(
            t_a.get(id_a).unwrap().hash,
            t_b.get(id_b).unwrap().hash,
            "same command on different machines hashes differently"
        );
    }
}
