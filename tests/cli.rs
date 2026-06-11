//! End-to-end tests driving the real `fern` binary: a daemon per test on its
//! own socket + store (via a private XDG_RUNTIME_DIR), exercised through the
//! actual CLI verbs and raw socket requests.
//!
//! The daemon is stopped with SIGTERM (graceful shutdown), so when these run
//! under cargo-llvm-cov the spawned processes flush their coverage too.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_fern")
}

struct TestDaemon {
    dir: PathBuf,
    child: Option<Child>,
}

impl TestDaemon {
    fn start() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "fern-it-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self::start_in(dir)
    }

    /// Start a daemon over an existing dir (same socket path + store) — used
    /// to test persistence across restarts.
    fn start_in(dir: PathBuf) -> Self {
        let child = Command::new(bin())
            .arg("daemon")
            .arg("--store")
            .arg(dir.join("tree.jsonl"))
            .env("XDG_RUNTIME_DIR", &dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn daemon");
        let sock = dir.join("fern.sock");
        let start = Instant::now();
        while !sock.exists() {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "daemon never bound {}",
                sock.display()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        Self {
            dir,
            child: Some(child),
        }
    }

    /// A `fern` Command wired to this daemon's runtime dir.
    fn fern(&self) -> Command {
        let mut c = Command::new(bin());
        c.env("XDG_RUNTIME_DIR", &self.dir);
        c.stdin(Stdio::null());
        c
    }

    /// Run a fern command, assert it succeeds, return stdout.
    fn ok(&self, args: &[&str]) -> String {
        let out = self.fern().args(args).output().unwrap();
        assert!(
            out.status.success(),
            "fern {args:?} failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// Run a fern command, assert it FAILS, return (stdout, stderr).
    fn err(&self, args: &[&str]) -> (String, String) {
        let out = self.fern().args(args).output().unwrap();
        assert!(
            !out.status.success(),
            "fern {args:?} unexpectedly succeeded: {}",
            String::from_utf8_lossy(&out.stdout)
        );
        (
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    /// Graceful shutdown: SIGTERM + wait. Returns the dir for a restart —
    /// Drop is skipped so the dir (socket path + store) survives; the next
    /// `start_in` TestDaemon takes over cleanup.
    fn stop(mut self) -> PathBuf {
        if let Some(mut child) = self.child.take() {
            let _ = Command::new("kill").arg(child.id().to_string()).status();
            let _ = child.wait();
        }
        let dir = self.dir.clone();
        std::mem::forget(self);
        dir
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = Command::new("kill").arg(child.id().to_string()).status();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Send one raw JSON request over the daemon's socket; return the first
/// response line. Exercises protocol paths the CLI doesn't reach.
fn raw_request(dir: &Path, json: &str) -> String {
    let sock = dir.join("fern.sock");
    let mut stream = std::os::unix::net::UnixStream::connect(&sock).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(json.as_bytes()).unwrap();
    stream.write_all(b"\n").unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).unwrap();
    line
}

// ---------- Core flows ---------------------------------------------------

#[test]
fn run_streams_output_and_propagates_exit_codes() {
    let d = TestDaemon::start();
    assert_eq!(d.ok(&["run", "echo hello world"]).trim(), "hello world");
    // Failing command's exit code becomes fern's exit code.
    let out = d.fern().args(["run", "false"]).output().unwrap();
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn state_inherits_down_the_chain() {
    let d = TestDaemon::start();
    d.ok(&["run", "export GREETING=hi"]);
    d.ok(&["run", "cd /tmp"]);
    // env + cwd + $? + $(...) all in one go.
    let out = d.ok(&["run", "echo $GREETING from $(pwd), last=$?"]);
    assert_eq!(out.trim(), "hi from /tmp, last=0");
    // $? carries a parent's failure into the next cell.
    let _ = d.fern().args(["run", "false"]).output().unwrap();
    assert_eq!(d.ok(&["run", "echo $?"]).trim(), "1");
}

#[test]
fn fork_from_historical_cell_makes_a_branch() {
    let d = TestDaemon::start();
    d.ok(&["run", "echo one"]);
    d.ok(&["run", "echo two"]);
    let out = d
        .fern()
        .args(["run", "--parent", "1", "echo forked"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("forked off"), "stderr: {stderr}");
    let tree = d.ok(&["tree"]);
    assert!(tree.contains("echo forked"));
    let branches = d.ok(&["branch"]);
    assert!(branches.contains("fork-"), "branches: {branches}");
}

#[test]
fn tree_shows_lineage_with_hashes() {
    let d = TestDaemon::start();
    d.ok(&["run", "echo a"]);
    let tree = d.ok(&["tree"]);
    assert!(tree.contains("(root) [system] exit 0"));
    assert!(tree.contains("echo a"));
}

// ---------- Branch lifecycle ---------------------------------------------

#[test]
fn branch_lifecycle() {
    let d = TestDaemon::start();
    d.ok(&["run", "echo base"]);
    d.ok(&["branch", "new", "feature"]);
    let list = d.ok(&["branch"]);
    assert!(list.contains("feature"));
    assert!(list.contains("* main"), "current marker: {list}");

    d.ok(&["switch", "feature"]);
    assert!(d.ok(&["branch"]).contains("* feature"));
    d.ok(&["run", "echo on-feature"]);

    d.ok(&["branch", "rename", "feature", "renamed"]);
    assert!(d.ok(&["branch"]).contains("* renamed"));

    d.ok(&["switch", "main"]);
    d.ok(&["branch", "rm", "renamed"]);
    assert!(!d.ok(&["branch"]).contains("renamed"));
}

#[test]
fn branch_validation_and_errors() {
    let d = TestDaemon::start();
    let (_o, e) = d.err(&["branch", "new", "bad name"]);
    assert!(e.contains("whitespace"), "stderr: {e}");
    let (_o, e) = d.err(&["branch", "new", "fork-abc"]);
    assert!(e.contains("reserved"), "stderr: {e}");
    let (_o, e) = d.err(&["branch", "new", "x", "--at", "999"]);
    assert!(e.contains("no such cell"), "stderr: {e}");
    let (_o, e) = d.err(&["branch", "rm", "main"]);
    assert!(e.contains("refusing"), "stderr: {e}");
    let (_o, e) = d.err(&["branch", "rm", "missing"]);
    assert!(e.contains("no such branch"), "stderr: {e}");
    let (_o, e) = d.err(&["switch", "missing"]);
    assert!(e.contains("no such branch"), "stderr: {e}");
    d.ok(&["branch", "new", "dup"]);
    let (_o, e) = d.err(&["branch", "new", "dup"]);
    assert!(e.contains("already exists"), "stderr: {e}");
    let (_o, e) = d.err(&["branch", "rename", "missing", "x"]);
    assert!(e.contains("no such branch"), "stderr: {e}");
    let (_o, e) = d.err(&["branch", "rename", "dup", "main"]);
    assert!(e.contains("already exists"), "stderr: {e}");
    let (_o, e) = d.err(&["run", "--branch", "ghost", "echo hi"]);
    assert!(e.contains("no such branch"), "stderr: {e}");
}

#[test]
fn stale_cursor_heals_to_main() {
    let d = TestDaemon::start();
    std::fs::write(d.dir.join("fern-branch"), "ghost-branch").unwrap();
    let out = d.fern().args(["run", "echo healed"]).output().unwrap();
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("falling back"), "stderr: {stderr}");
    assert_eq!(
        std::fs::read_to_string(d.dir.join("fern-branch"))
            .unwrap()
            .trim(),
        "main"
    );
}

// ---------- Detach / kill / tty ------------------------------------------

#[test]
fn detached_cell_can_be_killed() {
    let d = TestDaemon::start();
    let out = d.ok(&["run", "--detach", "sleep 30"]);
    assert!(out.contains("detached: cell #1"), "got: {out}");
    d.ok(&["kill", "1"]);
    // Poll until the kill is recorded.
    let start = Instant::now();
    loop {
        let tree = d.ok(&["tree"]);
        if tree.contains("sleep 30") && !tree.contains("running") {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "kill never landed: {tree}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let (_o, e) = d.err(&["kill", "99"]);
    assert!(e.contains("not running"), "stderr: {e}");
}

#[test]
fn tty_branch_send_and_capture() {
    let d = TestDaemon::start();
    d.ok(&["run", "export FERN_IO=tty"]);
    assert!(d.ok(&["branch"]).contains("(tty)"));
    d.ok(&["run", "--detach", "cat"]);
    d.ok(&["send", "main", "ping-through-pty"]);
    // cat echoes; PTY output is captured into the cell once killed.
    std::thread::sleep(Duration::from_millis(300));
    d.ok(&["kill", "2"]);
    let start = Instant::now();
    loop {
        let tree = d.ok(&["tree"]);
        if tree.contains("cat") {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(20));
    }
    // send to a non-tty cell fails fast
    d.ok(&["switch", "main"]);
    let sleep_out = d.ok(&[
        "run", "--branch", "main", "--parent", "0", "--detach", "sleep 30",
    ]);
    let id = sleep_out
        .trim()
        .trim_start_matches("detached: cell #")
        .trim_end_matches(" running")
        .to_string();
    let (_o, e) = d.err(&["send", &id, "nope"]);
    assert!(e.contains("not a terminal cell"), "stderr: {e}");
    d.ok(&["kill", &id]);
}

// ---------- Watch ---------------------------------------------------------

#[test]
fn watch_streams_other_clients_and_ends_with_daemon() {
    let d = TestDaemon::start();
    let watch = d
        .fern()
        .args(["watch"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(200)); // let it subscribe
    d.ok(&["run", "echo seen-by-watch"]);
    std::thread::sleep(Duration::from_millis(200));
    let dir = d.stop(); // graceful daemon shutdown ends the subscription
    let out = watch.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("seen-by-watch"), "watch saw: {stdout}");
    let _ = std::fs::remove_dir_all(dir);
}

// ---------- Cockpit (piped stdin) ------------------------------------------

fn cockpit(d: &TestDaemon, verb: &str, input: &str) -> (String, String) {
    let mut child = d
        .fern()
        .arg(verb)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn cockpit_runs_commands_and_metas() {
    let d = TestDaemon::start();
    d.ok(&["branch", "new", "side"]);
    let (stdout, _) = cockpit(
        &d,
        "attach",
        ":help\n:at\n:branches\n:tree\necho from-cockpit\n:switch side\n:switch nope\n:bogus\n\n:quit\n",
    );
    assert!(stdout.contains("attached to 'main'"));
    assert!(stdout.contains(":switch <name>")); // help text
    assert!(stdout.contains("on branch 'main'")); // :at
    assert!(stdout.contains("side")); // :branches
    assert!(stdout.contains("(root)")); // :tree
    assert!(stdout.contains("from-cockpit"));
    assert!(stdout.contains("switched to 'side'"));
    assert!(stdout.contains("error: no such branch 'nope'"));
    assert!(stdout.contains("unknown command"));
}

#[test]
fn cockpit_exits_on_stdin_eof_and_repl_is_alias() {
    let d = TestDaemon::start();
    // No :quit — closing stdin must end the session cleanly.
    let (stdout, _) = cockpit(&d, "repl", "echo via-repl\n");
    assert!(stdout.contains("via-repl"));
}

#[test]
fn cockpit_rejects_missing_branch_target() {
    let d = TestDaemon::start();
    let (_, stderr) = {
        let out = d.fern().args(["attach", "nope"]).output().unwrap();
        assert!(!out.status.success());
        (
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    assert!(stderr.contains("no such branch 'nope'"), "stderr: {stderr}");
}

// ---------- Persistence -----------------------------------------------------

#[test]
fn tree_survives_graceful_restart() {
    let d = TestDaemon::start();
    d.ok(&["run", "export K=persisted"]);
    d.ok(&["run", "echo $K"]);
    d.ok(&["branch", "new", "kept"]);
    let before = d.ok(&["tree"]);
    let dir = d.stop();

    let d2 = TestDaemon::start_in(dir);
    let after = d2.ok(&["tree"]);
    assert_eq!(before, after, "tree (with hashes) must survive restart");
    assert!(d2.ok(&["branch"]).contains("kept"));
    // State still flows on the restored lineage.
    assert_eq!(d2.ok(&["run", "echo $K"]).trim(), "persisted");
}

// ---------- No daemon / protocol-level ------------------------------------

#[test]
fn client_errors_cleanly_without_daemon() {
    let dir = std::env::temp_dir().join(format!(
        "fern-it-nodaemon-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    for args in [&["run", "echo hi"][..], &["tree"], &["branch"], &["watch"]] {
        let out = Command::new(bin())
            .args(args)
            .env("XDG_RUNTIME_DIR", &dir)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!out.status.success(), "{args:?} should fail with no daemon");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn run_with_no_command_fails() {
    let d = TestDaemon::start();
    let (_o, e) = d.err(&["run"]);
    assert!(e.contains("no command"), "stderr: {e}");
}

#[test]
fn protocol_error_paths() {
    let d = TestDaemon::start();
    // Bad JSON.
    let resp = raw_request(&d.dir, "this is not json");
    assert!(resp.contains("bad request"), "got: {resp}");
    // GetCell hit + miss.
    let resp = raw_request(&d.dir, r#"{"kind":"get_cell","id":0}"#);
    assert!(resp.contains("\"cell\""), "got: {resp}");
    let resp = raw_request(&d.dir, r#"{"kind":"get_cell","id":999}"#);
    assert!(resp.contains("no such cell"), "got: {resp}");
    // Input outside an attach session.
    let resp = raw_request(&d.dir, r#"{"kind":"input","id":0,"data":"x"}"#);
    assert!(resp.contains("outside Attach"), "got: {resp}");
    // Submit on a missing parent cell.
    let resp = raw_request(
        &d.dir,
        r#"{"kind":"submit","branch":"main","parent":999,"source":"echo x","who":"t"}"#,
    );
    assert!(resp.contains("no such parent"), "got: {resp}");
    // Attach to a cell that isn't running.
    let resp = raw_request(&d.dir, r#"{"kind":"attach","id":42}"#);
    assert!(resp.contains("not running"), "got: {resp}");
}
