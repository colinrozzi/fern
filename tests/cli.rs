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

#[test]
fn resize_reflows_the_terminal_and_rejects_bad_targets() {
    let d = TestDaemon::start();
    d.ok(&["run", "export FERN_IO=tty"]);
    // A tty cell that blocks on a line of input, then reports its window size.
    // Resizing before we unblock it means `stty size` observes the new size.
    let out = d.ok(&["run", "--detach", r#"sh -c "read x; stty size""#]);
    let id = out
        .trim()
        .trim_start_matches("detached: cell #")
        .trim_end_matches(" running")
        .to_string();

    // The PTY registers a beat after Started; poll the resize until it lands.
    let start = Instant::now();
    loop {
        let r = d
            .fern()
            .args(["resize", "main", "40", "100"])
            .output()
            .unwrap();
        if r.status.success() {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "resize never succeeded: {}",
            String::from_utf8_lossy(&r.stderr)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // Now unblock the cell — `stty size` runs strictly after the resize acked.
    d.ok(&["send", "main", ""]);

    let start = Instant::now();
    let reflowed = loop {
        let resp = raw_request(&d.dir, &format!(r#"{{"kind":"get_cell","id":{id}}}"#));
        if resp.contains("40 100") {
            break true;
        }
        if start.elapsed() > Duration::from_secs(5) {
            break false;
        }
        std::thread::sleep(Duration::from_millis(30));
    };
    assert!(reflowed, "cell never reported the resized 40x100 window");

    // Resizing a cell that isn't running fails.
    let (_o, e) = d.err(&["resize", "9999", "40", "100"]);
    assert!(e.contains("not running"), "stderr: {e}");

    // Resizing a running *pipe* cell (no PTY) fails with a clear message.
    let sleep_out = d.ok(&[
        "run", "--branch", "main", "--parent", "0", "--detach", "sleep 30",
    ]);
    let pipe_id = sleep_out
        .trim()
        .trim_start_matches("detached: cell #")
        .trim_end_matches(" running")
        .to_string();
    let (_o, e) = d.err(&["resize", &pipe_id, "40", "100"]);
    assert!(e.contains("not a terminal"), "stderr: {e}");
    d.ok(&["kill", &pipe_id]);
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

// ---------- Cockpit live feed (other clients render at the prompt) ---------

#[test]
fn cockpit_renders_other_clients_cells_live() {
    let d = TestDaemon::start();
    let mut child = d
        .fern()
        .arg("attach")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    // Park at the prompt, then land a cell from a second client.
    std::thread::sleep(Duration::from_millis(300));
    d.ok(&["run", "--who", "other", "echo feed-ping"]);
    std::thread::sleep(Duration::from_millis(300));
    stdin.write_all(b":quit\n").unwrap();
    drop(stdin);
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("other on main") && stdout.contains("feed-ping"),
        "cockpit feed: {stdout}"
    );
}

#[test]
fn cockpit_streams_a_running_pipe_tip_to_completion() {
    let d = TestDaemon::start();
    d.ok(&["run", "--detach", "sleep 0.4; echo done-late"]);
    // Tip is running and NOT a terminal cell → attach falls back to streaming.
    let (stdout, _) = cockpit(&d, "attach", ":quit\n");
    assert!(
        stdout.contains("done-late") || stdout.contains("exit 0"),
        "stream-until-done: {stdout}"
    );
}

// ---------- Raw-mode attach under a real PTY --------------------------------

mod pty {
    use super::*;
    use portable_pty::{CommandBuilder, PtySize, native_pty_system};
    use std::sync::{Arc, Mutex};

    pub struct PtyClient {
        pub child: Box<dyn portable_pty::Child + Send + Sync>,
        pub writer: Box<dyn std::io::Write + Send>,
        output: Arc<Mutex<String>>,
        /// Byte offset into the output stream that `wait_for_new` has already
        /// consumed, so a later wait for the same needle blocks for a *fresh*
        /// occurrence instead of re-matching stale output (e.g. an earlier
        /// prompt). The stream only grows, so the offset stays valid.
        cursor: usize,
        _master: Box<dyn portable_pty::MasterPty + Send>,
    }

    impl PtyClient {
        /// Spawn `fern <args>` inside a fresh PTY wired to `dir`'s daemon.
        pub fn spawn(dir: &Path, args: &[&str]) -> Self {
            let pair = native_pty_system()
                .openpty(PtySize {
                    rows: 24,
                    cols: 80,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .unwrap();
            let mut cmd = CommandBuilder::new(super::bin());
            cmd.args(args);
            cmd.env("XDG_RUNTIME_DIR", dir.to_str().unwrap());
            cmd.env("TERM", "xterm");
            let child = pair.slave.spawn_command(cmd).unwrap();
            drop(pair.slave);
            let mut reader = pair.master.try_clone_reader().unwrap();
            let writer = pair.master.take_writer().unwrap();
            let output = Arc::new(Mutex::new(String::new()));
            let sink = output.clone();
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                loop {
                    match std::io::Read::read(&mut reader, &mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => sink
                            .lock()
                            .unwrap()
                            .push_str(&String::from_utf8_lossy(&buf[..n])),
                    }
                }
            });
            Self {
                child,
                writer,
                output,
                cursor: 0,
                _master: pair.master,
            }
        }

        pub fn wait_for(&self, needle: &str) -> String {
            let start = Instant::now();
            loop {
                let snap = self.output.lock().unwrap().clone();
                if snap.contains(needle) {
                    return snap;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "never saw {needle:?} in:\n{snap}"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        /// Like `wait_for`, but only matches output produced *after* the
        /// previous `wait_for_new` returned. Use this when the same string
        /// (e.g. the cockpit prompt) appears more than once and the test must
        /// synchronize on a specific later occurrence — otherwise a whole-buffer
        /// `wait_for` matches the stale earlier one and races ahead.
        pub fn wait_for_new(&mut self, needle: &str) -> String {
            let start = Instant::now();
            loop {
                {
                    let snap = self.output.lock().unwrap();
                    let tail = snap.get(self.cursor..).unwrap_or("");
                    if let Some(rel) = tail.find(needle) {
                        self.cursor += rel + needle.len();
                        return snap.clone();
                    }
                }
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "never saw new {needle:?} past offset {}:\n{}",
                    self.cursor,
                    self.output.lock().unwrap()
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        /// Wait for the child to exit, with a watchdog: if it hasn't exited
        /// within 15s it's killed and the test panics with the captured output.
        /// A hung cockpit then fails in seconds as an ordinary test failure,
        /// instead of wedging until the CI job's 15-minute timeout.
        pub fn wait(&mut self) -> portable_pty::ExitStatus {
            let start = Instant::now();
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    return status;
                }
                if start.elapsed() > Duration::from_secs(15) {
                    let out = self.output.lock().unwrap().clone();
                    let _ = self.child.clone_killer().kill();
                    let _ = self.child.wait();
                    panic!("PtyClient did not exit within 15s; output so far:\n{out}");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

#[test]
fn raw_attach_drives_a_live_terminal_and_detaches() {
    let d = TestDaemon::start();
    d.ok(&["run", "export FERN_IO=tty"]);
    d.ok(&["run", "--detach", "cat"]);

    let mut c = pty::PtyClient::spawn(&d.dir, &["attach", "main"]);
    c.wait_for("driving cell");
    c.writer.write_all(b"hello-raw-pty\r").unwrap();
    c.writer.flush().unwrap();
    c.wait_for("hello-raw-pty");
    // Ctrl+] detaches; the cell keeps running and the cockpit exits.
    c.writer.write_all(&[0x1d]).unwrap();
    c.writer.flush().unwrap();
    c.wait_for("detached");
    let status = c.wait();
    assert!(status.success());

    // The cat cell survived the detach; kill it from a plain client.
    d.ok(&["kill", "2"]);
}

#[test]
fn cockpit_on_tty_branch_runs_typed_commands_raw() {
    let d = TestDaemon::start();
    d.ok(&["run", "export FERN_IO=tty"]);

    let mut c = pty::PtyClient::spawn(&d.dir, &["attach", "main"]);
    c.wait_for_new("(on main) >");
    // A typed external command on a tty branch launches detached + raw;
    // `tty` proves the program really got a terminal. The trailing bytes after
    // \r arrive in the same chunk and must hand off from the cooked line
    // reader to the raw follower without loss.
    c.writer.write_all(b"tty\rxx").unwrap();
    c.writer.flush().unwrap();
    c.wait_for_new("/dev/");
    // Must wait for the *post-Completed* prompt, not the startup one — typing
    // before the raw follower exits would send those bytes to the cell, not
    // the cockpit. `wait_for_new` ignores the earlier prompt already in buffer.
    c.wait_for_new("(on main) >");
    // Builtins stay inline on a tty branch.
    c.writer.write_all(b"cd /tmp\r").unwrap();
    c.writer.flush().unwrap();
    c.writer.write_all(b":quit\r").unwrap();
    c.writer.flush().unwrap();
    let status = c.wait();
    assert!(status.success());
}

#[test]
fn detaching_from_a_typed_command_leaves_the_cockpit() {
    let d = TestDaemon::start();
    d.ok(&["run", "export FERN_IO=tty"]);

    let mut c = pty::PtyClient::spawn(&d.dir, &["attach", "main"]);
    c.wait_for("(on main) >");
    // Type a long-running command, then Ctrl+] out of it: the cockpit exits
    // (typed-command detach is "leave the cockpit", same as a tip detach).
    c.writer.write_all(b"cat\r").unwrap();
    c.writer.flush().unwrap();
    c.wait_for("driving cell");
    c.writer.write_all(&[0x1d]).unwrap();
    c.writer.flush().unwrap();
    let status = c.wait();
    assert!(status.success());
    // The cat cell is still alive; clean it up.
    let tree = d.ok(&["tree"]);
    let id = tree
        .lines()
        .find(|l| l.contains(" cat "))
        .and_then(|l| l.trim_start().strip_prefix('#'))
        .and_then(|l| l.split_whitespace().next())
        .unwrap()
        .to_string();
    d.ok(&["kill", &id]);
}

#[test]
fn raw_attach_sees_completion_when_cell_killed_elsewhere() {
    let d = TestDaemon::start();
    d.ok(&["run", "export FERN_IO=tty"]);
    d.ok(&["run", "--detach", "cat"]);

    let mut c = pty::PtyClient::spawn(&d.dir, &["attach", "main"]);
    c.wait_for("driving cell");
    d.ok(&["kill", "2"]); // killed from another client → Completed flows to attacher
    c.wait_for("(on main) >"); // follow() returns Completed, cockpit prompts
    c.writer.write_all(b":quit\r").unwrap();
    c.writer.flush().unwrap();
    let status = c.wait();
    assert!(status.success());
}

// ---------- Client resilience against a broken daemon ----------------------

/// A fake daemon that accepts one connection, optionally reads a line, sends
/// a canned response (or nothing), and closes.
fn mock_daemon(dir: &Path, reply: Option<&'static str>) {
    let sock = dir.join("fern.sock");
    let listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { break };
            let mut line = String::new();
            let _ = BufReader::new(conn.try_clone().unwrap()).read_line(&mut line);
            if let Some(r) = reply {
                let _ = conn.write_all(r.as_bytes());
                let _ = conn.write_all(b"\n");
            }
            // close
        }
    });
}

#[test]
fn client_rejects_unexpected_responses() {
    let dir = std::env::temp_dir().join(format!(
        "fern-it-mock-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    mock_daemon(&dir, Some(r#"{"kind":"ok"}"#));
    for args in [&["tree"][..], &["branch"], &["run", "echo hi"]] {
        let out = Command::new(bin())
            .args(args)
            .env("XDG_RUNTIME_DIR", &dir)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(
            !out.status.success(),
            "{args:?} should reject an Ok-for-everything daemon"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn client_reports_connection_closed_early() {
    let dir = std::env::temp_dir().join(format!(
        "fern-it-mock-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    mock_daemon(&dir, None); // accept, read, close without replying
    for args in [
        &["tree"][..],
        &["branch"],
        &["run", "echo hi"],
        &["send", "main", "x"],
    ] {
        let out = Command::new(bin())
            .args(args)
            .env("XDG_RUNTIME_DIR", &dir)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!out.status.success(), "{args:?} should fail on early close");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("closed") || stderr.contains("error"),
            "{args:?} stderr: {stderr}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------- Daemon edge paths ----------------------------------------------

#[test]
fn submit_on_running_parent_is_rejected() {
    let d = TestDaemon::start();
    d.ok(&["run", "--detach", "sleep 5"]);
    let (_o, e) = d.err(&["run", "--parent", "1", "echo too-soon"]);
    assert!(e.contains("hasn't finished"), "stderr: {e}");
    d.ok(&["kill", "1"]);
}

#[test]
fn eval_errors_become_exit_2_cells() {
    let d = TestDaemon::start();
    // Inline: the parse error streams to the client and exits 2.
    let out = d.fern().args(["run", "echo $(oops"]).output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("unterminated"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Detached: same, recorded in the tree. Distinct marker so we don't match
    // the inline error cell above.
    d.ok(&["run", "--parent", "0", "--detach", "echo $(detached-oops"]);
    let start = Instant::now();
    loop {
        let tree = d.ok(&["tree"]);
        if tree
            .lines()
            .any(|l| l.contains("detached-oops") && l.contains("exit 2"))
        {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(5), "tree: {tree}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn fresh_flag_discards_the_store() {
    let d = TestDaemon::start();
    d.ok(&["run", "echo doomed-history"]);
    let dir = d.stop();

    // Restart with --fresh: the old tree is gone.
    let child = Command::new(bin())
        .arg("daemon")
        .arg("--store")
        .arg(dir.join("tree.jsonl"))
        .arg("--fresh")
        .env("XDG_RUNTIME_DIR", &dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let sock = dir.join("fern.sock");
    let start = Instant::now();
    while !sock.exists() {
        assert!(start.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(10));
    }
    let d2 = TestDaemon {
        dir,
        child: Some(child),
    };
    assert!(!d2.ok(&["tree"]).contains("doomed-history"));
}

#[test]
fn sigint_also_shuts_down_cleanly() {
    let d = TestDaemon::start();
    d.ok(&["run", "echo before-int"]);
    let pid = d.child.as_ref().unwrap().id().to_string();
    let _ = Command::new("kill").args(["-INT", &pid]).status();
    // The daemon exits and removes its socket.
    let start = Instant::now();
    while d.dir.join("fern.sock").exists() {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "socket not removed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn tty_pipeline_is_not_attachable() {
    let d = TestDaemon::start();
    d.ok(&["run", "export FERN_IO=tty"]);
    // A pipeline on a tty branch runs pipe-mode: tty cell, but no PTY ever
    // registers. Attach waits for the verdict and reports it.
    d.ok(&["run", "--detach", "sleep 0.4 | cat"]);
    let (_o, e) = d.err(&["send", "main", "anything"]);
    assert!(e.contains("no attachable terminal process"), "stderr: {e}");
}

#[test]
fn empty_branch_name_is_rejected() {
    let d = TestDaemon::start();
    let (_o, e) = d.err(&["branch", "new", ""]);
    assert!(e.contains("can't be empty"), "stderr: {e}");
}

// ---------- Fault injection -------------------------------------------------

#[test]
fn slow_subscriber_lag_recovery() {
    // Tiny broadcast buffer so a stalled subscriber lags quickly.
    let dir = std::env::temp_dir().join(format!(
        "fern-it-lag-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let child = Command::new(bin())
        .arg("daemon")
        .arg("--store")
        .arg(dir.join("tree.jsonl"))
        .env("XDG_RUNTIME_DIR", &dir)
        .env("FERN_EVENT_BUFFER", "8")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let sock = dir.join("fern.sock");
    let start = Instant::now();
    while !sock.exists() {
        assert!(start.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(10));
    }
    let d = TestDaemon {
        dir,
        child: Some(child),
    };

    // Subscribe but don't read: the daemon's forwarder fills the socket
    // buffer, blocks, and falls behind the 8-event window → Lagged arm.
    // Push enough bytes to actually fill a unix socket buffer (~200KB).
    let mut stalled = std::os::unix::net::UnixStream::connect(d.dir.join("fern.sock")).unwrap();
    stalled.write_all(b"{\"kind\":\"subscribe\"}\n").unwrap();
    std::thread::sleep(Duration::from_millis(100));
    d.ok(&["run", "bash -c 'yes fern-flood | head -c 500000'"]);
    for i in 0..10 {
        d.ok(&["run", &format!("echo flood-{i}")]);
    }
    // The stalled subscriber starts reading: connection must still be alive.
    stalled
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stalled);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(!line.is_empty(), "subscription died after lagging");
    // Drop the half-read subscription, then generate one more event: the
    // daemon's forwarder hits a write error and logs it without falling over.
    drop(reader);
    assert_eq!(d.ok(&["run", "echo after-flood"]).trim(), "after-flood");
    assert_eq!(d.ok(&["run", "echo still-fine"]).trim(), "still-fine");

    // Same trick against a raw *attach* loop: attach to a live tty cell,
    // stall, flood — the attach forwarder's Lagged arm must also recover.
    d.ok(&["run", "export FERN_IO=tty"]);
    d.ok(&["run", "--detach", "cat"]);
    let tree = d.ok(&["tree"]);
    let cat_id = tree
        .lines()
        .find(|l| l.trim_start().starts_with('#') && l.contains(" cat "))
        .and_then(|l| l.trim_start().strip_prefix('#'))
        .and_then(|l| l.split_whitespace().next())
        .unwrap()
        .to_string();
    let mut stalled_attach =
        std::os::unix::net::UnixStream::connect(d.dir.join("fern.sock")).unwrap();
    stalled_attach
        .write_all(format!("{{\"kind\":\"attach\",\"id\":{cat_id}}}\n").as_bytes())
        .unwrap();
    std::thread::sleep(Duration::from_millis(100)); // let it reach the event loop
    d.ok(&[
        "run",
        "--parent",
        "0",
        "bash -c 'yes lag2 | head -c 500000'",
    ]);
    for _ in 0..10 {
        d.ok(&["run", "--parent", "0", "echo more"]);
    }
    stalled_attach
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stalled_attach);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(!line.is_empty(), "attach died after lagging");
    d.ok(&["kill", &cat_id]);
}

#[test]
fn attach_survives_daemon_sigkill() {
    let d = TestDaemon::start();
    d.ok(&["run", "export FERN_IO=tty"]);
    d.ok(&["run", "--detach", "cat"]);

    let mut c = pty::PtyClient::spawn(&d.dir, &["attach", "main"]);
    c.wait_for("driving cell");
    // Hard-kill the daemon mid-attach: the client must notice and exit
    // (with an error), not wedge in raw mode.
    let pid = d.child.as_ref().unwrap().id().to_string();
    let _ = Command::new("kill").args(["-9", &pid]).status();
    std::thread::sleep(Duration::from_millis(200));
    // Typing after the daemon died exercises the client's write-failure arm.
    let _ = c.writer.write_all(b"too late\r");
    let _ = c.writer.flush();
    let status = c.wait();
    assert!(
        !status.success(),
        "client should exit nonzero after daemon death"
    );
}

// ---------- Scripted daemon: already-completed tip ---------------------------

/// A daemon impostor for one scenario: the branch tip reports running, but by
/// the time the client attaches the cell has finished. Forces the cockpit
/// through stream_until_done's snapshot-replay path deterministically.
fn scripted_completed_tip_daemon(dir: &std::path::Path) {
    use std::sync::atomic::AtomicU64 as Counter;
    let listener = std::os::unix::net::UnixListener::bind(dir.join("fern.sock")).unwrap();
    let lb_count = std::sync::Arc::new(Counter::new(0));
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { break };
            let mut line = String::new();
            let _ = BufReader::new(conn.try_clone().unwrap()).read_line(&mut line);
            if line.contains("list_branches") {
                let n = lb_count.fetch_add(1, Ordering::SeqCst);
                // First ask: tip running (so the cockpit follows it).
                // Later asks: finished (so the cockpit prompts and exits).
                let running = n == 0;
                let resp = format!(
                    "{{\"kind\":\"branches\",\"branches\":[{{\"name\":\"main\",\"tip\":1,\"tip_hash\":null,\"running\":{running},\"tty\":false}}]}}\n"
                );
                let _ = conn.write_all(resp.as_bytes());
                held.push(conn); // keep open; cockpit may not re-read
            } else if line.contains("attach") {
                // The cell finished a moment ago.
                let _ = conn
                    .write_all(b"{\"kind\":\"error\",\"message\":\"cell #1 is not running\"}\n");
            } else if line.contains("get_cell") {
                // Response::Cell is an internally-tagged newtype: the snapshot
                // fields sit inline next to "kind", not nested.
                let _ = conn.write_all(
                    b"{\"kind\":\"cell\",\"id\":1,\"parent\":0,\"submitter\":\"t\",\"source\":\"echo done\",\"exit_code\":0,\"duration_ms\":7,\"stdout\":\"stored-output\\n\",\"stderr\":\"\",\"hash\":\"abc123\"}\n",
                );
            } else if line.contains("subscribe") {
                held.push(conn); // hold open silently
            }
        }
    });
}

#[test]
fn cockpit_replays_snapshot_when_tip_finished_during_attach() {
    let dir = std::env::temp_dir().join(format!(
        "fern-it-script-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("fern-branch"), "main").unwrap();
    scripted_completed_tip_daemon(&dir);

    let mut child = Command::new(bin())
        .arg("attach")
        .env("XDG_RUNTIME_DIR", &dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // Close stdin: after the snapshot replay the cockpit prompts and exits.
    drop(child.stdin.take());
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("stored-output") && stdout.contains("[#1 exit 0 7ms]"),
        "snapshot replay missing: {stdout}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------- More scripted-daemon scenarios ----------------------------------

/// Mock where attach succeeds (raw mode engages) but the daemon then sends an
/// Error event. The tip reports running only once so the cockpit exits to a
/// prompt afterwards. Exercises follow()'s mid-attach error arm.
fn scripted_attach_error_daemon(dir: &std::path::Path) {
    use std::sync::atomic::AtomicU64 as Counter;
    let listener = std::os::unix::net::UnixListener::bind(dir.join("fern.sock")).unwrap();
    let lb_count = std::sync::Arc::new(Counter::new(0));
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { break };
            let mut line = String::new();
            let _ = BufReader::new(conn.try_clone().unwrap()).read_line(&mut line);
            if line.contains("list_branches") {
                let running = lb_count.fetch_add(1, Ordering::SeqCst) == 0;
                let resp = format!(
                    "{{\"kind\":\"branches\",\"branches\":[{{\"name\":\"main\",\"tip\":1,\"tip_hash\":null,\"running\":{running},\"tty\":false}}]}}\n"
                );
                let _ = conn.write_all(resp.as_bytes());
                held.push(conn);
            } else if line.contains("attach") {
                // Attach accepted... then the daemon reports an error event.
                let _ = conn.write_all(b"{\"kind\":\"ok\"}\n");
                let _ = conn.write_all(
                    b"{\"kind\":\"error\",\"message\":\"synthetic mid-attach error\"}\n",
                );
                held.push(conn);
            } else if line.contains("subscribe") {
                held.push(conn);
            }
        }
    });
}

#[test]
fn follow_handles_mid_attach_error_event() {
    let dir = std::env::temp_dir().join(format!(
        "fern-it-script2-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("fern-branch"), "main").unwrap();
    scripted_attach_error_daemon(&dir);

    // Needs a PTY: follow only reaches the event loop once raw mode engages.
    let mut c = pty::PtyClient::spawn(&dir, &["attach"]);
    c.wait_for("synthetic mid-attach error");
    c.wait_for("(on main) >");
    c.writer.write_all(b":quit\r").unwrap();
    c.writer.flush().unwrap();
    let status = c.wait();
    assert!(status.success());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Mock where the tip is running pipe-mode; attach is refused, the snapshot
/// fetch errors, and the held subscription then streams stderr + Completed.
fn scripted_streaming_daemon(dir: &std::path::Path) {
    use std::sync::atomic::AtomicU64 as Counter;
    let listener = std::os::unix::net::UnixListener::bind(dir.join("fern.sock")).unwrap();
    let lb_count = std::sync::Arc::new(Counter::new(0));
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { break };
            let mut line = String::new();
            let _ = BufReader::new(conn.try_clone().unwrap()).read_line(&mut line);
            if line.contains("list_branches") {
                let n = lb_count.fetch_add(1, Ordering::SeqCst);
                let running = n == 0;
                let resp = format!(
                    "{{\"kind\":\"branches\",\"branches\":[{{\"name\":\"main\",\"tip\":1,\"tip_hash\":null,\"running\":{running},\"tty\":false}}]}}\n"
                );
                let _ = conn.write_all(resp.as_bytes());
                held.push(conn);
            } else if line.contains("attach") {
                let _ = conn
                    .write_all(b"{\"kind\":\"error\",\"message\":\"cell #1 is not running\"}\n");
            } else if line.contains("get_cell") {
                let _ = conn.write_all(b"{\"kind\":\"error\",\"message\":\"no such cell\"}\n");
            } else if line.contains("subscribe") {
                // First subscription is the cockpit feed (hold silently).
                // The second belongs to stream_until_done: feed it chunks.
                // Response::Event flattens: the inner CellEvent's own tag is
                // "event", so the wire shape is {"kind":"event","event":"..."}.
                if held.iter().len() >= 2 {
                    let _ = conn.write_all(
                        b"{\"kind\":\"event\",\"event\":\"output_chunk\",\"id\":1,\"stream\":\"stderr\",\"data\":\"late-noise\\n\"}\n",
                    );
                    let _ = conn.write_all(
                        b"{\"kind\":\"event\",\"event\":\"completed\",\"id\":1,\"exit_code\":3,\"duration_ms\":9,\"hash\":null}\n",
                    );
                }
                held.push(conn);
            }
        }
    });
}

#[test]
fn stream_until_done_renders_stderr_and_survives_snapshot_failure() {
    let dir = std::env::temp_dir().join(format!(
        "fern-it-script3-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("fern-branch"), "main").unwrap();
    scripted_streaming_daemon(&dir);

    let mut child = Command::new(bin())
        .arg("attach")
        .env("XDG_RUNTIME_DIR", &dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    drop(child.stdin.take());
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stdout.contains("[#1 exit 3 9ms]"),
        "stdout: {stdout}\nstderr: {stderr}"
    );
    assert!(stderr.contains("late-noise"), "stderr: {stderr}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn detached_submit_handles_broken_daemons() {
    // Error reply.
    let dir = std::env::temp_dir().join(format!(
        "fern-it-mockd-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    mock_daemon(&dir, Some(r#"{"kind":"error","message":"synthetic"}"#));
    let out = Command::new(bin())
        .args(["run", "--detach", "echo hi"])
        .env("XDG_RUNTIME_DIR", &dir)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success());
    let _ = std::fs::remove_dir_all(&dir);

    // Closed before Started.
    let dir = std::env::temp_dir().join(format!(
        "fern-it-mockd-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    mock_daemon(&dir, None);
    let out = Command::new(bin())
        .args(["run", "--detach", "echo hi"])
        .env("XDG_RUNTIME_DIR", &dir)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("closed"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------- StdinFeed edges --------------------------------------------------

#[test]
fn cockpit_runs_final_unterminated_line_on_eof() {
    let d = TestDaemon::start();
    // No trailing newline: the pump flushes the partial line at EOF.
    let (stdout, _) = cockpit(&d, "attach", "echo no-trailing-newline");
    assert!(stdout.contains("no-trailing-newline"), "got: {stdout}");
}

// ---------- Persistence guard -------------------------------------------------

#[test]
fn branch_deleted_while_cell_runs_is_not_resurrected() {
    let d = TestDaemon::start();
    d.ok(&["branch", "new", "doomed"]);
    d.ok(&["run", "--branch", "doomed", "--detach", "sleep 0.5"]);
    d.ok(&["branch", "rm", "doomed"]);
    // Wait for the cell to complete; its SetBranch must be skipped.
    std::thread::sleep(Duration::from_millis(800));
    assert!(!d.ok(&["branch"]).contains("doomed"));
    // ...and stays gone across a restart (the log has no dangling SetBranch).
    let dir = d.stop();
    let d2 = TestDaemon::start_in(dir);
    assert!(!d2.ok(&["branch"]).contains("doomed"));
}

// ---------- Endgame coverage scenarios --------------------------------------

#[test]
fn inline_and_detached_stderr_is_captured() {
    let d = TestDaemon::start();
    // Inline: stderr streams through and lands in the cell.
    let out = d
        .fern()
        .args(["run", "bash -c 'echo only-err >&2'"])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("only-err"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // Detached: stderr chunks accumulate into the stored result.
    d.ok(&["run", "--detach", "bash -c 'echo det-err >&2'"]);
    let start = Instant::now();
    loop {
        let tree = d.ok(&["tree"]);
        if tree
            .lines()
            .any(|l| l.contains("det-err") && l.contains("exit 0"))
        {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(5), "tree: {tree}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn blank_lines_and_junk_in_protocol_are_tolerated() {
    let d = TestDaemon::start();
    // Leading blank lines are skipped by the daemon.
    let sock = d.dir.join("fern.sock");
    let mut stream = std::os::unix::net::UnixStream::connect(&sock).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(b"\n\n{\"kind\":\"get_cell\",\"id\":0}\n")
        .unwrap();
    let mut line = String::new();
    BufReader::new(stream.try_clone().unwrap())
        .read_line(&mut line)
        .unwrap();
    assert!(line.contains("\"cell\""), "got: {line}");
}

#[test]
fn attach_session_ignores_garbage_and_foreign_requests() {
    let d = TestDaemon::start();
    d.ok(&["run", "export FERN_IO=tty"]);
    d.ok(&["run", "--detach", "cat"]);
    // Raw attach, then send garbage + a non-Input request + Input for the
    // wrong id: all must be ignored without killing the session.
    let sock = d.dir.join("fern.sock");
    let mut stream = std::os::unix::net::UnixStream::connect(&sock).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(b"{\"kind\":\"attach\",\"id\":2}\n")
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    assert!(line.contains("\"ok\""), "got: {line}");
    stream.write_all(b"not json at all\n").unwrap();
    stream.write_all(b"{\"kind\":\"get_tree\"}\n").unwrap(); // non-Input: ignored
    stream
        .write_all(b"{\"kind\":\"input\",\"id\":999,\"data\":\"wrong id\"}\n")
        .unwrap();
    stream
        .write_all(b"{\"kind\":\"input\",\"id\":2,\"data\":\"right-id\\n\"}\n")
        .unwrap();
    // cat echoes the good input back through the PTY → an event arrives.
    let mut echoed = String::new();
    reader.read_line(&mut echoed).unwrap();
    assert!(echoed.contains("right-id"), "got: {echoed}");
    d.ok(&["kill", "2"]);
}

#[test]
fn cockpit_feed_renders_stderr_and_partial_lines() {
    let d = TestDaemon::start();
    let mut child = d
        .fern()
        .arg("attach")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    std::thread::sleep(Duration::from_millis(300));
    // Foreign cell with stderr output and no trailing newline.
    d.ok(&[
        "run",
        "--who",
        "other",
        "bash -c 'echo noisy >&2; echo -n partial'",
    ]);
    std::thread::sleep(Duration::from_millis(300));
    stdin.write_all(b":quit\n").unwrap();
    drop(stdin);
    let out = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stdout.contains("partial"), "stdout: {stdout}");
    assert!(stderr.contains("noisy"), "stderr: {stderr}");
}

#[test]
fn cockpit_inline_stderr_and_partial_output() {
    let d = TestDaemon::start();
    let (stdout, stderr) = cockpit(
        &d,
        "attach",
        "bash -c 'echo cerr >&2; echo -n no-newline'\n:quit\n",
    );
    assert!(stdout.contains("no-newline"), "stdout: {stdout}");
    assert!(stderr.contains("cerr"), "stderr: {stderr}");
}

#[test]
fn deleting_the_current_branch_falls_back_to_main() {
    let d = TestDaemon::start();
    d.ok(&["branch", "new", "here"]);
    d.ok(&["switch", "here"]);
    let out = d.ok(&["branch", "rm", "here"]);
    assert!(out.contains("switched to 'main'"), "got: {out}");
    assert_eq!(
        std::fs::read_to_string(d.dir.join("fern-branch"))
            .unwrap()
            .trim(),
        "main"
    );
}

#[test]
fn branch_list_marks_running_tips() {
    let d = TestDaemon::start();
    d.ok(&["run", "--detach", "sleep 2"]);
    assert!(d.ok(&["branch"]).contains("running"));
    d.ok(&["kill", "1"]);
}

#[test]
fn branch_new_with_ghost_cursor_requires_at() {
    let d = TestDaemon::start();
    std::fs::write(d.dir.join("fern-branch"), "ghost").unwrap();
    let (_o, e) = d.err(&["branch", "new", "orphan"]);
    assert!(e.contains("pass --at"), "stderr: {e}");
}

#[test]
fn watch_renders_stderr_and_error_lines() {
    let d = TestDaemon::start();
    let watch = d
        .fern()
        .args(["watch"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    d.ok(&["run", "bash -c 'echo werr >&2'"]);
    std::thread::sleep(Duration::from_millis(200));
    let dir = d.stop();
    let out = watch.wait_with_output().unwrap();
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("werr"),
        "watch stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _ = std::fs::remove_dir_all(dir);
}

// ---------- Mock daemon matrix ----------------------------------------------

/// Mock replying valid branches, then junk to everything else. Drives the
/// "unexpected response" arms in send/attach flows that need a branch lookup
/// to succeed first.
fn mock_branches_then_junk(dir: &Path) {
    let listener = std::os::unix::net::UnixListener::bind(dir.join("fern.sock")).unwrap();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { break };
            let mut line = String::new();
            let _ = BufReader::new(conn.try_clone().unwrap()).read_line(&mut line);
            if line.contains("list_branches") {
                let _ = conn.write_all(
                    b"{\"kind\":\"branches\",\"branches\":[{\"name\":\"main\",\"tip\":1,\"tip_hash\":null,\"running\":true,\"tty\":true}]}\n",
                );
            } else {
                // Valid Response, wrong variant for every caller.
                let _ = conn.write_all(b"{\"kind\":\"tree\",\"cells\":[]}\n");
            }
        }
    });
}

#[test]
fn unexpected_variant_responses_are_rejected_everywhere() {
    // Junk-replying daemon: every verb must fail cleanly, not hang or panic.
    let dir = std::env::temp_dir().join(format!(
        "fern-it-junk-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    mock_daemon(
        &dir,
        Some(
            r#"{"kind":"cell","id":0,"parent":null,"submitter":"x","source":"","exit_code":0,"duration_ms":0,"stdout":"","stderr":"","hash":null}"#,
        ),
    );
    for args in [
        &["tree"][..],
        &["branch"],
        &["branch", "rm", "x"],
        &["branch", "new", "y", "--at", "0"],
        &["branch", "rename", "a", "b"],
    ] {
        let out = Command::new(bin())
            .args(args)
            .env("XDG_RUNTIME_DIR", &dir)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!out.status.success(), "{args:?} accepted junk");
    }
    let _ = std::fs::remove_dir_all(&dir);

    // Branches-then-junk daemon: send + attach get past the branch lookup,
    // then hit the unexpected-variant arms.
    let dir = std::env::temp_dir().join(format!(
        "fern-it-junk2-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("fern-branch"), "main").unwrap();
    mock_branches_then_junk(&dir);
    for args in [&["send", "main", "hello"][..], &["attach"]] {
        let out = Command::new(bin())
            .args(args)
            .env("XDG_RUNTIME_DIR", &dir)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!out.status.success(), "{args:?} accepted junk");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("expected Ok"),
            "{args:?} stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn error_replying_daemon_propagates_messages() {
    let dir = std::env::temp_dir().join(format!(
        "fern-it-errd-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    mock_daemon(&dir, Some(r#"{"kind":"error","message":"synthetic-err"}"#));
    for args in [&["tree"][..], &["branch"], &["branch", "rm", "x"]] {
        let out = Command::new(bin())
            .args(args)
            .env("XDG_RUNTIME_DIR", &dir)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!out.status.success());
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("synthetic-err"),
            "{args:?} stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    // watch renders the error line and exits cleanly when the mock hangs up.
    let out = Command::new(bin())
        .args(["watch"])
        .env("XDG_RUNTIME_DIR", &dir)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("synthetic-err"),
        "watch stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _ = std::fs::remove_dir_all(&dir);
}
