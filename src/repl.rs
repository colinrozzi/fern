//! Interactive REPL — a thin frontend over the daemon's client API.
//!
//! No in-process tree; every submission goes to the daemon. The cursor lives
//! in the shared XDG cursor file, so `fern run` invocations from other
//! terminals stay in sync.

use anyhow::{Result, anyhow};
use std::io::Write;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::client;
use crate::tree::CellId;
use crate::wire::{Stream, TreeSnapshot};

pub async fn run() -> Result<()> {
    let mut cursor: CellId = client::read_cursor();

    println!("fern repl — type :help for commands, :quit to exit");

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    loop {
        print!("(at #{cursor}) > ");
        std::io::stdout().flush().ok();

        let Some(line) = lines.next_line().await? else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix(':') {
            match handle_meta(&mut cursor, rest).await {
                Ok(MetaResult::Continue) => {}
                Ok(MetaResult::Quit) => break,
                Err(e) => println!("error: {e}"),
            }
            continue;
        }

        let who = std::env::var("USER").unwrap_or_else(|_| "?".into());
        match client::submit_streaming(Some(cursor), Some(who), trimmed.to_string(), |stream, data| {
            match stream {
                Stream::Stdout => {
                    print!("{data}");
                    std::io::stdout().flush().ok();
                }
                Stream::Stderr => {
                    eprint!("{data}");
                    std::io::stderr().flush().ok();
                }
            }
        })
        .await
        {
            Ok(snap) => {
                // Ensure the exit line starts on its own line.
                let last = snap
                    .stderr
                    .chars()
                    .last()
                    .or_else(|| snap.stdout.chars().last());
                if matches!(last, Some(c) if c != '\n') {
                    println!();
                }
                let status = match snap.exit_code {
                    Some(code) => format!("exit {code}"),
                    None => "running".into(),
                };
                println!("[#{} {status} {}ms]", snap.id, snap.duration_ms);
                cursor = snap.id;
            }
            Err(e) => println!("error: {e}"),
        }
    }
    Ok(())
}

enum MetaResult {
    Continue,
    Quit,
}

async fn handle_meta(cursor: &mut CellId, line: &str) -> Result<MetaResult> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    let cmd = parts.first().copied().unwrap_or("");
    match cmd {
        "quit" | "q" | "exit" => return Ok(MetaResult::Quit),
        "help" | "h" | "" => {
            println!(":tree            show the cell tree (from daemon)");
            println!(":at              show current cursor");
            println!(":cd <id>         move cursor to cell <id> (branch from there next)");
            println!(":quit / :q       exit");
        }
        "at" => println!("cursor at cell #{cursor}"),
        "cd" => {
            let id_str = parts.get(1).ok_or_else(|| anyhow!(":cd needs a cell id"))?;
            let id: CellId = id_str.parse().map_err(|e| anyhow!("bad id: {e}"))?;
            // Validate against the daemon's tree.
            let snap = client::fetch_tree().await?;
            if !snap.cells.iter().any(|c| c.id == id) {
                return Err(anyhow!("no such cell #{id}"));
            }
            *cursor = id;
            client::write_cursor(id);
            println!("cursor moved to #{id}");
        }
        "tree" => {
            let snap = client::fetch_tree().await?;
            print_tree(&snap, *cursor);
        }
        other => return Err(anyhow!("unknown meta command :{other} (try :help)")),
    }
    Ok(MetaResult::Continue)
}

fn print_tree(snap: &TreeSnapshot, cursor: CellId) {
    use std::collections::HashMap;
    let mut children: HashMap<Option<CellId>, Vec<&crate::wire::CellSnapshot>> = HashMap::new();
    for c in &snap.cells {
        children.entry(c.parent).or_default().push(c);
    }
    fn walk(
        children: &HashMap<Option<CellId>, Vec<&crate::wire::CellSnapshot>>,
        parent: Option<CellId>,
        depth: usize,
        cursor: CellId,
    ) {
        if let Some(cs) = children.get(&parent) {
            for c in cs {
                let indent = "  ".repeat(depth);
                let src = if c.source.is_empty() {
                    "(root)".to_string()
                } else {
                    c.source.clone()
                };
                let marker = if c.id == cursor { "  <-- cursor" } else { "" };
                let status = match c.exit_code {
                    Some(code) => format!("exit {code}"),
                    None => "running".into(),
                };
                let short_hash = c
                    .hash
                    .as_deref()
                    .map(|h| format!(" {}", &h[..h.len().min(7)]))
                    .unwrap_or_default();
                println!(
                    "{indent}#{}{short_hash} {} [{}] {status}{marker}",
                    c.id, src, c.submitter
                );
                walk(children, Some(c.id), depth + 1, cursor);
            }
        }
    }
    walk(&children, None, 0, cursor);
}
