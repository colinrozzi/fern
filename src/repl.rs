//! Interactive REPL — a thin frontend over the daemon's client API.
//!
//! No in-process tree; every submission goes to the daemon. The current branch
//! lives in the shared XDG file, so `fern run` / `fern switch` from other
//! terminals stay in sync. Each command extends the current branch's tip.

use anyhow::{Result, anyhow};
use std::io::Write;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::client;
use crate::tree::CellId;
use crate::wire::{Stream, TreeSnapshot};

pub async fn run() -> Result<()> {
    let mut branch = client::read_current_branch();

    println!("fern repl — type :help for commands, :quit to exit");

    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();

    loop {
        print!("(on {branch}) > ");
        std::io::stdout().flush().ok();

        let Some(line) = lines.next_line().await? else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix(':') {
            match handle_meta(&mut branch, rest).await {
                Ok(MetaResult::Continue) => {}
                Ok(MetaResult::Quit) => break,
                Err(e) => println!("error: {e}"),
            }
            continue;
        }

        let who = std::env::var("USER").unwrap_or_else(|_| "?".into());
        match client::submit_streaming(
            Some(branch.clone()),
            None,
            Some(who),
            trimmed.to_string(),
            |stream, data| match stream {
                Stream::Stdout => {
                    print!("{data}");
                    std::io::stdout().flush().ok();
                }
                Stream::Stderr => {
                    eprint!("{data}");
                    std::io::stderr().flush().ok();
                }
            },
        )
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

async fn handle_meta(branch: &mut String, line: &str) -> Result<MetaResult> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    let cmd = parts.first().copied().unwrap_or("");
    match cmd {
        "quit" | "q" | "exit" => return Ok(MetaResult::Quit),
        "help" | "h" | "" => {
            println!(":tree              show the cell tree (from daemon)");
            println!(":branches          list branches");
            println!(":switch <name>     switch the current branch");
            println!(":at                show the current branch");
            println!(":quit / :q         exit");
        }
        "at" => println!("on branch '{branch}'"),
        "branches" => {
            let branches = client::fetch_branches().await?;
            for b in &branches {
                let marker = if &b.name == branch { "*" } else { " " };
                let state = if b.running {
                    "running".to_string()
                } else {
                    b.tip_hash
                        .as_deref()
                        .map(|h| h[..h.len().min(7)].to_string())
                        .unwrap_or_else(|| "-".to_string())
                };
                println!("{marker} {} #{} {state}", b.name, b.tip);
            }
        }
        "switch" => {
            let name = parts
                .get(1)
                .ok_or_else(|| anyhow!(":switch needs a branch name"))?;
            let branches = client::fetch_branches().await?;
            if !branches.iter().any(|b| b.name == *name) {
                return Err(anyhow!("no such branch '{name}'"));
            }
            *branch = name.to_string();
            client::write_current_branch(name);
            println!("switched to '{name}'");
        }
        "tree" => {
            let snap = client::fetch_tree().await?;
            print_tree(&snap);
        }
        other => return Err(anyhow!("unknown meta command :{other} (try :help)")),
    }
    Ok(MetaResult::Continue)
}

fn print_tree(snap: &TreeSnapshot) {
    use std::collections::HashMap;
    let mut children: HashMap<Option<CellId>, Vec<&crate::wire::CellSnapshot>> = HashMap::new();
    for c in &snap.cells {
        children.entry(c.parent).or_default().push(c);
    }
    fn walk(
        children: &HashMap<Option<CellId>, Vec<&crate::wire::CellSnapshot>>,
        parent: Option<CellId>,
        depth: usize,
    ) {
        if let Some(cs) = children.get(&parent) {
            for c in cs {
                let indent = "  ".repeat(depth);
                let src = if c.source.is_empty() {
                    "(root)".to_string()
                } else {
                    c.source.clone()
                };
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
                    "{indent}#{}{short_hash} {} [{}] {status}",
                    c.id, src, c.submitter
                );
                walk(children, Some(c.id), depth + 1);
            }
        }
    }
    walk(&children, None, 0);
}
