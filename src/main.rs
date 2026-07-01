mod client;
mod daemon;
mod eval;
mod mux;
mod parse;
mod store;
mod tree;
mod wire;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "fern", about = "shared shell session")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the daemon in the foreground
    Daemon {
        /// Path to the persistence log (defaults to $XDG_DATA_HOME/fern/tree.jsonl)
        #[arg(long)]
        store: Option<std::path::PathBuf>,
        /// Discard any existing log and start a brand-new tree
        #[arg(long)]
        fresh: bool,
    },
    /// Submit a command to the daemon and stream its output
    Run {
        /// Branch to run on (defaults to the current branch; see `fern switch`)
        #[arg(short, long)]
        branch: Option<String>,
        /// Parent cell. Defaults to the current branch's tip; pointing at a
        /// historical cell forks a new branch.
        #[arg(short, long)]
        parent: Option<u64>,
        /// Who is submitting (defaults to $USER)
        #[arg(short, long)]
        who: Option<String>,
        /// Start the cell in the background; return immediately with its id.
        /// Output streams via `fern watch`; terminate with `fern kill <id>`.
        /// On a `FERN_IO=tty` branch, a detached cell is attachable.
        #[arg(short, long)]
        detach: bool,
        /// Command line (joined with spaces)
        source: Vec<String>,
    },
    /// Kill a running detached cell
    Kill { id: u64 },
    /// Attach to a branch and work on it: a finished tip gives a cooked prompt
    /// (each line extends the branch); a live terminal tip drops you into raw
    /// mode. Defaults to the current branch. Ctrl+] / :quit to leave.
    Attach { target: Option<String> },
    /// Send one line of input to a running PTY cell (branch tip or cell id)
    Send { target: String, data: Vec<String> },
    /// Resize a running terminal cell's PTY (branch tip or cell id)
    Resize {
        target: String,
        rows: u16,
        cols: u16,
    },
    /// Tail every cell event from every client
    Watch,
    /// Dump the cell tree
    Tree,
    /// List branches, or manage them (new/rm/rename)
    Branch {
        #[command(subcommand)]
        action: Option<BranchAction>,
    },
    /// Switch the current branch (where the next `fern run` lands)
    Switch { name: String },
    /// Alias for `attach` on the current branch (cooked prompt cockpit).
    Repl,
    /// Open the terminal multiplexer: tiled panes, each a viewport onto a
    /// branch. Ctrl+a then %/" to split (forks a branch), o to switch, q to quit.
    Mux,
}

#[derive(Subcommand)]
enum BranchAction {
    /// Create a new branch
    New {
        name: String,
        /// Cell to base the branch on (defaults to the current branch's tip)
        #[arg(short, long)]
        at: Option<u64>,
    },
    /// Delete a branch
    Rm { name: String },
    /// Rename a branch, keeping its tip
    Rename { from: String, to: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Daemon { store, fresh } => daemon::run(store, fresh).await,
        Cmd::Run {
            branch,
            parent,
            who,
            detach,
            source,
        } => {
            let line = source.join(" ");
            if line.trim().is_empty() {
                anyhow::bail!("no command");
            }
            if detach {
                let id = client::submit_detached(branch, parent, who, line).await?;
                println!("detached: cell #{id} running");
                Ok(())
            } else {
                let code = client::run(branch, parent, who, line).await?;
                std::process::exit(code);
            }
        }
        Cmd::Kill { id } => client::kill(id).await,
        Cmd::Attach { target } => exit_after(client::cockpit(target).await),
        Cmd::Send { target, data } => client::send(target, data.join(" ")).await,
        Cmd::Resize { target, rows, cols } => client::resize(target, rows, cols).await,
        Cmd::Watch => client::watch().await,
        Cmd::Tree => client::tree().await,
        Cmd::Branch { action } => match action {
            None => client::branch_list().await,
            Some(BranchAction::New { name, at }) => client::branch_new(name, at).await,
            Some(BranchAction::Rm { name }) => client::branch_rm(name).await,
            Some(BranchAction::Rename { from, to }) => client::branch_rename(from, to).await,
        },
        Cmd::Switch { name } => client::switch(name).await,
        Cmd::Repl => exit_after(client::cockpit(None).await),
        Cmd::Mux => exit_after(mux::run().await),
    }
}

/// The cockpit's shared stdin pump may be parked in a blocking read that
/// tokio's runtime shutdown would wait on forever (an idle terminal never
/// yields the read). Exit explicitly instead — atexit handlers (including
/// coverage profile flushing) still run.
fn exit_after(res: anyhow::Result<()>) -> ! {
    match res {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("Error: {e:#}");
            std::process::exit(1);
        }
    }
}
