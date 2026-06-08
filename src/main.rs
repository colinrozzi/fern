mod client;
mod daemon;
mod eval;
mod parse;
mod repl;
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
    Daemon,
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
    /// Attach to a running PTY cell at a branch's tip (or a cell id):
    /// bidirectional raw-mode terminal. Ctrl+] detaches without killing.
    Attach { target: String },
    /// Send one line of input to a running PTY cell (branch tip or cell id)
    Send { target: String, data: Vec<String> },
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
    /// Standalone REPL on the cell tree (no daemon, single-user)
    Repl,
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
        Cmd::Daemon => daemon::run().await,
        Cmd::Run { branch, parent, who, detach, source } => {
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
        Cmd::Attach { target } => client::attach(target).await,
        Cmd::Send { target, data } => client::send(target, data.join(" ")).await,
        Cmd::Watch => client::watch().await,
        Cmd::Tree => client::tree().await,
        Cmd::Branch { action } => match action {
            None => client::branch_list().await,
            Some(BranchAction::New { name, at }) => client::branch_new(name, at).await,
            Some(BranchAction::Rm { name }) => client::branch_rm(name).await,
            Some(BranchAction::Rename { from, to }) => client::branch_rename(from, to).await,
        },
        Cmd::Switch { name } => client::switch(name).await,
        Cmd::Repl => repl::run().await,
    }
}
