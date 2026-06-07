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
        /// Parent cell (defaults to the last cell submitted by anyone)
        #[arg(short, long)]
        parent: Option<u64>,
        /// Who is submitting (defaults to $USER)
        #[arg(short, long)]
        who: Option<String>,
        /// Start the cell in the background; return immediately with its id.
        /// Output streams via `fern watch`; terminate with `fern kill <id>`.
        #[arg(short, long)]
        detach: bool,
        /// Run the cell under a PTY (so isatty-aware programs see a terminal).
        /// Required for `fern attach`. Implies --detach.
        #[arg(short, long)]
        interactive: bool,
        /// Command line (joined with spaces)
        source: Vec<String>,
    },
    /// Kill a running detached cell
    Kill { id: u64 },
    /// Attach to a running interactive cell (bidirectional: raw-mode terminal)
    Attach { id: u64 },
    /// Tail every cell event from every client
    Watch,
    /// Dump the cell tree
    Tree,
    /// Standalone REPL on the cell tree (no daemon, single-user)
    Repl,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Daemon => daemon::run().await,
        Cmd::Run { parent, who, detach, interactive, source } => {
            let line = source.join(" ");
            if line.trim().is_empty() {
                anyhow::bail!("no command");
            }
            if interactive || detach {
                let id = client::submit_detached(parent, who, line, interactive).await?;
                if interactive {
                    println!("interactive: cell #{id} running; attach with `fern attach {id}`");
                } else {
                    println!("detached: cell #{id} running");
                }
                Ok(())
            } else {
                let code = client::run(parent, who, line).await?;
                std::process::exit(code);
            }
        }
        Cmd::Kill { id } => client::kill(id).await,
        Cmd::Attach { id } => client::attach(id).await,
        Cmd::Watch => client::watch().await,
        Cmd::Tree => client::tree().await,
        Cmd::Repl => repl::run().await,
    }
}
