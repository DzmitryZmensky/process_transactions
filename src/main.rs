use anyhow::{self, bail, Context, Ok};

mod accounting;

fn main() -> anyhow::Result<()> {
    let cmdline_params = parse_cmdline().context("cannot parse command line parametes")?;

    let ledger = accounting::load_transactions(&cmdline_params.transactions_fpath)
        .context("cannot load transactions")?;

    accounting::print_accounts(&ledger).context("cannot print accounts")?;

    Ok(())
}

struct CmdlineParams {
    transactions_fpath: String,
}

fn parse_cmdline() -> anyhow::Result<CmdlineParams> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        bail!("expected parameter: <file_path>");
    } else {
        Ok(CmdlineParams {
            transactions_fpath: args[1].clone(),
        })
    }
}
