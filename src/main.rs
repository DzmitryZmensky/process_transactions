use std::io::stdout;

use anyhow::{self, bail, Context, Ok};

mod accounting;

fn main() -> anyhow::Result<()> {
    let cmdline_params = parse_cmdline()
        .context("cannot parse command line parametes")?;

    let ledger = accounting::load_transactions(&cmdline_params.transactions_fpath)
        .context("cannot load transactions")?;

    accounting::output_accounts(&ledger, &mut stdout())
        .context("cannot print accounts")?;

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

#[cfg(test)]
mod tests {
    use crate::accounting;

// see the rest of the unit tests in accounting.rs module

    #[test]
    fn happy_path_e2e() {
        let ledger = accounting::load_transactions("test_data/happy_path.csv").unwrap();
        
        let mut output = vec![];
        accounting::output_accounts(&ledger, &mut output).unwrap();
        let output_string = String::from_utf8(output).unwrap();
        let mut lines: Vec<&str> = output_string.split('\n').collect();
        lines[1..4].sort(); // sorting counteracts the fact that random seed in HashMaps causes different order every run
        
        assert_eq!(lines.len(), 5); // header + 3 accounts + empty line at the end 
        assert_eq!(lines[0], "client,available,held,total,locked");
        assert_eq!(lines[1], "1,1.0001,0,1.0001,false");
        assert_eq!(lines[2], "2,4.0005,0.0000,4.0005,false");
        assert_eq!(lines[3], "3,2.5005,0.0000,2.5005,true");
        assert_eq!(lines[4], "");
    }
}