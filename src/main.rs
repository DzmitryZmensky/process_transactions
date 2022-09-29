use std::{collections::HashMap, io};
use anyhow::{self, bail, Context, Ok};
use csv::Trim;
use serde::{Deserialize, Serialize};

fn main()  -> anyhow::Result<()> {
    let cmdline_params = parse_cmdline()
        .context("cannot parse command line parametes")?;
    
    let accounts = load_transactions(&cmdline_params.transactions_fpath)
        .context("cannot load transactions")?;

    print_accounts(&accounts)
        .context("cannot print accounts")?;

    Ok(())
}

struct CmdlineParams {
    transactions_fpath: String
}

type ClientIdType = u16;
type MoneyAmountType = f64;
type AccountsType = HashMap<ClientIdType, AccountData>;
type TransactionIdType = u32;

#[derive(Debug, Serialize)]
struct AccountData {
    client: ClientIdType, 
    available: MoneyAmountType, 
    held: MoneyAmountType, 
    total: MoneyAmountType,
    locked: bool,
}

#[derive(Debug, Deserialize)]
enum TransactionType {
    #[serde(rename = "deposit")]
    Deposit,

    #[serde(rename = "withdrawal")]
    Withdrawal,

    #[serde(rename = "dispute")]
    Dispute,

    #[serde(rename = "resolve")]
    Resolve,

    #[serde(rename = "chargeback")]
    Chargeback,
}

#[derive(Debug, Deserialize)]
struct TransactionData {
    #[serde(rename = "type")]
    type_: TransactionType,

    #[serde(rename = "client")]
    client: ClientIdType,

    #[serde(rename = "tx")]
    id: TransactionIdType,

    #[serde(rename = "amount")]
    amount: Option<MoneyAmountType>,
}

impl AccountData {
    fn new(client: ClientIdType) -> AccountData {
        AccountData { client, available: 0., held: 0., total: 0., locked: false }
    }
}

fn load_transactions(transactions_fpath: &str) -> anyhow::Result<AccountsType> {
    let mut accounts = AccountsType::new();
    let mut deposit_transactions_cache = HashMap::<TransactionIdType, MoneyAmountType>::new();

    let mut rdr = csv::ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(transactions_fpath)?;

    for line in rdr.deserialize() {
        let transaction: TransactionData = line?;
        //println!("{:?}", transaction);
        process_transaction(&mut accounts, &mut deposit_transactions_cache, &transaction)
            .context(format!("cannot process transaction: id={}",transaction.id))?;
    }

    Ok(accounts)
}

fn process_transaction(
    accounts: &mut HashMap<u16, AccountData>, 
    deposit_transactions_cache: &mut HashMap::<TransactionIdType, MoneyAmountType>, 
    transaction: &TransactionData) -> anyhow::Result<()> {

    let client = transaction.client;
    let account = accounts.entry(client).or_insert(AccountData::new(client));

    match transaction.type_ {
        TransactionType::Deposit => {
            let amount = transaction.amount
                .context("'deposit' transaction must have 'amount' value")?;
            account.available += amount;
            account.total += amount;
            // assumption: only 'deposit' transactions can be disputed
            deposit_transactions_cache.insert(transaction.id, amount);
        },
        TransactionType::Withdrawal => {
            let amount = transaction.amount
                .context("'withdrawal' transaction must have 'amount' value")?;
            if account.available >= amount && account.total >= amount {
                account.available -= amount;
                account.total -= amount;
            } 
            else {
                bail!("funds are not sufficient")
            }
        },
        TransactionType::Dispute => {
            if let Some(amount) = deposit_transactions_cache.get(&transaction.id){
                account.held += amount;
                // assumption: 'available' value can be negative, e.g. if the sequence is 'deposit', balance 'withdraw', 'dispute'
                account.available -= amount; 
            }
        },
        TransactionType::Resolve => {
            if let Some(amount) = deposit_transactions_cache.get(&transaction.id){
                // assumption: there are no duplicate 'resolve' transactions; 'resolve' reference matches 'dispute' one.
                account.held -= amount;
                account.available += amount; 
            };
        },
        TransactionType::Chargeback => {
            if let Some(amount) = deposit_transactions_cache.get(&transaction.id){
                // assumption: there are no duplicate 'resolve' transactions; 'resolve' reference matches 'dispute' one.
                account.held -= amount;
                account.total -= amount;
                account.locked = true;
            };
        },
    }

    Ok(())
}

fn parse_cmdline() -> anyhow::Result<CmdlineParams> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
       bail!("expected parameter: <file_path>");
    } else {
        Ok(CmdlineParams { transactions_fpath: args[1].clone() })
    }
}

fn print_accounts(accounts: &AccountsType) -> anyhow::Result<()> {
    let mut wtr = csv::Writer::from_writer(io::stdout());
    for account in accounts.values() {
        //println!("{:?}", account);
        wtr.serialize(account)?;
    }

    wtr.flush()?;
    Ok(())
}