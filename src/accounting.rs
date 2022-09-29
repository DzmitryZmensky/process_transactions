use anyhow::{self, bail, Context, Ok};
use csv::Trim;
use decimal::d128;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io};

type ClientId = u16;
type TransactionId = u32;
type Money = d128;

pub struct Ledger {
    accounts: HashMap<ClientId, Account>,
    deposit_transactions_cache: HashMap<TransactionId, Money>,
}

#[derive(Debug, Serialize)]
struct Account {
    client: ClientId,
    available: Money,
    held: Money,
    total: Money,
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
    client: ClientId,

    #[serde(rename = "tx")]
    id: TransactionId,

    #[serde(rename = "amount")]
    amount: Option<Money>,
}

impl Account {
    fn new(client: ClientId) -> Account {
        Account {
            client,
            available: Default::default(),
            held: Default::default(),
            total: Default::default(),
            locked: false,
        }
    }
}

impl Ledger {
    fn new() -> Ledger {
        Ledger {
            accounts: Default::default(),
            deposit_transactions_cache: Default::default(),
        }
    }

    fn process_transaction(&mut self, transaction: &TransactionData) -> anyhow::Result<()> {
        let client = transaction.client;
        let account = self.accounts.entry(client).or_insert(Account::new(client));

        match transaction.type_ {
            TransactionType::Deposit => {
                let amount = transaction
                    .amount
                    .context("'deposit' transaction must have 'amount' value")?;
                account.available += amount;
                account.total += amount;
                // assumption: only 'deposit' transactions can be disputed
                self.deposit_transactions_cache
                    .insert(transaction.id, amount);
            }
            TransactionType::Withdrawal => {
                let amount = transaction
                    .amount
                    .context("'withdrawal' transaction must have 'amount' value")?;
                if account.available >= amount && account.total >= amount {
                    account.available -= amount;
                    account.total -= amount;
                } else {
                    bail!("funds are not sufficient for withdrawal")
                }
            }
            TransactionType::Dispute => {
                if let Some(amount) = self.deposit_transactions_cache.get(&transaction.id) {
                    account.held += *amount;
                    if account.available >= *amount {
                        account.available -= *amount;
                    } else {
                        bail!("disputed value is greater than 'available'")
                    }
                }
            }
            TransactionType::Resolve => {
                if let Some(amount) = self.deposit_transactions_cache.get(&transaction.id) {
                    if account.held >= *amount {
                        account.held -= *amount;
                    } else {
                        bail!("'held' is greater than resolved value")
                    }
                    account.available += *amount;
                };
            }
            TransactionType::Chargeback => {
                if let Some(amount) = self.deposit_transactions_cache.get(&transaction.id) {
                    account.held -= *amount;
                    account.total -= *amount;
                    account.locked = true;
                };
            }
        }

        Ok(())
    }
}

pub fn load_transactions(transactions_fpath: &str) -> anyhow::Result<Ledger> {
    let mut ledger = Ledger::new();

    let mut reader = csv::ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(transactions_fpath)?;

    for line in reader.deserialize() {
        let transaction: TransactionData = line?;
        ledger
            .process_transaction(&transaction)
            .context(format!("cannot process transaction: id={}", transaction.id))?;
    }

    Ok(ledger)
}

pub fn print_accounts(ledger: &Ledger) -> anyhow::Result<()> {
    let mut writer = csv::Writer::from_writer(io::stdout());
    for account in ledger.accounts.values() {
        writer.serialize(account)?;
    }

    writer.flush()?;
    Ok(())
}
