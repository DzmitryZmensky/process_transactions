use anyhow::{self, bail, Context, Ok};
use csv::Trim;
use decimal::d128;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::{Write}};

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

    #[serde(skip_serializing)]
    disputed_txs: HashMap<TransactionId, Money>,
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
struct Transaction {
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
            disputed_txs: Default::default(),
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

    fn process_transaction(&mut self, transaction: &Transaction) -> anyhow::Result<()> {
        let client = transaction.client;
        let account = self.accounts.entry(client).or_insert(Account::new(client));

        match transaction.type_ {
            // assumption: 'locked' state is only an indicator of chargeback and doesn't impact any operation - see all assumptions in readme.txt
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
                if let Some(amount) = self.deposit_transactions_cache.remove(&transaction.id) {
                    account.disputed_txs.insert(transaction.id, amount);
                    account.held += amount;
                    account.available -= amount; // the balance can become negative - see all assumptions in readme.txt
                }
            }
            TransactionType::Resolve => {
                if let Some(amount) = account.disputed_txs.remove(&transaction.id) {
                    account.held -= amount;
                    account.available += amount;
                };
            }
            TransactionType::Chargeback => {
                if let Some(amount) = account.disputed_txs.remove(&transaction.id) {
                    account.held -= amount;
                    account.total -= amount;
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
        .from_path(transactions_fpath)
        .context(transactions_fpath.to_string())?;

    for line in reader.deserialize() {
        let transaction: Transaction = line?;
        ledger
            .process_transaction(&transaction)
            .context(format!("cannot process transaction: id={}", transaction.id))?;
    }

    Ok(ledger)
}

pub fn output_accounts<W: Write>(ledger: &Ledger, output: &mut W) -> anyhow::Result<()> {
    let mut writer = csv::Writer::from_writer(output);
    for account in ledger.accounts.values() {
        writer.serialize(account)?;
    }

    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Account, ClientId, Ledger, Money, Transaction, TransactionId, TransactionType};
    use decimal::d128;

    const CLIENT1: ClientId = 1;

    #[test]
    fn deposit_partial_withdraw_success() {
        let mut ledger = Ledger::new();

        {
            // deposit
            let tx = Transaction {
                type_: TransactionType::Deposit,
                client: CLIENT1,
                id: 1,
                amount: Some(d128!(0.0003)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0003),
                d128!(0),
                d128!(0.0003),
                false,
            );
        }

        {
            // withdraw
            let tx = Transaction {
                type_: TransactionType::Withdrawal,
                client: CLIENT1,
                id: 2,
                amount: Some(d128!(0.0001)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0002),
                d128!(0),
                d128!(0.0002),
                false,
            );
        }
    }

    #[test]
    fn excessive_withdraw_should_fail() {
        let mut ledger = Ledger::new();

        {
            // deposit
            let tx = Transaction {
                type_: TransactionType::Deposit,
                client: CLIENT1,
                id: 1,
                amount: Some(d128!(0.0003)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0003),
                d128!(0),
                d128!(0.0003),
                false,
            );
        }

        {
            // try to withdraw
            let tx = Transaction {
                type_: TransactionType::Withdrawal,
                client: CLIENT1,
                id: 2,
                amount: Some(d128!(0.0004)), // the value is greater than deposited one
            };

            let error_text = ledger
                .process_transaction(&tx)
                .expect_err("expected Error")
                .to_string();
            assert_eq!(error_text, "funds are not sufficient for withdrawal");
        }
    }

    #[test]
    fn dispute_resolve_success() {
        let mut ledger = Ledger::new();

        {
            // deposit 1
            let tx = Transaction {
                type_: TransactionType::Deposit,
                client: CLIENT1,
                id: 1,
                amount: Some(d128!(0.0001)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0001),
                d128!(0),
                d128!(0.0001),
                false,
            );
        }

        {
            // deposit 2
            let tx = Transaction {
                type_: TransactionType::Deposit,
                client: CLIENT1,
                id: 2,
                amount: Some(d128!(0.0002)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0003),
                d128!(0),
                d128!(0.0003),
                false,
            );
        }

        {
            // dispute
            let tx = Transaction {
                type_: TransactionType::Dispute,
                client: CLIENT1,
                id: 2, // second deposit
                amount: None,
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0001),
                d128!(0.0002),
                d128!(0.0003),
                false,
            );
        }

        {
            // resolve
            let tx = Transaction {
                type_: TransactionType::Resolve,
                client: CLIENT1,
                id: 2, // second deposit
                amount: None,
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0003),
                d128!(0.0000),
                d128!(0.0003),
                false,
            );
        }
        {
            // duplicate 'resolve'
            let tx = Transaction {
                type_: TransactionType::Resolve,
                client: CLIENT1,
                id: 2, // second deposit
                amount: None,
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0003),
                d128!(0.0000),
                d128!(0.0003),
                false,
            );
        }
    }

    #[test]
    fn dispute_chargeback_success() {
        let mut ledger = Ledger::new();

        {
            // deposit 1
            let tx = Transaction {
                type_: TransactionType::Deposit,
                client: CLIENT1,
                id: 1,
                amount: Some(d128!(0.0001)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0001),
                d128!(0),
                d128!(0.0001),
                false,
            );
        }

        {
            // deposit 2
            let tx = Transaction {
                type_: TransactionType::Deposit,
                client: CLIENT1,
                id: 2,
                amount: Some(d128!(0.0002)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0003),
                d128!(0),
                d128!(0.0003),
                false,
            );
        }

        {
            // dispute
            let tx = Transaction {
                type_: TransactionType::Dispute,
                client: CLIENT1,
                id: 2, // second deposit
                amount: None,
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0001),
                d128!(0.0002),
                d128!(0.0003),
                false,
            );
        }

        {
            // chargeback
            let tx = Transaction {
                type_: TransactionType::Chargeback,
                client: CLIENT1,
                id: 2, // second deposit
                amount: None,
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0001),
                d128!(0.0000),
                d128!(0.0001),
                true,
            );
        }
    }

    #[test]
    fn negative_balance_after_chargeback() {
        let mut ledger = Ledger::new();

        {
            // deposit
            let tx = Transaction {
                type_: TransactionType::Deposit,
                client: CLIENT1,
                id: 1,
                amount: Some(d128!(0.0001)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0001),
                d128!(0),
                d128!(0.0001),
                false,
            );
        }

        {
            // withdraw
            let tx = Transaction {
                type_: TransactionType::Withdrawal,
                client: CLIENT1,
                id: 2,
                amount: Some(d128!(0.0001)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0),
                d128!(0),
                d128!(0),
                false,
            );
        }

        {
            // dispute
            let tx = Transaction {
                type_: TransactionType::Dispute,
                client: CLIENT1,
                id: 1,
                amount: None,
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(-0.0001),
                d128!(0.0001),
                d128!(0.0000),
                false,
            );
        }

        {
            // chargeback
            let tx = Transaction {
                type_: TransactionType::Chargeback,
                client: CLIENT1,
                id: 1,
                amount: None,
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(-0.0001),
                d128!(0.0000),
                d128!(-0.0001),
                true,
            );
        }
    }

    #[test]
    fn invalid_dispute_reference_ignored() {
        let mut ledger = Ledger::new();
        let not_existing_tx_id: TransactionId = 999;

        {
            // deposit 1
            let tx = Transaction {
                type_: TransactionType::Deposit,
                client: CLIENT1,
                id: 1,
                amount: Some(d128!(0.0001)),
            };

            ledger.process_transaction(&tx).unwrap();

            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0001),
                d128!(0),
                d128!(0.0001),
                false,
            );
        }

        for tx_type in vec![
            TransactionType::Dispute,
            TransactionType::Resolve,
            TransactionType::Chargeback,
        ] {
            let tx = Transaction {
                type_: tx_type,
                client: CLIENT1,
                id: not_existing_tx_id,
                amount: None,
            };

            ledger.process_transaction(&tx).unwrap();
            // balance shouldn't change
            assert_account(
                &ledger.accounts[&CLIENT1],
                d128!(0.0001),
                d128!(0),
                d128!(0.0001),
                false,
            );
        }
    }

    fn assert_account(
        account: &Account,
        available: Money,
        held: Money,
        total: Money,
        locked: bool,
    ) {
        assert_eq!(available, account.available, "unexpected 'available'");
        assert_eq!(held, account.held, "unexpected 'held'");
        assert_eq!(total, account.total, "unexpected 'total'");
        assert_eq!(locked, account.locked, "unexpected 'locked'");
    }
}
