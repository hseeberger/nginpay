use anyhow::{anyhow, Context, Error, Result};
use csv::{ReaderBuilder, Trim};
use log::error;
use serde::Deserialize;
use std::collections::HashMap;
use structopt::StructOpt;

/// Command line options.
#[derive(Debug, StructOpt)]
#[structopt(about = "Simple toy payments engine in Rust")]
struct Opt {
    input_path: String,
}

/// Represents a transaction in the CSV input.
#[derive(Debug, Deserialize)]
struct TxRow {
    #[serde(rename = "type")]
    tx_row_type: TxRowType,
    #[serde(rename = "client")]
    client_id: u16,
    #[serde(rename = "tx")]
    tx_id: u32,
    amount: Option<f64>,
}

/// Possible transaction types in the CSV input.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum TxRowType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// A domain transaction. All transaction types have a client and a transaction ID.
#[derive(Debug)]
struct Tx {
    tx_type: TxType,
    client_id: u16,
    tx_id: u32,
}

/// Possible transaction types. Deposit and Withdrawal have an amount.
#[derive(Debug)]
enum TxType {
    Deposit(f64),
    Withdrawal(f64),
    Dispute,
    Resolve,
    Chargeback,
}

impl TryFrom<TxRow> for Tx {
    type Error = Error;

    fn try_from(tx_row: TxRow) -> Result<Self, Self::Error> {
        let TxRow {
            tx_row_type,
            client_id,
            tx_id,
            amount,
        } = tx_row;

        match (tx_row_type, amount) {
            (TxRowType::Deposit, Some(amount)) => Ok(Tx {
                tx_type: TxType::Deposit(amount),
                client_id,
                tx_id,
            }),

            (TxRowType::Deposit, _) => Err(anyhow!("deposit is lacking amount")),

            (TxRowType::Withdrawal, Some(amount)) => Ok(Tx {
                tx_type: TxType::Withdrawal(amount),
                client_id,
                tx_id,
            }),

            (TxRowType::Withdrawal, _) => Err(anyhow!("withdrawal is lacking amount")),

            (TxRowType::Dispute, _) => Ok(Tx {
                tx_type: TxType::Dispute,
                client_id,
                tx_id,
            }),

            (TxRowType::Resolve, _) => Ok(Tx {
                tx_type: TxType::Resolve,
                client_id,
                tx_id,
            }),

            (TxRowType::Chargeback, _) => Ok(Tx {
                tx_type: TxType::Chargeback,
                client_id,
                tx_id,
            }),
        }
    }
}

/// Accumulator for folding over domain transactions.
#[derive(Debug, Default)]
struct State {
    /// Map from client ID to account. Used as actual fold result.
    accounts: HashMap<u16, Account>,

    /// Map from deposit and withdrawal ID to amount.
    /// Used for backtracking when running dispute, resolve and chargeback transactions.
    amounts: HashMap<u32, f64>,
}

/// A domain account.
#[derive(Debug, Default)]
struct Account {
    available: f64,
    held: f64,
    total: f64,
    locked: bool,
}

impl Account {
    fn run(&mut self, amounts: &mut HashMap<u32, f64>, tx: Tx) {
        let tx_id = tx.tx_id;

        match tx.tx_type {
            TxType::Deposit(amount) => {
                self.available += amount;
                self.total += amount;
                amounts.insert(tx_id, amount);
            }

            TxType::Withdrawal(amount) => {
                if self.available < amount {
                    error!("Insufficient available funds for tx with ID `{tx_id}`");
                } else {
                    self.available -= amount;
                    self.total -= amount;
                    amounts.insert(tx_id, -amount);
                }
            }

            TxType::Dispute => match amounts.get(&tx_id) {
                Some(amount) => {
                    self.available -= amount;
                    self.held += amount;
                }
                None => error!("Ignoring dispute for unknown tx with ID `{tx_id}`"),
            },

            TxType::Resolve => match amounts.get(&tx_id) {
                Some(amount) => {
                    self.available += amount;
                    self.held -= amount;
                }
                None => error!("Ignoring resolve for unknown tx with ID `{tx_id}`"),
            },

            TxType::Chargeback => match amounts.get(&tx_id) {
                Some(amount) => {
                    self.held -= amount;
                    self.total -= amount;
                    self.locked = true;
                }
                None => error!("Ignoring dispute for unknown tx with ID `{tx_id}`"),
            },
        }
    }
}

fn main() -> Result<()> {
    env_logger::init();

    let Opt { input_path } = Opt::from_args();

    let mut reader = ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(&input_path)
        .context(format!("Cannot create reader for `{input_path}`"))?;

    let State {
        accounts,
        amounts: _,
    } = reader
        .deserialize::<TxRow>()
        .map(|result| result.context("Cannot read/deserialize tx row"))
        .filter_map(into_tx)
        .fold(State::default(), run_tx);

    print_accounts(accounts);

    Ok(())
}

fn into_tx(tx_row: Result<TxRow>) -> Option<Tx> {
    match tx_row.and_then(|row| {
        let tx_id = row.tx_id;
        row.try_into()
            .context(format!("Cannot convert tx row with ID `{tx_id}` into tx"))
    }) {
        Ok(tx) => Some(tx),
        Err(e) => {
            error!("{e}");
            None
        }
    }
}

fn run_tx(mut state: State, tx: Tx) -> State {
    match state.accounts.get_mut(&tx.client_id) {
        Some(account) => account.run(&mut state.amounts, tx),
        None => {
            let mut account = Account::default();
            let client_id = tx.client_id;
            account.run(&mut state.amounts, tx);
            state.accounts.insert(client_id, account);
        }
    }
    state
}

fn print_accounts(accounts: HashMap<u16, Account>) {
    println!("client, available, held, total, locked");
    for (
        client_id,
        Account {
            available,
            held,
            total,
            locked,
        },
    ) in accounts
    {
        println!("{client_id}, {available:.4}, {held:.4}, {total:.4}, {locked}")
    }
}
