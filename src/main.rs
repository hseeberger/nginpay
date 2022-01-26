use anyhow::{anyhow, Context, Error, Result};
use bigdecimal::BigDecimal;
use csv::{ReaderBuilder, Trim};
use log::error;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
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
    amount: Option<String>,
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
#[derive(Debug, PartialEq)]
struct Tx {
    tx_type: TxType,
    client_id: u16,
    tx_id: u32,
}

/// Possible transaction types. Deposit and Withdrawal have an amount.
#[derive(Debug, PartialEq)]
enum TxType {
    Deposit(BigDecimal),
    Withdrawal(BigDecimal),
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
            (TxRowType::Deposit, Some(amount)) => BigDecimal::from_str(&amount)
                .context("Cannot parse amount as decimal number")
                .map(|amount| Tx {
                    tx_type: TxType::Deposit(amount),
                    client_id,
                    tx_id,
                }),

            (TxRowType::Deposit, _) => Err(anyhow!("deposit is lacking amount")),

            (TxRowType::Withdrawal, Some(amount)) => BigDecimal::from_str(&amount)
                .context("Cannot parse amount as decimal number")
                .map(|amount| Tx {
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
    amounts: HashMap<u32, BigDecimal>,
}

/// A domain account.
#[derive(Debug, Default, PartialEq)]
struct Account {
    available: BigDecimal,
    held: BigDecimal,
    total: BigDecimal,
    locked: bool,
}

impl Account {
    fn run(&mut self, amounts: &mut HashMap<u32, BigDecimal>, tx: Tx) {
        let tx_id = tx.tx_id;

        match tx.tx_type {
            TxType::Deposit(amount) => {
                self.available += &amount;
                self.total += &amount;
                amounts.insert(tx_id, amount);
            }

            TxType::Withdrawal(amount) => {
                if self.available < amount {
                    error!("Insufficient available funds for tx with ID `{tx_id}`");
                } else {
                    self.available -= &amount;
                    self.total -= &amount;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_into_tx_ok_deposit() {
        let tx_row = Ok(TxRow {
            tx_row_type: TxRowType::Deposit,
            client_id: 42,
            tx_id: 666,
            amount: Some("1.2345".to_string()),
        });
        let tx = into_tx(tx_row);
        assert!(tx.is_some());
        let tx = tx.unwrap();
        assert_eq!(
            tx,
            Tx {
                tx_type: TxType::Deposit(amount_12345()),
                client_id: 42,
                tx_id: 666
            }
        )
    }

    #[test]
    fn test_into_tx_ok_withdrawal() {
        let tx_row = Ok(TxRow {
            tx_row_type: TxRowType::Withdrawal,
            client_id: 42,
            tx_id: 666,
            amount: Some("1.2345".to_string()),
        });
        let tx = into_tx(tx_row);
        assert!(tx.is_some());
        let tx = tx.unwrap();
        assert_eq!(
            tx,
            Tx {
                tx_type: TxType::Withdrawal(amount_12345()),
                client_id: 42,
                tx_id: 666
            }
        )
    }

    #[test]
    fn test_into_tx_err_amount_format() {
        let tx_row = Ok(TxRow {
            tx_row_type: TxRowType::Deposit,
            client_id: 42,
            tx_id: 666,
            amount: Some("INVALID".to_string()),
        });
        let tx = into_tx(tx_row);
        assert!(tx.is_none());
    }

    #[test]
    fn test_into_tx_err_amount_missing() {
        let tx_row = Ok(TxRow {
            tx_row_type: TxRowType::Withdrawal,
            client_id: 42,
            tx_id: 666,
            amount: None,
        });
        let tx = into_tx(tx_row);
        assert!(tx.is_none());
    }

    #[test]
    fn test_into_tx_err() {
        let tx_row = Err(anyhow!("SOME ERROR"));
        let tx = into_tx(tx_row);
        assert!(tx.is_none());
    }

    #[test]
    fn test_run_tx_deposit() {
        let state = State::default();
        let tx = Tx {
            tx_type: TxType::Deposit(amount_12345()),
            client_id: 42,
            tx_id: 666,
        };
        let State { accounts, amounts } = run_tx(state, tx);
        assert_eq!(
            accounts.get(&42),
            Some(&Account {
                available: amount_12345(),
                held: BigDecimal::default(),
                total: amount_12345(),
                locked: false,
            })
        );
        assert_eq!(amounts.get(&666), Some(&amount_12345()));
    }

    #[test]
    fn test_run_tx_withdrawal_insufficient_available() {
        let state = State::default();
        let tx = Tx {
            tx_type: TxType::Withdrawal(amount_12345()),
            client_id: 42,
            tx_id: 666,
        };
        let State { accounts, amounts } = run_tx(state, tx);
        assert_eq!(
            accounts.get(&42),
            Some(&Account {
                available: BigDecimal::default(),
                held: BigDecimal::default(),
                total: BigDecimal::default(),
                locked: false,
            })
        );
        assert_eq!(amounts.get(&666), None);
    }

    #[test]
    fn test_run_tx_withdrawal() {
        let mut state = State::default();
        state.accounts.insert(
            42,
            Account {
                available: amount_12345(),
                held: BigDecimal::default(),
                total: amount_12345(),
                locked: false,
            },
        );
        state.amounts.insert(666, amount_12345());

        let tx = Tx {
            tx_type: TxType::Withdrawal(BigDecimal::from_str("0.2345").unwrap()),
            client_id: 42,
            tx_id: 999,
        };
        let State { accounts, amounts } = run_tx(state, tx);
        assert_eq!(
            accounts.get(&42),
            Some(&Account {
                available: BigDecimal::from_str("1").unwrap(),
                held: BigDecimal::default(),
                total: BigDecimal::from_str("1").unwrap(),
                locked: false,
            })
        );
        assert_eq!(amounts.get(&666), Some(&amount_12345()));
        assert_eq!(
            amounts.get(&999),
            Some(&BigDecimal::from_str("-0.2345").unwrap())
        );
    }

    #[test]
    fn test_run_tx_dispute() {
        let mut state = State::default();
        state.accounts.insert(
            42,
            Account {
                available: amount_12345(),
                held: BigDecimal::default(),
                total: amount_12345(),
                locked: false,
            },
        );
        state.amounts.insert(666, amount_12345());

        let tx = Tx {
            tx_type: TxType::Dispute,
            client_id: 42,
            tx_id: 666,
        };
        let State {
            accounts,
            amounts: _,
        } = run_tx(state, tx);
        assert_eq!(
            accounts.get(&42),
            Some(&Account {
                available: BigDecimal::default(),
                held: amount_12345(),
                total: amount_12345(),
                locked: false,
            })
        );
    }

    #[test]
    fn test_run_tx_resolve() {
        let mut state = State::default();
        state.accounts.insert(
            42,
            Account {
                available: BigDecimal::default(),
                held: amount_12345(),
                total: amount_12345(),
                locked: false,
            },
        );
        state.amounts.insert(666, amount_12345());

        let tx = Tx {
            tx_type: TxType::Resolve,
            client_id: 42,
            tx_id: 666,
        };
        let State {
            accounts,
            amounts: _,
        } = run_tx(state, tx);
        assert_eq!(
            accounts.get(&42),
            Some(&Account {
                available: amount_12345(),
                held: BigDecimal::default(),
                total: amount_12345(),
                locked: false,
            })
        );
    }

    #[test]
    fn test_run_tx_chargeback() {
        let mut state = State::default();
        state.accounts.insert(
            42,
            Account {
                available: BigDecimal::default(),
                held: amount_12345(),
                total: amount_12345(),
                locked: false,
            },
        );
        state.amounts.insert(666, amount_12345());

        let tx = Tx {
            tx_type: TxType::Chargeback,
            client_id: 42,
            tx_id: 666,
        };
        let State {
            accounts,
            amounts: _,
        } = run_tx(state, tx);
        assert_eq!(
            accounts.get(&42),
            Some(&Account {
                available: BigDecimal::default(),
                held: BigDecimal::default(),
                total: BigDecimal::default(),
                locked: true,
            })
        );
    }

    fn amount_12345() -> BigDecimal {
        BigDecimal::from_str("1.2345").unwrap()
    }
}
