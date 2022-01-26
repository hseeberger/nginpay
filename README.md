# nginpay #

Toy payment engine in Rust for learning and demo purposes. It reads a series of transactions from a CSV file, creates/updates client accounts, handles deposits, withdrawals, disputes, resolutions and chargebacks, and finally outputs the state of clients accounts to the console in CSV format.

## Running & testing

The most critical parts of the application code are unit tested, see [main.rs](https://github.com/hseeberger/nginpay/blob/main/src/main.rs#L256). Just run `cargo test`.

There is also a [sample CVS file](https://github.com/hseeberger/nginpay/blob/main/transactions.csv) which can be used to run nginpay in anger. Just run `cargo run -- transactions.csv`. Don't worry if you see an ERROR logged, this is expected, because of insufficient funds of some client.

## Error handling

Errors are dealt with in a twofold manner.

First, if nginpay is not invoked with the expected argument for the path to the input CSV file or if the file does not exist, the application terminates with appropriate error messages.

Second, if there are invalid row in the input CSV file which cannot be parsed as valid domain transactions, these lines are ignored and a respective ERROR is logged. The same is true for withdrawals if there are insufficient available funds. In these case the application continues and completes normally.

## Design & implementation

nginpay makes use of serveral useful open source crates, e.g. [anyhow](https://crates.io/crates/anyhow), [serde](https://crates.io/crates/serde) and [structopt](https://crates.io/crates/structopt).

In order to avoid the imprecision of floating point operations nginpay is using `BigDecimal`s from the [bigdecimal](https://crates.io/crates/bigdecimal) crate.

The actual transaction processing is designed and implemented as a [fold](https://en.wikipedia.org/wiki/Fold_%28higher-order_function%29) over the transaction iterator, thereby making use of several powerful and high-level yet zero-cost abstractions of Rust.

It is assumed, yet not verified, that a resolution transaction can only happen after a dispute. The same is true for a chargeback.

As the processing of some of the transactions requires backtracking of some former transactions, the fold has to accumulate all deposits and withdrawals. While this is simple to implement and easy to understand, i.e. well suited for this demo, this approach would not work well for real world scenarios with high volume transactions. In such cases only a small cache of recent transactions should – if any – be kept in memory and backtracking of older transactions would have to happen via storage.

Also, for real world scenarios, the design potentially could be changed to an asynchronous streaming approach where streams can be grouped into substreams by client IDs or client ID groups and handled on different cluster nodes in order to scale out if necessary.

## Contribution policy ##

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license the
work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you agree
to license the material under the project's open source license and warrant that you have the legal
authority to do so.

## License ##

This code is open source software licensed under the
[Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
