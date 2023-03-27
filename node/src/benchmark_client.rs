// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use bytes::{BufMut as _, BytesMut};
use clap::{crate_name, crate_version, App, AppSettings};
use eyre::Context;
use futures::future::join_all;
use itertools::Itertools;
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tokio::{
    net::TcpStream,
    time::{interval, sleep, Duration, Instant},
};
use tracing::{info, subscriber::set_global_default, warn};
use tracing_subscriber::filter::EnvFilter;
use types::{TransactionProto, TransactionsClient};
use url::Url;

// We are distributing the transactions that need to be sent
// within a second to sub-buckets. The precision here represents
// the number of such buckets within the period of 1 second.
const PRECISION: u64 = 20;

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .long_about("To run the benchmark client following are required:\n\
        * the size of the transactions via the --size property\n\
        * the worker address <ADDR> to send the transactions to. A url format is expected ex http://127.0.0.1:7000\n\
        * the rate of sending transactions via the --rate parameter\n\
        \n\
        Optionally the --nodes parameter can be passed where a list (comma separated string) of worker addresses\n\
        should be passed. The benchmarking client will first try to connect to all of those nodes before start sending\n\
        any transactions. That confirms the system is up and running and ready to start processing the transactions.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs. A url format is expected ex http://127.0.0.1:7000'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses, comma separated, that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    cfg_if::cfg_if! {
        if #[cfg(feature = "benchmark")] {
            let timer = tracing_subscriber::fmt::time::UtcTime::rfc_3339();
            let subscriber_builder = tracing_subscriber::fmt::Subscriber::builder()
                                     .with_env_filter(env_filter)
                                     .with_timer(timer).with_ansi(false);
        } else {
            let subscriber_builder = tracing_subscriber::fmt::Subscriber::builder().with_env_filter(env_filter);
        }
    }
    let subscriber = subscriber_builder.with_writer(std::io::stderr).finish();

    set_global_default(subscriber).expect("Failed to set subscriber");

    let target_str = matches.value_of("ADDR").unwrap();
    let target = target_str.parse::<Url>().with_context(|| {
        format!(
            "Invalid url format {target_str}. Should provide something like http://127.0.0.1:7000"
        )
    })?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<Url>())
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("Invalid url format {target_str}"))?;

    info!("Node address: {target}");

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {size} B");

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {rate} tx/s");

    let mut rng = ChaCha8Rng::seed_from_u64(target.port().unwrap().try_into().unwrap());

    let client = Client {
        target,
        size,
        rate,
        nodes,
        id: rng.next_u64(),
    };

    let txs = client.prepare_transaction(rng);

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client
        .send(txs)
        .await
        .context("Failed to submit transactions")
}

struct Client {
    target: Url,
    size: usize,
    rate: u64,
    nodes: Vec<Url>,
    id: u64,
}

impl Client {
    pub async fn send(&self, txs: Vec<Vec<TransactionProto>>) -> Result<(), eyre::Report> {
        // The BURST_DURATION represents the period for each bucket we
        // have split. For example if precision is 20 the 1 second (1000ms)
        // will be split in 20 buckets where each one will be 50ms apart.
        // Basically we are looking to send a list of transactions every 50ms.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        let burst = self.rate / PRECISION;

        if burst == 0 {
            return Err(eyre::Report::msg(format!(
                "Transaction rate is too low, should be at least {} tx/s and multiples of {}",
                PRECISION, PRECISION
            )));
        }

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(eyre::Report::msg(
                "Transaction size must be at least 9 bytes",
            ));
        }

        // Connect to the mempool.
        let mut client = TransactionsClient::connect(self.target.as_str().to_owned())
            .await
            .context(format!("failed to connect to {}", self.target))?;

        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);
        // create tx before senidng to reduce the load during sending txs.xd

        let mut burst_nb = 0;
        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();
            info!("Sending sample transaction {}", self.id + burst_nb as u64);
            if let Err(e) = client
                .submit_transaction_stream(tokio_stream::iter(txs[burst_nb].clone()))
                .await
            {
                warn!("Failed to send transaction: {e}");
                break 'main;
            }

            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            burst_nb += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(&*address.socket_addrs(|| None).unwrap())
                    .await
                    .is_err()
                {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }

    // NOTE:  We only prepare for test within 100 seconds
    // we store these transactions in a vector and save the running cost
    fn prepare_transaction(&self, mut rng: ChaCha8Rng) -> Vec<Vec<TransactionProto>> {
        let rate = self.rate;
        let size = self.size;
        let id = self.id;

        let rounds = 100 * PRECISION;
        let burst = rate / PRECISION;
        let mut r = rng.gen();
        let mut txo: u64 = rng.next_u64();

        let mut tx = BytesMut::with_capacity(size);
        let mut counter = 0;
        (0..rounds)
            .map(|_| {
                let result = (0..burst)
                    .map(|x| {
                        if x == counter % burst {
                            tx.put_u8(0u8); // Sample txs start with 0.
                            tx.put_u64(id + counter); // This counter identifies the tx.

                            tx.put_u8(2); // push inputs
                            tx.put_u64(txo + x);
                            tx.put_u64(txo + burst);
                        } else {
                            tx.put_u8(1u8); // Standard txs start with 1.
                            tx.put_u64(r + x); // Ensures all clients send different txs.

                            tx.put_u8(1); // push inputs
                            tx.put_u64(txo + x);
                        };
                        tx.resize(size, 0u8);
                        let bytes = tx.split().freeze();
                        TransactionProto { transaction: bytes }
                    })
                    .collect_vec();
                counter += 1;

                // update random value to makesure all txs are different!
                txo = rng.next_u64();
                r = rng.next_u64();
                return result;
            })
            .collect_vec()
    }
}
