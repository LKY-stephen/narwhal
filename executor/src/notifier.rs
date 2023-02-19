use std::collections::HashMap;

// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{ExecutionIndices, ExecutionState};
use consensus::TxResults;
use tokio::task::JoinHandle;
use types::{get_transaction_id, metered_channel, Batch, Certificate};

#[derive(Clone, Debug)]
pub struct BatchIndex {
    pub certificate: Certificate,
    pub next_block_index: u64,
    pub batch_index: u64,
}

pub struct Notifier<State: ExecutionState> {
    rx_notifier: metered_channel::Receiver<(BatchIndex, Batch)>,
    rx_results: metered_channel::Receiver<TxResults>,
    rx_fast_commit: metered_channel::Receiver<Batch>,
    callback: State,
    results_cache: HashMap<u64, bool>,
}

impl<State: ExecutionState + Send + Sync + 'static> Notifier<State> {
    pub fn spawn(
        rx_notifier: metered_channel::Receiver<(BatchIndex, Batch)>,
        rx_results: metered_channel::Receiver<TxResults>,
        rx_fast_commit: metered_channel::Receiver<Batch>,
        callback: State,
    ) -> JoinHandle<()> {
        let results_cache = HashMap::new();
        let notifier = Notifier {
            rx_notifier,
            rx_results,
            rx_fast_commit,
            callback,
            results_cache,
        };
        tokio::spawn(notifier.run())
    }

    async fn run(mut self) {
        let mut miss_counter = 0;
        loop {
            tokio::select! {
                    Some((index, batch)) = self.rx_notifier.recv() => {
                        for (transaction_index, transaction) in batch.0.into_iter().enumerate() {
                            let execution_indices = ExecutionIndices {
                                next_certificate_index: index.next_block_index,
                                next_batch_index: index.batch_index + 1,
                                next_transaction_index: transaction_index as u64 + 1,
                            };

                            let id = get_transaction_id(&transaction);

                            self.callback
                                .handle_consensus_transaction(
                                    &index.certificate,
                                    execution_indices,
                                    transaction,
                                    match self.results_cache.remove(&id) {
                                        Some(result)=> result,
                                        None => {
                                            miss_counter+=1;

                                            #[cfg(not(feature = "benchmark"))]
                                            if miss_counter%1000 ==0{
                                                tracing::warn!("miss tx result {miss_counter} times");
                                            }
                                            false
                                        }
                                    },
                                )
                                .await;
                         }
                    },

                    Some(results) = self.rx_results.recv() => {
                        // TODO: For disaster recovery, this should be in storage.
                        self.results_cache.extend(results.0.into_iter());

                    },
                    Some(results) = self.rx_fast_commit.recv() => {

                        // TODO: For disaster recovery, this should be in storage.
                        for tx in results.0 {
                            self.results_cache.insert(get_transaction_id(&tx), true);
                            self.callback.fast_commit(tx).await;
                        }
                    },

            };
        }
    }
}
