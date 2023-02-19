use std::sync::Arc;

// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use store::Store;
use tokio::task::JoinHandle;
use types::{metered_channel::Receiver, BatchDigest, BatchMeta};

#[cfg(test)]
#[path = "tests/payload_receiver_tests.rs"]
mod payload_receiver_tests;

/// Receives batches' digests of other authorities. These are only needed to verify incoming
/// headers (i.e.. make sure we have their payload).
pub struct BatchMetaManager {
    /// The persistent storage.
    store: Arc<Store<BatchDigest, BatchMeta>>,

    // Receive new meta from worker
    rx_meta_worker: Receiver<(BatchDigest, BatchMeta)>,

    // Receive meta to delete
    rx_meta_primary: Receiver<Vec<BatchDigest>>,
}

impl BatchMetaManager {
    #[must_use]
    pub fn spawn(
        store: Arc<Store<BatchDigest, BatchMeta>>,
        rx_meta_worker: Receiver<(BatchDigest, BatchMeta)>,
        rx_meta_primary: Receiver<Vec<BatchDigest>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                store,
                rx_meta_worker,
                rx_meta_primary,
            }
            .run()
            .await;
        })
    }

    // This manager maintains the write of meta info until the batch is committed.
    async fn run(&mut self) {
        loop {
            _ = tokio::select! {
                Some((digest, meta)) = self.rx_meta_worker.recv() => {
                    self.store.write(digest, meta).await
                }

                Some(digest) = self.rx_meta_primary.recv()=>{
                    if self.store.remove_all(digest).await.is_ok() {
                        return ();
                    }

                }

            };
        }
    }
}
