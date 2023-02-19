// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use async_trait::async_trait;
use executor::{ExecutionIndices, ExecutionState};

use tokio::sync::mpsc::Sender;
use types::Certificate;

/// A simple/dumb execution engine.
pub struct SimpleExecutionState {
    tx_transaction_confirmation: Sender<Vec<u8>>,
}

impl SimpleExecutionState {
    pub fn new(tx_transaction_confirmation: Sender<Vec<u8>>) -> Self {
        Self {
            tx_transaction_confirmation,
        }
    }
}

#[async_trait]
impl ExecutionState for SimpleExecutionState {
    async fn handle_consensus_transaction(
        &self,
        _certificate: &Certificate,
        _execution_indices: ExecutionIndices,
        transaction: Vec<u8>,
        _result: bool,
    ) {
        // now tx reach the states.
        if transaction[0] == 0u8 && transaction.len() > 9 {
            tracing::info!(
                "Execute sample tx {} ",
                u64::from_be_bytes(transaction[1..9].try_into().unwrap())
            )
        }
        if let Err(err) = self.tx_transaction_confirmation.send(transaction).await {
            eprintln!("Failed to send txn in SimpleExecutionState: {}", err);
        }
    }

    async fn fast_commit(&self, transaction: Vec<u8>) {
        if transaction[0] == 0u8 && transaction.len() > 9 {
            tracing::info!(
                "Fast Committed sample tx {} ",
                u64::from_be_bytes(transaction[1..9].try_into().unwrap())
            )
        }
    }

    async fn load_execution_indices(&self) -> ExecutionIndices {
        ExecutionIndices::default()
    }

    async fn handle_fast_commitment(
        &self,
        _execution_indices: ExecutionIndices,
        transaction: Vec<u8>,
    ) {
        // now tx reach the states.
        if transaction[0] == 0u8 && transaction.len() > 9 {
            tracing::info!(
                "Fast Commit sample tx {} ",
                u64::from_be_bytes(transaction[1..9].try_into().unwrap())
            )
        }
        if let Err(err) = self.tx_transaction_confirmation.send(transaction).await {
            eprintln!("Failed to send txn in SimpleExecutionState: {}", err);
        }
    }
}
