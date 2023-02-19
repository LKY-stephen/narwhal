// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

// pub mod bullshark;
pub mod clerk;
pub mod consensus;
pub mod dag;
pub mod metrics;
// pub mod tusk;
pub mod board;
mod utils;

pub use crate::consensus::Consensus;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use types::{Certificate, SequenceNumber};
/// The default channel size used in the consensus and subscriber logic.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// The output format of the consensus.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConsensusOutput {
    pub leader: Certificate,
    pub index: u64,
    pub blocks: Vec<Certificate>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct TxResults(pub HashMap<u64, bool>);
