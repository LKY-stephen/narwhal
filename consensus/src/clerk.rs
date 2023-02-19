// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    consensus::{ConsensusProtocol, ConsensusState, Dag, NodeVotes},
    utils, ConsensusOutput, SequenceNumber,
};
use config::{Committee, Stake};
use fastcrypto::{traits::EncodeDecodeBase64, Hash};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use store::Store;
use tracing::debug;
use types::{
    BatchDigest, BatchMeta, Certificate, CertificateDigest, ConsensusStore, Round, StoreResult,
};

//#[cfg(any(test))]
//#[path = "tests/clerk_tests.rs"]
//pub mod clerk;

pub struct Clerk {
    /// The committee information.
    pub committee: Committee,
    /// Persistent storage to safe ensure crash-recovery.
    pub store: Arc<ConsensusStore>,
    /// The depth of the garbage collector.
    pub gc_depth: Round,
    /// Persistent storage to store batch meta
    pub batch_meta_store: Arc<Store<BatchDigest, BatchMeta>>,

    /// Persistent storage to store batch votes
    pub batch_votes_store: Store<BatchDigest, NodeVotes>,

    /// Persistent storage to store tx votes
    pub tx_votes_store: Store<u64, NodeVotes>,

    /// Persistent storage to store txo-tx map
    pub txo_tx_store: Store<u64, Vec<u64>>,

    /// Fasted Committed Set wit its result
    pub fast_committed: HashMap<u64, bool>,

    /// Fasted Committed Set wit its result
    pub conflicted_txo: HashSet<u64>,
}

impl ConsensusProtocol for Clerk {
    fn process_certificate(
        &mut self,
        state: &mut ConsensusState,
        consensus_index: SequenceNumber,
        certificate: Certificate,
    ) -> StoreResult<ConsensusOutput> {
        debug!("Processing {:?}", certificate);
        let round = certificate.round();
        let mut consensus_index = consensus_index;

        // Add the new certificate to the local storage.
        state.dag.entry(round).or_insert_with(HashMap::new).insert(
            certificate.origin(),
            (certificate.digest(), certificate.clone()),
        );

        // Update the votes
        // The Certificate has already been ordered in primary(i.e., ancestor reach here before its child)
        // now we only need to process for the author of this certificate.
        self.update_votes(&certificate);

        // We do fast commit for odd rounds
        if round % 2 == 1 {
            self.fast_commit();
        }

        // Try to order the dag to commit. Start from the highest round for which we have at least
        // 2f+1 certificates. This is because we need them to reveal the common coin.
        let r = round - 1;

        // We only elect leaders for even round numbers.
        if r % 2 != 0 || r < 4 {
            return Ok(ConsensusOutput::default());
        }

        // Get the certificate's digest of the leader of round r-2. If we already ordered this leader,
        // there is nothing to do.
        let leader_round = r - 2;
        if leader_round <= state.last_committed_round {
            return Ok(ConsensusOutput {
                certificates: vec![],
                transactions: HashMap::new(),
            });
        }
        let (leader_digest, leader) = match Self::leader(&self.committee, leader_round, &state.dag)
        {
            Some(x) => x,
            None => return Ok(ConsensusOutput::default()),
        };

        // Check if the leader has f+1 support from its children (ie. round r-1).
        let stake: Stake = state
            .dag
            .get(&(r - 1))
            .expect("We should have the whole history by now")
            .values()
            .filter(|(_, x)| x.header.parents.contains(leader_digest))
            .map(|(_, x)| self.committee.stake(&x.origin()))
            .sum();

        // If it is the case, we can commit the leader. But first, we need to recursively go back to
        // the last committed leader, and commit all preceding leaders in the right order. Committing
        // a leader block means committing all its dependencies.
        if stake < self.committee.validity_threshold() {
            debug!("Leader {:?} does not have enough support", leader);
            return Ok(ConsensusOutput::default());
        }

        // Get an ordered list of past leaders that are linked to the current leader.
        debug!("Leader {:?} has enough support", leader);
        let mut sequence = Vec::new();
        for leader in utils::order_leaders(&self.committee, leader, state, Self::leader)
            .iter()
            .rev()
        {
            // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
            for x in utils::order_dag(self.gc_depth, leader, state) {
                let digest = x.digest();

                // Update and clean up internal state.
                state.update(&x, self.gc_depth);

                // Add the certificate to the sequence.
                sequence.push((x, consensus_index));

                // Increase the global consensus index.
                consensus_index += 1;

                // Persist the update.
                // TODO [issue #116]: Ensure this is not a performance bottleneck.
                self.store.write_consensus_state(
                    &state.last_committed,
                    &consensus_index,
                    &digest,
                )?;
            }
        }

        // Log the latest committed round of every authority (for debug).
        // Performance note: if tracing at the debug log level is disabled, this is cheap, see
        // https://github.com/tokio-rs/tracing/pull/326
        for (name, round) in &state.last_committed {
            debug!("Latest commit of {}: Round {}", name.encode_base64(), round);
        }

        Ok(ConsensusOutput {
            certificates: sequence,
            transactions: self.output_results(&certificate),
        })
    }

    fn update_committee(&mut self, new_committee: Committee) -> StoreResult<()> {
        // Alert! clerk also needs to update the vote cases according to the
        // the new committee. However, it depends on how the dag of previous round
        // is handled. A possible way is to commit the fastted committed transactions
        // among old committee with its latest dag such that the new committee can
        // include the fastted committed transactions and reset votes for other
        // transactions. This is a complicated corner case not in our experiment
        // so we will not do corresponding changes in this repo.

        self.committee = new_committee;
        self.store.clear()
    }
}

impl Clerk {
    /// Create a new Tusk consensus instance.
    pub fn new(
        committee: Committee,
        store: Arc<ConsensusStore>,
        gc_depth: Round,
        batch_meta_store: Arc<Store<BatchDigest, BatchMeta>>,
        batch_votes_store: Store<BatchDigest, NodeVotes>,
        tx_votes_store: Store<u64, NodeVotes>,
        txo_tx_store: Store<u64, Vec<u64>>,
    ) -> Self {
        let fast_committed = HashMap::new();
        let conflicted_txo = HashSet::new();
        Self {
            committee,
            store,
            gc_depth,
            batch_meta_store,
            batch_votes_store,
            tx_votes_store,
            txo_tx_store,
            fast_committed,
            conflicted_txo,
        }
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(
        committee: &Committee,
        round: Round,
        dag: &'a Dag,
    ) -> Option<&'a (CertificateDigest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use a stake-weighted choise seeded by the round.
        //
        // Note: this function is often called with even rounds only. While we do not aim at random selection
        // yet (see issue #10), repeated calls to this function should still pick from the whole roster of leaders.
        cfg_if::cfg_if! {
            if #[cfg(test)] {
                // consensus tests rely on returning the same leader.
                let leader = committee.authorities.iter().next().expect("Empty authorities table!").0;
            } else {
                // Elect the leader in a stake-weighted choice seeded by the round
                let leader = &committee.leader(round);
            }
        }

        // Return its certificate and the certificate's digest.
        dag.get(&round).and_then(|x| x.get(leader))
    }

    fn update_votes(&self, certificate: &Certificate) {}
    fn fast_commit(&self) {}

    fn output_results(&self, certificate: &Certificate) -> HashMap<u64, bool> {
        HashMap::new()
    }
}
