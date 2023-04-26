// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    consensus::{Dag, NodeVotes},
    TxResults,
};
use config::{Committee, Stake, WorkerId};
use crypto::PublicKey;
use fastcrypto::Hash;
use futures::future::join_all;
use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
};
use store::Store;
use tracing::debug;
use types::{
    metered_channel, BatchDigest, BatchMeta, Certificate, CertificateDigest, Round, TransactionId,
    TransactionInput,
};

//#[cfg(any(test))]
//#[path = "tests/clerk_tests.rs"]
//pub mod clerk;

pub struct Board {
    /// Persistent storage to store batch meta
    pub batch_meta_store: Arc<Store<BatchDigest, BatchMeta>>,

    /// Persistent storage to store batch votes
    batch_votes_store: HashMap<BatchDigest, NodeVotes>,

    /// Persistent storage to store batch votes
    batch_worker_store: HashMap<BatchDigest, WorkerId>,

    /// Persistent storage to store tx votes
    tx_votes_store: HashMap<TransactionId, NodeVotes>,

    /// Persistent storage to store txo-tx map
    txo_tx_store: HashMap<TransactionInput, HashSet<TransactionId>>,

    /// Fasted Committed Set wit its result
    fast_committed_tx: HashMap<TransactionId, bool>,

    /// Fasted Committed Set wit its result
    fast_committed_batch: HashSet<BatchDigest>,

    /// Fasted Committed Set wit its result
    conflicted_txo: HashSet<TransactionInput>,

    // cache for consumed txo of each batch
    batch_txo_store: HashMap<BatchDigest, HashSet<TransactionInput>>,

    // table for stake of each public key
    stake_table: HashMap<PublicKey, Stake>,

    // quorum of the stake
    quorum: u64,

    // channel for informming fast commit
    tx_fast_commit: metered_channel::Sender<HashMap<BatchDigest, WorkerId>>,

    // frontier of committed dag
    frontier: HashMap<PublicKey, Round>,
}

#[derive(Clone)]
pub struct CommitOutputs {
    pub certificates: HashSet<Certificate>,
    pub transaction_results: TxResults,
}

impl Board {
    /// Create a new Tusk consensus instance.
    pub fn new(
        committee: &Committee,
        batch_meta_store: Arc<Store<BatchDigest, BatchMeta>>,
        tx_fast_commit: metered_channel::Sender<HashMap<BatchDigest, WorkerId>>,
    ) -> Self {
        let fast_committed_tx = HashMap::new();
        let fast_committed_batch = HashSet::new();
        let conflicted_txo = HashSet::new();
        let batch_votes_store = HashMap::new();
        let batch_worker_store = HashMap::new();
        let tx_votes_store = HashMap::new();
        let txo_tx_store = HashMap::new();
        let batch_txo_store = HashMap::new();
        let stake_table = committee
            .authorities
            .clone()
            .into_iter()
            .map(|(k, v)| (k, v.stake))
            .collect::<HashMap<_, _>>();
        let quorum = committee.quorum_threshold();
        let frontier = committee
            .authorities
            .keys()
            .map(|key| (key.to_owned(), 0))
            .collect();
        Self {
            batch_meta_store,
            batch_votes_store,
            batch_worker_store,
            tx_votes_store,
            txo_tx_store,
            batch_txo_store,
            fast_committed_tx,
            fast_committed_batch,
            conflicted_txo,
            stake_table,
            quorum,
            tx_fast_commit,
            frontier,
        }
    }

    // update the committee.
    // theoratically, we should also process for fast committed txs
    pub fn change_committee(&mut self, committee: &Committee) {
        self.stake_table = committee
            .authorities
            .clone()
            .into_iter()
            .map(|(k, v)| (k, v.stake))
            .collect::<HashMap<_, _>>();
        self.quorum = committee.quorum_threshold();
        self.frontier = committee
            .authorities
            .keys()
            .map(|key| {
                (
                    key.to_owned(),
                    match self.frontier.get(key) {
                        Some(r) => *r,
                        None => 0,
                    },
                )
            })
            .collect();
    }

    pub async fn process_certificate(&mut self, dag: &mut Dag, certificate: &Certificate) -> bool {
        debug!("Processing {:?}", certificate);
        let voter = certificate.header.author.clone();
        let round = certificate.round();

        // Add the new certificate to the local storage.
        if None
            != dag.entry(round).or_insert_with(HashMap::new).insert(
                certificate.origin(),
                (certificate.digest(), certificate.clone()),
            )
        {
            return false;
        }

        let batches = certificate
            .header
            .payload
            .clone()
            .into_iter()
            .map(|(digest, id)| {
                self.batch_worker_store.insert(digest.clone(), id);
                digest
            })
            .collect::<Vec<_>>();

        let new_batches = batches
            .clone()
            .into_iter()
            .filter(|b| !self.batch_txo_store.contains_key(b))
            .collect::<Vec<_>>();

        // update for new batches
        for batch in new_batches {
            match self
                .batch_meta_store
                .notify_read(batch.clone())
                .await
                .expect("Failed to notify read meta")
            {
                Some(meta) => {
                    for (txo, tx) in meta.0.clone() {
                        // update maps
                        self.txo_tx_store
                            .entry(txo)
                            .and_modify(|s| {
                                if s.insert(tx) {
                                    // added one more tx
                                    // update conflicted set
                                    self.conflicted_txo.insert(txo);
                                }
                            })
                            .or_insert(vec![tx].into_iter().collect());
                        update_votes(&mut self.tx_votes_store, txo, &voter, round);
                    }

                    self.batch_txo_store.insert(batch.clone(), meta.get_txos());

                    update_votes(&mut self.batch_votes_store, batch.clone(), &voter, round);
                }
                None => {
                    let cert_digest = certificate.digest();
                    tracing::warn!("meta for {batch} in {cert_digest} at round {round} is missing");

                    // if the meta has been deleted, we can skip it, no need to panic
                }
            }
        }

        let mut cert_to_process = VecDeque::new();

        // record votes for previous rounds
        if let Some(parents) =
            self.get_parents_for_counting(dag, &certificate.header.parents, round - 1)
        {
            cert_to_process.extend(parents.into_iter());
        }

        let mut tx_to_commit = HashSet::new();
        let mut batch_to_commit = HashSet::new();

        while !cert_to_process.is_empty() {
            let cert = cert_to_process
                .pop_front()
                .expect("Cannot push from non empty to_process");

            let (txs, batches, vote_changed) = self.update_batches_vote(
                &cert
                    .header
                    .payload
                    .keys()
                    .map(|digest| digest.to_owned())
                    .collect(),
                voter.clone(),
                round,
            );

            if vote_changed {
                // some votes updated may need to check its parent
                if let Some(parents) =
                    self.get_parents_for_counting(dag, &cert.header.parents, cert.round() - 1)
                {
                    cert_to_process.extend(parents.into_iter());
                };
            }

            tx_to_commit.extend(txs.into_iter());
            batch_to_commit.extend(batches.into_iter());
        }

        // fast commit
        if round % 2 == 1 {
            let conflicted_txs = self
                .conflicted_txo
                .clone()
                .into_iter()
                .filter_map(|txo| self.txo_tx_store.get(&txo))
                .flatten()
                .map(|x| *x)
                .collect::<HashSet<_>>();

            self.fast_commit(
                tx_to_commit
                    .into_iter()
                    .filter(|x| !conflicted_txs.contains(x))
                    .map(|tx| (tx, true))
                    .collect(),
                batch_to_commit
                    .into_iter()
                    .filter_map(|batch| match self.batch_txo_store.get(&batch) {
                        None => {
                            tracing::warn!("batch missed txo!");
                            None
                        }
                        Some(txos) => {
                            if txos.is_disjoint(&self.conflicted_txo) {
                                Some((
                                    batch.to_owned(),
                                    self.batch_worker_store
                                        .get(&batch)
                                        .expect("workers missed")
                                        .to_owned(),
                                ))
                            } else {
                                None
                            }
                        }
                    })
                    .collect(),
            )
            .await;
        }

        true
    }

    /// Update votes for batches and underlying txs
    /// return txs and batches that might be fast committed
    fn update_batches_vote(
        &mut self,
        digests: &Vec<BatchDigest>,
        voter: PublicKey,
        round: Round,
    ) -> (HashSet<TransactionId>, HashSet<BatchDigest>, bool) {
        let mut batches_to_commit = HashSet::new();
        let mut txs_to_commit = HashSet::new();
        let mut changed = false;
        for digest in digests {
            if self.fast_committed_batch.contains(digest) {
                // quick skip
                continue;
            }

            let (batch_vote, batch_changed) = update_votes(
                &mut self.batch_votes_store,
                digest.to_owned(),
                &voter,
                round,
            );

            if batch_changed {
                // we may commit the batch?
                if self.count_stackes(&batch_vote) > self.quorum {
                    batches_to_commit.insert(digest.to_owned());
                }

                // some txs may change?
                if let Some(txs) = self.get_txs_from_batch(digest) {
                    for tx in txs {
                        if self.fast_committed_tx.contains_key(&tx) {
                            // quick skip
                            continue;
                        }

                        let (tx_vote, tx_changed) =
                            update_votes(&mut self.tx_votes_store, tx, &voter, round);

                        // we may commit the tx?
                        if tx_changed && self.count_stackes(&tx_vote) > self.quorum {
                            txs_to_commit.insert(tx);
                        }
                    }
                }
            }
            changed |= batch_changed;
        }

        (txs_to_commit, batches_to_commit, changed)
    }

    /// fast commit can be trigger by two cases
    /// 1. a batch reach sufficient votes, we send to process it.
    /// 2. a transaction's result has been changed, we record it for future formal commit
    async fn fast_commit(
        &mut self,
        txs: HashMap<TransactionId, bool>,
        digest: HashMap<BatchDigest, WorkerId>,
    ) {
        if !digest.is_empty() {
            self.tx_fast_commit
                .send(digest.clone())
                .await
                .expect("Failed to send fast commit message");

            self.fast_committed_batch.extend(digest.keys());
        }

        for (tx, result) in txs {
            // the design of this arch does not allow to commit a single tx,
            // so we only record it for fast desicion.
            self.fast_committed_tx.insert(tx, result);
        }
    }

    pub async fn output_results(&mut self, blocks: Vec<Certificate>) -> TxResults {
        let mut results = HashMap::new();

        let all_batches = blocks
            .clone()
            .into_iter()
            .map(|cert| cert.header.payload.keys().copied().collect::<Vec<_>>())
            .flatten();

        let batches_to_commit = all_batches
            .clone()
            .filter(|batch| {
                if self.fast_committed_batch.remove(&batch) {
                    // filter fast committed batches
                    match self.get_txs_from_batch(batch) {
                        Some(txs) => {
                            results.extend(txs.into_iter().map(|tx| (tx, true)));
                        }
                        None => {
                            tracing::warn!("fast committed batch cannot find txs")
                        }
                    };
                    return false;
                }
                true
            })
            .collect::<HashSet<_>>();

        let mut nb_of_txs = results.len();

        let txs_to_commit: HashSet<TransactionId> = batches_to_commit
            .clone()
            .into_iter()
            .filter_map(|cert| self.get_txs_from_batch(&cert))
            .flatten()
            .collect();

        nb_of_txs += txs_to_commit.len();
        // step1 we update the frontier
        self.frontier = compute_frontier(blocks.clone());

        if self.conflicted_txo.is_empty() {
            // quick path, nothing to resolve
            results.extend(txs_to_commit.clone().into_iter().map(|tx| {
                match self.fast_committed_tx.remove(&tx) {
                    // copy fast committe result, others are all success
                    Some(res) => (tx, res),
                    None => (tx, true),
                }
            }));
        } else {
            // we have conflicted txs, need to process them

            let txo_to_commit = batches_to_commit
                .clone()
                .into_iter()
                .filter_map(|digest| self.batch_txo_store.get(&digest))
                .flatten()
                .map(|x| x.to_owned())
                .collect();
            // step2, we compute the conflicted tx-txo pair
            // we compute by intersection of conflicted txo and consumed txo
            // then we reverse the map to make the tx as key
            // this set is realtively small, so we compute it at runtime
            // if this is the bottleneck, we can store it in storage
            let mut conflicted_tx: HashSet<TransactionId> = HashSet::new();
            let mut conflicted_txo_tx: HashMap<TransactionInput, HashSet<TransactionId>> =
                HashMap::new();
            for c_txo in self.conflicted_txo.intersection(&txo_to_commit) {
                if let Some(c_txs) = self.txo_tx_store.get(c_txo) {
                    let intersect = txs_to_commit.intersection(&c_txs);
                    conflicted_txo_tx
                        .insert(c_txo.to_owned(), intersect.clone().map(|x| *x).collect());
                    conflicted_tx.extend(intersect);
                }
            }

            // step3 we remove all txs with known result
            for tx in txs_to_commit {
                if let Some(res) = self.fast_committed_tx.remove(&tx) {
                    results.insert(tx, res);
                } else if !conflicted_tx.contains(&tx) {
                    results.insert(tx, true);
                }
            }

            // step4, stable sort according to txo
            let mut txo_to_process = conflicted_txo_tx
                .keys()
                .clone()
                .map(|k| *k)
                .collect::<Vec<_>>();

            let touched_txo = txo_to_process.clone();

            txo_to_process.sort_by_key(|x| *x);

            // step5, we start to loop for deciding successful txs for each txo.
            let mut txo_to_process = VecDeque::from_iter(txo_to_process.into_iter());

            let mut succeed = Vec::new();
            let mut failed = HashSet::new();

            while !txo_to_process.is_empty() {
                let c_txo = txo_to_process
                    .pop_front()
                    .expect("failed to processing conflicted txs");

                // pop the set to process
                let c_txs = conflicted_txo_tx
                    .remove(&c_txo)
                    .expect("missed txs for conflicted txo");

                let mut votes = self
                    .prune_votes(
                        c_txs
                            .to_owned()
                            .into_iter()
                            .map(|tx| {
                                (
                                    tx,
                                    self.tx_votes_store
                                        .get(&tx)
                                        .expect("failed to read tx votes")
                                        .to_owned(),
                                )
                            })
                            .collect(),
                    )
                    .into_iter()
                    .map(|(tx, v)| (tx, self.count_stackes(&v)))
                    .collect::<Vec<_>>();

                votes.sort_by_key(|(_, v)| *v);

                succeed.push(votes[0].0);
                for (tx, _) in votes[1..].into_iter() {
                    failed.insert(tx.to_owned());
                }

                // now we need to check if any other txo's results can be decided now
                conflicted_txo_tx = conflicted_txo_tx
                    .into_iter()
                    .filter(|(txo, txs)| {
                        // we only check the last succeed results
                        let to_check = succeed.last().expect("No succeed tx");
                        if txs.to_owned().remove(to_check) {
                            // this set has the succeed tx
                            for tx in txs {
                                // remainging txs should also be marked as failed
                                failed.insert(tx.to_owned());
                            }

                            // we can reduce one round
                            txo_to_process.retain(|x| x != txo);

                            return false;
                        }
                        true
                    })
                    .collect();

                // do it again for failed set, since we may update it.
                conflicted_txo_tx = conflicted_txo_tx
                    .into_iter()
                    .map(|(txo, txs)| (txo, txs.difference(&failed).map(|x| *x).collect()))
                    .collect();

                // now we can process next conflicted txo
            }

            // now we have succeed txs and failed txs
            // finally fast commit other results
            let succeed = succeed.into_iter().collect::<HashSet<_>>();
            let mut fastcommit = vec![];
            for txo in touched_txo {
                // remove from txo_tx
                match self.txo_tx_store.remove(&txo) {
                    Some(txs) => {
                        for tx in txs {
                            if succeed.contains(&tx) {
                                results.insert(tx, true);
                            } else if failed.contains(&tx) {
                                results.insert(tx, false);
                            } else {
                                fastcommit.push((tx, false));
                            }
                        }
                    }
                    None => {
                        tracing::error!("conflicted txs disapeared");
                    }
                }
            }

            // These txs are failed anyway, fast commit them for future round

            let fastcommit: HashMap<TransactionId, bool> = fastcommit.into_iter().collect();
            self.fast_commit(fastcommit.clone(), HashMap::new()).await;

            // one additional step, we need to make sure these fast committed
            // txs will not be considered as valid txs in future
            for txo in self.conflicted_txo.clone() {
                self.txo_tx_store.entry(txo).and_modify(|txs| {
                    txs.retain(|x| !fastcommit.contains_key(x));
                });
            }
        }

        // clean up
        let mut tasks = vec![];
        for b in all_batches {
            tasks.push(self.batch_meta_store.remove(b));
            if let Some(txos) = self.batch_txo_store.remove(&b) {
                for txo in txos {
                    // we need to make sure no more tx consume this txo
                    // will reach the board.
                    self.conflicted_txo.remove(&txo);

                    if let Some(txs) = self.txo_tx_store.remove(&txo) {
                        for tx in txs {
                            self.tx_votes_store.remove(&tx);
                            self.fast_committed_tx.remove(&tx);
                        }
                    }
                }
            }

            self.batch_votes_store.remove(&b);
            self.batch_worker_store.remove(&b);
            self.fast_committed_batch.remove(&b);
        }

        join_all(tasks.into_iter()).await;

        let full_length = results.len();

        if full_length != nb_of_txs {
            tracing::warn!("Among all {nb_of_txs} transactions {full_length} are processed")
        }

        TxResults(results)
    }

    /// Compute the stakes of a give vote results
    fn count_stackes(&self, votes: &NodeVotes) -> Stake {
        votes
            .0
            .clone()
            .into_iter()
            .filter_map(|(pk, _)| self.stake_table.get(&pk))
            .map(|x| x.to_owned())
            .sum()
    }

    /// get txs according to the batch
    fn get_txs_from_batch(&self, batch: &BatchDigest) -> Option<Vec<TransactionId>> {
        match self.batch_txo_store.get(batch) {
            Some(v) => Some(
                v.to_owned()
                    .into_iter()
                    .filter_map(|txo| self.txo_tx_store.get(&txo))
                    .flatten()
                    .map(|x| x.to_owned())
                    .collect::<Vec<_>>(),
            ),
            None => None,
        }
    }

    fn get_parents_for_counting(
        &self,
        dag: &Dag,
        parents: &BTreeSet<CertificateDigest>,
        round: Round,
    ) -> Option<Vec<Certificate>> {
        match dag.get(&round) {
            Some(candidates) => Some(
                candidates
                    .to_owned()
                    .into_iter()
                    .filter_map(|(pk, (digest, cert))| match self.frontier.get(&pk) {
                        Some(bound) => {
                            if *bound < round && parents.contains(&digest) {
                                // available round and in parents
                                Some(cert)
                            } else {
                                None
                            }
                        }
                        None => None,
                    })
                    .collect(),
            ),
            None => None,
        }
    }

    /// Given a set of (tx, votes) return a purned set of <tx, votes>
    /// this method is used for deciding the winner of a set of conflicted txs
    fn prune_votes(
        &self,
        votes: Vec<(TransactionId, NodeVotes)>,
    ) -> Vec<(TransactionId, NodeVotes)> {
        let mut vote_frontier = self.frontier.clone();
        let mut pruned_votes = vec![];

        for (tx, vote) in votes {
            let new_vote = vote
                .0
                .into_iter()
                .filter_map(|(pk, r)| match vote_frontier.get(&pk) {
                    Some(bound) => {
                        if r > bound.to_owned() {
                            // future votes
                            return None;
                        } else {
                            // fetch the lowest bound
                            vote_frontier.insert(pk.clone(), r)?;
                            // keep the vote
                            Some((pk, r))
                        }
                    }
                    None => None,
                })
                .collect::<HashMap<_, _>>();

            pruned_votes.push((tx, new_vote));
        }

        let mut result = vec![];
        // do it again to remove the votes with high rounds
        for (tx, vote) in pruned_votes {
            let new_vote = vote
                .into_iter()
                .filter_map(|(pk, r)| {
                    if let Some(min) = vote_frontier.get(&pk) {
                        if r == *min {
                            return Some((pk, r));
                        }
                    }
                    return None;
                })
                .collect::<HashMap<_, _>>();

            result.push((tx, NodeVotes(new_vote)));
        }
        result
    }
}

fn compute_frontier(blocks: Vec<Certificate>) -> HashMap<PublicKey, Round> {
    let mut frontier: HashMap<PublicKey, Round> = HashMap::new();

    for (a, mut b) in blocks
        .into_iter()
        .map(|cert| (cert.header.author.clone(), cert.round()))
    {
        // Insert or update the map with key a and value b
        // If there is already a value for key a, use max function to keep the bigger one
        frontier
            .entry(a)
            .and_modify(|e| *e = *e.max(&mut b))
            .or_insert(b);
    }
    return frontier;
}

/// update the votes of a give hash map store
/// return if a copy of the latest vote and if it has been changed
fn update_votes<K>(
    store: &mut HashMap<K, NodeVotes>,
    key: K,
    voter: &PublicKey,
    round: Round,
) -> (NodeVotes, bool)
where
    K: Eq + std::hash::Hash,
{
    let mut changed = !store.contains_key(&key);
    (
        store
            .entry(key)
            .and_modify(|v| {
                changed = v.update(&voter, round);
            })
            .or_insert(NodeVotes::new(&voter, round))
            .to_owned(),
        changed,
    )
}
