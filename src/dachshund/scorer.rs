/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::candidate::Candidate;
use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::graph_base::GraphBase;

/// Used to compute the "cliqueness" score of a particular candidate.
pub struct Scorer {
    num_non_core_types: usize,
    alpha: f32,
    global_thresh: Option<f32>,
    local_thresh: Option<f32>,
}

impl Scorer {
    /// Creates a new Scorer class. Typically called by the `Beam` "searcher" class,
    /// with the following parameters:
    /// - `num_non_core_types`: the number of non-core types in the graph.
    /// - `alpha`: Controls the contribution of density to the ``cliqueness'' score. Higher
    /// values mean denser cliques are prefered, all else being equal.
    /// - `global_thresh`: If provided, candidates must be at least this dense to be considered
    /// valid (quasi-)cliques.
    /// - `local_thresh`: If provided, each node in the candidate must have at least `local_thresh`
    /// proportion of ties to other nodes in the candidate, for the candidate to be considered valid.
    pub fn new(
        num_non_core_types: usize,
        alpha: f32,
        global_thresh: Option<f32>,
        local_thresh: Option<f32>,
    ) -> Scorer {
        Scorer {
            num_non_core_types,
            alpha,
            global_thresh,
            local_thresh,
        }
    }
    // computes "cliqueness" score, i.e. the objective the search algorithm is maximizing.
    pub fn score<TGraph: GraphBase>(&self, candidate: &mut Candidate<TGraph>) -> CLQResult<f32> {
        // degenerate case where there are no edges.
        if candidate.core_ids.is_empty() || candidate.non_core_ids.is_empty() {
            return Ok(-1.0);
        }
        // the more core nodes we have, the better
        let mut score = (candidate.core_ids.len() as f32 + 1.0).ln();

        // the more diverse the non-core types, the better
        let non_core_diversity_score = self.get_non_core_diversity_score(candidate)?;
        score += non_core_diversity_score;

        // Debug
        // eprintln!("Cliqueness");

        // the denser the ties, the better
        let cliqueness: f32 = candidate.get_cliqueness()?;
        score += cliqueness * self.alpha;

        // enforce a minimum density threshold on cliqueness (1.0 for true cliques)
        score *= self.get_global_thresh_score(cliqueness);

        // enforce a minimum density threshold for each core node.
        score *= self.get_local_thresh_score(candidate);
        Ok(score)
    }

    pub fn get_global_thresh_score(&self, cliqueness: f32) -> f32 {
        match self.global_thresh {
            Some(n) => (cliqueness >= n) as i64 as f32,
            None => 1.0,
        }
    }
    // used to ensure that each core node has at least % of ties with non-core nodes.
    pub fn get_local_thresh_score<TGraph: GraphBase>(
        &self,
        candidate: &mut Candidate<TGraph>,
    ) -> f32 {
        match self.local_thresh {
            Some(thresh) => candidate.local_thresh_score_at_least(thresh) as i64 as f32,
            None => 1.0,
        }
    }
    /// returns a non-core diversity score that is higher with more diverse non-core types.
    pub fn get_non_core_diversity_score<TGraph: GraphBase>(
        &self,
        candidate: &Candidate<TGraph>,
    ) -> CLQResult<f32> {
        // non_core_counts[0] currently corresponds to core nodes
        let mut non_core_counts: Vec<usize> = vec![0; self.num_non_core_types + 1];
        for &non_core_id in &candidate.non_core_ids {
            let non_core_id = candidate
                .get_node(non_core_id)
                .non_core_type
                .ok_or_else(CLQError::err_none)?;
            non_core_counts[non_core_id.value()] += 1;
        }
        let mut score: f32 = 0.0;
        for non_core_count in non_core_counts {
            score += (non_core_count as f32 + 1.0).ln();
        }
        Ok(score)
    }
}
