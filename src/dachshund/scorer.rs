/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::candidate::Candidate;
use crate::dachshund::error::CLQResult;
use crate::dachshund::node::Node;
use crate::dachshund::search_problem::SearchProblem;
use crate::dachshund::typed_graph::LabeledGraph;
use std::rc::Rc;

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
    pub fn new(num_non_core_types: usize, search_problem: &Rc<SearchProblem>) -> Scorer {
        Scorer {
            num_non_core_types,
            alpha: search_problem.alpha,
            global_thresh: search_problem.global_thresh,
            local_thresh: search_problem.local_thresh,
        }
    }
    // computes "cliqueness" score, i.e. the objective the search algorithm is maximizing.
    pub fn score<TGraph: LabeledGraph<NodeType = Node>>(
        &self,
        candidate: &mut Candidate<TGraph>,
    ) -> CLQResult<f32> {
        // degenerate case where there are no edges.
        if candidate.core_ids.is_empty() || candidate.non_core_ids.is_empty() {
            return Ok(-1.0);
        }
        // the more core nodes we have, the better
        let mut score = (candidate.core_ids.len() as f32 + 1.0).ln();

        // the more diverse the non-core types, the better
        let non_core_diversity_score = self.get_non_core_diversity_score(candidate)?;
        score += non_core_diversity_score;

        // the denser the ties, the better
        let cliqueness: f32 = candidate.get_cliqueness()?;
        score += cliqueness * self.alpha;

        // enforce a minimum density threshold on cliqueness (1.0 for true cliques)
        score *= self.get_global_thresh_score(cliqueness);

        // enforce a minimum density threshold for each core node.
        score *= self.get_local_thresh_score(candidate);
        Ok(score)
    }

    pub fn get_num_non_core_types(&self) -> usize {
        self.num_non_core_types
    }

    pub fn get_global_thresh_score(&self, cliqueness: f32) -> f32 {
        match self.global_thresh {
            Some(n) => (cliqueness >= n) as i64 as f32,
            None => 1.0,
        }
    }
    // used to ensure that each core node has at least % of ties with non-core nodes.
    pub fn get_local_thresh_score<TGraph: LabeledGraph<NodeType = Node>>(
        &self,
        candidate: &mut Candidate<TGraph>,
    ) -> f32 {
        match self.local_thresh {
            Some(thresh) => candidate.local_thresh_score_at_least(thresh) as i64 as f32,
            None => 1.0,
        }
    }
    /// returns a non-core diversity score that is higher with more diverse non-core types.
    pub fn get_non_core_diversity_score<TGraph: LabeledGraph<NodeType = Node>>(
        &self,
        candidate: &Candidate<TGraph>,
    ) -> CLQResult<f32> {
        let non_core_counts = candidate.get_non_core_counts();
        let mut score: f32 = 0.0;
        for &non_core_count in non_core_counts.values() {
            score += (non_core_count as f32 + 1.0).ln();
        }
        Ok(score)
    }
}
