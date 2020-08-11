/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate rustc_serialize;

use std::cmp::Reverse;
use std::cmp::{Eq, PartialEq};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};

use rustc_serialize::json;

use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::{GraphId, NodeId, NodeTypeId};
use crate::dachshund::node::Node;
use crate::dachshund::row::CliqueRow;
use crate::dachshund::scorer::Scorer;

use std::sync::mpsc::Sender;

/// This data structure contains everything that identifies a candidate (fuzzy) clique. To
/// reiterate, a (fuzzy) clique is a subgraph of edges going from some set of "core" nodes
/// to some set of "non_core" nodes. A "true" clique involves this subgraph being complete,
/// whereas a "fuzzy" clique allows for some edges to be missing. Note that the Candidate
/// data structure itself enforces no such consistency guarantees. It just provides a
/// convenient bookkeeping abstraction with which the search algorithm can work.
///
/// The struct keeps state in two `HashSets`, of core and non_core node ids. There's also a
/// convenience reference to `Graph`, a checksum summarising the full state, and a field
/// in which to maintain the candidate's current score.
///
/// Note that in the current implementation, ``core'' ids must all be of the same type,
/// whereas non-core ids can be of any type is desired.
pub struct Candidate<'a, TGraph>
where
    TGraph: GraphBase,
{
    pub graph: &'a TGraph,
    pub core_ids: HashSet<NodeId>,
    pub non_core_ids: HashSet<NodeId>,
    pub checksum: Option<u64>,
    score: Option<f32>,
}
impl<'a, T: GraphBase> Hash for Candidate<'a, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.checksum.unwrap().hash(state);
    }
}
impl<'a, T: GraphBase> PartialEq for Candidate<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.checksum == other.checksum && self.score == other.score
    }
}
impl<'a, T: GraphBase> Eq for Candidate<'a, T> {}
impl<'a, T: GraphBase> fmt::Display for Candidate<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.checksum.unwrap())
    }
}

impl<'a, TGraph: GraphBase> Candidate<'a, TGraph> {
    /// creates an empty candidate object, refering to a graph.
    pub fn init_blank(graph: &'a TGraph) -> Self {
        Self {
            graph,
            core_ids: HashSet::new(),
            non_core_ids: HashSet::new(),
            checksum: None,
            score: None,
        }
    }

    /// creates a Candidate object from a single node ID.
    pub fn new(node_id: NodeId, graph: &'a TGraph, scorer: &Scorer) -> CLQResult<Self> {
        let mut candidate: Self = Candidate::init_blank(graph);
        candidate.add_node(node_id)?;
        let score = scorer.score(&candidate)?;
        candidate.set_score(score)?;
        Ok(candidate)
    }

    /// creates a Candidate object from an array of CliqueRows.
    pub fn from_clique_rows(
        rows: &'a Vec<CliqueRow>,
        graph: &'a TGraph,
        scorer: &Scorer,
    ) -> CLQResult<Option<Self>> {
        assert!(!rows.is_empty());
        let mut candidate: Candidate<TGraph> = Candidate::init_blank(graph);
        for row in rows {
            if graph.has_node(row.node_id) {
                let node: &Node = graph.get_node(row.node_id);
                assert_eq!(node.non_core_type, row.target_type);
                candidate.add_node(node.node_id)?;
            }
        }
        // could be that no nodes overlapped
        if candidate.checksum == None {
            return Ok(None);
        }
        let score = scorer.score(&candidate)?;
        candidate.set_score(score)?;
        Ok(Some(candidate))
    }

    /// add node to the clique -- this results in the score being reset, and the
    /// clique checksum being changed.
    pub fn add_node(&mut self, node_id: NodeId) -> CLQResult<()> {
        let mut s = DefaultHasher::new();
        node_id.hash(&mut s);
        let node_hash: u64 = s.finish();
        if self.checksum != None {
            self.checksum = Some(self.checksum.unwrap().wrapping_add(node_hash));
        } else {
            self.checksum = Some(node_hash);
        }
        if self.graph.get_node(node_id).is_core() {
            self.core_ids.insert(node_id);
        } else {
            self.non_core_ids.insert(node_id);
        }
        self.reset_score();
        Ok(())
    }

    /// returns sorted vector of core IDs -- useful for printing
    pub fn sorted_core_ids(&self) -> Vec<NodeId> {
        let mut vec: Vec<NodeId> = self.core_ids.iter().cloned().collect();
        vec.sort();
        vec
    }

    /// returns sorted vector of non-core IDs -- useful for printing
    pub fn sorted_non_core_ids(&self) -> Vec<NodeId> {
        let mut vec: Vec<NodeId> = self.non_core_ids.iter().cloned().collect();
        vec.sort();
        vec
    }

    /// sets score, as computed by a Scorer class.
    pub fn set_score(&mut self, score: f32) -> CLQResult<()> {
        if self.score.is_some() {
            return Err(CLQError::from(
                "Tried to set score on an already scored candidate.",
            ));
        }
        self.score = Some(score);
        Ok(())
    }

    /// resets its own score -- use case: if self has been cloned and then expanded with
    /// a new node.
    fn reset_score(&mut self) {
        self.score = None;
    }

    /// given a node ID, returns a reference to that node.
    pub fn get_node(&self, node_id: NodeId) -> &Node {
        self.graph.get_node(node_id)
    }

    /// obtains cliqueness score (higher means ``better'' quality clique, however defined)
    pub fn get_score(&self) -> CLQResult<f32> {
        let score = self
            .score
            .ok_or_else(|| "Tried to get score from an unscored candidate.")?;
        Ok(score)
    }

    /// encodes self as tab-separated "wide" format
    pub fn to_printable_row(&self, target_types: &[String]) -> CLQResult<String> {
        let encode_err_handler = |e: json::EncoderError| Err(CLQError::from(e.to_string()));

        let cliqueness = self.get_cliqueness()?;
        let core_ids: Vec<i64> = self.sorted_core_ids().iter().map(|x| x.value()).collect();
        let non_core_ids: Vec<i64> = self
            .sorted_non_core_ids()
            .iter()
            .map(|x| x.value())
            .collect();

        let mut s = String::new();
        s.push_str(&core_ids.len().to_string());
        s.push_str("\t");
        s.push_str(&non_core_ids.len().to_string());
        s.push_str("\t");

        s.push_str(&json::encode(&core_ids).or_else(encode_err_handler)?);
        s.push_str("\t");

        s.push_str(&json::encode(&non_core_ids).or_else(encode_err_handler)?);
        s.push_str("\t");

        let non_core_types_str: Vec<String> = self
            .non_core_ids
            .clone()
            .into_iter()
            .map(|id| target_types[self.get_node(id).non_core_type.unwrap().value() - 1].clone())
            .collect();
        s.push_str(&json::encode(&non_core_types_str).or_else(encode_err_handler)?);
        s.push_str("\t");
        s.push_str(&cliqueness.to_string());
        s.push_str("\t");
        s.push_str(&json::encode(&self.get_core_densities()).or_else(encode_err_handler)?);
        s.push_str("\t");
        s.push_str(
            &json::encode(&self.get_non_core_densities(target_types.len())?)
                .or_else(encode_err_handler)?,
        );
        Ok(s)
    }

    /// used for interaction with Transformer classes.
    pub fn get_output_rows(&self, graph_id: GraphId) -> CLQResult<Vec<CliqueRow>> {
        let mut out: Vec<CliqueRow> = Vec::new();

        for core_id in self.sorted_core_ids() {
            let row: CliqueRow = CliqueRow {
                graph_id,
                node_id: core_id,
                target_type: None,
            };
            out.push(row);
        }

        for non_core_id in self.sorted_non_core_ids() {
            let non_core_node = self.get_node(non_core_id);
            let row: CliqueRow = CliqueRow {
                graph_id,
                node_id: non_core_id,
                target_type: non_core_node.non_core_type,
            };
            out.push(row);
        }
        Ok(out)
    }

    /// convenience function, used for debugging and "long-format" printing
    pub fn print(
        &self,
        graph_id: GraphId,
        target_types: &[String],
        core_type: &str,
        output: &Sender<(String, bool)>,
    ) -> CLQResult<()> {
        for output_row in &self.get_output_rows(graph_id)? {
            let node_type: String = match output_row.target_type {
                // this is hacky -- when t is 0 it's an indication of this being the
                // core type, but not for TypedGraphBuilder
                Some(t) => target_types[t.value() - 1].clone(),
                None => core_type.to_string(),
            };
            output.send((format!(
                "{}\t{}\t{}",
                graph_id.value(),
                output_row.node_id.value(),
                node_type
            ), false)).unwrap();
        }
        Ok(())
    }

    /// create a copy of itself, needed for expand_with_node
    pub fn replicate(&self, keep_score: bool) -> Self {
        Self {
            graph: self.graph,
            core_ids: self.core_ids.clone(),
            non_core_ids: self.non_core_ids.clone(),
            checksum: self.checksum,
            score: match keep_score {
                true => self.score,
                false => None,
            },
        }
    }

    /// creates a copy of itself and adds a node to said copy,
    /// checking that the node does not already belong to itself.
    fn expand_with_node(&self, node_id: NodeId) -> CLQResult<Self> {
        let mut candidate = self.replicate(false);
        if self.get_node(node_id).is_core() {
            assert!(!candidate.core_ids.contains(&node_id));
            assert!(!self.core_ids.contains(&node_id));
        } else {
            assert!(!candidate.non_core_ids.contains(&node_id));
            assert!(!self.non_core_ids.contains(&node_id));
        }
        candidate.add_node(node_id)?;
        Ok(candidate)
    }

    /// finds nodes that are already connected to the candidate's members, but not
    /// among the members themselves. Sorts in descending order by the number of
    /// ties with members, returning at most num_to_search expansion candidates.
    fn get_expansion_candidates(
        &self,
        num_to_search: usize,
        visited_candidates: &mut HashSet<u64>,
    ) -> CLQResult<Vec<Self>> {
        assert!(!visited_candidates.contains(&self.checksum.unwrap()));
        let mut tie_counts: Vec<(NodeId, usize)> = Vec::new();
        for node_id in self.get_adjacent_nodes()? {
            let node = self.get_node(node_id);
            let num_ties = match node.is_core() {
                true => node.count_ties_with_ids(&self.non_core_ids),
                false => node.count_ties_with_ids(&self.core_ids),
            };
            tie_counts.push((node_id, num_ties));
        }
        // sort by number of ties, with node_id as tie breaker for deterministic behaviour
        tie_counts.sort_by_key(|k| (Reverse(k.1), k.0));

        let mut i = 0;
        let mut expansion_candidates: Vec<Self> = Vec::new();
        for (node_id, _num_ties) in tie_counts {
            let candidate = self.expand_with_node(node_id)?;
            assert!(self.checksum != candidate.checksum);
            if !visited_candidates.contains(&candidate.checksum.unwrap()) {
                expansion_candidates.push(candidate);
                i += 1;
            }
            if i == num_to_search {
                return Ok(expansion_candidates);
            }
        }
        assert!(self.checksum.unwrap() != 0);
        visited_candidates.insert(self.checksum.unwrap());
        Ok(expansion_candidates)
    }

    /// finds (up to) num_to_search expansion candidates and scores them.
    pub fn one_step_search(
        &self,
        num_to_search: usize,
        visited_candidates: &mut HashSet<u64>,
        scorer: &Scorer,
    ) -> CLQResult<Vec<Self>> {
        let mut expansion_candidates: Vec<Self> =
            self.get_expansion_candidates(num_to_search, visited_candidates)?;
        for candidate in &mut expansion_candidates {
            let score = scorer.score(candidate)?;
            candidate.set_score(score)?;
        }
        Ok(expansion_candidates)
    }

    /// used to find nodes that are "adjacent" -- i.e., connected with existing members,
    /// but not among the members themselves.
    fn get_adjacent_nodes(&self) -> CLQResult<HashSet<NodeId>> {
        let mut node_set: HashSet<NodeId> = HashSet::new();
        for &node_id in &self.core_ids {
            let node = self.get_node(node_id);
            for edge in &node.neighbors {
                let non_core_id = edge.target_id;
                if !self.non_core_ids.contains(&non_core_id) {
                    node_set.insert(non_core_id);
                }
            }
        }
        for &non_core_id in &self.non_core_ids {
            let non_core_node = self.get_node(non_core_id);
            for edge in &non_core_node.neighbors {
                let node_id = edge.target_id;
                if !self.core_ids.contains(&node_id) {
                    node_set.insert(node_id);
                }
            }
        }
        Ok(node_set)
    }

    /// Returns ``size'' of candidate, defined as the maximum number of edges
    /// that could connect all nodes currently in the candidate. For weighted graphs
    /// this is the sum of maximum weights for edges that could connect nodes currently
    /// in the candidates.
    pub fn get_size(&self) -> CLQResult<usize> {
        let mut size: usize = 0;
        for &non_core_id in &self.non_core_ids {
            let non_core_node = self.get_node(non_core_id);
            size += self.core_ids.len()
                * non_core_node
                    .max_edge_count_with_core_node()?
                    .ok_or_else(CLQError::err_none)?;
        }
        Ok(size)
    }

    /// computes "cliqueness", the density of ties between core and non-core nodes.
    pub fn get_cliqueness(&self) -> CLQResult<f32> {
        let size = self.get_size()?;
        let ties_between_nodes = self.count_ties_between_nodes()?;
        let cliqueness: f32 = if size > 0 {
            ties_between_nodes as f32 / size as f32
        } else {
            1.0
        };
        Ok(cliqueness)
    }

    /// checks if Candidate is a true clique, defined as a subgraph where the total number
    /// of ties between nodes is equal to the maximum number of ties between nodes.
    pub fn is_clique(&self) -> CLQResult<bool> {
        Ok(self.count_ties_between_nodes()? == self.get_size()?)
    }

    /// counts the total number of ties between candidate's core nodes and non_cores
    pub fn count_ties_between_nodes(&self) -> CLQResult<usize> {
        let mut num_ties: usize = 0;
        for &non_core_id in &self.non_core_ids {
            num_ties += self
                .get_node(non_core_id)
                .count_ties_with_ids(&self.core_ids);
        }
        Ok(num_ties)
    }

    /// gets densities over each non-core type (useful to compute non-core diversity score)
    fn get_non_core_densities(&self, num_non_core_types: usize) -> CLQResult<Vec<f32>> {
        let mut non_core_max_counts: Vec<usize> = vec![0; num_non_core_types + 1];
        let mut non_core_out_counts: Vec<usize> = vec![0; num_non_core_types + 1];
        for &non_core_id in &self.non_core_ids {
            let non_core = self.get_node(non_core_id);
            let non_core_type_id: NodeTypeId =
                non_core.non_core_type.ok_or_else(CLQError::err_none)?;
            let num_ties: usize = non_core.count_ties_with_ids(&self.core_ids);
            let max_density = non_core
                .max_edge_count_with_core_node()?
                .ok_or_else(CLQError::err_none)?;
            non_core_max_counts[non_core_type_id.value()] += max_density * self.core_ids.len();
            non_core_out_counts[non_core_type_id.value()] += num_ties;
        }
        let mut non_core_density: Vec<f32> = Vec::new();
        for i in 1..non_core_max_counts.len() {
            non_core_density.push(non_core_out_counts[i] as f32 / non_core_max_counts[i] as f32);
        }
        Ok(non_core_density)
    }

    /// gets core densities for each non-core node
    fn get_core_densities(&self) -> Vec<f32> {
        let mut counts: Vec<f32> = Vec::new();
        let max_size: usize = self
            .non_core_ids
            .iter()
            .map(|&id| {
                self.get_node(id)
                    .max_edge_count_with_core_node()
                    .unwrap()
                    .unwrap()
            })
            .sum();
        for &node_id in &self.core_ids {
            let node = self.get_node(node_id);
            let num_ties: usize = node.count_ties_with_ids(&self.non_core_ids);
            counts.push(num_ties as f32 / max_size as f32);
        }
        counts
    }
}
