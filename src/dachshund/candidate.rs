/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cmp::{min, Eq, PartialEq, Reverse};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};

use fxhash::FxHashMap;

use roaring::RoaringBitmap;

use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::id_types::{GraphId, NodeLabel, NodeTypeIdInternal};
use crate::dachshund::node::{Node, NodeBase};
use crate::dachshund::row::CliqueRow;
use crate::dachshund::scorer::Scorer;
use crate::dachshund::typed_graph::LabeledGraph;

use std::sync::mpsc::Sender;

/// This data structure represents a guarantee or promise about the local cliqueness
/// for some core nodes. It should be interpreted as saying
/// "Every core node in a candidate clique has at least 'num_edges'
/// possible edges, except *maybe* the nodes listed in 'exceptions'
/// ("maybe" because we might not have inspected them yet)."
///
/// If we're interested in knowing whether every core candidate has local density over some
/// value that's corresponds to a number of edges lower than our guaranteed 'num_edges',
/// we only need to inspect the exceptions.
#[derive(Clone)]
pub struct LocalDensityGuarantee {
    pub num_edges: usize,
    pub exceptions: RoaringBitmap,
}

/// A recipe for a candidate is a checksum of another and a node id.
/// This represents the claim that you can generate the candidate in question
/// by adding node node_id to an existing candidate identified with checksum.
/// Recipes allow us to identify the best candidates for the next generation
/// and only do candidate replication lazily.

#[derive(Clone)]
pub struct Recipe {
    pub checksum: Option<u64>,
    pub node_id: Option<u32>,
    pub score: Option<f32>,
    pub local_guarantee: Option<LocalDensityGuarantee>,
}

impl PartialEq for Recipe {
    fn eq(&self, other: &Recipe) -> bool {
        (self.checksum == other.checksum) && (self.node_id == other.node_id)
    }
}
impl Eq for Recipe {}
impl Hash for Recipe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Some(node_id) = self.node_id {
            merge_checksum(self.checksum, node_id).unwrap().hash(state);
        } else {
            self.checksum.unwrap().hash(state);
        }
    }
}

type NeigbhorhoodMap = HashMap<u32, u32>;

/// This data structure contains everything that identifies a candidate (fuzzy) clique. To
/// reiterate, a (fuzzy) clique is a subgraph of edges going from some set of "core" nodes
/// to some set of "non_core" nodes. A "true" clique involves this subgraph being complete,
/// whereas a "fuzzy" clique allows for some edges to be missing. Note that the Candidate
/// data structure itself enforces no such consistency guarantees. It just provides a
/// convenient bookkeeping abstraction with which the search algorithm can work.
///
/// The struct keeps state in two `RoaringBitmap`s, of core and non_core node ids. There's also a
/// convenience reference to `Graph`, a checksum summarising the full state, and a field
/// in which to maintain the candidate's current score.
///
/// Some attributes are tracked for the convenience of the scorer and adjusted incrementally
/// during add node.
/// - ties_between_nodes and max_core_node_edges help calculate cliqueness
///     (maintainted by increment_max_core_node_edges and increment_ties_between_nodes)
/// - neighborhood: of nodes adjacent to the clique and the edge count from
///     'in the clique' to help with candidate generation
///     (maintained by adjust_neighborhood)
/// - local_guarantee: a guarantee about the local density to help check
///     the candidate maintains a sufficiently high local density.
///     NB: This optimizes for memory consumption and the case where the cliques
///     are core-heavy.
/// - node_counts: a counter of the number of nodes by type. First entry is always the core type.
///
/// Note that in the current implementation, ``core'' ids must all be of the same type,
/// whereas non-core ids can be of any type is desired.

pub struct Candidate<'a, TGraph>
where
    TGraph: LabeledGraph,
{
    pub graph: &'a TGraph,
    pub core_ids: RoaringBitmap,
    pub non_core_ids: RoaringBitmap,
    pub checksum: Option<u64>,
    score: Option<f32>,
    max_core_node_edges: usize,
    ties_between_nodes: usize,
    local_guarantee: LocalDensityGuarantee,
    neighborhood: NeigbhorhoodMap,
    node_counts: Vec<usize>,
}

impl<'a, T: LabeledGraph> Hash for Candidate<'a, T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.checksum.unwrap().hash(state);
    }
}
impl<'a, T: LabeledGraph> PartialEq for Candidate<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.checksum == other.checksum && self.score == other.score
    }
}
impl<'a, T: LabeledGraph> Eq for Candidate<'a, T> {}
impl<'a, T: LabeledGraph> fmt::Display for Candidate<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.checksum.unwrap())
    }
}

impl<'a, TGraph: LabeledGraph> Candidate<'a, TGraph>
where
    TGraph: LabeledGraph<NodeType = Node>,
{
    /// creates an empty candidate object, refering to a graph.
    pub fn init_blank(graph: &'a TGraph, num_non_core_types: usize) -> Self {
        Self {
            graph,
            core_ids: RoaringBitmap::new(),
            non_core_ids: RoaringBitmap::new(),
            checksum: None,
            score: None,
            max_core_node_edges: 0,
            ties_between_nodes: 0,
            local_guarantee: LocalDensityGuarantee {
                num_edges: 0,
                exceptions: RoaringBitmap::new(),
            },
            neighborhood: HashMap::new(),
            node_counts: vec![0; num_non_core_types + 1],
        }
    }

    /// creates a Candidate object from a single node ID.
    pub fn new(node_id: u32, graph: &'a TGraph, scorer: &Scorer) -> CLQResult<Self> {
        let mut candidate: Self = Candidate::init_blank(graph, scorer.get_num_non_core_types());
        candidate.add_node(node_id)?;
        let score = scorer.score(&mut candidate)?;
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
        let mut candidate: Candidate<TGraph> =
            Candidate::init_blank(graph, scorer.get_num_non_core_types());
        for row in rows {
            if graph.has_node_by_label(row.node_id) {
                let node = graph.get_node_by_label(row.node_id);
                assert_eq!(node.non_core_type, row.target_type);
                candidate.add_node(node.node_id)?;
            }
        }
        // could be that no nodes overlapped
        if candidate.checksum.is_none() {
            return Ok(None);
        }
        let score = scorer.score(&mut candidate)?;
        candidate.set_score(score)?;
        Ok(Some(candidate))
    }

    /// add node to the clique -- this results in the score being reset, and the
    /// clique checksum being changed.
    pub fn add_node(&mut self, node_id: u32) -> CLQResult<()> {
        self.checksum = merge_checksum(self.checksum, node_id);

        if self.graph.get_node(node_id).is_core() {
            self.core_ids.insert(node_id);
            self.local_guarantee.exceptions.insert(node_id);
            self.node_counts[0_usize] += 1;
        } else {
            self.non_core_ids.insert(node_id);
            self.increment_max_core_node_edges(node_id)?;
            self.node_counts[self.graph.get_node(node_id).non_core_type.unwrap().value()] += 1;
        }

        self.increment_ties_between_nodes(node_id);
        self.reset_score();

        self.adjust_neighborhood(node_id);
        Ok(())
    }

    /// returns sorted vector of core IDs -- useful for printing
    pub fn sorted_core_labels(&self, reverse_labels_map: &FxHashMap<u32, NodeLabel>) -> Vec<i64> {
        let mut vec: Vec<i64> = self
            .core_ids
            .iter()
            .map(|x| reverse_labels_map[&x].value())
            .collect();
        vec.sort();
        vec
    }

    pub fn as_recipe(&self) -> Recipe {
        Recipe {
            checksum: self.checksum,
            node_id: None,
            score: self.score,
            local_guarantee: Some(self.local_guarantee.clone()),
        }
    }

    /// returns sorted vector of non-core IDs -- useful for printing
    pub fn sorted_non_core_labels(
        &self,
        reverse_labels_map: &FxHashMap<u32, NodeLabel>,
    ) -> Vec<i64> {
        let mut vec: Vec<i64> = self
            .non_core_ids
            .iter()
            .map(|x| reverse_labels_map[&x].value())
            .collect();
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
    pub fn get_node(&self, node_id: u32) -> &Node {
        self.graph.get_node(node_id)
    }

    /// obtains cliqueness score (higher means ``better'' quality clique, however defined)
    pub fn get_score(&self) -> CLQResult<f32> {
        let score = self
            .score
            .ok_or("Tried to get score from an unscored candidate.")?;
        Ok(score)
    }

    /// Get a clone of the candidates neighborhood (which is a map from
    /// every node adjacent to the clique to the number of edges between
    /// that node and the members of the clique.)
    pub fn get_neighborhood(&self) -> NeigbhorhoodMap {
        self.neighborhood.clone()
    }

    /// Get a clone of the local guarantee which makes a promise about the
    /// number of edges.
    pub fn get_local_guarantee(&self) -> LocalDensityGuarantee {
        self.local_guarantee.clone()
    }

    /// Get a reference to node counts which is a vector recording
    /// the number of nodes in the clique by type.
    /// First element is the count of core nodes, since NodeTypeId 0 is the Core Type.
    pub fn get_node_counts(&self) -> Vec<usize> {
        self.node_counts.clone()
    }

    /// encodes self as tab-separated "wide" format
    pub fn to_printable_row(
        &self,
        target_types: &[String],
        reverse_labels_map: FxHashMap<u32, NodeLabel>,
    ) -> CLQResult<String> {
        let encode_err_handler = |e: serde_json::Error| Err(CLQError::from(e.to_string()));

        let cliqueness = self.get_cliqueness()?;
        let core_ids: Vec<i64> = self.sorted_core_labels(&reverse_labels_map);
        let non_core_ids: Vec<i64> = self.sorted_non_core_labels(&reverse_labels_map);

        let mut s = String::new();
        s.push_str(&core_ids.len().to_string());
        s.push('\t');
        s.push_str(&non_core_ids.len().to_string());
        s.push('\t');

        s.push_str(&serde_json::to_string(&core_ids).or_else(encode_err_handler)?);
        s.push('\t');

        s.push_str(&serde_json::to_string(&non_core_ids).or_else(encode_err_handler)?);
        s.push('\t');

        let non_core_types_str: Vec<String> = self
            .non_core_ids
            .clone()
            .into_iter()
            .map(|id| target_types[self.get_node(id).non_core_type.unwrap().value() - 1].clone())
            .collect();
        s.push_str(&serde_json::to_string(&non_core_types_str).or_else(encode_err_handler)?);
        s.push('\t');
        s.push_str(&cliqueness.to_string());
        s.push('\t');
        s.push_str(&serde_json::to_string(&self.get_core_densities()).or_else(encode_err_handler)?);
        s.push('\t');
        s.push_str(
            &serde_json::to_string(&self.get_non_core_densities(target_types.len())?)
                .or_else(encode_err_handler)?,
        );
        Ok(s)
    }

    /// used for interaction with Transformer classes.
    pub fn get_output_rows(
        &self,
        graph_id: GraphId,
        reverse_labels_map: FxHashMap<u32, NodeLabel>,
    ) -> CLQResult<Vec<CliqueRow>> {
        let mut out: Vec<CliqueRow> = Vec::new();

        for core_id in self.sorted_core_labels(&reverse_labels_map) {
            let row: CliqueRow = CliqueRow {
                graph_id,
                node_id: core_id.into(),
                target_type: None,
            };
            out.push(row);
        }

        for non_core_id in self.sorted_non_core_labels(&reverse_labels_map) {
            let non_core_node = self.graph.get_node_by_label(non_core_id.into());
            let row: CliqueRow = CliqueRow {
                graph_id,
                node_id: non_core_id.into(),
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
        output: &Sender<(Option<String>, bool)>,
    ) -> CLQResult<()> {
        for output_row in &self.get_output_rows(graph_id, self.graph.get_reverse_labels_map())? {
            let node_type: String = match output_row.target_type {
                // this is hacky -- when t is 0 it's an indication of this being the
                // core type, but not for TypedGraphBuilder
                Some(t) => target_types[t.value() - 1].clone(),
                None => core_type.to_string(),
            };
            output
                .send((
                    Some(format!(
                        "{}\t{}\t{}",
                        graph_id.value(),
                        output_row.node_id.value(),
                        node_type
                    )),
                    false,
                ))
                .unwrap();
        }
        Ok(())
    }

    /// Create a copy of itself, needed for initializing candidate
    /// from node. This happens for every candidate we want to add to
    /// the beam for the next epoch. So the performance of the
    /// search is sensitive to the cost of this operation.
    pub fn replicate(&self, keep_score: bool) -> Self {
        // We clone these hashmaps very frequently so
        // we keep capacity artificially low.
        let mut new_neighborhood = self.neighborhood.clone();
        if 2 * new_neighborhood.capacity() > new_neighborhood.len() {
            new_neighborhood.shrink_to_fit()
        }

        Self {
            graph: self.graph,
            core_ids: self.core_ids.clone(),
            non_core_ids: self.non_core_ids.clone(),
            checksum: self.checksum,
            score: match keep_score {
                true => self.score,
                false => None,
            },
            max_core_node_edges: self.max_core_node_edges,
            ties_between_nodes: self.ties_between_nodes,
            local_guarantee: self.local_guarantee.clone(),
            neighborhood: new_neighborhood,
            node_counts: self.node_counts.clone(),
        }
    }

    pub fn expand_from_recipe(&self, recipe: &Recipe) -> CLQResult<Self> {
        let mut candidate = self.replicate(false);

        if let Some(node_id) = recipe.node_id {
            if self.get_node(node_id).is_core() {
                assert!(!candidate.core_ids.contains(node_id));
                assert!(!self.core_ids.contains(node_id));
            } else {
                assert!(!candidate.non_core_ids.contains(node_id));
                assert!(!self.non_core_ids.contains(node_id));
            }
            candidate.add_node(node_id)?;
            candidate.score = recipe.score;
            if let Some(local_guarantee) = &recipe.local_guarantee {
                candidate.local_guarantee = local_guarantee.clone();
            }
        } else {
            candidate.score = self.score;
        }
        Ok(candidate)
    }

    fn get_expansion_candidates(
        &self,
        num_to_search: usize,
        visited_candidates: &mut HashSet<u64>,
    ) -> CLQResult<Vec<Recipe>> {
        assert!(!visited_candidates.contains(&self.checksum.unwrap()));
        let mut h = BinaryHeap::with_capacity(num_to_search);

        // Use the heap to keep track of the nodes with the most ties to the
        // current candidate: If the heap is already full, look at the max element
        // (the one with fewest ties because of Reverse). If the element we're
        // considering is smaller (more ties) we can remove the max element
        // and push the new element onto the heap.
        for (node_id, num_ties) in self.neighborhood.iter() {
            let heap_element = (Reverse(num_ties), node_id);
            if h.len() < num_to_search {
                h.push(heap_element);
            } else if heap_element < *h.peek().unwrap() {
                h.pop();
                h.push(heap_element);
            }
        }

        let mut expansion_candidates: Vec<Recipe> = Vec::with_capacity(num_to_search);

        for (_num_ties, &node_id) in h.into_sorted_vec().iter() {
            let recipe = Recipe {
                checksum: self.checksum,
                node_id: Some(node_id),
                score: None,
                local_guarantee: None,
            };

            let new_checksum = merge_checksum(self.checksum, node_id).unwrap();
            if !visited_candidates.contains(&new_checksum) {
                expansion_candidates.push(recipe);
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
    ) -> CLQResult<Vec<Recipe>> {
        let mut expansion_recipes: Vec<Recipe> =
            self.get_expansion_candidates(num_to_search, visited_candidates)?;
        for recipe in &mut expansion_recipes {
            let score = scorer.score_recipe(recipe, self)?;
            recipe.score = Some(score);
        }
        Ok(expansion_recipes)
    }

    /// Returns ``size'' of candidate, defined as the maximum number of edges
    /// that could connect all nodes currently in the candidate. For weighted graphs
    /// this is the sum of maximum weights for edges that could connect nodes currently
    /// in the candidates.
    pub fn get_size(&self) -> CLQResult<usize> {
        Ok(self.core_ids.len() as usize * self.max_core_node_edges)
    }

    /// Returns size of candidate if the given node were to be added. Assumes that
    /// node is not currently part of the candidate.
    pub fn get_size_with_node(&self, node: &Node) -> CLQResult<usize> {
        if node.is_core() {
            Ok((self.core_ids.len() as usize + 1) * self.max_core_node_edges)
        } else {
            let new_edge_count = node
                .max_edge_count_with_core_node()?
                .ok_or_else(CLQError::err_none)?;
            Ok(self.core_ids.len() as usize * (self.max_core_node_edges + new_edge_count))
        }
    }

    // Update the size to account for for adding node_id. Can be called immediately before
    // or after inserting the node into the set of ids. Only call this when adding a noncore node.
    fn increment_max_core_node_edges(&mut self, node_id: u32) -> CLQResult<()> {
        let new_edge_count = self
            .get_node(node_id)
            .max_edge_count_with_core_node()?
            .ok_or_else(CLQError::err_none)?;
        self.max_core_node_edges += new_edge_count;
        Ok(())
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

    /// computes "cliqueness", the density of ties between core and non-core nodes.
    pub fn get_cliqueness_with_node(&self, node: &Node) -> CLQResult<f32> {
        let size = self.get_size_with_node(node)?;

        let new_ties = if node.is_core() {
            node.count_ties_with_ids(&self.non_core_ids)
        } else {
            node.count_ties_with_ids(&self.core_ids)
        };

        let ties_between_nodes = self.count_ties_between_nodes()? + new_ties;
        let cliqueness: f32 = if size > 0 {
            ties_between_nodes as f32 / size as f32
        } else {
            1.0
        };
        Ok(cliqueness)
    }

    // Returns true if every core node has at least thresh fraction
    // of the possible edges (when node is added), using the
    // local density guarantee as applicable.
    pub fn local_thresh_score_with_node_at_least(
        &self,
        thresh: f32,
        node: &Node,
    ) -> (bool, Option<LocalDensityGuarantee>) {
        if thresh == 0.0 {
            return (true, None);
        }

        let new_max_core_node_edges = if node.is_core() {
            self.max_core_node_edges
        } else {
            self.max_core_node_edges + node.max_edge_count_with_core_node().unwrap().unwrap()
        };

        let implied_edge_thresh = (thresh * new_max_core_node_edges as f32).ceil() as usize;
        // If the existing local guarantee is stricter than the threshold we're
        // we're checking now, we only need to check the (newly added) exceptions.
        let check_all = self.local_guarantee.num_edges < implied_edge_thresh;
        let nodes_to_check = if !check_all {
            &self.local_guarantee.exceptions
        } else {
            &self.core_ids
        };

        let mut min_edges = None;
        for node_id in nodes_to_check {
            let mut edge_count = self
                .get_node(node_id)
                .count_ties_with_ids(&self.non_core_ids);
            if !node.is_core() {
                edge_count += node.count_ties_with_id(node_id)
            }
            if edge_count < implied_edge_thresh {
                return (false, None);
            }
            match min_edges {
                Some(num) => min_edges = Some(min(edge_count, num)),
                None => min_edges = Some(edge_count),
            }
        }

        // If this is a core node; we also need to check it.
        if node.is_core() {
            let new_edge_count = node.count_ties_with_ids(&self.non_core_ids);
            if new_edge_count < implied_edge_thresh {
                return (false, None);
            }
            match min_edges {
                Some(num) => min_edges = Some(min(new_edge_count, num)),
                None => min_edges = Some(new_edge_count),
            }
        }

        // If we passed the local density check, we can update the guarantee.
        // In practice, we tend to call this function repeatedly with the same
        // threshold, so we opt for fewer exceptions instead of guaranteeing
        // a higher number of edges.
        let mut new_num_edges = min_edges.unwrap_or(self.local_guarantee.num_edges);
        if !check_all {
            new_num_edges = min(self.local_guarantee.num_edges, new_num_edges);
        }

        (
            true,
            Some(LocalDensityGuarantee {
                num_edges: new_num_edges,
                exceptions: RoaringBitmap::new(),
            }),
        )
    }

    // Returns true if every core node has at least thresh fraction
    // of the possible edges, using/updating the local density guarantee
    // as applicable.
    pub fn local_thresh_score_at_least(&mut self, thresh: f32) -> bool {
        if thresh == 0.0 {
            return true;
        }

        let implied_edge_thresh = (thresh * self.max_core_node_edges as f32).ceil() as usize;
        // If the existing local guarantee is stricter than the threshold we're
        // we're checking now, we only need to check the (newly added) exceptions.
        let check_all = self.local_guarantee.num_edges < implied_edge_thresh;
        let nodes_to_check = if !check_all {
            &self.local_guarantee.exceptions
        } else {
            &self.core_ids
        };

        let mut min_edges = None;
        for node_id in nodes_to_check {
            let edge_count = self
                .get_node(node_id)
                .count_ties_with_ids(&self.non_core_ids);
            if edge_count < implied_edge_thresh {
                return false;
            }
            match min_edges {
                Some(num) => min_edges = Some(min(edge_count, num)),
                None => min_edges = Some(edge_count),
            }
        }

        // If we passed the local density check, we can update the guarantee.
        // In practice, we tend to call this function repeatedly with the same
        // threshold, so we opt for fewer exceptions instead of guaranteeing
        // a higher number of edges.
        let mut new_num_edges = min_edges.unwrap_or(self.local_guarantee.num_edges);
        if !check_all {
            new_num_edges = min(self.local_guarantee.num_edges, new_num_edges);
        }

        self.local_guarantee = LocalDensityGuarantee {
            num_edges: new_num_edges,
            exceptions: RoaringBitmap::new(),
        };
        true
    }

    /// checks if Candidate is a true clique, defined as a subgraph where the total number
    /// of ties between nodes is equal to the maximum number of ties between nodes.
    pub fn is_clique(&self) -> CLQResult<bool> {
        Ok(self.count_ties_between_nodes()? == self.get_size()?)
    }

    /// counts the total number of ties between candidate's core nodes and non_cores
    pub fn count_ties_between_nodes(&self) -> CLQResult<usize> {
        Ok(self.ties_between_nodes)
    }

    // Update the count of ties between nodes to account for adding node_id. Can be called
    // immediately before or immediately after inserting node into the set of ids.
    fn increment_ties_between_nodes(&mut self, node_id: u32) {
        let new_ties = if self.graph.get_node(node_id).is_core() {
            self.get_node(node_id)
                .count_ties_with_ids(&self.non_core_ids)
        } else {
            self.get_node(node_id).count_ties_with_ids(&self.core_ids)
        };
        self.ties_between_nodes += new_ties;
    }

    // Adjust the neighborhood hashmap to account for adding added_node:
    // Any neighbor that isn't already in our graph should have its
    // edges count in self.neighborhood increased by one, and the node we're
    // adding needs to be removed, since it is no longer adjacent to the clique.
    fn adjust_neighborhood(&mut self, node_id: u32) {
        let opposite_shore = if self.graph.get_node(node_id).is_core() {
            &self.non_core_ids
        } else {
            &self.core_ids
        };

        let neighbors: Vec<u32> = self
            .get_node(node_id)
            .edges
            .iter()
            .map(|x| x.target_id)
            .collect();

        for target_id in neighbors {
            if !opposite_shore.contains(target_id) {
                let counter = self.neighborhood.entry(target_id).or_insert(0);
                *counter += 1;
            }
        }
        self.neighborhood.remove(&node_id);
    }

    /// TODO: Can this use the non_core_counts?
    /// gets densities over each non-core type
    fn get_non_core_densities(&self, num_non_core_types: usize) -> CLQResult<Vec<f32>> {
        let mut non_core_max_counts: Vec<usize> = vec![0; num_non_core_types + 1];
        let mut non_core_out_counts: Vec<usize> = vec![0; num_non_core_types + 1];

        for non_core_id in &self.non_core_ids {
            let non_core = self.get_node(non_core_id);
            let non_core_type_id: NodeTypeIdInternal = non_core
                .non_core_type
                .ok_or_else(CLQError::err_none)?
                .value();
            let num_ties: usize = non_core.count_ties_with_ids(&self.core_ids);
            let max_density = non_core
                .max_edge_count_with_core_node()?
                .ok_or_else(CLQError::err_none)?;
            non_core_max_counts[non_core_type_id] += max_density * (self.core_ids.len() as usize);
            non_core_out_counts[non_core_type_id] += num_ties;
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
            .map(|id| {
                self.get_node(id)
                    .max_edge_count_with_core_node()
                    .unwrap()
                    .unwrap()
            })
            .sum();
        for node_id in &self.core_ids {
            let node = self.get_node(node_id);
            let num_ties: usize = node.count_ties_with_ids(&self.non_core_ids);
            counts.push(num_ties as f32 / max_size as f32);
        }
        counts
    }
}

fn merge_checksum(checksum: Option<u64>, node_id: u32) -> Option<u64> {
    let mut s = DefaultHasher::new();
    node_id.hash(&mut s);
    let node_hash: u64 = s.finish();
    if let Some(candidate_hash) = checksum {
        Some(candidate_hash.wrapping_add(node_hash))
    } else {
        Some(node_hash)
    }
}
