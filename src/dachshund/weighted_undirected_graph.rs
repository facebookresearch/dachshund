/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate fxhash;
extern crate ordered_float;
extern crate priority_queue;

use crate::dachshund::algorithms::adjacency_matrix::AdjacencyMatrix;
use crate::dachshund::algorithms::algebraic_connectivity::AlgebraicConnectivity;
use crate::dachshund::algorithms::betweenness::Betweenness;
use crate::dachshund::algorithms::clustering::Clustering;
use crate::dachshund::algorithms::connected_components::{
    ConnectedComponents, ConnectedComponentsUndirected,
};
use crate::dachshund::algorithms::connectivity::{Connectivity, ConnectivityUndirected};
use crate::dachshund::algorithms::coreness::{Coreness, FractionalCoreness};
use crate::dachshund::algorithms::eigenvector_centrality::EigenvectorCentrality;
use crate::dachshund::algorithms::laplacian::Laplacian;
use crate::dachshund::algorithms::shortest_paths::ShortestPaths;
use crate::dachshund::algorithms::transitivity::Transitivity;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase, WeightedNode, WeightedNodeBase};
use crate::dachshund::simple_undirected_graph::UndirectedGraph;
use std::cmp::Reverse;

use fxhash::FxHashMap;
use std::collections::hash_map::{HashMap, Keys, Values};

use ordered_float::NotNan;
use priority_queue::PriorityQueue;

/// Keeps track of a weighted undirected graph, composed of nodes that have weighed.
pub struct WeightedUndirectedGraph {
    pub nodes: FxHashMap<NodeId, WeightedNode>,
    pub ids: Vec<NodeId>,
}
impl GraphBase for WeightedUndirectedGraph {
    type NodeType = WeightedNode;

    /// core and non-core IDs are the same for a `SimpleUndirectedGraph`.
    fn get_core_ids(&self) -> &Vec<NodeId> {
        &self.ids
    }
    /// core and non-core IDs are the same for a `SimpleUndirectedGraph`.
    fn get_non_core_ids(&self) -> Option<&Vec<NodeId>> {
        Some(&self.ids)
    }
    fn get_ids_iter(&self) -> Keys<NodeId, WeightedNode> {
        self.nodes.keys()
    }
    fn get_nodes_iter(&self) -> Values<NodeId, WeightedNode> {
        self.nodes.values()
    }
    fn get_mut_nodes(&mut self) -> &mut FxHashMap<NodeId, WeightedNode> {
        &mut self.nodes
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }
    fn get_node(&self, node_id: NodeId) -> &WeightedNode {
        &self.nodes[&node_id]
    }
    fn count_edges(&self) -> usize {
        let mut num_edges: usize = 0;
        for node in self.nodes.values() {
            num_edges += node.neighbors.len();
        }
        num_edges / 2
    }
    fn count_nodes(&self) -> usize {
        self.nodes.len()
    }
    fn create_empty() -> Self {
        WeightedUndirectedGraph {
            nodes: FxHashMap::default(),
            ids: Vec::new(),
        }
    }
}
impl WeightedUndirectedGraph {
    pub fn as_input_rows(&self, graph_id: usize) -> String {
        let mut rows: Vec<String> = Vec::new();
        for (id, node) in &self.nodes {
            for e in node.get_edges() {
                if *id < e.get_neighbor_id() {
                    rows.push(format!(
                        "{}\t{}\t{}\t{}",
                        graph_id,
                        id.value(),
                        e.get_neighbor_id().value(),
                        e.weight
                    ));
                }
            }
        }
        rows.join("\n")
    }
    pub fn get_node_degree(&self, id: NodeId) -> usize {
        self.nodes[&id].degree()
    }
    pub fn get_node_weight(&self, id: NodeId) -> f64 {
        self.nodes[&id].weight()
    }
}
impl UndirectedGraph for WeightedUndirectedGraph {}

impl ConnectedComponents for WeightedUndirectedGraph {}
impl ConnectedComponentsUndirected for WeightedUndirectedGraph {}
impl Coreness for WeightedUndirectedGraph {}
impl FractionalCoreness for WeightedUndirectedGraph {
    // [TODO Fix trait bounds and move this to coreness.rs.]
    fn get_fractional_coreness_values(&self) -> HashMap<NodeId, f64> {
        // The fractional coreness value is the same as standard k-cores except
        // using total edge weight for each vertex in the k-core, instead of the
        // degree inside the subgraph.

        // The fractional k-core (sometimes called the s-core) is a set of nodes
        // where every node has edges with total weight at least k between it and the other
        // nodes in the fractional k-core.

        // Start by making a priority queue with each node. Priority will be equal to weight
        // of that node from all edges where we haven't removed the other ends yet.
        // Use PriorityQueue instead of BinaryHeap because the workload uses change priority.
        // [TODO:Perf] Switch to hashbrown. Benchmark performance.
        let mut pq = PriorityQueue::with_capacity(self.nodes.len());

        // Initially the priority of the of each node is the node weight (the total edge weight
        // of each incident edge.)
        for node in self.get_nodes_iter() {
            pq.push(node.get_id(), Reverse(NotNan::new(node.weight()).unwrap()));
        }
        let mut coreness: HashMap<NodeId, f64> = HashMap::new();
        let mut next_shell_coreness = NotNan::new(f64::NEG_INFINITY).unwrap();

        loop {
            // Take the minimum (remaining) weight node that hasn't yet been processed.
            match pq.pop() {
                Some((node_id, Reverse(nn))) => {
                    // If the remaining weight for that node is larger than the value for the current
                    // shell, we've progressed to the next shell (all remaining nodes comprise the k-core.)
                    if nn > next_shell_coreness{
                        next_shell_coreness = nn
                    }

                    // The coreness value is the node is the current shell value we're on
                    // (Note: not its current priority; if removals from the current shell have
                    // reduced its priority below the current shell's coreness value, the node
                    // is still in this shell, not one we've already processed.)
                    coreness.insert(node_id, next_shell_coreness.into_inner());

                    // Process a removal: For each neighbor that hasn't been removed yet,
                    // decrement their priority by the weight of the edge.
                    for e in self.get_node(node_id).get_edges() {
                        let neighbor_id = e.target_id;
                        match pq.get_priority(&neighbor_id) {
                            Some(Reverse(old_priority)) => {
                                let new_priority: f64 = old_priority.into_inner() - e.weight;
                                pq.change_priority(
                                    &neighbor_id,
                                    Reverse(NotNan::new(new_priority).unwrap()),
                                );
                            }
                            None => (),
                        };
                    }
                },
                None => break
            }
        }
        coreness
    }
}

impl AdjacencyMatrix for WeightedUndirectedGraph {}
impl Clustering for WeightedUndirectedGraph {}
impl Connectivity for WeightedUndirectedGraph {}
impl ConnectivityUndirected for WeightedUndirectedGraph {}
impl Betweenness for WeightedUndirectedGraph {}
impl Laplacian for WeightedUndirectedGraph {}
impl Transitivity for WeightedUndirectedGraph {}
impl ShortestPaths for WeightedUndirectedGraph {}
impl AlgebraicConnectivity for WeightedUndirectedGraph {}
impl EigenvectorCentrality for WeightedUndirectedGraph {}
