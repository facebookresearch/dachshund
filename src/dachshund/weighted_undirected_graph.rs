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
    fn get_fractional_coreness_values(&self) -> HashMap<NodeId, f64> {
        // Start by making a priority queue of each node with its weight as a priority.
        // Use PriorityQueue instead of BinaryHeap because the workload uses change priority.

        // [TODO Fix trait bounds and move this to coreness.rs.]
        // [TODO:Perf] Switch to hashbrown. Benchmark performance.
        let mut pq = PriorityQueue::with_capacity(self.nodes.len());

        for node in self.get_nodes_iter() {
            pq.push(node.get_id(), Reverse(NotNan::new(node.weight()).unwrap()));
        }
        let mut coreness: HashMap<NodeId, f64> = HashMap::new();

        while !pq.is_empty() {
            let (first_node_id, Reverse(next_shell_coreness)) = pq.pop().unwrap();
            let mut next_shell: Vec<NodeId> = vec![first_node_id];

            while !next_shell.is_empty() {
                let node_id = next_shell.pop().unwrap();

                loop {
                    match pq.peek() {
                        Some((node_id, Reverse(nn))) => {
                            if *nn == next_shell_coreness {
                                next_shell.push(*node_id);
                                pq.pop();
                            } else {
                                break;
                            }
                        }
                        None => break,
                    }
                }

                coreness.insert(node_id, next_shell_coreness.into_inner());

                // For each neighbor that hasn't been removed yet,
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
                            ();
                        }
                        None => (),
                    };
                }
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
