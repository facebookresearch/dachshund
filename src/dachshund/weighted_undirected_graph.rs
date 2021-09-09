/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate fxhash;
use crate::dachshund::algorithms::adjacency_matrix::AdjacencyMatrix;
use crate::dachshund::algorithms::algebraic_connectivity::AlgebraicConnectivity;
use crate::dachshund::algorithms::betweenness::Betweenness;
use crate::dachshund::algorithms::clustering::Clustering;
use crate::dachshund::algorithms::connected_components::{
    ConnectedComponents, ConnectedComponentsUndirected,
};
use crate::dachshund::algorithms::connectivity::{Connectivity, ConnectivityUndirected};
use crate::dachshund::algorithms::coreness::Coreness;
use crate::dachshund::algorithms::eigenvector_centrality::EigenvectorCentrality;
use crate::dachshund::algorithms::laplacian::Laplacian;
use crate::dachshund::algorithms::shortest_paths::ShortestPaths;
use crate::dachshund::algorithms::transitivity::Transitivity;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase, WeightedNode, WeightedNodeBase};
use crate::dachshund::simple_undirected_graph::UndirectedGraph;

use fxhash::FxHashMap;
use std::collections::hash_map::{Keys, Values};

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
