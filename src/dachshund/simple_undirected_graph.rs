/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::adjacency_matrix::AdjacencyMatrix;
use crate::dachshund::algebraic_connectivity::AlgebraicConnectivity;
use crate::dachshund::betweenness::Betweenness;
use crate::dachshund::clustering::Clustering;
use crate::dachshund::cnm_communities::CNMCommunities;
use crate::dachshund::connected_components::ConnectedComponents;
use crate::dachshund::connectivity::Connectivity;
use crate::dachshund::coreness::Coreness;
use crate::dachshund::eigenvector_centrality::EigenvectorCentrality;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::laplacian::Laplacian;
use crate::dachshund::node::{SimpleNode, NodeBase, NodeEdgeBase};
use crate::dachshund::shortest_paths::ShortestPaths;
use crate::dachshund::transitivity::Transitivity;
use std::collections::hash_map::{Keys, Values};
use std::collections::HashMap;

/// Keeps track of a simple undirected graph, composed of nodes without any type information.
pub struct SimpleUndirectedGraph {
    pub nodes: HashMap<NodeId, SimpleNode>,
    pub ids: Vec<NodeId>,
}
impl GraphBase for SimpleUndirectedGraph {
    type NodeType = SimpleNode;

    /// core and non-core IDs are the same for a `SimpleUndirectedGraph`.
    fn get_core_ids(&self) -> &Vec<NodeId> {
        &self.ids
    }
    /// core and non-core IDs are the same for a `SimpleUndirectedGraph`.
    fn get_non_core_ids(&self) -> Option<&Vec<NodeId>> {
        Some(&self.ids)
    }
    fn get_ids_iter(&self) -> Keys<NodeId, SimpleNode> {
        self.nodes.keys()
    }
    fn get_nodes_iter(&self) -> Values<NodeId, SimpleNode> {
        self.nodes.values()
    }
    fn get_mut_nodes(&mut self) -> &mut HashMap<NodeId, SimpleNode> {
        &mut self.nodes
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }
    fn get_node(&self, node_id: NodeId) -> &SimpleNode {
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
}
impl SimpleUndirectedGraph {
    pub fn as_input_rows(&self, graph_id: usize) -> String {
        let mut rows: Vec<String> = Vec::new();
        for (id, node) in &self.nodes {
            for e in node.get_edges() {
                if *id < e.get_neighbor_id() {
                    rows.push(format!(
                        "{}\t{}\t{}",
                        graph_id,
                        id.value(),
                        e.get_neighbor_id().value()
                    ));
                }
            }
        }
        rows.join("\n")
    }
    pub fn get_node_degree(&self, id: NodeId) -> usize {
        self.nodes[&id].degree()
    }
    pub fn create_empty() -> Self {
        Self {
            nodes: HashMap::new(),
            ids: Vec::new(),
        }
    }
}

impl CNMCommunities for SimpleUndirectedGraph {}
impl ConnectedComponents for SimpleUndirectedGraph {}
impl Coreness for SimpleUndirectedGraph {}

impl AdjacencyMatrix for SimpleUndirectedGraph {}
impl Betweenness for SimpleUndirectedGraph {}
impl Clustering for SimpleUndirectedGraph {}
impl Connectivity for SimpleUndirectedGraph {}
impl Laplacian for SimpleUndirectedGraph {}
impl Transitivity for SimpleUndirectedGraph {}
impl ShortestPaths for SimpleUndirectedGraph {}
impl AlgebraicConnectivity for SimpleUndirectedGraph {}
impl EigenvectorCentrality for SimpleUndirectedGraph {}
