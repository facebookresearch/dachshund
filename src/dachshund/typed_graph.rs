/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate nalgebra as na;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::Node;
use std::collections::hash_map::{Keys, Values};
use std::collections::HashMap;

/// Keeps track of a bipartite graph composed of "core" and "non-core" nodes. Only core ->
/// non-core connections may exist in the graph. The neighbors of core nodes are non-cores, the
/// neighbors of non-core nodes are cores. Graph edges are stored in the neighbors field of
/// each node. If the id of a node is known, its Node object can be retrieved via the
/// nodes HashMap. To iterate over core and non-core nodes, the struct also provides the
/// core_ids and non_core_ids vectors.
pub struct TypedGraph {
    pub nodes: HashMap<NodeId, Node>,
    pub core_ids: Vec<NodeId>,
    pub non_core_ids: Vec<NodeId>,
}
impl GraphBase for TypedGraph {
    type NodeType = Node;

    fn get_core_ids(&self) -> &Vec<NodeId> {
        &self.core_ids
    }
    fn get_non_core_ids(&self) -> Option<&Vec<NodeId>> {
        Some(&self.non_core_ids)
    }
    fn get_ids_iter(&self) -> Keys<NodeId, Node> {
        self.nodes.keys()
    }
    fn get_mut_nodes(&mut self) -> &mut HashMap<NodeId, Node> {
        &mut self.nodes
    }
    fn get_nodes_iter(&self) -> Values<NodeId, Node> {
        self.nodes.values()
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }
    fn get_node(&self, node_id: NodeId) -> &Node {
        &self.nodes[&node_id]
    }
    fn count_edges(&self) -> usize {
        let mut num_edges: usize = 0;
        for node in self.nodes.values() {
            num_edges += node.edges.len();
        }
        num_edges
    }
    fn count_nodes(&self) -> usize {
        self.nodes.len()
    }
    fn create_empty() -> Self {
        TypedGraph {
            nodes: HashMap::new(),
            core_ids: Vec::new(),
            non_core_ids: Vec::new(),
        }
    }
}
