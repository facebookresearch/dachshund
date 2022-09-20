/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate fxhash;
extern crate nalgebra as na;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeLabel;
use crate::dachshund::node::Node;
use fxhash::FxHashMap;
use std::collections::hash_map::{Keys, Values};

pub trait LabeledGraph: GraphBase {
    fn get_core_labels(&self) -> Vec<NodeLabel>;
    fn get_non_core_labels(&self) -> Option<Vec<NodeLabel>>;
    fn get_node_by_label(&self, node_id: NodeLabel) -> &Node;
    fn has_node_by_label(&self, node_id: NodeLabel) -> bool;
    fn get_reverse_labels_map(&self) -> FxHashMap<u32, NodeLabel>;
}

/// Keeps track of a bipartite graph composed of "core" and "non-core" nodes. Only core ->
/// non-core connections may exist in the graph. The neighbors of core nodes are non-cores, the
/// neighbors of non-core nodes are cores. Graph edges are stored in the neighbors field of
/// each node. If the id of a node is known, its Node object can be retrieved via the
/// nodes HashMap. To iterate over core and non-core nodes, the struct also provides the
/// core_ids and non_core_ids vectors.
pub struct TypedGraph {
    pub nodes: FxHashMap<u32, Node>,
    pub core_ids: Vec<u32>,
    pub non_core_ids: Vec<u32>,
    pub labels_map: FxHashMap<NodeLabel, u32>,
}
impl LabeledGraph for TypedGraph {
    fn get_core_labels(&self) -> Vec<NodeLabel> {
        self.labels_map
            .iter()
            .filter(|(_label, node_id)| self.nodes[node_id].is_core)
            .map(|(label, _node_id)| *label)
            .collect()
    }
    fn get_non_core_labels(&self) -> Option<Vec<NodeLabel>> {
        Some(
            self.labels_map
                .iter()
                .filter(|(_label, node_id)| !self.nodes[node_id].is_core)
                .map(|(label, _node_id)| *label)
                .collect(),
        )
    }
    fn get_node_by_label(&self, node_id: NodeLabel) -> &Node {
        &self.nodes[&self.labels_map[&node_id]]
    }

    fn has_node_by_label(&self, node_id: NodeLabel) -> bool {
        self.labels_map.contains_key(&node_id)
            && ((self.labels_map[&node_id] as usize) < self.nodes.len())
    }

    fn get_reverse_labels_map(&self) -> FxHashMap<u32, NodeLabel> {
        self.labels_map
            .iter()
            .map(|(label, node_id)| (*node_id, *label))
            .collect()
    }
}
impl GraphBase for TypedGraph {
    type NodeType = Node;

    fn get_core_ids(&self) -> &Vec<u32> {
        &self.core_ids
    }
    fn get_non_core_ids(&self) -> Option<&Vec<u32>> {
        Some(&self.non_core_ids)
    }

    fn get_ids_iter(&self) -> Keys<u32, Node> {
        self.nodes.keys()
    }
    fn get_mut_nodes(&mut self) -> &mut FxHashMap<u32, Node> {
        &mut self.nodes
    }
    fn get_nodes_iter(&self) -> Values<u32, Node> {
        self.nodes.values()
    }
    fn has_node(&self, node_id: u32) -> bool {
        (node_id as usize) < self.nodes.len()
    }
    fn get_node(&self, node_id: u32) -> &Node {
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
            nodes: FxHashMap::default(),
            core_ids: Vec::new(),
            non_core_ids: Vec::new(),
            labels_map: FxHashMap::default(),
        }
    }
}
