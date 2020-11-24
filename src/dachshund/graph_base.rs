/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::NodeBase;
use std::collections::hash_map::{Keys, Values};
use std::collections::HashMap;

/// General-purpose trait which indicates the minimum amount of shared context
/// required between all graph objects. Currently built to accommodate a graph
/// with "core" and "non-core" ids. A GraphBase is built by a GraphBuilder.
pub trait GraphBase
where
    Self: Sized,
    Self::NodeType: NodeBase,
{
    type NodeType;

    fn get_core_ids(&self) -> &Vec<NodeId>;
    fn get_non_core_ids(&self) -> Option<&Vec<NodeId>>;
    fn get_ids_iter(&self) -> Keys<NodeId, Self::NodeType>;
    fn get_nodes_iter(&self) -> Values<NodeId, Self::NodeType>;
    fn get_mut_nodes(&mut self) -> &mut HashMap<NodeId, Self::NodeType>;
    fn has_node(&self, node_id: NodeId) -> bool;
    fn get_node(&self, node_id: NodeId) -> &Self::NodeType;
    fn count_edges(&self) -> usize;
    fn count_nodes(&self) -> usize;

    fn get_ordered_node_ids(&self) -> Vec<NodeId> {
        let mut node_ids: Vec<NodeId> = self.get_ids_iter().cloned().collect();
        node_ids.sort();
        node_ids
    }
}
