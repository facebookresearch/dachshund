/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::Node;
use std::collections::HashMap;
use std::collections::hash_map::Keys;

/// General-purpose trait which indicates the minimum amount of shared context
/// required between all graph objects. Currently built to accommodate a graph
/// with "core" and "non-core" ids. A GraphBase is built by a GraphBuilder.
pub trait GraphBase
where
    Self: Sized,
{
    type NodeType;

    fn get_core_ids(&self) -> &Vec<NodeId>;
    fn get_non_core_ids(&self) -> Option<&Vec<NodeId>>;
    fn get_ids_iter(&self) -> Keys<NodeId, Self::NodeType>;
    fn get_mut_nodes(&mut self) -> &mut HashMap<NodeId, Node>;
    fn has_node(&self, node_id: NodeId) -> bool;
    fn get_node(&self, node_id: NodeId) -> &Node;
    fn count_edges(&self) -> usize;
    fn count_nodes(&self) -> usize;
}
