/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use std::cmp::{Eq, PartialEq};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::id_types::{EdgeTypeId, NodeId, NodeTypeId};

/// Used to indicate a typed edge leading to the neighbor of a node.
pub struct NodeEdge {
    pub edge_type: EdgeTypeId,
    pub target_id: NodeId,
}
impl NodeEdge {
    pub fn new(edge_type: EdgeTypeId, target_id: NodeId) -> Self {
        Self {
            edge_type,
            target_id,
        }
    }
}

/// Core data structure used to represent a node in our graph. A node can be
/// either a "core" node, or a non-core node. Non-core nodes also have a type (e.g.
/// IP, URL, etc.) Each node also keeps track of its neighbors, via a vector of
/// edges that specify edge type and target node.
pub struct Node {
    pub node_id: NodeId,
    pub is_core: bool,
    pub non_core_type: Option<NodeTypeId>,
    pub edges: Vec<NodeEdge>,
    pub neighbors: HashMap<NodeId, Vec<NodeEdge>>,
}
impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
    }
}
impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}
impl Eq for Node {}

impl Node {
    pub fn new(
        node_id: NodeId,
        is_core: bool,
        non_core_type: Option<NodeTypeId>,
        edges: Vec<NodeEdge>,
        neighbors: HashMap<NodeId, Vec<NodeEdge>>,
    ) -> Node {
        Node {
            node_id,
            is_core,
            non_core_type,
            edges,
            neighbors,
        }
    }
    /// used to determine degree in a subgraph (i.e., the clique we're considering).
    /// HashSet is supplied by Candidate struct.
    pub fn count_ties_with_ids(&self, ids: &HashSet<NodeId>) -> usize {
        let mut num_ties: usize = 0;
        // If we have low degree and we're checking against a big set, iterate through our neighbors
        if self.neighbors.len() <= ids.len() {
            for (neighbor_id, edges) in &self.neighbors {
                if ids.contains(&neighbor_id) {
                    num_ties += edges.len();
                }
            }
        // otherwise iterate through the hashset and check against our neighbors.
        } else {
            for node_id in ids {
                match self.neighbors.get(node_id) {
                    Some(edges) => num_ties += edges.len(),
                    None => (),
                }
            }
        };
        num_ties
    }
    pub fn is_core(&self) -> bool {
        self.is_core
    }
    pub fn max_edge_count_with_core_node(&self) -> CLQResult<Option<usize>> {
        let non_core_type = self.non_core_type.ok_or_else(|| {
            CLQError::from(format!(
                "Node {} is unexpextedly a core node.",
                self.node_id.value()
            ))
        })?;
        Ok(non_core_type.max_edge_count_with_core_node())
    }
    /// degree is the edge count (in an unweighted graph)
    pub fn degree(&self) -> usize {
        self.edges.len()
    }
}
