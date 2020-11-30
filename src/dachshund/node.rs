/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use std::cmp::{Eq, PartialEq};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::id_types::{EdgeTypeId, NodeId, NodeTypeId};

/// Used to indicate a typed edge leading to the neighbor of a node.
pub struct NodeEdge {
    pub edge_type: EdgeTypeId,
    pub target_id: NodeId,
}
pub trait NodeEdgeBase 
where Self: Sized {
    fn get_neighbor_id(&self) -> NodeId;
}
impl NodeEdgeBase for NodeEdge {
    fn get_neighbor_id(&self) -> NodeId {
        self.target_id
    }
}
impl NodeEdge {
    pub fn new(edge_type: EdgeTypeId, target_id: NodeId) -> Self {
        Self {
            edge_type,
            target_id,
        }
    }
}

impl NodeEdgeBase for NodeId {
    fn get_neighbor_id(&self) -> NodeId {
        *self
    }
}

pub trait NodeBase where
    Self: Sized
{
    type NodeEdgeType: NodeEdgeBase + Sized;

    fn get_id(&self) -> NodeId;
    fn get_edges(&self) -> Box<dyn Iterator<Item = &Self::NodeEdgeType> + '_>;
    fn degree(&self) -> usize;
    fn count_ties_with_ids(&self, ids: &HashSet<NodeId>) -> usize;
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
impl NodeBase for Node {
    type NodeEdgeType = NodeEdge;
    fn get_id(&self) -> NodeId {
        self.node_id
    }
    fn get_edges(&self) -> Box<dyn Iterator<Item = &NodeEdge> + '_> {
        Box::new(self.edges.iter())
    }
    /// degree is the edge count (in an unweighted graph)
    fn degree(&self) -> usize {
        self.edges.len()
    }
    /// used to determine degree in a subgraph (i.e., the clique we're considering).
    /// HashSet is supplied by Candidate struct.
    fn count_ties_with_ids(&self, ids: &HashSet<NodeId>) -> usize {
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
                if let Some(edges) = self.neighbors.get(node_id) {
                    num_ties += edges.len()
                }
            }
        };
        num_ties
    }
}

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
}

pub struct SimpleNode {
    pub node_id: NodeId,
    pub neighbors: BTreeSet<NodeId>,
}
impl Hash for SimpleNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
    }
}
impl PartialEq for SimpleNode {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}
impl Eq for SimpleNode {}
impl NodeBase for SimpleNode {
    type NodeEdgeType = NodeId;

    fn get_id(&self) -> NodeId {
        self.node_id
    }
    fn get_edges(&self) -> Box<dyn Iterator<Item = &NodeId> + '_> {
        Box::new(self.neighbors.iter())
    }
    /// degree is the edge count (in an unweighted graph)
    fn degree(&self) -> usize {
        self.neighbors.len()
    }
    /// used to determine degree in a subgraph (i.e., the clique we're considering).
    /// HashSet is supplied by Candidate struct.
    fn count_ties_with_ids(&self, ids: &HashSet<NodeId>) -> usize {
        ids.iter().filter(|x| self.neighbors.contains(x)).collect::<Vec<&NodeId>>().len()
    }
}
