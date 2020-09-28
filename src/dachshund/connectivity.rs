/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase};
use crate::dachshund::simple_undirected_graph::UndirectedGraph;
use std::collections::BTreeSet;

type OrderedNodeSet = BTreeSet<NodeId>;

pub trait Connectivity: GraphBase {
    fn _get_is_connected<'a>(
        &'a self,
        edge_fn: fn(
            &'a Self::NodeType,
        ) -> Box<
            dyn Iterator<Item = &'a <<Self as GraphBase>::NodeType as NodeBase>::NodeEdgeType> + 'a,
        >,
    ) -> Result<bool, &'static str> {
        let mut visited: OrderedNodeSet = BTreeSet::new();
        if self.count_nodes() == 0 {
            return Err("Graph is empty");
        }
        let root = self.get_ids_iter().next().unwrap();
        self.visit_nodes_from_root(&root, &mut visited, edge_fn);
        Ok(visited.len() == self.count_nodes())
    }
}
pub trait ConnectivityUndirected: GraphBase
where
    Self: Connectivity,
    Self: UndirectedGraph,
{
    fn get_is_connected(&self) -> Result<bool, &'static str> {
        self._get_is_connected(Self::NodeType::get_edges)
    }
}
