/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase};
use nalgebra::DMatrix;
use std::collections::HashMap;

type GraphMatrix = DMatrix<f64>;
pub trait AdjacencyMatrix: GraphBase {
    fn get_adjacency_matrix_given_node_ids(&self, node_ids: &[NodeId]) -> GraphMatrix {
        let num_nodes = node_ids.len();
        let mut data: Vec<f64> = vec![0.0; num_nodes * num_nodes];
        let pos_map: HashMap<NodeId, usize> = node_ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, item)| (item, i))
            .collect();

        for (i, node_id) in node_ids.iter().enumerate() {
            for e in self.get_node(*node_id).get_edges() {
                let j = pos_map.get(&e.get_neighbor_id()).unwrap();
                let pos = i * num_nodes + j;
                data[pos] += 1.0;
            }
        }
        GraphMatrix::from_vec(num_nodes, num_nodes, data)
    }
    fn get_adjacency_matrix(&self) -> (GraphMatrix, Vec<NodeId>) {
        let node_ids = self.get_ordered_node_ids();
        (
            self.get_adjacency_matrix_given_node_ids(&node_ids),
            node_ids,
        )
    }
}
