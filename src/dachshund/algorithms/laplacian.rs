/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::algorithms::adjacency_matrix::AdjacencyMatrix;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::NodeBase;
use nalgebra::{DMatrix, DVector};

type GraphMatrix = DMatrix<f64>;
pub trait Laplacian: GraphBase + AdjacencyMatrix {
    fn get_degree_matrix(&self) -> (GraphMatrix, Vec<NodeId>) {
        let node_ids = self.get_ordered_node_ids();
        let diag: Vec<f64> = node_ids
            .iter()
            .map(|x| self.get_node(*x).degree() as f64)
            .collect();
        (
            GraphMatrix::from_diagonal(&DVector::from_row_slice(&diag)),
            node_ids,
        )
    }
    fn get_laplacian_matrix(&self) -> (GraphMatrix, Vec<NodeId>) {
        let (deg_mat, node_ids) = self.get_degree_matrix();
        let adj_mat = self.get_adjacency_matrix_given_node_ids(&node_ids);
        (deg_mat - adj_mat, node_ids)
    }
}
