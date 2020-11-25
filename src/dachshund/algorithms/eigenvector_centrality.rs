/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::algorithms::adjacency_matrix::AdjacencyMatrix;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use nalgebra::DMatrix;
use std::collections::HashMap;

type GraphMatrix = DMatrix<f64>;

pub trait EigenvectorCentrality: GraphBase + AdjacencyMatrix {
    fn get_eigenvector_centrality(&self, eps: f64, max_iter: usize) -> HashMap<NodeId, f64> {
        let (adj_mat, node_ids) = self.get_adjacency_matrix();
        // Power iteration adaptation from
        // https://www.sci.unich.it/~francesc/teaching/network/eigenvector.html

        let n = node_ids.len();
        let mut x0: GraphMatrix = GraphMatrix::zeros(1, n);
        let mut x1: GraphMatrix = GraphMatrix::repeat(1, n, 1.0 / n as f64);
        let mut iter: usize = 0;
        while (&x0 - &x1).abs().sum() > eps && iter < max_iter {
            x0 = x1;
            x1 = &x0 * &adj_mat;
            let m = x1.max();
            x1 /= m;
            iter += 1;
        }
        let mut ev: HashMap<NodeId, f64> = HashMap::new();
        for i in 0..n {
            ev.insert(node_ids[i], x1[i]);
        }
        ev
    }
}
