/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::algorithms::laplacian::Laplacian;
use crate::dachshund::graph_base::GraphBase;

pub trait AlgebraicConnectivity: GraphBase + Laplacian {
    // Algebraic Connectivity, or the Fiedler Measure, is the second-smallest eigenvalue of the graph Laplacian.
    // The lower the value, the less decomposable the graph's adjacency matrix is. Thanks to the nalgebra
    // crate computing this is quite straightforward.
    fn get_algebraic_connectivity(&self) -> f64 {
        let (laplacian, _ids) = self.get_laplacian_matrix();
        let eigen = laplacian.symmetric_eigen();
        let mut eigenvalues: Vec<f64> = eigen.eigenvalues.iter().cloned().collect();
        eigenvalues.sort_by(|a, b| a.partial_cmp(b).unwrap());
        eigenvalues[1]
    }
}
