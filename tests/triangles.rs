/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
#![feature(test)]

extern crate lib_dachshund;
extern crate test;

use lib_dachshund::dachshund::algorithms::clustering::Clustering;
use lib_dachshund::dachshund::id_types::NodeId;
use lib_dachshund::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use lib_dachshund::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use lib_dachshund::dachshund::algorithms::transitivity::Transitivity;

use test::Bencher;

// The complete graph on 4 nodes with one edge removed.
// This is the minimal counterexample where T(G) != C(G).
fn get_almost_k4_graph() -> SimpleUndirectedGraph {
    let v = vec![(0, 1), (0, 2), (0, 3), (1, 2), (1, 3)];
    SimpleUndirectedGraphBuilder::from_vector(
        &v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
    )
}

#[test]
fn test_triangle_count() {
    let k4 = SimpleUndirectedGraphBuilder::get_complete_graph(4);
    for node_id in k4.nodes.keys() {
        assert_eq!(3, k4.triangle_count(*node_id));
    }

    let almost_k4 = get_almost_k4_graph();
    for i in 0..4 {
        let id = NodeId::from(i as i64);
        assert_eq!(if i <= 1 { 2 } else { 1 }, almost_k4.triangle_count(id));
    }
}

#[bench]
fn bench_triangle_count(b: &mut Bencher) {
    let k100 = SimpleUndirectedGraphBuilder::get_complete_graph(100);
    b.iter(|| {
        for node_id in k100.nodes.keys() {
            k100.triangle_count(*node_id);
        }
    });
}

#[test]
fn test_clustering_coefficient() {
    let k4 = &SimpleUndirectedGraphBuilder::get_complete_graph(4);
    for node_id in k4.nodes.keys() {
        assert_eq!(1.0, k4.get_clustering_coefficient(*node_id).unwrap());
    }
    assert_eq!(1.0, k4.get_avg_clustering());

    let almost_k4 = &get_almost_k4_graph();

    assert!(((5 as f64 / 6 as f64) - almost_k4.get_avg_clustering()).abs() <= 0.00001);
}

#[test]
fn test_transitivity() {
    let k4 = &SimpleUndirectedGraphBuilder::get_complete_graph(4);
    assert_eq!(1.0, k4.get_transitivity());

    let almost_k4 = &get_almost_k4_graph();
    assert_eq!(0.75, almost_k4.get_transitivity());
}

#[test]
fn test_approx_avg_clustering() {
    let k4 = &SimpleUndirectedGraphBuilder::get_complete_graph(4);
    assert_eq!(1.0, k4.get_approx_avg_clustering(10));

    let almost_k4 = &get_almost_k4_graph();
    let approx_clustering = almost_k4.get_approx_avg_clustering(100000);
    assert!(((5 as f64 / 6 as f64) - approx_clustering).abs() <= 0.01);
}

#[test]
fn test_approx_transitivity() {
    let k4 = &SimpleUndirectedGraphBuilder::get_complete_graph(4);
    assert_eq!(1.0, k4.get_approx_transitivity(10));

    let almost_k4 = &get_almost_k4_graph();
    let approx_transitivity = almost_k4.get_approx_transitivity(100000);

    println!("{}", approx_transitivity);

    assert!((0.75 - approx_transitivity).abs() <= 0.01);
}
