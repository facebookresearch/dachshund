/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
#![feature(test)]

extern crate lib_dachshund;
extern crate test;
use lib_dachshund::dachshund::algorithms::adjacency_matrix::AdjacencyMatrix;
use lib_dachshund::dachshund::algorithms::algebraic_connectivity::AlgebraicConnectivity;
use lib_dachshund::dachshund::algorithms::connected_components::ConnectedComponents;
use lib_dachshund::dachshund::algorithms::coreness::Coreness;
use lib_dachshund::dachshund::algorithms::betweenness::Betweenness;
use lib_dachshund::dachshund::algorithms::clustering::Clustering;
use lib_dachshund::dachshund::algorithms::cnm_communities::CNMCommunities;
use lib_dachshund::dachshund::algorithms::connectivity::Connectivity;
use lib_dachshund::dachshund::algorithms::eigenvector_centrality::EigenvectorCentrality;
use lib_dachshund::dachshund::graph_base::GraphBase;
use lib_dachshund::dachshund::id_types::{GraphId, NodeId};
use lib_dachshund::dachshund::algorithms::laplacian::Laplacian;
use lib_dachshund::dachshund::row::EdgeRow;
use lib_dachshund::dachshund::algorithms::shortest_paths::ShortestPaths;
use lib_dachshund::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use lib_dachshund::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use lib_dachshund::dachshund::algorithms::transitivity::Transitivity;
use lib_dachshund::dachshund::test_utils::{gen_test_transformer, process_raw_vector};
use lib_dachshund::dachshund::transformer::Transformer;
use lib_dachshund::dachshund::id_types::NodeId;
use lib_dachshund::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use lib_dachshund::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use std::collections::HashSet;
use test::Bencher;

fn get_karate_club_edges() -> Vec<(usize, usize)> {
    vec![
        (1, 2),
        (1, 3),
        (2, 3),
        (1, 4),
        (2, 4),
        (3, 4),
        (1, 5),
        (1, 6),
        (1, 7),
        (5, 7),
        (6, 7),
        (1, 8),
        (2, 8),
        (3, 8),
        (4, 8),
        (1, 9),
        (3, 9),
        (3, 10),
        (1, 11),
        (5, 11),
        (6, 11),
        (1, 12),
        (1, 13),
        (4, 13),
        (1, 14),
        (2, 14),
        (3, 14),
        (4, 14),
        (6, 17),
        (7, 17),
        (1, 18),
        (2, 18),
        (1, 20),
        (2, 20),
        (1, 22),
        (2, 22),
        (24, 26),
        (25, 26),
        (3, 28),
        (24, 28),
        (25, 28),
        (3, 29),
        (24, 30),
        (27, 30),
        (2, 31),
        (9, 31),
        (1, 32),
        (25, 32),
        (26, 32),
        (29, 32),
        (3, 33),
        (9, 33),
        (15, 33),
        (16, 33),
        (19, 33),
        (21, 33),
        (23, 33),
        (24, 33),
        (30, 33),
        (31, 33),
        (32, 33),
        (9, 34),
        (10, 34),
        (14, 34),
        (15, 34),
        (16, 34),
        (19, 34),
        (20, 34),
        (21, 34),
        (23, 34),
        (24, 34),
        (27, 34),
        (28, 34),
        (29, 34),
        (30, 34),
        (31, 34),
        (32, 34),
        (33, 34),
    ]
}
fn get_karate_club_graph_with_one_extra_edge() -> SimpleUndirectedGraph {
    let mut rows = get_karate_club_edges();
    rows.push((35, 36));
    SimpleUndirectedGraphBuilder::from_vector(
        &rows.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
    )
}

fn get_two_karate_clubs_edges() -> Vec<(usize, usize)> {
    let mut rows = get_karate_club_edges();
    for (i, j) in get_karate_club_edges() {
        rows.push((i + 35, j + 35));
    }
    rows
}

fn get_two_karate_clubs() -> SimpleUndirectedGraph {
    let rows = get_two_karate_clubs_edges();
    SimpleUndirectedGraphBuilder::from_vector(
        &rows.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
    )
}

fn get_two_karate_clubs_with_bridge() -> SimpleUndirectedGraph {
    let mut rows = get_two_karate_clubs_edges();
    rows.push((34, 35));
    SimpleUndirectedGraphBuilder::from_vector(
        &rows.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
    )
}
fn get_karate_club_graph() -> SimpleUndirectedGraph {
    let rows = get_karate_club_edges();
    SimpleUndirectedGraphBuilder::from_vector(
        &rows.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
    )
}

#[cfg(test)]
#[test]
fn test_karate_club() {
    let graph = get_karate_club_graph();
    assert_eq!(graph.nodes.len(), 34);
    assert_eq!(graph.count_edges(), 78);
    assert_eq!(graph.get_node_degree(NodeId::from(1 as i64)), 16);
    assert_eq!(graph.get_node_degree(NodeId::from(2 as i64)), 9);
    assert_eq!(graph.get_node_degree(NodeId::from(3 as i64)), 10);
    assert_eq!(graph.get_node_degree(NodeId::from(27 as i64)), 2);
    assert_eq!(graph.get_node_degree(NodeId::from(34 as i64)), 17);

    assert_eq!(
        graph
            .get_clustering_coefficient(NodeId::from(1 as i64))
            .unwrap(),
        0.15
    );
    assert!(
        (graph
            .get_clustering_coefficient(NodeId::from(34 as i64))
            .unwrap()
            - 0.1102941)
            <= 0.00001
    );
    assert_eq!(
        graph
            .get_clustering_coefficient(NodeId::from(22 as i64))
            .unwrap(),
        1.0
    );
    assert_eq!(
        graph.get_clustering_coefficient(NodeId::from(12 as i64)),
        None
    );
    assert_eq!(
        graph
            .get_clustering_coefficient(NodeId::from(10 as i64))
            .unwrap(),
        0.0
    );
}

#[test]
fn test_shortest_paths() {
    let graph = get_karate_club_graph();
    let source = NodeId::from(1 as i64);
    let (dist, parents) = graph.get_shortest_paths(source, &None);
    assert_eq!(dist[&NodeId::from(1 as i64)], Some(0));
    assert_eq!(parents[&NodeId::from(1 as i64)].len(), 1);
    assert!(parents[&NodeId::from(1 as i64)].contains(&NodeId::from(1 as i64)));
    assert_eq!(dist[&NodeId::from(2 as i64)], Some(1));
    assert_eq!(dist[&NodeId::from(33 as i64)], Some(2));
    assert_eq!(dist[&NodeId::from(30 as i64)], Some(3));
    assert!(parents[&NodeId::from(2 as i64)].contains(&NodeId::from(1 as i64)));
    assert!(parents[&NodeId::from(10 as i64)].contains(&NodeId::from(3 as i64)));
    assert_eq!(parents[&NodeId::from(10 as i64)].len(), 1);
    assert!(parents[&NodeId::from(33 as i64)].contains(&NodeId::from(3 as i64)));
    assert!(parents[&NodeId::from(33 as i64)].contains(&NodeId::from(9 as i64)));
    assert!(parents[&NodeId::from(33 as i64)].contains(&NodeId::from(32 as i64)));
    assert_eq!(parents[&NodeId::from(33 as i64)].len(), 3);
    assert!(parents[&NodeId::from(30 as i64)].contains(&NodeId::from(33 as i64)));
    assert!(parents[&NodeId::from(30 as i64)].contains(&NodeId::from(34 as i64)));
    assert_eq!(parents[&NodeId::from(30 as i64)].len(), 2);

    let shortest_paths = graph.enumerate_shortest_paths(&dist, &parents, source);
    assert_eq!(shortest_paths.len(), 34);
    let mut unrolled_paths: HashSet<String> = HashSet::new();
    for paths in shortest_paths.values() {
        for path in paths {
            unrolled_paths.insert(
                path.iter()
                    .map(|x| format!("{}", x.value()))
                    .collect::<Vec<String>>()
                    .join("-"),
            );
        }
    }
    assert_eq!(unrolled_paths.len(), 89);
    assert_eq!(shortest_paths[&NodeId::from(2 as i64)].len(), 1);
    assert_eq!(shortest_paths[&NodeId::from(2 as i64)][0].len(), 2);
    assert_eq!(shortest_paths[&NodeId::from(30 as i64)][0].len(), 4);
    assert_eq!(shortest_paths[&NodeId::from(16 as i64)].len(), 7);
    assert!(unrolled_paths.contains("1-9-34-16"));
    assert!(unrolled_paths.contains("1-14-34-16"));
    assert!(unrolled_paths.contains("1-20-34-16"));
    assert!(unrolled_paths.contains("1-32-34-16"));
    assert!(unrolled_paths.contains("1-3-33-16"));
    assert!(unrolled_paths.contains("1-9-33-16"));
    assert!(unrolled_paths.contains("1-32-33-16"));
}

#[bench]
fn bench_shortest_paths(b: &mut Bencher) {
    b.iter(|| {
        let graph = get_karate_club_graph();
        let source = NodeId::from(1 as i64);
        let (_dist, _parents) = graph.get_shortest_paths(source, &None);
    });
}

#[bench]
fn bench_shortest_paths_bfs(b: &mut Bencher) {
    b.iter(|| {
        let graph = get_karate_club_graph();
        let source = NodeId::from(1 as i64);
        let (_ordered_students, _dist, _preds) = graph.get_shortest_paths_bfs(source);
    });
}

#[test]
fn test_connectivity() {
    let graph = get_karate_club_graph();
    assert!(graph.get_is_connected().unwrap());
    let graph_unconnected = get_karate_club_graph_with_one_extra_edge();
    assert!(!graph_unconnected.get_is_connected().unwrap());
    let graph_empty = SimpleUndirectedGraph::create_empty();
    assert!(graph_empty.get_is_connected().is_err(), "Graph is empty");
    let cc = graph.get_connected_components();
    assert_eq!(cc[0].len(), 34);
    assert_eq!(cc.len(), 1);
    assert!(graph.get_is_connected().unwrap());

    let cc_unconnected = graph_unconnected.get_connected_components();
    assert_eq!(cc_unconnected[0].len(), 34);
    assert_eq!(cc_unconnected[1].len(), 2);
    assert_eq!(cc_unconnected.len(), 2);
    assert!(!graph_unconnected.get_is_connected().unwrap());
    assert_eq!(graph_empty.get_connected_components().len(), 0);
    assert!(graph_empty.get_is_connected().is_err(), "Graph is empty");
}

#[test]
fn test_betweenness() {
    let graph = get_karate_club_graph();
    let bet = graph.get_node_betweenness().unwrap();
    assert_eq!(bet[&NodeId::from(8 as i64)], 0.0);
    assert!((bet[&NodeId::from(34 as i64)] - 160.5515873).abs() <= 0.000001);
    assert!((bet[&NodeId::from(33 as i64)] - 76.6904762).abs() <= 0.000001);
    assert!((bet[&NodeId::from(32 as i64)] - 73.0095238).abs() <= 0.000001);
}

#[test]
fn test_betweenness_brandes() {
    let graph = get_karate_club_graph();
    let bet = graph.get_node_betweenness_brandes().unwrap();
    assert_eq!(bet[&NodeId::from(8 as i64)], 0.0);
    assert!((bet[&NodeId::from(34 as i64)] - 160.5515873).abs() <= 0.000001);
    assert!((bet[&NodeId::from(33 as i64)] - 76.6904762).abs() <= 0.000001);
    assert!((bet[&NodeId::from(32 as i64)] - 73.0095238).abs() <= 0.000001);
}

#[bench]
fn bench_betweenness(b: &mut Bencher) {
    b.iter(|| {
        let graph = get_karate_club_graph();
        let _bet = graph.get_node_betweenness();
    });
}

#[bench]
fn bench_betweenness_brandes(b: &mut Bencher) {
    b.iter(|| {
        let graph = get_karate_club_graph();
        let _bet = graph.get_node_betweenness_brandes();
    });
}

#[test]
fn test_matrices() {
    let graph = get_karate_club_graph();
    let (deg_mat, _ids) = graph.get_degree_matrix();
    assert_eq!(deg_mat.shape(), (34, 34));
    assert_eq!(deg_mat.row(0)[0], 16.0);
    assert_eq!(deg_mat.row(33)[33], 17.0);
    assert_eq!(deg_mat.row(2)[2], 10.0);
    assert_eq!(deg_mat.sum(), 156.0);
    let (adj_mat, _ids) = graph.get_adjacency_matrix();
    assert_eq!(adj_mat.shape(), (34, 34));
    assert_eq!(adj_mat.sum(), 156.0);
    assert_eq!(adj_mat.row(0).sum(), 16.0);
    assert_eq!(adj_mat.row(6)[16], 1.0);
    assert_eq!(adj_mat.row(6)[17], 0.0);
    let (laplacian, _ids) = graph.get_laplacian_matrix();
    assert_eq!(laplacian.shape(), (34, 34));
    assert_eq!(laplacian.sum(), 0.0);
    assert_eq!(laplacian + adj_mat, deg_mat);
}

#[test]
fn test_eigen() {
    let graph = get_karate_club_graph();
    let fiedler = graph.get_algebraic_connectivity();
    assert!((fiedler - 0.469).abs() <= 0.001);

    let eps = 0.001;
    let ev = graph.get_eigenvector_centrality(eps, 1000);
    assert!((ev[&NodeId::from(34 as i64)] - 1.0).abs() <= eps);
    assert!((ev[&NodeId::from(1 as i64)] - 0.95213237).abs() <= eps);
    assert!((ev[&NodeId::from(19 as i64)] - 0.27159396).abs() <= eps);
}

#[test]
fn test_k_cores() {
    let graph = get_karate_club_graph();
    let k_cores = graph.get_k_cores(1);
    assert_eq!(k_cores.len(), 1);
    assert_eq!(k_cores[0].len(), 34);
    let k_cores_4 = graph.get_k_cores(4);
    assert_eq!(k_cores_4.len(), 1);
    assert_eq!(k_cores_4[0].len(), 10);
    let k_cores_5 = graph.get_k_cores(5);
    assert_eq!(k_cores_5.len(), 0);

    let double_karate = get_two_karate_clubs_with_bridge();
    let k_cores_4_2 = double_karate.get_k_cores(4);
    assert_eq!(k_cores_4_2.len(), 2);
    assert_eq!(k_cores_4_2[0].len(), 10);
    assert_eq!(k_cores_4_2[1].len(), 10);

    let (core_assignments, coreness) = graph.get_coreness();
    assert_eq!(core_assignments[0][0].len(), 34);
    assert_eq!(core_assignments[1][0].len(), 33);
    assert_eq!(core_assignments[2][0].len(), 22);
    assert_eq!(core_assignments[3][0].len(), 10);

    assert_eq!(coreness[&NodeId::from(34 as i64)], 4);
}

#[test]
fn test_connected_components() {
    let graph = get_karate_club_graph();
    let conn_comp = graph.get_connected_components();
    assert_eq!(conn_comp.len(), 1);
    assert_eq!(conn_comp[0].len(), 34);

    let double_karate = get_two_karate_clubs();
    let conn_comp_2 = double_karate.get_connected_components();
    assert_eq!(conn_comp_2.len(), 2);
    assert_eq!(conn_comp_2[0].len(), 34);
    assert_eq!(conn_comp_2[1].len(), 34);
}

#[test]
fn test_transitivity() {
    let graph = get_karate_club_graph();
    let trans = graph.get_transitivity();
    println!("{}", trans);
    assert!((trans - 0.2556818181818182).abs() <= f64::EPSILON);

    let approx_trans = graph.get_approx_transitivity(1000);
    println!("{}", approx_trans);
    assert!((approx_trans - trans).abs() <= 0.05);
}

#[test]
fn test_cnm_community() {
    let expected: Vec<f64> = vec![
        0.012163050624589085,
        0.023668639053254437,
        0.012491781722550954,
        0.019230769230769232,
        0.03131163708086785,
        0.012163050624589085,
        0.017258382642998026,
        0.016190006574621957,
        0.01643655489809336,
        0.012080867850098619,
        0.022682445759368834,
        0.011834319526627219,
        0.011341222879684417,
        0.011176857330703484,
        0.011176857330703484,
        0.01676528599605523,
        0.01101249178172255,
        0.010190664036817884,
        0.010190664036817882,
        0.01380670611439842,
        0.015779092702169626,
        0.0202991452991453,
        0.009861932938856014,
        0.011834319526627215,
        0.009368836291913214,
        0.009040105193951348,
        0.008711374095989481,
        0.011094674556213022,
        0.013477975016436557,
        0.01314924391847469,
        0.004684418145956606,
    ];

    let g = get_karate_club_graph();
    let (_, modularity_changes) = g.get_cnm_communities();
    for i in 0..expected.len() {
        println!(
            "Modularity changes: {}, {}, {}",
            i, modularity_changes[i], expected[i]
        );
        assert!((modularity_changes[i] - expected[i]).abs() <= 0.001);
    }
}
