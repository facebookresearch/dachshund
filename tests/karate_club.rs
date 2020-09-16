/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
#![feature(test)]

extern crate lib_dachshund;
extern crate test;
use lib_dachshund::dachshund::graph_base::GraphBase;
use lib_dachshund::dachshund::id_types::{GraphId, NodeId};
use lib_dachshund::dachshund::row::EdgeRow;
use lib_dachshund::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use lib_dachshund::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use lib_dachshund::dachshund::test_utils::{gen_test_transformer, process_raw_vector};
use lib_dachshund::dachshund::transformer::Transformer;
use std::collections::HashSet;
use test::Bencher;

fn get_rows(transformer: &Transformer) -> Vec<EdgeRow> {
    let mut raw = vec![
        "0\t1\t2\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t3\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t2\t3\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t4\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t2\t4\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t3\t4\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t5\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t6\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t7\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t5\t7\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t6\t7\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t8\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t2\t8\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t3\t8\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t4\t8\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t9\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t3\t9\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t3\t10\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t11\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t5\t11\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t6\t11\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t12\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t13\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t4\t13\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t14\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t2\t14\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t3\t14\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t4\t14\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t6\t17\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t7\t17\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t18\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t2\t18\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t20\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t2\t20\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t22\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t2\t22\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t24\t26\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t25\t26\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t3\t28\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t24\t28\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t25\t28\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t3\t29\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t24\t30\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t27\t30\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t2\t31\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t9\t31\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t1\t32\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t25\t32\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t26\t32\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t29\t32\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t3\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t9\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t15\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t16\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t19\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t21\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t23\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t24\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t30\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t31\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t32\t33\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t9\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t10\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t14\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t15\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t16\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t19\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t20\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t21\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t23\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t24\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t27\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t28\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t29\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t30\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t31\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t32\t34\tstudent\tis_friends_with\tstudent".to_string(),
        "0\t33\t34\tstudent\tis_friends_with\tstudent".to_string(),
    ];
    let mut reversed: Vec<String> = Vec::new();
    for el in &raw {
        let mut vec: Vec<&str> = el.split('\t').collect();
        vec.swap(1, 2);
        reversed.push(vec.join("\t"));
    }
    for el in &reversed {
        raw.push(el.to_string());
    }
    assert_eq!(raw.len(), 78 * 2);
    let rows: Vec<EdgeRow> = process_raw_vector(&transformer, raw).unwrap();
    rows
}

fn get_transformer() -> Transformer {
    let typespec: Vec<Vec<String>> = vec![vec![
        "student".to_string(),
        "is_friends_with".into(),
        "student".into(),
    ]];
    gen_test_transformer(typespec, "student".to_string()).unwrap()
}

fn get_karate_club_graph_with_one_extra_edge() -> SimpleUndirectedGraph {
    let graph_id: GraphId = 0.into();
    let transformer = get_transformer();
    let mut rows: Vec<EdgeRow> = get_rows(&transformer);
    let source = NodeId::from(35 as i64);
    let target = NodeId::from(36 as i64);
    let new_edge = EdgeRow {
        graph_id: rows[0].graph_id,
        source_id: source,
        target_id: target,
        source_type_id: rows[0].source_type_id,
        target_type_id: rows[0].target_type_id,
        edge_type_id: rows[0].edge_type_id,
    };
    let rev_edge = EdgeRow {
        graph_id: rows[0].graph_id,
        source_id: target,
        target_id: source,
        source_type_id: rows[0].source_type_id,
        target_type_id: rows[0].target_type_id,
        edge_type_id: rows[0].edge_type_id,
    };
    rows.push(new_edge);
    rows.push(rev_edge);
    let graph: SimpleUndirectedGraph = transformer
        .build_pruned_graph::<SimpleUndirectedGraphBuilder, SimpleUndirectedGraph>(graph_id, &rows)
        .unwrap();
    graph
}

fn get_two_karate_clubs_edges(transformer: &Transformer) -> Vec<EdgeRow> {
    let mut rows: Vec<EdgeRow> = get_rows(&transformer);
    for i in 1..rows.len() {
        let new_edge = EdgeRow {
            graph_id: rows[i].graph_id,
            source_id: NodeId::from(rows[i].source_id.value() + 35),
            target_id: NodeId::from(rows[i].target_id.value() + 35),
            source_type_id: rows[0].source_type_id,
            target_type_id: rows[0].target_type_id,
            edge_type_id: rows[0].edge_type_id,
        };
        rows.push(new_edge);
    }
    rows
}

fn get_two_karate_clubs() -> SimpleUndirectedGraph {
    let graph_id: GraphId = 0.into();
    let transformer = get_transformer();
    let rows = get_two_karate_clubs_edges(&transformer);
    let graph: SimpleUndirectedGraph = transformer
        .build_pruned_graph::<SimpleUndirectedGraphBuilder, SimpleUndirectedGraph>(graph_id, &rows)
        .unwrap();
    return graph;
}

fn get_two_karate_clubs_with_bridge() -> SimpleUndirectedGraph {
    let graph_id: GraphId = 0.into();
    let transformer = get_transformer();
    let mut rows = get_two_karate_clubs_edges(&transformer);
    let source = NodeId::from(34 as i64);
    let target = NodeId::from(35 as i64);
    let new_edge = EdgeRow {
        graph_id: rows[0].graph_id,
        source_id: source,
        target_id: target,
        source_type_id: rows[0].source_type_id,
        target_type_id: rows[0].target_type_id,
        edge_type_id: rows[0].edge_type_id,
    };
    let rev_edge = EdgeRow {
        graph_id: rows[0].graph_id,
        source_id: target,
        target_id: source,
        source_type_id: rows[0].source_type_id,
        target_type_id: rows[0].target_type_id,
        edge_type_id: rows[0].edge_type_id,
    };
    rows.push(new_edge);
    rows.push(rev_edge);
    let graph: SimpleUndirectedGraph = transformer
        .build_pruned_graph::<SimpleUndirectedGraphBuilder, SimpleUndirectedGraph>(graph_id, &rows)
        .unwrap();
    graph
}
fn get_karate_club_graph() -> SimpleUndirectedGraph {
    let graph_id: GraphId = 0.into();
    let transformer = get_transformer();
    let rows: Vec<EdgeRow> = get_rows(&transformer);
    let graph: SimpleUndirectedGraph = transformer
        .build_pruned_graph::<SimpleUndirectedGraphBuilder, SimpleUndirectedGraph>(graph_id, &rows)
        .unwrap();
    graph
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
    let (dist, parents) = graph.get_shortest_paths(source, None);
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
        let (_dist, _parents) = graph.get_shortest_paths(source, None);
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
