/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;
use lib_dachshund::dachshund::graph_builder_base::GraphBuilderBase;
use lib_dachshund::dachshund::graph_base::GraphBase;
use lib_dachshund::dachshund::simple_directed_graph::SimpleDirectedGraph;
use lib_dachshund::dachshund::simple_directed_graph_builder::SimpleDirectedGraphBuilder;
use std::collections::HashSet;
fn get_rows(idx: usize) -> Result<Vec<(usize, usize)>, String> {
    match idx {
        0 => Ok(vec![
            (0, 1),
            (0, 10),
            (0, 14),
            (0, 9),
            (1, 9),
            (1, 2),
            (1, 3),
            (1, 18),
            (2, 8),
            (3, 6),
            (4, 6),
            (4, 7),
            (5, 12),
            (6, 8),
            (7, 8),
            (7, 19),
            (8, 16),
            (8, 9),
            (8, 10),
            (8, 13),
            (9, 19),
            (9, 15),
            (10, 18),
            (10, 16),
            (10, 17),
            (12, 19),
            (14, 19),
            (15, 17),
        ]),
        1 => Ok(vec![(0, 1), (1, 2), (2, 0)]),
        2 => Ok(vec![(0, 1), (1, 2), (2, 0), (1, 3), (3, 0)]),
        3 => Ok(vec![(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)]),
        4 => Ok(vec![(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3), (0, 3)]),
        5 => Ok(vec![(0, 1), (1, 2), (2, 0), (2, 3)]),
        6 => Ok(vec![
            (0, 19),
            (0, 1),
            (0, 18),
            (0, 11),
            (0, 9),
            (1, 19),
            (1, 5),
            (1, 7),
            (1, 8),
            (1, 12),
            (2, 23),
            (3, 18),
            (3, 19),
            (3, 20),
            (3, 5),
            (3, 8),
            (3, 9),
            (4, 16),
            (4, 17),
            (4, 19),
            (4, 20),
            (4, 22),
            (4, 23),
            (4, 13),
            (5, 11),
            (5, 14),
            (5, 23),
            (6, 16),
            (6, 15),
            (7, 21),
            (7, 17),
            (7, 9),
            (7, 11),
            (7, 15),
            (8, 15),
            (8, 18),
            (8, 9),
            (9, 12),
            (9, 13),
            (9, 15),
            (9, 16),
            (9, 17),
            (9, 18),
            (9, 20),
            (9, 23),
            (10, 17),
            (10, 12),
            (10, 20),
            (11, 16),
            (11, 19),
            (11, 21),
            (11, 15),
            (12, 22),
            (12, 17),
            (12, 13),
            (13, 18),
            (13, 24),
            (13, 15),
            (14, 21),
            (14, 15),
            (15, 24),
            (15, 19),
            (15, 21),
            (16, 19),
            (16, 23),
            (16, 24),
            (17, 24),
            (18, 21),
            (18, 23),
            (19, 20),
            (20, 22),
            (20, 24),
        ]),
        _ => return Err("Invalid index".to_string()),
    }
}

fn get_graph(idx: usize) -> Result<SimpleDirectedGraph, String> {
    Ok(SimpleDirectedGraphBuilder::from_vector(
        &get_rows(idx)?
            .into_iter()
            .map(|(x, y)| (x as i64, y as i64))
            .collect(),
    ))
}

#[cfg(test)]
#[test]
fn test_build_graph() {
    for i in 0..7 {
        let rows = get_rows(i).unwrap();
        let graph = get_graph(i).unwrap();
        assert_eq!(rows.len(), graph.count_edges());
        assert_eq!(
            rows.iter()
                .map(|x| x.0)
                .chain(rows.iter().map(|x| x.1))
                .collect::<HashSet<usize>>()
                .len(),
            graph.count_nodes()
        );
    }
}
