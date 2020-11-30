/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;
use crate::lib_dachshund::TransformerBase;
use lib_dachshund::dachshund::algorithms::connected_components::ConnectedComponents;
use lib_dachshund::dachshund::algorithms::coreness::Coreness;
use lib_dachshund::dachshund::algorithms::cnm_communities::CNMCommunities;
use lib_dachshund::dachshund::id_types::NodeId;
use lib_dachshund::dachshund::input::Input;
use lib_dachshund::dachshund::output::Output;
use lib_dachshund::dachshund::simple_transformer::{
    GraphStatsTransformerBase, SimpleParallelTransformer, SimpleTransformer,
};
use lib_dachshund::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use lib_dachshund::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use std::collections::{BTreeSet, HashSet};
use std::iter::FromIterator;

fn get_graph(idx: usize) -> Result<SimpleUndirectedGraph, String> {
    let v = match idx {
        0 => vec![
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
        ],
        1 => vec![(0, 1), (1, 2), (2, 0)],
        2 => vec![(0, 1), (1, 2), (2, 0), (1, 3), (3, 0)],
        3 => vec![(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)],
        4 => vec![(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3), (0, 3)],
        5 => vec![(0, 1), (1, 2), (2, 0), (2, 3)],
        6 => vec![
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
        ],
        _ => return Err("Invalid index".to_string()),
    };
    Ok(SimpleUndirectedGraphBuilder::from_vector(
        &v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
    ))
}
fn get_expected_modularity_changes(idx: usize) -> Result<Vec<f64>, String> {
    match idx {
        0 => Ok(vec![
            0.03443877551020408,
            0.033163265306122444,
            0.03188775510204082,
            0.03188775510204082,
            0.03125,
            0.030612244897959183,
            0.02933673469387755,
            0.02933673469387755,
            0.03571428571428571,
            0.02806122448979592,
            0.026785714285714284,
            0.022959183673469385,
            0.019770408163265307,
            0.008928571428571432,
        ]),
        1 => Ok(vec![0.1111111111111111, 0.2222222222222222]),
        2 => Ok(vec![
            0.07999999999999999,
            0.09999999999999998,
            0.07999999999999996,
        ]),
        3 => Ok(vec![
            0.1111111111111111,
            0.2222222222222222,
            0.1111111111111111,
            0.2222222222222222,
        ]),
        4 => Ok(vec![
            0.10204081632653061,
            0.163265306122449,
            0.10204081632653061,
            0.163265306122449,
        ]),
        5 => Ok(vec![0.15625, 0.125]),
        6 => Ok(vec![
            0.013310185185185185,
            0.01253858024691358,
            0.01244212962962963,
            0.012152777777777776,
            0.02256944444444444,
            0.011863425925925927,
            0.021026234567901234,
            0.01707175925925926,
            0.012345679012345678,
            0.020254629629629633,
            0.01099537037037037,
            0.019290123456790122,
            0.014949845679012346,
            0.012731481481481483,
            0.012345679012345678,
            0.019868827160493825,
            0.019290123456790122,
            0.01099537037037037,
            0.02035108024691358,
            0.020833333333333336,
            0.007812500000000002,
        ]),
        _ => return Err("Invalid index".to_string()),
    }
}

#[cfg(test)]
#[test]
fn test_truss_graph() {
    assert_eq!(get_graph(5).unwrap().get_connected_components().len(), 1);
    assert_eq!(
        get_graph(5)
            .unwrap()
            ._get_connected_components(
                None,
                Some(&HashSet::from_iter(
                    vec![(NodeId::from(2 as i64), NodeId::from(3 as i64))].into_iter()
                ))
            )
            .len(),
        2
    );

    assert_eq!(get_graph(1).unwrap().get_k_trusses(3).0.len(), 1);
    assert_eq!(get_graph(2).unwrap().get_k_trusses(3).0.len(), 1);
    assert_eq!(get_graph(3).unwrap().get_k_trusses(3).0.len(), 2);
    assert_eq!(get_graph(4).unwrap().get_k_trusses(3).0.len(), 2);

    assert_eq!(get_graph(1).unwrap().get_k_trusses(3).0[0].len(), 3);
    assert_eq!(get_graph(2).unwrap().get_k_trusses(3).0[0].len(), 5);
    assert_eq!(get_graph(5).unwrap().get_k_trusses(3).0[0].len(), 3);

    let (truss, truss_nodes) = get_graph(0).unwrap().get_k_trusses(3);
    assert_eq!(truss.len(), 2);
    assert!(truss_nodes.contains(&BTreeSet::from_iter(
        vec![0, 1, 9].into_iter().map(|x| NodeId::from(x as i64))
    )));
    assert!(truss_nodes.contains(&BTreeSet::from_iter(
        vec![8, 10, 16].into_iter().map(|x| NodeId::from(x as i64))
    )));

    let (truss2, truss_nodes2) = get_graph(6).unwrap().get_k_trusses(4);
    assert_eq!(truss2.len(), 2);
    assert!(truss_nodes2.contains(&BTreeSet::from_iter(
        vec![3, 8, 9, 18]
            .into_iter()
            .map(|x| NodeId::from(x as i64))
    )));
    assert!(truss_nodes2.contains(&BTreeSet::from_iter(
        vec![7, 11, 15, 21]
            .into_iter()
            .map(|x| NodeId::from(x as i64))
    )));
}

#[test]
fn test_simple_transformer() {
    let mut transformer = SimpleTransformer::new();
    let graphs = (0..1)
        .map(|x| get_graph(x as usize).unwrap())
        .collect::<Vec<SimpleUndirectedGraph>>();
    let text = graphs
        .iter()
        .enumerate()
        .map(|(i, x)| x.as_input_rows(i))
        .collect::<Vec<String>>()
        .join("\n");
    let expected = graphs
        .iter()
        .enumerate()
        .map(|(i, x)| format!("{}\t{}", i, SimpleTransformer::compute_graph_stats_json(x)))
        .collect::<Vec<String>>()
        .join("\n");

    let bytes = text.as_bytes();
    let input = Input::string(&bytes);
    let mut buffer: Vec<u8> = Vec::new();
    let output = Output::string(&mut buffer);
    transformer.run(input, output).unwrap();
    let output_str: String = String::from_utf8(buffer).unwrap();
    assert_eq!(output_str, expected + "\n");
}

#[test]
fn test_parallel_transformer() {
    let mut transformer = SimpleParallelTransformer::new();
    let graphs = (0..1)
        .map(|x| get_graph(x as usize).unwrap())
        .collect::<Vec<SimpleUndirectedGraph>>();
    let text = graphs
        .iter()
        .enumerate()
        .map(|(i, x)| x.as_input_rows(i))
        .collect::<BTreeSet<String>>() //sorting
        .into_iter()
        .collect::<Vec<String>>()
        .join("\n");
    let expected = graphs
        .iter()
        .enumerate()
        .map(|(i, x)| {
            format!(
                "{}\t{}",
                i,
                SimpleParallelTransformer::compute_graph_stats_json(x)
            )
        })
        .collect::<Vec<String>>()
        .join("\n")
        + "\n";

    let bytes = text.as_bytes();
    let input = Input::string(&bytes);
    let mut buffer: Vec<u8> = Vec::new();
    let output = Output::string(&mut buffer);
    transformer.run(input, output).unwrap();
    let output_str: String = String::from_utf8(buffer).unwrap();
    let output_set = BTreeSet::from_iter(output_str.split('\n'));
    let expected_set = BTreeSet::from_iter(expected.split('\n'));
    assert_eq!(output_set, expected_set);
}

#[test]
fn test_modularity_changes() {
    for i in 0..7 {
        let g = get_graph(i).unwrap();
        let (_, modularity_changes) = g.get_cnm_communities();
        let expected = get_expected_modularity_changes(i).unwrap();
        for i in 0..expected.len() {
            println!(
                "Modularity changes: {}, {}, {}",
                i, modularity_changes[i], expected[i]
            );
            assert!((modularity_changes[i] - expected[i]).abs() <= 0.001);
        }
    }
}
