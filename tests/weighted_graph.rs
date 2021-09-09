/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;

use lib_dachshund::dachshund::algorithms::coreness::Coreness;
use lib_dachshund::dachshund::error::{CLQError, CLQResult};
use lib_dachshund::dachshund::graph_builder_base::GraphBuilderBase;
use lib_dachshund::dachshund::id_types::NodeId;
use lib_dachshund::dachshund::node::WeightedNodeBase;
use lib_dachshund::dachshund::graph_base::GraphBase;
use lib_dachshund::dachshund::weighted_undirected_graph::WeightedUndirectedGraph;
use lib_dachshund::dachshund::weighted_undirected_graph_builder::WeightedUndirectedGraphBuilder;

fn get_graph(idx: usize) -> CLQResult<WeightedUndirectedGraph> {
    let v = match idx {
        // Simple star graph.
        0 => vec![
            (0, 1, 1.0),
            (0, 2, 2.0),
            (0, 3, 3.0),
        ],
        // Malformed Input: Duplicate Edge Different Weights
        1 => vec![
            (0, 1, 1.5),
            (0, 1, 2.5),
        ],
        // Malformed Input Duplicate Edge, Reversed
        2 => vec![
            (0, 1, -0.1),
            (1, 0, 0.1)
        ],
        // Uneven Square
        3 => vec![
            (0, 1, 1.0),
            (1, 2, 2.0),
            (2, 3, 3.0),
            (3, 0, 4.0),

        ],
        _ => return Err(CLQError::Generic("Invalid index".to_string())),
    };
    WeightedUndirectedGraphBuilder {}
        .from_vector(v.into_iter().map(|(x, y, z)| (x as i64, y as i64, z as f64)).collect())
}

#[cfg(test)]
#[test]
fn test_node_weight() {
    // The weight of a node is equal to the sum of the weights of all its edges.
    let weighted_star_graph = get_graph(0).unwrap();
    assert_eq!(weighted_star_graph.nodes.len(), 4);
    assert_eq!(weighted_star_graph.get_node(NodeId::from(0 as i64)).weight(), 6.0);

    // Only a single edge can exist between a pair of nodes (because the graph is not directed).
    // The graph builder should take the weight from the last value.
    let doubled_edge_graph = get_graph(1).unwrap();
    let node_zero = doubled_edge_graph.get_node(NodeId::from(0 as i64));
    assert_eq!(node_zero.edges.len(), 1);
    assert_eq!(node_zero.edges[0].weight, 2.5);

    let doubled_edge_graph_two = get_graph(2).unwrap();
    let node_zero = doubled_edge_graph_two.get_node(NodeId::from(0 as i64));
    assert_eq!(node_zero.edges.len(), 1);
    assert_eq!(node_zero.edges[0].weight, 0.1);
}

#[cfg(test)]
#[test]
fn test_coreness() {
    // This graph is a star, so every node should have coreness 1.
    let (_cores, coreness) = get_graph(0).unwrap().get_coreness();

    for i in 0..3 {
        assert_eq!(*coreness.get(&NodeId::from(i as i64)).unwrap(), 1);
    }

    // This graph is a square, so every node should have coreness 2.
    let (_cores, coreness) = get_graph(3).unwrap().get_coreness();

    for i in 0..4 {
        assert_eq!(*coreness.get(&NodeId::from(i as i64)).unwrap(), 2);
    }

}
